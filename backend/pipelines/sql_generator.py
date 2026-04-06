#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Generator Module (Phase 3)
================================
สร้าง SQL จาก normalized_query + target_db ผ่าน 3 ขั้นตอน:
1. build_sql_prompt    — ประกอบ System Prompt แบบ Dynamic (PM vs REPAIR schema)
2. call_llm_for_sql    — เรียก MODEL_NAME (Qwen Coder) เพื่อสร้าง SQL
3. extract_clean_sql   — สกัด SQL สะอาดจาก LLM response

Model Routing (CRITICAL — ห้ามสลับ):
  ⚠️ ใช้ MODEL_NAME (Qwen Coder) เท่านั้นในไฟล์นี้
  ⚠️ ห้ามใช้ CHAT_MODEL (Typhoon-8B) — นั่นสำหรับ Intent + Chat เท่านั้น
"""

import re
import os
import logging
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from core.config import (
    BANGKOK_TZ,
    WORK_DB_PATH, PM2025_DB_PATH,
    OLLAMA_GENERATE_URL, OLLAMA_HOST,
    MODEL_NAME,  # ⚠️ Qwen Coder — SQL Generation ONLY
    OLLAMA_REQUEST_TIMEOUT,
)

logger = logging.getLogger("[SQL_GEN]")


# =====================================================
# 1. DYNAMIC PROMPT BUILDER
# =====================================================

def _compute_date_context() -> Dict[str, str]:
    """
    คำนวณวันที่ทั้งหมดที่ LLM ต้องใช้ (Bangkok timezone)
    คืนเป็น dict ของค่าวันที่สำเร็จรูป เพื่อให้ LLM ไม่ต้องคำนวณเอง
    """
    now = pd.Timestamp.now(tz=BANGKOK_TZ)
    today = now.strftime("%Y-%m-%d")
    yesterday = (now - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    tomorrow = (now + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    # สัปดาห์ (จันทร์–อาทิตย์)
    this_week_start = (now - pd.Timedelta(days=now.dayofweek)).strftime("%Y-%m-%d")
    this_week_end = (now + pd.Timedelta(days=6 - now.dayofweek)).strftime("%Y-%m-%d")
    last_week_start = (now - pd.Timedelta(days=now.dayofweek + 7)).strftime("%Y-%m-%d")
    last_week_end = (now - pd.Timedelta(days=now.dayofweek + 1)).strftime("%Y-%m-%d")
    next_week_start = (now + pd.Timedelta(days=7 - now.dayofweek)).strftime("%Y-%m-%d")
    next_week_end = (now + pd.Timedelta(days=13 - now.dayofweek)).strftime("%Y-%m-%d")

    # เดือน
    this_month_start = now.replace(day=1).strftime("%Y-%m-%d")
    this_month_end = (now.replace(day=1) + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")
    last_month_end = (now.replace(day=1) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    last_month_start = (now.replace(day=1) - pd.Timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")
    _first_next = now.replace(year=now.year + 1, month=1, day=1) if now.month == 12 else now.replace(month=now.month + 1, day=1)
    next_month_start = _first_next.strftime("%Y-%m-%d")
    next_month_end = (_first_next + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")

    # ปี
    year_start = now.replace(month=1, day=1).strftime("%Y-%m-%d")
    year_end = now.replace(month=12, day=31).strftime("%Y-%m-%d")

    return {
        "today": today, "yesterday": yesterday, "tomorrow": tomorrow,
        "this_week_start": this_week_start, "this_week_end": this_week_end,
        "last_week_start": last_week_start, "last_week_end": last_week_end,
        "next_week_start": next_week_start, "next_week_end": next_week_end,
        "this_month_start": this_month_start, "this_month_end": this_month_end,
        "last_month_start": last_month_start, "last_month_end": last_month_end,
        "next_month_start": next_month_start, "next_month_end": next_month_end,
        "year_start": year_start, "year_end": year_end,
    }


def _fetch_distinct_values() -> Dict[str, List[str]]:
    """
    ดึง Distinct Line, Process, Tech จาก repairs_enriched (ค่าจริงใน DB)
    ใช้แนบใน Prompt เพื่อให้ LLM เลือกใช้คำที่ถูกต้อง (Few-shot context)
    """
    result: Dict[str, List[str]] = {"lines": [], "processes": [], "techs": []}
    if not os.path.exists(WORK_DB_PATH):
        return result
    try:
        with sqlite3.connect(WORK_DB_PATH) as conn:
            lines_df = pd.read_sql(
                "SELECT DISTINCT Line FROM repairs_enriched WHERE Line IS NOT NULL AND Line != '' ORDER BY Line", conn
            )
            result["lines"] = lines_df["Line"].tolist() if not lines_df.empty else []

            proc_df = pd.read_sql(
                "SELECT DISTINCT Process FROM repairs_enriched WHERE Process IS NOT NULL AND Process != '' ORDER BY Process LIMIT 25", conn
            )
            result["processes"] = proc_df["Process"].tolist() if not proc_df.empty else []

            tech_df = pd.read_sql(
                "SELECT DISTINCT Tech FROM repairs_enriched WHERE Tech IS NOT NULL AND Tech != '' AND Tech != 'Unknown' ORDER BY Tech LIMIT 15", conn
            )
            techs_raw = tech_df["Tech"].tolist() if not tech_df.empty else []
            # กรองชื่อ test/support ออก
            TECH_EXCLUDE = {"KUSOL", "POOLAWAT Suphakson", "Support", "SupportA", "อนุวัฒน์ ลบออก", "อนุวัฒน์"}
            result["techs"] = [t for t in techs_raw if str(t).strip() not in TECH_EXCLUDE]
    except Exception as e:
        logger.warning(f"[SQL_GEN] _fetch_distinct_values error: {e}")
    return result


def _fetch_pm_schema() -> str:
    """ดึง Schema ของตาราง PM (column names) คืนเป็น text"""
    cols: List[str] = []
    try:
        if os.path.exists(WORK_DB_PATH):
            with sqlite3.connect(WORK_DB_PATH) as conn:
                cols = [r[1] for r in conn.execute("PRAGMA table_info(PM)").fetchall()]
        if not cols and os.path.exists(PM2025_DB_PATH):
            with sqlite3.connect(PM2025_DB_PATH) as conn:
                cols = [r[1] for r in conn.execute("PRAGMA table_info(PM)").fetchall()]
    except Exception as e:
        logger.warning(f"[SQL_GEN] _fetch_pm_schema error: {e}")
    if not cols:
        return ""
    return ", ".join(cols)


def build_sql_prompt(normalized_query: str, target_db: str) -> str:
    """
    ประกอบ System Prompt สำหรับ SQL Generation แบบ Dynamic

    Logic:
    - target_db == 'PM'     → แนบเฉพาะ Schema ของ PM + CRITICAL RULES สำหรับ PM
    - target_db == 'REPAIR' → แนบเฉพาะ Schema ของ repairs_enriched + distinct values
    - แนบ DateContext เสมอ (วันที่คำนวณสำเร็จรูป ลดโอกาส LLM คำนวณผิด)
    - แนบ CRITICAL RULES 6 หมวดหมู่เป็นแกนหลัก

    Returns:
        str: Prompt พร้อมส่งให้ LLM (MODEL_NAME / Qwen Coder)
    """
    dates = _compute_date_context()
    distinct = _fetch_distinct_values()
    
    # Pre-process the query to replace date terms with actual dates
    processed_query = normalized_query
    processed_query = processed_query.replace("วันนี้", f"วันที่ {dates['today']}")
    processed_query = processed_query.replace("เมื่อวาน", f"วันที่ {dates['yesterday']}")
    processed_query = processed_query.replace("พรุ่งนี้", f"วันที่ {dates['tomorrow']}")
    processed_query = processed_query.replace("today", f"date {dates['today']}")
    processed_query = processed_query.replace("yesterday", f"date {dates['yesterday']}")
    processed_query = processed_query.replace("tomorrow", f"date {dates['tomorrow']}")
    
    # Log the query processing
    if processed_query != normalized_query:
        logger.info(f"[SQL_GEN] Query processed: '{normalized_query}' -> '{processed_query}'")

    # Detect if preprocessing already inserted a quoted, comma-separated list
    # e.g. 'LED_A_INS','LED_A_PCB' and instruct the SQL LLM to use WHERE Line IN(...)
    provided_line_list = None
    m = re.search(r"('(?:[A-Za-z0-9_\\-]+)'(?:\s*,\s*'(?:[A-Za-z0-9_\\-]+)')+)", processed_query)
    if m:
        provided_line_list = m.group(1)

    # --- Date Context (ส่งทุกครั้ง) ---
    date_block = f"""DateContext (use these exact YYYY-MM-DD only):
today={dates['today']}, yesterday={dates['yesterday']},
this_week={dates['this_week_start']} to {dates['this_week_end']},
last_week={dates['last_week_start']} to {dates['last_week_end']},
next_week={dates['next_week_start']} to {dates['next_week_end']},
this_month={dates['this_month_start']} to {dates['this_month_end']},
last_month={dates['last_month_start']} to {dates['last_month_end']},
next_month={dates['next_month_start']} to {dates['next_month_end']},
year={dates['year_start']} to {dates['year_end']}."""

    # --- Schema + Values (ต่างกันตาม target_db) ---
    if target_db == "PM":
        pm_cols = _fetch_pm_schema()
        schema_block = f"""Target Table: PM (Preventive Maintenance)
Columns: {pm_cols if pm_cols else 'Task Name, Due_date_ymd, Progress, Description'}
IMPORTANT: Use FROM PM only. Do NOT use repairs_enriched.
- Progress values: 'Not started' (ยังไม่เสร็จ), 'Completed' (เสร็จ) — ห้ามใช้ Pending
- Date column: Due_date_ymd (format YYYY-MM-DD) — ห้ามใช้ "Created Date" หรือ Date
- SELECT columns: ALWAYS use SELECT "Task Name", Due_date_ymd, Progress, Description (ห้ามใส่ Line ในผลลัพธ์)
- กรองตามไลน์: ใช้ WHERE "Task Name" LIKE '%keyword%' (เช่น LIKE '%MOPF1%', LIKE '%PCB%', LIKE '%TOYOTA%')
  ห้ามใช้ WHERE Line = '...' หรือ Line LIKE '...'
- สำหรับ MOPF2: ใช้ ("Task Name" LIKE '%MOPF2%' OR "Task Name" LIKE '%MOPF#2%')
- เลื่อน PM = WHERE "Description" LIKE '%ย้าย%'
- PM ไม่มี Process, Tech, RepairMinutes, ResponseMinutes
- ห้ามใช้ ReportPM, Checksheet, PMTest, PM_SMT — ใช้แค่ table PM"""
    else:
        lines_str = ", ".join(str(x) for x in distinct["lines"][:20])
        proc_str = ", ".join(str(x) for x in distinct["processes"][:25])
        techs_str = ", ".join(str(x) for x in distinct["techs"][:15])
        schema_block = f"""Target Table: repairs_enriched
Columns: Date, Shift, Shift_Date, Team, Tech, Line, Process, RepairMinutes, ResponseMinutes, ปัญหา, สาเหตุ, การแก้ไข, บันทึกเพิ่มเติม
Avoid: id, Tech_ID, Call_Time, Start_Time, End_Time, Call_Time_m, Repair_Time_m, col15-17, extracted_at, Date_Obj
IMPORTANT: Use FROM repairs_enriched only. Do NOT use PM table.

SHIFT COLUMNS EXPLAINED:
- Date: วันที่ของ timestamp (เช่น 2026-03-18)
- Shift: กะงาน ('Day' หรือ 'Night')  
- Shift_Date: วันที่ของกะงาน (สำหรับกะดึกข้ามวัน)
  * กะเช้า: Date = Shift_Date
  * กะดึก: Shift_Date อาจต่างจาก Date (เช่น กะดึกวันที่ 17 ครอบคลุม 17/03 20:00 - 18/03 07:59)

Distinct LINE examples: [{lines_str}]
Distinct PROCESS examples: [{proc_str}]
Distinct TECH examples: [{techs_str}]
If the user provided a quoted, comma-separated list of Line values (e.g. 'LED_A_INS','LED_A_PCB'), use WHERE Line IN (<that list>) as-is.

Line vs Process rules:
- LINE = สายผลิต (e.g. LCM, PCB-C, LED_M_PCB, MOPF2_Ins)
- PROCESS = เครื่อง/กระบวนการ (e.g. COATING, GREASE, ASSY, PACKING)
- Look up user's keyword in PROCESS first; if not found, check LINE
- PCB → WHERE Line LIKE '%PCB%' (covers all 18 PCB lines: C-PCB, LED_A_PCB, PCB-A, PCB-B, etc.)
- MOPF1 → WHERE Line LIKE '%MOPF1%' OR Line LIKE '%MOPF#1%'
- MOPF2 → WHERE Line LIKE '%MOPF2%' OR Line LIKE '%MOPF#2%'
- LED Manual → WHERE Line IN ('LED_M_ASSY', 'LED_M_PCB', 'LED_Manul')
- LED Auto → WHERE Line IN ('LED_A_INS','LED_A_PCB')
- SDA → WHERE Line IN ('SDA2_Insp', 'SDA2_Assy')

IMPORTANT LINE MATCHING RULES:
- When user asks about "PCB", ALWAYS use LIKE '%PCB%' to include all PCB variants
- When entity constraints are provided, use them exactly as specified
- Prefer LIKE patterns over IN clauses for broad categories (PCB, MOPF, LED)"""

        # If preprocessing provided an explicit list, add a short hint to prompt
        if provided_line_list:
            schema_block += f"\nUSER_PROVIDED_LINE_LIST: {provided_line_list} -- When present, use WHERE Line IN ({provided_line_list})\n"

    # --- CRITICAL RULES (6 หมวดหมู่) ---
    critical_rules = """CRITICAL RULES:
[1. TABLE SELECTION]
- PM questions → FROM PM only. REPAIR questions → FROM repairs_enriched only.
- ห้ามใช้ ATTACH. No cross-table joins.

[2. DATE HANDLING]
- Use ONLY dates from DateContext above. Do NOT hallucinate years (2023/2024/2025) unless user explicitly asks.
- เมื่อวาน = yesterday from DateContext. ห้ามใช้ CURRENT_DATE - 1.
- Date ranges: ALWAYS use BETWEEN 'start_date' AND 'end_date' with actual dates (e.g., BETWEEN '2026-03-17' AND '2026-03-23').
- NEVER use {{dates['...']}} or date["..."] syntax in SQL - use actual date values only.
- Shift: 'Day' (08:00-17:05), 'Night' (20:00-07:59). 
- IMPORTANT SHIFT RULES:
  * กะเช้า/Day shift: Use normal Date column → WHERE Date = 'YYYY-MM-DD' AND Shift = 'Day'
  * กะดึก/Night shift: Use Shift_Date column → WHERE Shift_Date = 'YYYY-MM-DD' AND Shift = 'Night'
  * Example: "กะดึกเมื่อวาน" = WHERE Shift_Date = '{dates['yesterday']}' AND Shift = 'Night'
  * Example: "กะเช้าวันนี้" = WHERE Date = '{dates['today']}' AND Shift = 'Day'

[3. COLUMN SELECTION]
- อาการ/สาเหตุ/แก้ไข → SELECT Process, Line, "ปัญหา", "สาเหตุ", "การแก้ไข", Date (ไม่ใส่ Tech, RepairMinutes).
- ประวัติช่าง → SELECT Date, Tech, Line, Process, RepairMinutes (ห้ามใช้ COUNT, GROUP BY). Show all rows for the specified tech.
- ประวัติการซ่อม (History) → FROM repairs_enriched, SELECT Date, Line, Process, "ปัญหา", "สาเหตุ", "การแก้ไข". 
- CRITICAL FILTERING RULE for "ประวัติ" (History): ONLY if the user asks for "History" (ประวัติ), include "AND (ปัญหา IS NOT NULL OR สาเหตุ IS NOT NULL OR การแก้ไข IS NOT NULL)".
- If it is a generic query ("What happened today?", "What is broken?"), DO NOT filter out NULL/empty detail columns. Show ALL records.

[4. AGGREGATION LOGIC]
- "ซ่อมเยอะที่สุด" (จำนวนครั้ง) → COUNT(*) per Tech
- "ซ่อมกี่นาที" (เวลารวม) → SUM(CAST(RepairMinutes AS FLOAT))
- "ตอบสนอง/เรียก/ไปซ่อม" → ใช้ ResponseMinutes (ไม่ใช่ RepairMinutes)
- "เร็วที่สุด" → ORDER BY ASC. "ช้าที่สุด" → ORDER BY DESC.
- เมื่อวิเคราะห์ไม่ได้ชัดว่าถาม เวลา vs ครั้ง → ถือว่าเป็น เวลา (SUM)

[5. FILTER RULES]
- If user provided a quoted, comma-separated list (e.g., 'MOPF1_ASSY','MOPF1_Ins','MOPF1_Isp') → ALWAYS use WHERE Line IN (<full list>). NEVER collapse to a single line.
- ถ้าไม่ระบุ Line/Process → ห้ามใส่ WHERE Line/Process (ดึงทุกค่า)
- ถ้าไม่ระบุวัน/เดือน/ปี → ห้ามใส่ WHERE Date (ดึงทุกวัน)
- "ไม่มี X หรอ" = "หา X ให้" → ใช้ LIKE '%X%' (positive) ห้ามใช้ NOT LIKE
- Team A/ทีม A/ทีมเอ → WHERE Team = 'A'

[6. OUTPUT FORMAT]
- PM queries: ห้ามใส่ LIMIT (แสดงทั้งหมด)
- REPAIR queries: LIMIT ตามที่ถาม (เช่น 3 อันดับ = LIMIT 3) หรือ LIMIT 20 default
- Return ONLY one SQL statement, no explanation, no markdown.

EXAMPLE QUERIES:
- "กะดึกเมื่อวานมีอะไรเสียบ้าง" → SELECT Line, Process, "ปัญหา", Date FROM repairs_enriched WHERE Shift_Date = '{dates["yesterday"]}' AND Shift = 'Night' ORDER BY Date DESC LIMIT 20
- "กะเช้าวันนี้มีอะไรเสียบ้าง" → SELECT Line, Process, "ปัญหา", Date FROM repairs_enriched WHERE Date = '{dates["today"]}' AND Shift = 'Day' ORDER BY Date DESC LIMIT 20
- "วันที่ {dates['today']} มีอะไรเสียบ้าง" → SELECT Line, Process, "ปัญหา", Date FROM repairs_enriched WHERE Date = '{dates["today"]}' ORDER BY Date DESC LIMIT 20
- "สัปดาห์นี้ไลน์ไหนเสียบ่อยที่สุด" → SELECT Line, COUNT(*) AS count FROM repairs_enriched WHERE Date BETWEEN '{dates["this_week_start"]}' AND '{dates["this_week_end"]}' GROUP BY Line ORDER BY count DESC LIMIT 10
- "PCB ไลน์วันนี้เสียกี่ครั้ง" → SELECT COUNT(*) FROM repairs_enriched WHERE Date = '{dates["today"]}' AND Line LIKE '%PCB%'
- "PCB ไลน์มีปัญหาอะไรบ้าง" → SELECT Line, Process, "ปัญหา", Date FROM repairs_enriched WHERE Line LIKE '%PCB%' ORDER BY Date DESC LIMIT 20
- "ช่างเมฆซ่อมอะไรบ้างวันนี้" → SELECT Date, Tech, Line, Process, RepairMinutes FROM repairs_enriched WHERE Date = '{dates["today"]}' AND Tech = 'เมฆ' ORDER BY Date DESC LIMIT 20"""

    # --- ประกอบ Prompt สุดท้าย ---
    matched_entities_block = ""
    if "ข้อมูล Entity ที่พบในฐานข้อมูล" in processed_query:
        # If the input already contains entity matching results from preprocessing
        parts = processed_query.split("ข้อมูล Entity ที่พบในฐานข้อมูล", 1)
        user_question = parts[0].strip()
        entities_text = parts[1].strip()
        
        # [NEW] Hybrid Vector-SQL matching for PM Line
        if target_db == "PM":
            m = re.search(r"- Line Entity:\s*(.+)", entities_text)
            if m:
                line_val = m.group(1).strip()
                try:
                    from pipelines.pm_vector_db import match_pm_task_name
                    matched_tasks = match_pm_task_name(line_val)
                    if matched_tasks:
                        tasks_str = ", ".join(f"'{t}'" for t in matched_tasks)
                        entities_text += f"\n- CRITICAL MAP MATCH: user asked for Line '{line_val}', but in PM table you MUST STRICTLY filter using: WHERE \"Task Name\" IN ({tasks_str})"
                except Exception as e:
                    logger.warning(f"[SQL_GEN] Vector match error: {e}")
                    
        matched_entities_block = "\nENTITY CONSTRAINTS (MANDATORY - Use these for WHERE clauses):\n" + entities_text
    else:
        user_question = processed_query

    prompt = f"""You are a SQLite expert. Generate ONE SQL query for the question below.

{schema_block}

{date_block}

{critical_rules}
{matched_entities_block}

Question: {user_question}
SQL:"""

    # Replace date placeholders in the entire prompt to ensure LLM sees actual dates
    prompt = prompt.replace("'today_date'", f"'{dates['today']}'")
    prompt = prompt.replace("'yesterday_date'", f"'{dates['yesterday']}'")
    prompt = prompt.replace("'tomorrow_date'", f"'{dates['tomorrow']}'")

    return prompt


# =====================================================
# 2. LLM API CALLER (MODEL_NAME / Qwen Coder only)
# =====================================================

def call_llm_for_sql(prompt: str, retries: int = 2) -> str:
    """
    เรียก MODEL_NAME (Qwen Coder) เพื่อสร้าง SQL จาก Prompt

    ⚠️ CRITICAL: ใช้ MODEL_NAME เท่านั้น — ห้ามใช้ CHAT_MODEL (Typhoon-8B)
    
    Parameters:
        prompt:  Full prompt string from build_sql_prompt()
        retries: จำนวนครั้ง retry ถ้า attempt แรกไม่ได้ SQL

    Settings:
        temperature=0.0 (deterministic), top_p=0.9, num_predict=512

    Returns:
        str: Raw LLM response text (ยังไม่ clean) หรือ "" ถ้า error
    """
    import requests
    from core.config import OLLAMA_GENERATE_URL, MODEL_NAME, OLLAMA_REQUEST_TIMEOUT
    
    # ลำดับ temperature สำหรับ retry: 0.0 → 0.3 → 0.1
    temps = [0.0, 0.3, 0.1]

    for attempt in range(retries + 1):
        temp = temps[attempt] if attempt < len(temps) else 0.0
        try:
            logger.info(f"[SQL_GEN] Calling MODEL_NAME={MODEL_NAME}, attempt={attempt+1}, temp={temp}")
            
            # เรียก Ollama API ตรงๆ
            response = requests.post(
                OLLAMA_GENERATE_URL,
                json={
                    "model": MODEL_NAME,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": temp,
                        "top_p": 0.9,
                        "num_predict": 512
                    }
                },
                timeout=OLLAMA_REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                raw = response.json().get("response", "").strip()
            else:
                logger.error(f"[SQL_GEN] HTTP {response.status_code}: {response.text}")
                raw = ""
            
            if raw:
                logger.info(f"[SQL_GEN] Got response ({len(raw)} chars) on attempt {attempt+1}")
                # ถ้าได้ SQL กลับมา ไม่ต้อง retry
                if "SELECT" in raw.upper():
                    return raw
                # ถ้าไม่มี SELECT → retry ด้วย temp สูงขึ้น
                logger.warning(f"[SQL_GEN] No SELECT in response, retrying...")
            else:
                logger.warning(f"[SQL_GEN] Empty response on attempt {attempt+1}")

        except Exception as e:
            logger.error(f"[SQL_GEN] Error on attempt {attempt+1}: {e}")

    logger.warning(f"[SQL_GEN] All {retries+1} attempts failed")
    return ""


# =====================================================
# 3. SQL EXTRACTOR & CLEANER
# =====================================================

# คำสั่ง SQL ที่ห้ามปรากฏใน output
_FORBIDDEN_STMTS = re.compile(
    r"^\s*(ATTACH|DETACH|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|PRAGMA)\b",
    re.IGNORECASE | re.MULTILINE,
)


def extract_clean_sql(raw_llm_response: str) -> str:
    """
    สกัด SQL สะอาดจาก LLM response

    Pipeline:
      1. ลบ Markdown code blocks (```sql ... ```)
      2. ลบคำอธิบาย/ข้อความที่ไม่ใช่ SQL
      3. หา SELECT ... ; (หรือ SELECT ... ถ้าไม่มี ;)
      4. ลบคำสั่งอันตราย (ATTACH, INSERT, DELETE, DROP ฯลฯ)
      5. แทนที่ date placeholders ด้วยวันที่จริง
      6. ลบ semicolons ซ้ำ / whitespace ส่วนเกิน

    Returns:
        str: SQL statement สะอาด (ขึ้นต้นด้วย SELECT, จบด้วย ;)
             หรือ "" ถ้าไม่มี SQL ที่ valid
    """
    if not raw_llm_response or not raw_llm_response.strip():
        return ""

    text = raw_llm_response.strip()

    # Step 1: ลบ Markdown code blocks
    text = re.sub(r"```(?:sql|sqlite)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"```", "", text)
    text = text.strip()

    # Step 2: ลบ CLARIFY prefix (ถ้า LLM ตอบว่าต้องถามกลับ)
    if text.upper().startswith("CLARIFY:") or text.upper().startswith("CLARIFY "):
        # คืน CLARIFY ตามที่ LLM ตอบ (ให้ handler จัดการ)
        return text

    # Step 3: หา SELECT statement
    # 3a: SELECT ... ; (มี semicolon)
    match = re.search(r"(SELECT\s+.+?;)", text, re.DOTALL | re.IGNORECASE)
    if match:
        sql = match.group(1).strip()
    else:
        # 3b: SELECT ... (ไม่มี semicolon)
        match_loose = re.search(r"(SELECT\s+.+)", text, re.DOTALL | re.IGNORECASE)
        if match_loose:
            sql = match_loose.group(1).strip()
            if not sql.endswith(";"):
                sql += ";"
        else:
            # ไม่เจอ SELECT เลย
            logger.warning(f"[SQL_GEN] No SELECT found in LLM response ({len(text)} chars)")
            return ""

    # Step 4: ลบคำสั่งอันตราย (ATTACH, INSERT, DELETE ฯลฯ)
    if _FORBIDDEN_STMTS.search(sql):
        # ลบบรรทัดที่มีคำสั่งต้องห้าม
        lines = sql.split("\n")
        clean_lines = [ln for ln in lines if not _FORBIDDEN_STMTS.match(ln.strip())]
        sql = "\n".join(clean_lines).strip()
        if not sql or "SELECT" not in sql.upper():
            logger.warning("[SQL_GEN] SQL contained only forbidden statements")
            return ""

    # Step 5: แทนที่ date placeholders ด้วยวันที่จริง (Case-insensitive)
    dates = _compute_date_context()
    original_sql = sql
    
    # แทนที่ทั้งแบบมี quote และไม่มี quote
    placeholders = {
        "today_date": dates['today'],
        "yesterday_date": dates['yesterday'],
        "tomorrow_date": dates['tomorrow']
    }
    
    for placeholder, actual_date in placeholders.items():
        # แทนที่แบบมี single quote: 'today_date' -> '2024-01-01'
        sql = re.sub(rf"'{placeholder}'", f"'{actual_date}'", sql, flags=re.IGNORECASE)
        # แทนที่แบบมี double quote: "today_date" -> '2024-01-01'
        sql = re.sub(rf"\"{placeholder}\"", f"'{actual_date}'", sql, flags=re.IGNORECASE)
        # แทนที่แบบไม่มี quote: today_date -> '2024-01-01'
        sql = re.sub(rf"\b{placeholder}\b", f"'{actual_date}'", sql, flags=re.IGNORECASE)
    
    # Log if any replacements were made
    if original_sql != sql:
        logger.info(f"[SQL_GEN] Date placeholder replaced: {original_sql} -> {sql}")
    # Step 6: ทำความสะอาด whitespace
    sql = re.sub(r"\s+", " ", sql).strip()
    # ลบ semicolons ซ้ำ
    sql = re.sub(r";+\s*$", ";", sql)

    return sql
