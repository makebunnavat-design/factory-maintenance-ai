#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Execution & Auto-Fix Module (Phase 4)
==========================================
จัดการการรัน SQL อย่างปลอดภัย พร้อมระบบแก้ไขตัวเองอัตโนมัติ:
1. sanitize_sql_for_execution — ตรวจความปลอดภัย + enforce LIMIT + แก้ date literals
2. execute_sql_query          — รัน SQL กับ DB ที่ถูกต้อง (PM vs REPAIR)
3. execute_with_auto_fix      — Auto-Fix Loop: ส่ง error กลับให้ LLM แก้ SQL

Model Routing (CRITICAL — ห้ามสลับ):
  ⚠️ Auto-Fix Loop ใช้ MODEL_NAME (Qwen Coder) เท่านั้น
  ⚠️ ห้ามใช้ CHAT_MODEL ในไฟล์นี้
"""

import re
import os
import logging
import sqlite3
import pandas as pd
from datetime import datetime
from typing import Tuple, Optional

from core.config import (
    BANGKOK_TZ,
    WORK_DB_PATH, PM2025_DB_PATH,
    OLLAMA_GENERATE_URL, OLLAMA_HOST,
    MODEL_NAME,  # ⚠️ Qwen Coder — SQL Generation / Fix ONLY
    OLLAMA_REQUEST_TIMEOUT,
)
from pipelines.sql_generator import call_llm_for_sql, extract_clean_sql

logger = logging.getLogger("[DB_EXEC]")


# =====================================================
# 1. SQL SANITIZER & VALIDATOR
# =====================================================

# คำสั่ง SQL ที่ห้ามรัน (อนุญาตแค่ SELECT / WITH ... SELECT)
_DANGEROUS_KEYWORDS = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|ALTER|TRUNCATE|REPLACE|CREATE|ATTACH|DETACH)\b",
    re.IGNORECASE,
)

# Default LIMIT สำหรับ REPAIR queries ที่ไม่ได้ระบุ LIMIT
_DEFAULT_REPAIR_LIMIT = 15


def sanitize_sql_for_execution(clean_sql: str, target_db: str) -> str:
    """
    ตรวจสอบความปลอดภัยและทำความสะอาด SQL ก่อนรัน

    Pipeline:
      1. Security Check — block คำสั่งอันตราย (DROP, DELETE, UPDATE ฯลฯ)
      2. Enforce LIMIT  — REPAIR queries ที่ไม่มี LIMIT → ต่อท้าย LIMIT 15
                          PM queries ไม่บังคับ LIMIT (แสดงทั้งหมด)
      3. Date Correction — แก้ date literals ที่ LLM เขียนผิด
                          เช่น Date = 'today' → Date = '2026-02-26'

    Parameters:
        clean_sql:  SQL ที่ผ่าน extract_clean_sql แล้ว
        target_db:  "PM" หรือ "REPAIR"

    Returns:
        str: SQL ที่ปลอดภัยพร้อมรัน

    Raises:
        ValueError: ถ้า SQL มีคำสั่งอันตราย หรือไม่ใช่ SELECT
    """
    if not clean_sql or not clean_sql.strip():
        raise ValueError("[SANITIZE] Empty SQL")

    sql = clean_sql.strip()

    # --- Step 1: Security Check ---
    sql_upper = sql.upper().strip()
    # อนุญาตเฉพาะ SELECT หรือ WITH ... SELECT
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        # อาจเป็น CLARIFY → ปล่อยผ่าน
        if sql_upper.startswith("CLARIFY"):
            return sql
        raise ValueError(f"[SANITIZE] SQL must start with SELECT or WITH, got: {sql[:50]}")

    # ตรวจหาคำสั่งอันตราย
    dangerous = _DANGEROUS_KEYWORDS.findall(sql)
    if dangerous:
        raise ValueError(f"[SANITIZE] Dangerous SQL keywords detected: {dangerous}")

    # --- Step 2: Enforce LIMIT (เฉพาะ REPAIR) ---
    if target_db == "REPAIR":
        if "LIMIT" not in sql_upper:
            # ลบ semicolon ท้ายก่อนต่อ LIMIT
            sql = re.sub(r";\s*$", "", sql).strip()
            sql += f" LIMIT {_DEFAULT_REPAIR_LIMIT};"
            logger.info(f"[SANITIZE] Added LIMIT {_DEFAULT_REPAIR_LIMIT} to REPAIR query")
        # PM: ไม่ต้องบังคับ LIMIT (แสดงทั้งหมด)

    # --- Step 3: Date Correction ---
    now = datetime.now(BANGKOK_TZ)
    today_str = now.strftime("%Y-%m-%d")
    yesterday_str = (now - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    # แก้ Date = 'today' → Date = 'YYYY-MM-DD'
    sql = re.sub(
        r"(?i)(Date\s*=\s*)'today'",
        rf"\g<1>'{today_str}'",
        sql,
    )
    # แก้ Date = 'yesterday'
    sql = re.sub(
        r"(?i)(Date\s*=\s*)'yesterday'",
        rf"\g<1>'{yesterday_str}'",
        sql,
    )
    # แก้ Date = 'now' (บางครั้ง LLM เขียน)
    sql = re.sub(
        r"(?i)(Date\s*=\s*)'now'",
        rf"\g<1>'{today_str}'",
        sql,
    )
    # แก้ CURRENT_DATE → 'YYYY-MM-DD' (SQLite function ที่ใช้ UTC → อาจคลาดเคลื่อน)
    sql = re.sub(
        r"\bCURRENT_DATE\b",
        f"'{today_str}'",
        sql,
        flags=re.IGNORECASE,
    )
    # แก้ date('now') → 'YYYY-MM-DD'
    sql = re.sub(
        r"date\s*\(\s*'now'\s*\)",
        f"'{today_str}'",
        sql,
        flags=re.IGNORECASE,
    )

    # ให้แน่ใจว่าจบด้วย semicolon
    sql = sql.strip()
    if not sql.endswith(";"):
        sql += ";"

    return sql


# =====================================================
# 2. DATABASE EXECUTOR
# =====================================================

def _resolve_pm_db_path() -> Optional[str]:
    """หา path จริงของ PM2025.db (รองรับรันจากโฟลเดอร์อื่นหรือ Docker volume)"""
    candidates = [
        PM2025_DB_PATH,
        os.path.join(os.getcwd(), "data", "PM2025.db"),
        os.path.join(os.getcwd(), "backend", "data", "PM2025.db"),
    ]
    for p in candidates:
        if p and os.path.isfile(p):
            return p
    return None


def _open_pm_readonly(pm_path: str) -> sqlite3.Connection:
    """เปิด PM2025.db แบบ read-only (กัน write/delete)"""
    abs_path = os.path.abspath(pm_path).replace("\\", "/")
    if abs_path.startswith("/"):
        uri = f"file:{abs_path}?mode=ro"
    else:
        uri = f"file:///{abs_path}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def execute_sql_query(sanitized_sql: str, target_db: str) -> pd.DataFrame:
    """
    รัน SQL กับ DB ที่ถูกต้อง แล้วคืน DataFrame

    Logic:
      - target_db == 'PM' หรือ SQL มี FROM PM → ลองรัน PM2025.db (read-only) ก่อน
        ถ้าล้มเหลว fallback ไป WORK_DB_PATH
      - target_db == 'REPAIR' → รันกับ WORK_DB_PATH (repairs_enriched)

    Parameters:
        sanitized_sql: SQL ที่ผ่าน sanitize_sql_for_execution แล้ว
        target_db:     "PM" หรือ "REPAIR"

    Returns:
        pd.DataFrame: ผลลัพธ์จาก query

    Raises:
        sqlite3.OperationalError: ถ้า SQL ยังผิดอยู่ (เช่น no such column)
        Exception: อื่นๆ ที่ไม่คาดคิด
    """
    sql_upper = (sanitized_sql or "").upper()
    is_pm_sql = "FROM PM" in sql_upper or "JOIN PM" in sql_upper

    # --- PM path: ลอง PM2025.db read-only ก่อน ---
    if target_db == "PM" or is_pm_sql:
        pm_path = _resolve_pm_db_path()
        # ตรวจว่า SQL ไม่ใช้คอลัมน์ที่มีเฉพาะใน work DB (PM_real_date, IsPostponed, Due_date_ymd)
        has_work_only_cols = any(k in sql_upper for k in ["PM_REAL_DATE", "ISPOSTPONED", "DUE_DATE_YMD"])

        if pm_path and not has_work_only_cols:
            try:
                with _open_pm_readonly(pm_path) as conn:
                    df = pd.read_sql_query(sanitized_sql, conn)
                logger.info(f"[EXECUTION] PM query OK on PM2025.db (read-only): {pm_path}")
                return df
            except Exception as e:
                logger.warning(f"[EXECUTION] PM2025.db failed (will try work DB): {e}")

    # --- Work DB (repairs_enriched + PM synced copy) ---
    if not os.path.exists(WORK_DB_PATH):
        raise FileNotFoundError(f"[EXECUTION] Work DB not found: {WORK_DB_PATH}")

    with sqlite3.connect(WORK_DB_PATH) as conn:
        df = pd.read_sql_query(sanitized_sql, conn)
    logger.info(f"[EXECUTION] Query OK on work DB ({len(df)} rows)")
    return df


# =====================================================
# 3. AUTO-FIX LOOP
# =====================================================

_FIX_PROMPT_TEMPLATE = """The following SQLite query failed.

Original SQL:
{failed_sql}

Error:
{error_msg}

{schema_hint}

Task: Fix the SQL query so it runs without errors.
- Use table PM (not pm.PM) for PM data. Use repairs_enriched for repair data.
- PM columns: "Task Name", Due_date_ymd, Progress, Description, Line
- Progress values: 'Not started', 'Completed' — do NOT use 'Pending'
- Do NOT use columns: ปัญหา, สาเหตุ, Process, Tech, RepairMinutes in PM table
- Output ONLY the fixed SQL, nothing else."""


def execute_with_auto_fix(
    original_prompt: str,
    initial_sql: str,
    target_db: str,
    max_retries: int = 2,
) -> Tuple[pd.DataFrame, str]:
    """
    รัน SQL พร้อม Auto-Fix Loop: ถ้า error ให้ LLM แก้ SQL แล้วลองใหม่

    Flow:
      1. sanitize + execute initial_sql
      2. ถ้าสำเร็จ → คืน (DataFrame, final_sql)
      3. ถ้า error → สร้าง fix prompt (แนบ SQL เดิม + error message)
         → ส่งให้ call_llm_for_sql (MODEL_NAME / Qwen Coder)
         → extract_clean_sql → sanitize → execute
         → ทำซ้ำจนสำเร็จหรือครบ max_retries

    Parameters:
        original_prompt: Prompt ดั้งเดิมจาก build_sql_prompt (สำหรับ context)
        initial_sql:     SQL แรกที่ extract จาก LLM response
        target_db:       "PM" หรือ "REPAIR"
        max_retries:     จำนวนครั้ง retry สูงสุด (default=2)

    Returns:
        Tuple[pd.DataFrame, str]: (ผลลัพธ์ DataFrame, SQL สุดท้ายที่สำเร็จ)

    Raises:
        RuntimeError: ถ้า retry ครบแล้วยังไม่สำเร็จ
    """
    current_sql = initial_sql
    last_error = ""

    for attempt in range(max_retries + 1):
        # --- Sanitize ---
        try:
            safe_sql = sanitize_sql_for_execution(current_sql, target_db)
        except ValueError as e:
            last_error = str(e)
            logger.warning(f"[AUTO-FIX] Sanitize failed (attempt {attempt+1}): {e}")
            if attempt >= max_retries:
                break
            # สร้าง fix prompt จาก sanitize error
            current_sql = _request_fix_from_llm(current_sql, last_error, target_db)
            if not current_sql:
                break
            continue

        # --- Execute ---
        try:
            df = execute_sql_query(safe_sql, target_db)
            logger.info(f"[AUTO-FIX] Success on attempt {attempt+1} ({len(df)} rows)")
            return df, safe_sql
        except Exception as e:
            last_error = str(e)
            logger.warning(f"[AUTO-FIX] Execute failed (attempt {attempt+1}/{max_retries+1}): {e}")
            if attempt >= max_retries:
                break
            # --- Request fix from LLM ---
            current_sql = _request_fix_from_llm(safe_sql, last_error, target_db)
            if not current_sql:
                logger.error("[AUTO-FIX] LLM returned empty fix — giving up")
                break

    raise RuntimeError(
        f"[AUTO-FIX] Failed after {max_retries+1} attempts. Last error: {last_error}"
    )


def _request_fix_from_llm(failed_sql: str, error_msg: str, target_db: str) -> str:
    """
    ส่ง SQL ที่ล้มเหลว + error message กลับให้ LLM (MODEL_NAME/Qwen) แก้ไข

    Returns:
        str: SQL ที่ LLM แก้แล้ว (ผ่าน extract_clean_sql) หรือ "" ถ้า error
    """
    schema_hint = ""
    if target_db == "PM":
        schema_hint = 'PM table columns: "Task Name", Due_date_ymd, Progress, Description, Line. Use FROM PM.'
    else:
        schema_hint = "Table: repairs_enriched. Columns: Date, Shift, Team, Tech, Line, Process, RepairMinutes, ResponseMinutes."

    # ตัด failed_sql ถ้ายาวเกิน 500 chars เพื่อป้องกัน LLM ก๊อบ IN(...) list ขนาดใหญ่ซ้ำ
    failed_sql_trimmed = failed_sql if len(failed_sql) <= 500 else failed_sql[:500] + "\n... [truncated]"
    fix_prompt = _FIX_PROMPT_TEMPLATE.format(
        failed_sql=failed_sql_trimmed,
        error_msg=error_msg,
        schema_hint=schema_hint,
    )

    logger.info(f"[AUTO-FIX] Requesting LLM fix (MODEL_NAME={MODEL_NAME})")
    raw_response = call_llm_for_sql(fix_prompt, retries=0)  # ไม่ retry ภายใน call เพราะ loop ภายนอกจัดการ
    if not raw_response:
        return ""

    fixed_sql = extract_clean_sql(raw_response)
    logger.info(f"[AUTO-FIX] LLM fixed SQL: {fixed_sql[:200] if fixed_sql else '(empty)'}")
    return fixed_sql
