#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Query Pre-processing Pipeline (Phase 2)
==========================================
ทำหน้าที่ 3 อย่าง:
1. normalize_user_query     — ทำความสะอาดคำถาม (synonym, ชื่อเล่น→ชื่อจริง, วันที่)
2. rewrite_query_for_sql    — ใช้ Typhoon classify CHAT/SQL + rewrite query
3. determine_target_database — เลือก DB เป้าหมาย: 'PM' หรือ 'REPAIR'

Flow ใน _chat_impl:
  Step 1: normalized = normalize_user_query(raw_msg)
  Step 2: intent, content = rewrite_query_for_sql(normalized)
  Step 3: if intent == 'CHAT' → return chat response
  Step 4: target_db = determine_target_database(normalized)
  Step 5: sql_prompt = build_sql_prompt(content, target_db)
  Step 6: sql = call_llm_for_sql(sql_prompt) → extract_clean_sql
"""

import re
import logging
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from requests.exceptions import ConnectionError, Timeout

from core.config import (
    BANGKOK_TZ,
    CHAT_FALLBACK_RESPONSE,
    ROUTER_TIMEOUT,
    TechDataStore,
)
from pipelines.router_llm_client import call_router_llm

logger = logging.getLogger("[ROUTER]")


# =====================================================
# 1. QUERY NORMALIZER
# =====================================================

# --- 1a. Synonym Dictionary (ศัพท์เฉพาะโรงงาน → คำมาตรฐานใน DB) ---
# Key = regex pattern (case-insensitive), Value = replacement
# ⚠️ เรียงจากยาว→สั้น เพื่อป้องกัน partial match (เช่น "pcb e" ก่อน "pcb")
# หมายเหตุ: \b ใช้กับ English/alphanumeric ได้ แต่ไม่ทำงานกับภาษาไทยต่อเนื่อง
#           สำหรับคำไทย ใช้ (?:^|(?<=\s)) ... (?:$|(?=\s)) หรือ plain string แทน
SYNONYM_MAP: List[Tuple[str, str]] = [
    # --- Equipment / Process synonyms (English → ใช้ \b ได้) ---
    (r"\bpcb[\s\-]?e\b", "PCB-E"),
    (r"\bpcb[\s\-]?f\b", "PCB-F"),
    (r"\bled[\s_]?manual\b", "LED Manual"),
    (r"\bled[\s_]?auto\b", "LED Auto"),
    (r"\bsda\s*2?\b", "SDA"),
    (r"\bbreakdown\b", "เสีย"),
    # --- Thai synonyms (ใช้ lookaround แทน \b เพราะไทยต่อเนื่องไม่มี space) ---
    (r"แอร์", "Air-Conditioner"),
    (r"ดรอป", "เสีย"),
    (r"พัง", "เสีย"),
    (r"เสร็จแล้ว", "Completed"),
    (r"ยังไม่ได้ทำ", "Not started"),
    (r"เลื่อน", "Postponed"),
]


# --- Line expansion map: when user uses short/common names, expand to full DB line names
# Replacement uses quoted, comma-separated values so downstream rewrite can form IN clauses

def _build_pm_expansion_map() -> List[Tuple[str, str]]:
    """
    สร้าง PM_EXPANSION_MAP สำหรับคำถาม PM
    ไม่ใช้ mapping ที่ซับซ้อน แค่เก็บ keyword เพื่อใช้ LIKE '%keyword%' ใน Task Name
    
    เช่น: "MOPF1" → จะใช้ LIKE '%MOPF1%' ใน Task Name
          "MOPF2" → จะใช้ LIKE '%MOPF2%' หรือ LIKE '%MOPF#2%'
    
    Returns:
        List[Tuple[str, str]]: [(pattern, keyword_for_like), ...]
        keyword_for_like จะเป็น marker เช่น "LIKE_MOPF1" เพื่อให้ SQL generator รู้ว่าต้องใช้ LIKE
    """
    # สำหรับ PM queries ไม่ต้องขยายเป็น list
    # แค่เก็บ keyword ไว้ให้ SQL generator ใช้ LIKE '%keyword%'
    # ดังนั้น return empty list เพื่อไม่ให้ normalize_user_query ขยายอะไร
    # SQL generator จะจัดการเอง
    return []


def _build_line_expansion_map() -> List[Tuple[str, str]]:
    """
    สร้าง LINE_EXPANSION_MAP จาก line_pm_mapping.json อัตโนมัติ
    สำหรับคำถาม REPAIR เท่านั้น - แปลง keyword → Line Names
    
    Logic:
    1. อ่าน pairs จาก line_pm_mapping.json
    2. จัดกลุ่มตาม keyword หลัก (MOPF1, LED Auto, TOYOTA, etc.)
    3. สร้าง regex pattern → quoted list ของ Line Names
    
    Returns:
        List[Tuple[str, str]]: [(pattern, quoted_list), ...]
    """
    import json
    import os
    from collections import defaultdict
    
    mapping_path = os.path.join(os.path.dirname(__file__), "..", "data", "line_pm_mapping.json")
    if not os.path.isfile(mapping_path):
        # Fallback to hardcoded if file not found
        return [
            (r"\bled[\s_]?auto\b", "'LED_A_INS','LED_A_PCB'"),
            (r"\bled[\s_]?manual\b", "'LED_M_ASSY','LED_M_PCB','LED_Manul'"),
            (r"\bled[\s_]?manul\b", "'LED_M_ASSY','LED_M_PCB','LED_Manul'"),
            (r"\bmopf1\b", "'MOPF1_ASSY','MOPF1_Ins','MOPF1_Isp'"),
            (r"\bmopf2\b", "'MOPF2_ASSY','MOPF2_Ins'"),
            (r"\btoyota\b", "'TOYOTA_ASSY','TOYOTA_Grease','TOYOTA_INS'"),
        ]
    
    try:
        with open(mapping_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        pairs = data.get("pairs", [])
    except Exception as e:
        logging.getLogger("query_preprocessing").warning(f"Failed to load line_pm_mapping.json: {e}")
        return []
    
    # จัดกลุ่มตาม keyword หลัก
    groups = defaultdict(set)
    
    for line_name, pm_name in pairs:
        if not line_name or not isinstance(line_name, str):
            continue
        
        line_upper = line_name.upper().strip()
        
        # Skip ถ้าเป็นชื่อที่มี "PM" ข้างหน้า (เป็น PM name ไม่ใช่ Line name)
        # แต่ไม่ skip ถ้าเป็น "CMM1 MOPF1" เพราะนั่นคือ alias ของ Line
        if line_upper.startswith("PM "):
            continue
        
        # กลุ่ม MOPF1
        if "MOPF1" in line_upper:
            groups["mopf1"].add(line_name)
        # กลุ่ม MOPF2
        elif "MOPF2" in line_upper:
            groups["mopf2"].add(line_name)
        # กลุ่ม SCA3
        elif "SCA3" in line_upper:
            groups["sca3"].add(line_name)
        # กลุ่ม TOYOTA
        elif "TOYOTA" in line_upper:
            groups["toyota"].add(line_name)
        # กลุ่ม LED Auto (LED_OFF_LINE คือ LED Auto)
        elif "LED" in line_upper and ("AUTO" in line_upper or "OFF" in line_upper):
            groups["led_auto"].add(line_name)
        # กลุ่ม LED Manual
        elif "LED" in line_upper and ("MANUAL" in line_upper or "MANUL" in line_upper):
            groups["led_manual"].add(line_name)
        # กลุ่ม ELM
        elif "ELM" in line_upper:
            groups["elm"].add(line_name)
        # กลุ่ม SKY-G
        elif "SKY" in line_upper:
            groups["skyg"].add(line_name)
        # กลุ่ม FPC1
        elif "FPC" in line_upper and "1" in line_upper:
            groups["fpc1"].add(line_name)
        # กลุ่ม FPC2
        elif "FPC" in line_upper and "2" in line_upper:
            groups["fpc2"].add(line_name)
        # กลุ่ม 8G ECU
        elif "8G" in line_upper:
            groups["8g"].add(line_name)
        # กลุ่ม TRUCK
        elif "TRUCK" in line_upper:
            groups["truck"].add(line_name)
        # กลุ่ม VANm
        elif "VAN" in line_upper:
            groups["vanm"].add(line_name)
        # กลุ่ม AIRBAG
        elif "AIR" in line_upper or "BAG" in line_upper:
            groups["airbag"].add(line_name)
        # กลุ่ม MAZDA LED
        elif "MAZDA" in line_upper:
            groups["mazda_led"].add(line_name)
        # กลุ่ม CID1
        elif "CID1" in line_upper:
            groups["cid1"].add(line_name)
        # กลุ่ม CID2
        elif "CID2" in line_upper:
            groups["cid2"].add(line_name)
        # กลุ่ม CID3
        elif "CID3" in line_upper:
            groups["cid3"].add(line_name)
        # กลุ่ม BMW
        elif "BMW" in line_upper:
            groups["bmw"].add(line_name)
        # กลุ่ม R1LOW
        elif "R1LOW" in line_upper or "R1L" in line_upper:
            groups["r1low"].add(line_name)
        # กลุ่ม EPS ECU
        elif "EPS" in line_upper or "S-ASSY" in line_upper or "S.ASSY" in line_upper:
            groups["eps_ecu"].add(line_name)
        # กลุ่ม C-PCB
        elif "C-PCB" in line_upper or "C.PCB" in line_upper:
            groups["c_pcb"].add(line_name)
        # กลุ่ม ECU (ทั่วไป - ต้องเช็คหลังสุดเพราะหลายกลุ่มมี ECU)
        elif "ECU" in line_upper and "8G" not in line_upper and "EPS" not in line_upper:
            groups["ecu"].add(line_name)
        # กลุ่ม BCM1
        elif "BCM1" in line_upper:
            groups["bcm1"].add(line_name)
        # กลุ่ม BCM2
        elif "BCM2" in line_upper:
            groups["bcm2"].add(line_name)
        # กลุ่ม TCU
        elif "TCU" in line_upper:
            groups["tcu"].add(line_name)
    
    # สร้าง expansion map
    expansion_map = []
    
    # Pattern mapping: keyword → regex pattern
    pattern_map = {
        "mopf1": r"\bmopf1\b",
        "mopf2": r"\bmopf2\b",
        "sca3": r"\bsca3\b",
        "toyota": r"\btoyota\b",
        "led_auto": r"\bled[\s_]?auto\b",
        "led_manual": r"\bled[\s_]?manual\b",
        "elm": r"\belm\b",
        "skyg": r"\bsky[\s\-]?g\b",
        "fpc1": r"\bfpc[\s]?1\b",
        "fpc2": r"\bfpc[\s]?2\b",
        "8g": r"\b8g\b",
        "truck": r"\btruck\b",
        "vanm": r"\bvan[\s]?m\b",
        "airbag": r"\bair[\s_]?bag\b",
        "mazda_led": r"\bmazda[\s]?led\b",
        "cid1": r"\bcid1\b",
        "cid2": r"\bcid2\b",
        "cid3": r"\bcid3\b",
        "bmw": r"\bbmw\b",
        "r1low": r"\br1low[123]?\b",
        "eps_ecu": r"\beps[\s]?ecu\b",
        "c_pcb": r"\bc[\s\-\.]?pcb\b",
        "ecu": r"\becu\b",
        "bcm1": r"\bbcm1\b",
        "bcm2": r"\bbcm2\b",
        "tcu": r"\btcu\b",
    }
    
    for key, lines in groups.items():
        if not lines:
            continue
        pattern = pattern_map.get(key)
        if not pattern:
            continue
        # สร้าง quoted list
        quoted_list = ",".join(f"'{line}'" for line in sorted(lines))
        expansion_map.append((pattern, quoted_list))
    
    # เรียงตามความยาว pattern (ยาวก่อน) เพื่อ match แบบเฉพาะก่อน
    expansion_map.sort(key=lambda x: -len(x[0]))
    
    return expansion_map


# Build expansion maps once at module load
LINE_EXPANSION_MAP: List[Tuple[str, str]] = _build_line_expansion_map()
PM_EXPANSION_MAP: List[Tuple[str, str]] = _build_pm_expansion_map()


def _build_tech_nickname_map() -> List[Tuple[str, str]]:
    """
    สร้าง mapping ชื่อเล่น/ชื่ออังกฤษ → ชื่อเต็มภาษาไทยใน DB
    จาก TechDataStore singleton (tech_mapping + id_to_display_name)

    ตัวอย่าง: "WORRACHART" → "วรชาติ เวียงยา"
              "MANUS"      → "มนัส คุ้มทรัพย์"

    คืนเป็น List[(regex_pattern, display_name)] เรียงจากยาว→สั้น
    """
    store = TechDataStore.instance()
    tech_mapping = store.tech_mapping        # {"วรชาติ เวียงยา": "50122553", "WORRACHART": "50122553", ...}
    
    # โหลด id_to_display_name จากไฟล์ผ่าน store (ถ้ามี) — ถ้าไม่มีจะสร้างจาก tech_mapping
    # สร้าง reverse map: employee_id → ชื่อเต็มภาษาไทย (ชื่อที่ยาวที่สุดคือชื่อจริง)
    id_to_names: Dict[str, List[str]] = {}
    for name, emp_id in tech_mapping.items():
        emp_id_str = str(emp_id).strip()
        if emp_id_str not in id_to_names:
            id_to_names[emp_id_str] = []
        id_to_names[emp_id_str].append(name)
    
    # เลือก display_name = ชื่อที่มีตัวอักษรไทย + ยาวที่สุด (ชื่อเต็มภาษาไทย)
    id_to_display: Dict[str, str] = {}
    for emp_id, names in id_to_names.items():
        thai_names = [n for n in names if any("\u0E00" <= c <= "\u0E7F" for c in n)]
        if thai_names:
            id_to_display[emp_id] = max(thai_names, key=len)
        elif names:
            id_to_display[emp_id] = names[0]
    
    # สร้าง nickname → display_name pairs (เฉพาะชื่อที่ไม่ใช่ display_name เอง)
    pairs: List[Tuple[str, str]] = []
    for name, emp_id in tech_mapping.items():
        emp_id_str = str(emp_id).strip()
        display = id_to_display.get(emp_id_str)
        if display and name != display:
            # สร้าง regex: exact word boundary match ป้องกันทับซ้อน
            pattern = r"\b" + re.escape(name) + r"\b"
            pairs.append((pattern, display))
    
    # เรียงจาก pattern ยาว→สั้น เพื่อ match ชื่อยาวก่อน (กัน partial match)
    pairs.sort(key=lambda p: len(p[0]), reverse=True)
    return pairs


# Cache สร้างครั้งเดียว (lazy init)
_tech_nickname_pairs: Optional[List[Tuple[str, str]]] = None


def _get_tech_nickname_pairs() -> List[Tuple[str, str]]:
    """Lazy-load tech nickname mapping (สร้างครั้งเดียวแล้ว cache)"""
    global _tech_nickname_pairs
    if _tech_nickname_pairs is None:
        _tech_nickname_pairs = _build_tech_nickname_map()
        logger.info(f"Built tech nickname map: {len(_tech_nickname_pairs)} pairs")
    return _tech_nickname_pairs


def _replace_relative_dates(msg: str) -> str:
    """
    แปลงคำบอกเวลาสัมพัทธ์เป็นวันที่จริง (YYYY-MM-DD) ตาม Bangkok timezone
    เช่น "เมื่อวาน" → "2026-02-25", "เมื่อวานซืน" → "2026-02-24"
    
    หมายเหตุ: แปลงเฉพาะคำที่ชัดเจน ไม่แปลง "วันนี้" เพราะ SQL ใช้ date('now') ได้
    """
    now = datetime.now(BANGKOK_TZ)
    today = now.date()
    
    # หมายเหตุ: ไม่ใช้ \b เพราะภาษาไทยเขียนต่อเนื่องไม่มี space คั่น
    # เรียงจากยาว→สั้น เพื่อ match "เมื่อวานซืน" ก่อน "เมื่อวาน"
    replacements = [
        (r"เมื่อวานซืน", (today - timedelta(days=2)).strftime("%Y-%m-%d")),
        (r"เมื่อวาน", (today - timedelta(days=1)).strftime("%Y-%m-%d")),
        (r"พรุ่งนี้", (today + timedelta(days=1)).strftime("%Y-%m-%d")),
        (r"มะรืนนี้", (today + timedelta(days=2)).strftime("%Y-%m-%d")),
    ]
    for pattern, date_str in replacements:
        msg = re.sub(pattern, date_str, msg)
    return msg


def normalize_user_query(user_msg: str, target_db: Optional[str] = None) -> str:
    """
    ทำความสะอาดคำถามก่อนส่งเข้า Intent Classifier และ SQL Generator

    Pipeline:
      1. ตัด whitespace หัว/ท้าย + รวมช่องว่างซ้ำ
      2. แปลงชื่อเล่น/ชื่ออังกฤษของช่าง → ชื่อเต็มภาษาไทยใน DB
      3. แปลง synonym ศัพท์เฉพาะโรงงาน (แอร์ → Air-Conditioner, ดรอป → เสีย)
      4. แปลงวันที่สัมพัทธ์ (เมื่อวาน → YYYY-MM-DD)
      5. ขยายชื่อไลน์สั้น → ชื่อไลน์/PM ใน DB (ขึ้นอยู่กับ target_db)

    Args:
        user_msg: คำถามจากผู้ใช้
        target_db: "PM" หรือ "REPAIR" - ใช้เลือก expansion map ที่เหมาะสม
                   ถ้าไม่ระบุ จะใช้ LINE_EXPANSION_MAP (default)

    Returns:
        str: ประโยคที่ถูก normalize แล้ว พร้อมส่งต่อไป classify_intent / generate_sql
    """
    if not user_msg or not user_msg.strip():
        return ""
    
    msg = user_msg.strip()
    msg = re.sub(r"\s+", " ", msg)  # collapse multiple spaces
    
    # Step 2: แปลงชื่อเล่นช่าง → ชื่อเต็มใน DB (ใช้ regex exact match)
    for pattern, display_name in _get_tech_nickname_pairs():
        msg = re.sub(pattern, display_name, msg, flags=re.IGNORECASE)
    
    # Step 3: แปลง synonym ศัพท์เฉพาะ
    for pattern, replacement in SYNONYM_MAP:
        msg = re.sub(pattern, replacement, msg, flags=re.IGNORECASE)

    # Step 3b: ขยายชื่อไลน์สั้น → ชื่อไลน์/PM ใน DB (เลือก map ตาม target_db)
    # ถ้าเป็นคำถาม PM ใช้ PM_EXPANSION_MAP (PM Task Names)
    # ถ้าเป็นคำถาม REPAIR ใช้ LINE_EXPANSION_MAP (Line Names)
    expansion_map = PM_EXPANSION_MAP if target_db == "PM" else LINE_EXPANSION_MAP
    for pattern, replacement in expansion_map:
        msg = re.sub(pattern, replacement, msg, flags=re.IGNORECASE)
    
    # Step 4: แปลงวันที่สัมพัทธ์
    msg = _replace_relative_dates(msg)
    
    # Step 5: แทรก space ระหว่างตัวอักษรไทย↔อังกฤษ/ตัวเลข (ป้องกัน "Air-Conditionerเสีย" ติดกัน)
    msg = re.sub(r"([\u0E00-\u0E7F])([A-Za-z0-9])", r"\1 \2", msg)
    msg = re.sub(r"([A-Za-z0-9])([\u0E00-\u0E7F])", r"\1 \2", msg)
    msg = re.sub(r"\s+", " ", msg)  # collapse any double spaces
    
    return msg.strip()


# =====================================================
# 2. (REMOVED) Old Intent Classifier — replaced by rewrite_query_for_sql
# =====================================================


# =====================================================
# 3. DATABASE ROUTER (PM vs REPAIR)
# =====================================================

PM_KEYWORDS: List[str] = [
    "pm", "พีเอ็ม", "แผน", "บำรุงรักษา", "บำรุง", "preventive",
    "maintenance", "เลื่อน", "postpone", "due date",
    "task name", "progress", "not started", "completed",
    "ยังไม่ได้ทำ", "เสร็จแล้ว", "แผนซ่อมบำรุง",
]

# คำที่ชี้ชัดว่าเป็น Repair (ใช้เมื่อมี PM keyword ด้วย เพื่อแยกกรณีคลุมเครือ)
REPAIR_KEYWORDS: List[str] = [
    "เสีย", "ซ่อม", "breakdown", "repair", "เครื่องเสีย",
    "ดรอป", "พัง", "อาการ", "สาเหตุ", "การแก้ไข",
    "responseminutes", "repairminutes", "ตอบสนอง",
    "กะเช้า", "กะดึก", "shift",
]


def determine_target_database(normalized_msg: str) -> str:
    """
    วิเคราะห์ว่าคำถามควรดึงข้อมูลจากตารางไหน

    Returns:
        "PM"     — คำถามเกี่ยวกับแผนบำรุงรักษา (ตาราง PM)
        "REPAIR" — คำถามเกี่ยวกับการซ่อม (ตาราง repairs_enriched)

    Logic:
        1. ถ้ามี PM keyword แต่ไม่มี REPAIR keyword → "PM"
        2. ถ้ามี REPAIR keyword → "REPAIR" (แม้จะมี PM keyword ด้วย
           เพราะ "เครื่องเสีย PM อะไรบ้าง" ≠ ถาม PM plan แต่ถาม repair)
        3. ถ้ามีทั้ง PM + REPAIR keyword → "REPAIR" (conservative: ซ่อมสำคัญกว่า)
        4. ไม่มีทั้งคู่ → "REPAIR" (default)
    """
    if not normalized_msg:
        return "REPAIR"
    
    msg_lower = normalized_msg.lower()
    
    has_pm = any(kw in msg_lower for kw in PM_KEYWORDS)
    has_repair = any(kw in msg_lower for kw in REPAIR_KEYWORDS)
    
    if has_pm and not has_repair:
        logger.info(f"[DB_ROUTER] PM keywords detected → target: PM")
        return "PM"
    
    if has_repair:
        logger.info(f"[DB_ROUTER] REPAIR keywords detected → target: REPAIR")
        return "REPAIR"
    
    # Default: ไม่มี keyword ชี้ชัด → ถือว่าถามเรื่องซ่อม (conservative)
    logger.info(f"[DB_ROUTER] No clear keywords → default: REPAIR")
    return "REPAIR"


# =====================================================
# 4. QUERY REWRITER (Router LLM as Filter + Rewriter)
# =====================================================
# ใช้แทน Intent Classifier — Router LLM ทำ 2 อย่างใน 1 call:
#   1) คัดกรอง: CHAT หรือ SQL?
#   2) เรียงคำ: ถ้า SQL → rewrite คำถามให้ชัดเจนสำหรับ Qwen
#
# ⚠️ ใช้ router_llm_client (กำหนด provider/model ผ่าน config) — ห้ามใช้ MODEL_NAME

_REWRITE_PROMPT = """System: You are a query classifier and rewriter for a factory maintenance chatbot named "Elin".

YOUR JOB:
1. If the message is casual chat / greeting / not about factory data → reply "CHAT: <friendly Thai response as Elin>"
2. If the message asks about Repair data or PM (Preventive Maintenance) plan → reply "SQL:" followed by a structured English specification.

OUTPUT FORMAT for SQL (strictly follow this format):
SQL: TABLE: <table_name> | ACTION: <SELECT/COUNT/SUM/AVG> | COLUMNS: <col1, col2, ...> | FILTER: <conditions> | GROUP_BY: <columns> | ORDER: <column ASC/DESC> | LIMIT: <number>

COLUMN MAPPING (Thai → English):
- เครื่องเสีย/ซ่อม = repairs_enriched table
- ช่าง/เทค = Tech
- ไลน์/สายผลิต = Line
- กระบวนการ/เครื่อง = Process
- เวลาซ่อม = RepairMinutes
- เวลาตอบสนอง = ResponseMinutes
- ปัญหา/อาการ = "ปัญหา"
- สาเหตุ = "สาเหตุ"
- การแก้ไข = "การแก้ไข"
- กะเช้า = Shift='Day', กะดึก = Shift='Night'
- กะดึกวันที่ X = Shift_Date='YYYY-MM-DD' AND Shift='Night' (ครอบคลุมข้ามวัน)
- ทีม A/B/C = Team='A' or Team='B' or Team='C'
- แผน PM/บำรุงรักษา = PM table
- สถานะ PM = Progress ('Not started' or 'Completed')
- วันกำหนด PM = Due_date_ymd

TIME MAPPING:
- วันนี้ = today, เมื่อวาน = yesterday
- สัปดาห์นี้ = this_week, เดือนนี้ = this_month
- เดือนที่แล้ว = last_month, เดือนหน้า = next_month
- เดือน 1 = January (month 1), เดือน 2 = February (month 2), etc.
- เดือน 1-2 = January to February (months 1 to 2 of current year)
- IMPORTANT: "เดือน X" always refers to month number X of the year, NOT X months from now

RULES:
- Always write in English for the SQL spec
- Omit fields that are not mentioned (e.g., no GROUP_BY if not aggregating)
- For "ซ่อมเยอะสุด" (most repairs) use ACTION: COUNT
- For "ซ่อมนานสุด" (longest repair) use ACTION: SUM on RepairMinutes
- For "ประวัติ/อาการ/สาเหตุ" use COLUMNS with "ปัญหา","สาเหตุ","การแก้ไข"
- For PM questions use TABLE: PM

EXAMPLES:

User: "วันนี้มีอะไรเสียบ้าง"
SQL: TABLE: repairs_enriched | ACTION: SELECT | COLUMNS: Date, Line, Process, Tech, RepairMinutes, "ปัญหา" | FILTER: Date = today | ORDER: Date DESC

User: "ช่างไหนซ่อมเยอะสุดเดือนนี้"
SQL: TABLE: repairs_enriched | ACTION: COUNT | COLUMNS: Tech, COUNT(*) as repair_count | FILTER: Date BETWEEN this_month_start AND this_month_end | GROUP_BY: Tech | ORDER: repair_count DESC | LIMIT: 10

User: "ประวัติการซ่อม LCM"
SQL: TABLE: repairs_enriched | ACTION: SELECT | COLUMNS: Date, Line, Process, "ปัญหา", "สาเหตุ", "การแก้ไข" | FILTER: Line = 'LCM' | ORDER: Date DESC | LIMIT: 10

User: "แผน PM เดือนหน้า"
SQL: TABLE: PM | ACTION: SELECT | COLUMNS: "Task Name", Due_date_ymd, Progress, Line | FILTER: Due_date_ymd BETWEEN next_month_start AND next_month_end, Progress = 'Not started' | ORDER: Due_date_ymd ASC

User: "ทีม A ใครซ่อมนานสุด"
SQL: TABLE: repairs_enriched | ACTION: SUM | COLUMNS: Tech, SUM(CAST(RepairMinutes AS FLOAT)) as total_minutes | FILTER: Team = 'A' | GROUP_BY: Tech | ORDER: total_minutes DESC | LIMIT: 10

User: "เปรียบเทียบการเสียของ Line ระหว่างเดือน 1-2 เป็นนาที"
SQL: TABLE: repairs_enriched | ACTION: SUM | COLUMNS: Line, SUM(CAST(RepairMinutes AS FLOAT)) as total_minutes | FILTER: Date BETWEEN '2026-01-01' AND '2026-02-29' | GROUP_BY: Line | ORDER: total_minutes DESC

User: "สรุปการเสียของแต่ละไลน์เป็นนาทีเดือน1-2"
SQL: TABLE: repairs_enriched | ACTION: SUM | COLUMNS: Line, Date, COUNT(*) as repair_count, SUM(CAST(RepairMinutes AS FLOAT)) as total_minutes | FILTER: Date BETWEEN '2026-01-01' AND '2026-02-29' | GROUP_BY: Line | ORDER: total_minutes DESC

User: "สบายดีไหม"
CHAT: สบายดีค่ะพี่ หนูพร้อมช่วยเสมอเลยนะคะ ถามเรื่องการซ่อมหรือ PM ได้เลยค่ะ 😊

User: "มีแฟนยัง"
CHAT: หนูเป็น AI ค่ะ ยังไม่มีแฟนหรอกค่ะ 555 แต่พร้อมช่วยพี่เรื่องงานซ่อมเสมอนะคะ �

User: {user_msg}
"""

REWRITE_TIMEOUT: int = 100


def rewrite_query_for_sql(normalized_msg: str) -> Tuple[str, str]:
    """
    ใช้ Router LLM คัดกรอง + เรียงคำถามใน 1 call

    Returns:
        Tuple[str, str]:
            - intent: "CHAT" | "SQL"
            - content:
                ถ้า CHAT → คำตอบ chat จาก Typhoon (พร้อมส่งกลับ user)
                ถ้า SQL  → คำถามที่ถูก rewrite แล้ว (พร้อมส่งให้ Qwen)

    ⚠️ ใช้ router_llm_client เท่านั้น — ห้ามใช้ MODEL_NAME
    """
    if not normalized_msg or not normalized_msg.strip():
        return "CHAT", CHAT_FALLBACK_RESPONSE

    prompt = _REWRITE_PROMPT.format(user_msg=normalized_msg)

    try:
        raw = call_router_llm(
            prompt,
            temperature=0.2,
            max_tokens=256,
            top_p=0.9,
            timeout=REWRITE_TIMEOUT,
        ).strip()
        if not raw:
            logger.warning("[REWRITE] Empty response — fallback heuristic")
            return _fallback_rewrite_decision(normalized_msg, reason="empty_response")

        logger.info(f"[REWRITE] Typhoon response: {raw[:200]}")

        # Parse response: "CHAT: ..." or "SQL: ..."
        raw_upper = raw.upper()
        if raw_upper.startswith("CHAT:"):
            chat_response = raw[5:].strip()
            if not chat_response:
                chat_response = CHAT_FALLBACK_RESPONSE
            logger.info(f"[REWRITE] → CHAT (response: {chat_response[:80]})")
            return "CHAT", chat_response

        if raw_upper.startswith("SQL:"):
            rewritten = raw[4:].strip()
            if not rewritten:
                rewritten = normalized_msg
            logger.info(f"[REWRITE] → SQL (rewritten: {rewritten[:120]})")
            return "SQL", rewritten

        # ไม่ขึ้นต้นด้วย CHAT:/SQL: → ลอง detect
        if any(kw in raw_upper for kw in ["SELECT", "COUNT", "SUM", "GROUP", "WHERE"]):
            # Typhoon อาจตอบเป็น SQL ตรงๆ → ถือเป็น rewritten query
            logger.info(f"[REWRITE] → SQL (detected SQL-like: {raw[:120]})")
            return "SQL", raw

        # ไม่มี prefix ชัดเจน: ถ้าดูเป็น domain query ให้ส่งต่อ SQL pipeline
        fb_intent, fb_content = _fallback_rewrite_decision(normalized_msg, reason="no_prefix")
        if fb_intent == "SQL":
            logger.info("[REWRITE] no prefix but fallback heuristics => SQL")
            return "SQL", normalized_msg

        logger.info("[REWRITE] → CHAT (no prefix, treating as chat)")
        return "CHAT", (raw or fb_content)

    except (ConnectionError, Timeout) as e:
        logger.warning(f"[REWRITE] Timeout/connection ({REWRITE_TIMEOUT}s): {e} — fallback heuristic")
        return _fallback_rewrite_decision(normalized_msg, reason="timeout_or_connection")
    except Exception as e:
        logger.warning(f"[REWRITE] Error: {e} — fallback heuristic")
        return _fallback_rewrite_decision(normalized_msg, reason="unexpected_error")


def _fallback_rewrite_decision(normalized_msg: str, reason: str = "") -> Tuple[str, str]:
    """
    Heuristic fallback for rewrite step when router LLM is unavailable.
    Prevents routing every query to CHAT on transient LLM failures.
    """
    msg = str(normalized_msg or "").strip()
    if not msg:
        return "CHAT", CHAT_FALLBACK_RESPONSE

    msg_lower = msg.lower()

    chat_only_patterns = [
        "สวัสดี", "หวัดดี", "hello", "hi", "good morning", "good afternoon",
        "ขอบคุณ", "thank", "ขอบใจ",
    ]
    if any(p in msg_lower for p in chat_only_patterns) and len(msg_lower.split()) <= 4:
        return "CHAT", CHAT_FALLBACK_RESPONSE

    domain_keywords = [
        # Repair
        "เสีย", "ซ่อม", "ปัญหา", "สาเหตุ", "แก้ไข", "ประวัติ", "line", "process", "tech",
        "repair", "breakdown", "downtime", "error", "alarm", "fault", "issue", "problem",
        # PM
        "pm", "บำรุง", "แผน", "due", "progress", "task",
        # Time/data intent
        "วันนี้", "เมื่อวาน", "เดือน", "สัปดาห์", "ปี", "รายการ", "มีอะไรบ้าง", "แสดง", "ดู",
    ]
    has_domain = any(k in msg_lower for k in domain_keywords)
    has_entity_token = bool(re.search(r"\b[A-Za-z0-9][A-Za-z0-9_\-]{2,}\b", msg))

    if has_domain or has_entity_token:
        logger.info("[REWRITE] Heuristic fallback => SQL (reason=%s)", reason)
        return "SQL", msg

    logger.info("[REWRITE] Heuristic fallback => CHAT (reason=%s)", reason)
    return "CHAT", CHAT_FALLBACK_RESPONSE
