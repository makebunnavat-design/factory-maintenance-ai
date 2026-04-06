#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Post-Processing & NLG Module (Phase 5)
============================================
จัดการข้อมูลหลังการรัน SQL และสร้างคำตอบภาษาคน:
1. apply_business_logic_filters — กรองข้อมูลตาม Business Logic (ซ่อนช่าง Leader/Support)
2. generate_friendly_response    — NLG สรุปผลเป็นภาษาคน

Model Routing (CRITICAL — ห้ามสลับ):
  ⚠️ NLG ใช้ CHAT_MODEL (Typhoon) เท่านั้น — สำหรับสรุปผลเป็นภาษาคน
  ⚠️ ห้ามใช้ MODEL_NAME (Qwen) ในไฟล์นี้
"""

import logging
import json
import os
import pandas as pd
import numpy as np
from typing import Optional, Dict

from core.config import TECH_MAPPING_JSON_PATH

logger = logging.getLogger("[DATA_POST]")

# Cache tech_mapping data
_TECH_MAPPING_CACHE: Optional[Dict] = None


# =====================================================
# 1. DATA POST-PROCESSOR (Business Logic Filters)
# =====================================================

def apply_business_logic_filters(df: pd.DataFrame, target_db: str) -> pd.DataFrame:
    """
    ปรับใช้ Business Logic กับ DataFrame หลังการรัน SQL

    Pipeline:
      1. Filter tech names — ตัดรายชื่อช่าง Leader/Support ออก (ใช้ tech_mapping.json)
      2. Format numbers     — RepairMinutes, ResponseMinutes → ทศนิยม 2 ตำแหน่ง
      3. Handle NaN/Null    — แปลง NaN → empty string หรือ 0 เพื่อไม่ให้ JSON พัง

    Parameters:
        df:        DataFrame จาก execute_with_auto_fix
        target_db: "PM" หรือ "REPAIR"

    Returns:
        pd.DataFrame: DataFrame ที่ผ่านการกรองและจัดรูปแบบแล้ว
    """
    if df is None or df.empty:
        return df

    df = df.copy()  # ป้องกันแก้ original DataFrame

    # --- Step 1: Filter Tech Names (REPAIR only) ---
    if target_db == "REPAIR":
        df = _filter_excluded_techs(df)

    # --- Step 2: Format Numbers ---
    df = _format_numeric_columns(df)

    # --- Step 3: Handle NaN/Null ---
    df = _clean_null_values(df)

    logger.info(f"[POST-PROCESS] Filtered & formatted: {len(df)} rows remaining")
    return df


def _load_tech_mapping() -> Dict:
    """โหลด tech_mapping.json (cache ครั้งเดียว)"""
    global _TECH_MAPPING_CACHE
    if _TECH_MAPPING_CACHE is not None:
        return _TECH_MAPPING_CACHE

    try:
        if os.path.isfile(TECH_MAPPING_JSON_PATH):
            with open(TECH_MAPPING_JSON_PATH, "r", encoding="utf-8") as f:
                _TECH_MAPPING_CACHE = json.load(f)
                logger.info(f"[FILTER] Loaded tech_mapping.json")
                return _TECH_MAPPING_CACHE
    except Exception as e:
        logger.warning(f"[FILTER] Could not load tech_mapping.json: {e}")

    _TECH_MAPPING_CACHE = {}
    return _TECH_MAPPING_CACHE


def _filter_excluded_techs(df: pd.DataFrame) -> pd.DataFrame:
    """
    ตัดรายชื่อช่างที่เป็น Leader/Support ออกจากผลลัพธ์

    Logic:
      - ใช้ hide_in_tech_status_by_team จาก tech_mapping.json
      - ตรวจสอบคอลัมน์ 'Tech' หรือ 'ช่าง' (case-insensitive)
      - ลบแถวที่ชื่อช่างอยู่ใน hide list
    """
    tech_col = None
    for col in df.columns:
        if col.lower() in ["tech", "ช่าง", "technician"]:
            tech_col = col
            break

    if not tech_col:
        return df  # ไม่มีคอลัมน์ช่าง → ไม่ต้องกรอง

    try:
        tech_mapping_data = _load_tech_mapping()
        hide_by_team = tech_mapping_data.get("hide_in_tech_status_by_team", {})

        # รวม hide list จากทุกทีม
        all_hidden_names = set()
        for team, names in hide_by_team.items():
            all_hidden_names.update(names)

        if not all_hidden_names:
            return df  # ไม่มีรายชื่อที่ต้องซ่อน

        # กรองออก
        original_len = len(df)
        df = df[~df[tech_col].astype(str).str.strip().isin(all_hidden_names)]
        filtered_count = original_len - len(df)

        if filtered_count > 0:
            logger.info(f"[FILTER] Removed {filtered_count} rows (hidden tech names)")

    except Exception as e:
        logger.warning(f"[FILTER] Tech filtering failed: {e} — returning unfiltered data")

    return df


def _format_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    จัดรูปแบบคอลัมน์ตัวเลข (RepairMinutes, ResponseMinutes) → ทศนิยม 2 ตำแหน่ง
    """
    numeric_cols = ["RepairMinutes", "ResponseMinutes", "repair_minutes", "response_minutes"]

    for col in numeric_cols:
        if col in df.columns:
            try:
                # แปลงเป็น numeric (ถ้ายังไม่ใช่)
                df[col] = pd.to_numeric(df[col], errors="coerce")
                # ปัดเศษ 2 ตำแหน่ง
                df[col] = df[col].round(2)
            except Exception as e:
                logger.warning(f"[FORMAT] Failed to format column {col}: {e}")

    return df


def _clean_null_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    แปลง NaN/None → empty string สำหรับ string columns, None สำหรับ numeric columns
    เพื่อไม่ให้ JSON.stringify ใน Frontend พัง
    """
    for col in df.columns:
        # ถ้าเป็น numeric → คงความหมายว่า "ไม่มีข้อมูล" ไว้ ไม่บังคับเป็น 0
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].astype(object).where(df[col].notna(), None)
        else:
            # ถ้าเป็น string → แทนที่ NaN ด้วย empty string
            df[col] = df[col].fillna("")

    return df


# =====================================================
# 2. NLG - RESULT EXPLAINER
# =====================================================

_EMPTY_RESPONSES = [
    "หนูค้นหาข้อมูลแล้ว แต่ไม่พบรายการที่ตรงกับที่พี่ถามเลยค่ะ ลองปรับคำถามใหม่ดูนะคะ 🥺",
    "ไม่พบข้อมูลที่ตรงกับคำถามของพี่เลยค่ะ อาจจะลองถามใหม่ด้วยคำอื่นดูนะคะ 🤔",
    "หนูหาข้อมูลให้แล้วนะคะ แต่ไม่เจอเลย ลองเปลี่ยนคำถามดูไหมคะ? 😊",
]

_NON_EMPTY_TEMPLATES = [
    "หนูพบข้อมูลทั้งหมด {count} รายการค่ะ! รายละเอียดตามตารางด้านล่างเลยนะคะ 👇",
    "เจอแล้วค่ะ! มีข้อมูล {count} รายการตามที่พี่ถามค่ะ ดูรายละเอียดด้านล่างเลยนะคะ 📊",
    "ค้นเจอข้อมูล {count} รายการค่ะ รายละเอียดอยู่ในตารางด้านล่างนะคะ ✨",
]


def generate_friendly_response(
    df: pd.DataFrame,
    target_db: str,
    user_msg: str,
) -> str:
    """
    สร้างคำตอบภาษาคนจาก DataFrame

    Logic:
      - DataFrame ว่าง → คืนคำตอบน่ารัก (random จาก _EMPTY_RESPONSES)
      - มีข้อมูล → สรุปสั้นๆ (template: "พบ X รายการ...")

    Parameters:
        df:        DataFrame ที่ผ่าน apply_business_logic_filters แล้ว
        target_db: "PM" หรือ "REPAIR"
        user_msg:  คำถามของ user (สำหรับ context ถ้าต้องการ LLM summarize)

    Returns:
        str: คำตอบภาษาคน
    """
    if df is None or df.empty:
        # Random เลือกคำตอบ empty (ใช้ hash จาก user_msg เพื่อให้คำถามเดิมได้คำตอบเดิม)
        idx = hash(user_msg) % len(_EMPTY_RESPONSES)
        response = _EMPTY_RESPONSES[idx]
        logger.info("[NLG] Empty result → friendly empty response")
        return response

    # มีข้อมูล → สรุปสั้นๆ
    count = len(df)
    idx = hash(user_msg) % len(_NON_EMPTY_TEMPLATES)
    response = _NON_EMPTY_TEMPLATES[idx].format(count=count)

    logger.info(f"[NLG] Non-empty result ({count} rows) → template response")
    return response


# =====================================================
# 3. OPTIONAL: LLM-POWERED NLG (ไม่ใช้ในเวอร์ชันนี้)
# =====================================================

def generate_llm_summary(
    df: pd.DataFrame,
    user_msg: str,
    timeout: int = 5,
) -> Optional[str]:
    """
    (OPTIONAL) ใช้ LLM (CHAT_MODEL/Typhoon) สรุปผล DataFrame เป็นภาษาคน

    ⚠️ ไม่ได้ใช้งานในเวอร์ชันปัจจุบัน — เก็บไว้สำหรับอนาคต
    ถ้าต้องการใช้: ให้เรียกฟังก์ชันนี้ใน generate_friendly_response
    และ fallback ไปใช้ template ถ้า LLM timeout/error

    Parameters:
        df:       DataFrame (จำกัดแค่ 5 แถวแรก)
        user_msg: คำถามของ user
        timeout:  Timeout (วินาที) สำหรับ LLM call

    Returns:
        str: คำตอบจาก LLM หรือ None ถ้า error
    """
    # TODO: Implement LLM summarization with CHAT_MODEL
    # 1. df.head(5).to_dict(orient="records")
    # 2. Build prompt: "User asked: {user_msg}\nData: {data}\nSummarize in Thai (1-2 sentences)"
    # 3. Call CHAT_MODEL with timeout
    # 4. Return response or None
    return None
