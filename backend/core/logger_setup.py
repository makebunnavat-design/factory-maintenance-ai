#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Logging Setup for Repair Chatbot Backend
==========================================
Logger แยก prefix ตามส่วนงาน เช่น [ROUTER], [SQL_GEN], [DB_EXEC]
เพื่อให้ดู log แล้วรู้ทันทีว่ามาจากส่วนไหน
"""

import logging
import sys

# =====================================================
# FORMAT & HANDLER
# =====================================================
_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_initialized = False


def setup_logging(level: int = logging.INFO) -> None:
    """ตั้งค่า root logger ครั้งเดียว — เรียกตอน startup"""
    global _initialized
    if _initialized:
        return
    root = logging.getLogger()
    root.setLevel(level)
    # ลบ handler เก่า (กัน basicConfig ซ้ำ)
    for h in root.handlers[:]:
        root.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(handler)
    _initialized = True


def get_logger(prefix: str) -> logging.Logger:
    """
    สร้าง logger ที่มี prefix ชัดเจน เช่น:
        log = get_logger("ROUTER")   → name = "[ROUTER]"
        log = get_logger("SQL_GEN")  → name = "[SQL_GEN]"
        log = get_logger("DB_EXEC")  → name = "[DB_EXEC]"
        log = get_logger("CHAT")     → name = "[CHAT]"
        log = get_logger("CONFIG")   → name = "[CONFIG]"
        log = get_logger("DATA")     → name = "[DATA]"

    ใช้:
        log.info("Generating SQL for: %s", user_msg)
        log.error("SQL execution failed: %s", err)

    Output:
        2026-02-26 15:40:00 - INFO - [SQL_GEN] - generate_sql_simple - Generating SQL for: ...
    """
    setup_logging()
    return logging.getLogger(f"[{prefix}]")
