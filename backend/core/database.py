#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Connection Helpers for Repair Chatbot Backend
=======================================================
Context managers สำหรับ SQLite connections — รับประกันว่า connection จะถูกปิดเสมอ
แม้จะเกิด error ก็ตาม เพื่อป้องกัน Database Locked
"""

import os
import sqlite3
import logging
from contextlib import contextmanager
from typing import Optional

from core.config import WORK_DB_PATH, PM2025_DB_PATH

logger = logging.getLogger("[DB_EXEC]")


# =====================================================
# CONTEXT MANAGERS
# =====================================================

@contextmanager
def get_work_db():
    """
    Context manager สำหรับเปิด work DB (repair_enriched.db) แบบ read-write
    ใช้:
        with get_work_db() as conn:
            df = pd.read_sql_query("SELECT ...", conn)
    รับประกัน: conn.close() จะถูกเรียกเสมอแม้เกิด exception
    """
    conn = sqlite3.connect(WORK_DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_work_db_readonly():
    """
    Context manager สำหรับเปิด work DB แบบ read-only
    ใช้สำหรับ query อ่านอย่างเดียว เพื่อป้องกันการเขียนโดยไม่ตั้งใจ
    """
    abs_path = os.path.abspath(WORK_DB_PATH).replace("\\", "/")
    if abs_path.startswith("/"):
        uri = f"file:{abs_path}?mode=ro"
    else:
        uri = f"file:///{abs_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_pm_db_readonly(pm_db_path: Optional[str] = None):
    """
    Context manager สำหรับเปิด PM2025.db แบบ read-only
    กันไม่ให้คำสั่งเขียน/ลบทำลายตาราง PM
    ใช้:
        with get_pm_db_readonly() as conn:
            df = pd.read_sql_query("SELECT * FROM PM", conn)
    """
    path = pm_db_path or PM2025_DB_PATH
    abs_path = os.path.abspath(path).replace("\\", "/")
    if abs_path.startswith("/"):
        uri = f"file:{abs_path}?mode=ro"
    else:
        uri = f"file:///{abs_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_source_db_readonly(source_db_path: str):
    """
    Context manager สำหรับเปิด source DB (repair_data.db) แบบ read-only
    ใช้ตอน load_and_enrich_data()
    """
    conn = sqlite3.connect(f"file:{source_db_path}?mode=ro", uri=True)
    try:
        yield conn
    finally:
        conn.close()
