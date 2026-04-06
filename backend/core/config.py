#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Centralized Configuration for Repair Chatbot Backend
=====================================================
ย้าย constants, paths, timeouts, model names, และ singleton loaders ทั้งหมดมาไว้ที่เดียว
เพื่อลด hardcode ใน main.py และป้องกันการอ่านไฟล์ซ้ำทุก request
"""

import os
import json
import logging
import threading
from datetime import timezone, timedelta
from typing import Dict, List, Tuple, Optional, Set

# =====================================================
# 1. BASE DIRECTORY
# =====================================================
BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# =====================================================
# 2. TIMEZONE
# =====================================================
BANGKOK_TZ = timezone(timedelta(hours=7))

# =====================================================
# 3. DATABASE PATHS
# =====================================================
SOURCE_DB_PATH: str = os.path.join(BASE_DIR, "data", "repair_data.db")
WORK_DB_PATH: str = os.path.join(BASE_DIR, "data", "repair_enriched.db")
PM2025_DB_PATH: str = os.path.join(BASE_DIR, "data", "PM2025.db")

# =====================================================
# 4. DATA FILE PATHS
# =====================================================
QA_LOG_FILE: str = os.path.join(BASE_DIR, "data", "qa_log.jsonl")
TECH_LIST_JSON_PATH: str = os.path.join(BASE_DIR, "data", "tech_list_from_db.json")
TECH_MAPPING_JSON_PATH: str = os.path.join(BASE_DIR, "data", "tech_mapping.json")
LINE_PM_MAPPING_PATH: str = os.path.join(BASE_DIR, "data", "line_pm_mapping.json")
FAILED_LOG_FILE: str = "failed_logs.txt"

# =====================================================
# 5. OLLAMA / LLM SETTINGS
# =====================================================
OLLAMA_HOST: str = os.getenv("OLLAMA_HOST", "http://ollama_service:11434")
OLLAMA_GENERATE_URL: str = f"{OLLAMA_HOST}/api/generate"
OLLAMA_PULL_URL: str = f"{OLLAMA_HOST}/api/pull"
OLLAMA_TAGS_URL: str = f"{OLLAMA_HOST}/api/tags"

# MODEL_NAME: ใช้สำหรับสร้าง SQL / ตอบจากข้อมูล (เปลี่ยนเป็น 7B เพื่อความเสถียรบน CPU)
MODEL_NAME: str = os.getenv("OLLAMA_MODEL", "hf.co/bartowski/Qwen2.5-Coder-7B-Instruct-GGUF:Q4_K_M")
# CHAT_MODEL: ใช้สำหรับ (1) ตัวแยกโหมด Intent Router และ (2) โหมดคุยเล่น — เลือกโมเดลเร็วขนาด 8B
CHAT_MODEL: str = os.getenv("OLLAMA_CHAT_MODEL", "scb10x/llama3.1-typhoon2-8b-instruct:latest")

# =====================================================
# 6. TIMEOUT SETTINGS (seconds)
# =====================================================
ROUTER_TIMEOUT: int = int(os.getenv("ROUTER_TIMEOUT", "100"))
OLLAMA_REQUEST_TIMEOUT: int = 180  # เพิ่มจาก 100 เป็น 180 วินาที
OLLAMA_TAGS_TIMEOUT: int = 30
OLLAMA_PULL_TIMEOUT: int = 600

# =====================================================
# 6.1 ROUTER LLM PROVIDER SETTINGS
# =====================================================
# Provider options:
# - "ollama"       : use OLLAMA_GENERATE_URL
# - "openai_compat": use ROUTER_API_URL (chat/completions compatible, incl. SCB10X gateway)
# - "scb10x"       : alias of openai_compat
ROUTER_LLM_PROVIDER: str = os.getenv("ROUTER_LLM_PROVIDER", "ollama")
ROUTER_MODEL: str = os.getenv("ROUTER_MODEL", CHAT_MODEL)
ROUTER_API_URL: str = os.getenv("ROUTER_API_URL", "")
ROUTER_API_KEY: str = os.getenv("ROUTER_API_KEY", "")
ROUTER_API_TIMEOUT: int = int(os.getenv("ROUTER_API_TIMEOUT", "100"))

# =====================================================
# 7. STATIC VERSION (cache bust)
# =====================================================
# Use the latest modification time from key static files
_frontend_dir = os.path.join(os.path.dirname(BASE_DIR), "frontend")
_static_files = [
    os.path.join(_frontend_dir, "js", "app.js"),
    os.path.join(_frontend_dir, "css", "style.css"),
]
_static_mtimes = [int(os.path.getmtime(f)) for f in _static_files if os.path.isfile(f)]
STATIC_VERSION: str = str(max(_static_mtimes)) if _static_mtimes else str(int(__import__("time").time()))

# =====================================================
# 8. CHAT FALLBACK
# =====================================================
CHAT_FALLBACK_RESPONSE: str = "ถามเกี่ยวกับข้อมูลการซ่อมหรือ PM ได้เลยครับ เช่น 'วันนี้มีอะไรเสียบ้าง' หรือ 'เดือนนี้มี PM อะไรบ้าง'"


# =====================================================
# 9. TECH DATA STORE (Singleton)
#    โหลดไฟล์ JSON ครั้งเดียวตอนเริ่มระบบ ไม่อ่านซ้ำทุก request
# =====================================================
class TechDataStore:
    """
    Singleton: โหลด tech_mapping.json และ tech_list_from_db.json ครั้งเดียว
    เข้าถึงผ่าน TechDataStore.instance() เพื่อป้องกันการอ่านไฟล์ซ้ำทุก request
    """
    _instance: Optional["TechDataStore"] = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._tech_mapping: Dict = {}
        self._team_assignment: Dict = {}
        self._tech_list: List[str] = []
        self._exclude_from_answer: Tuple[str, ...] = ("TEST",)
        self._loaded = False

    @classmethod
    def instance(cls) -> "TechDataStore":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
                    cls._instance._load_all()
        return cls._instance

    @classmethod
    def reload(cls) -> "TechDataStore":
        """Force reload ข้อมูลใหม่ (เช่น หลัง data reload)"""
        with cls._lock:
            cls._instance = cls()
            cls._instance._load_all()
        return cls._instance

    def _load_all(self) -> None:
        self._load_tech_mapping()
        self._load_tech_list()
        self._loaded = True

    def _load_tech_mapping(self) -> None:
        """โหลด tech_mapping.json (tech_mapping + team_assignment)"""
        try:
            if os.path.isfile(TECH_MAPPING_JSON_PATH):
                with open(TECH_MAPPING_JSON_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._tech_mapping = data.get("tech_mapping", {})
                self._team_assignment = data.get("team_assignment", {})
                logging.getLogger("config").info(
                    f"[CONFIG] Loaded tech_mapping: {len(self._tech_mapping)} techs, "
                    f"{len(self._team_assignment)} teams"
                )
        except Exception as e:
            logging.getLogger("config").warning(f"[CONFIG] Could not load tech_mapping: {e}")
            self._tech_mapping = {}
            self._team_assignment = {}

    def _load_tech_list(self) -> None:
        """โหลด tech_list_from_db.json (Tech list + ExcludeFromAnswer)"""
        try:
            if os.path.isfile(TECH_LIST_JSON_PATH):
                with open(TECH_LIST_JSON_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._tech_list = [str(x).strip() for x in (data.get("Tech") or []) if x]
                lst = data.get("ExcludeFromAnswer") or []
                self._exclude_from_answer = tuple(str(x).strip() for x in lst if x)
                logging.getLogger("config").info(
                    f"[CONFIG] Loaded tech_list: {len(self._tech_list)} techs, "
                    f"{len(self._exclude_from_answer)} excluded"
                )
        except Exception as e:
            logging.getLogger("config").warning(f"[CONFIG] Could not load tech_list: {e}")
            self._tech_list = []
            self._exclude_from_answer = ("TEST",)

    # --- Public accessors ---

    @property
    def tech_mapping(self) -> Dict:
        return self._tech_mapping

    @property
    def team_assignment(self) -> Dict:
        return self._team_assignment

    @property
    def tech_list(self) -> List[str]:
        return self._tech_list

    @property
    def exclude_from_answer(self) -> Tuple[str, ...]:
        return self._exclude_from_answer
