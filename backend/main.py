#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import requests
from requests.exceptions import Timeout, ConnectionError
import re
import os
import time
import logging
import sys

# Force UTF-8 encoding for all operations
import codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)
sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer)

# Set default encoding
if hasattr(sys, 'setdefaultencoding'):
    sys.setdefaultencoding('utf-8')
import threading
import json
import numpy as np
import uuid
from datetime import time as dt_time, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
from contextlib import asynccontextmanager

# FastAPI imports
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel

# Third-party imports
from thefuzz import process as fuzz_process, fuzz

# --- Phase 1: Import from refactored modules ---
from core.config import (
    BASE_DIR as _BASE_DIR,
    BANGKOK_TZ,
    SOURCE_DB_PATH, WORK_DB_PATH, PM2025_DB_PATH,
    QA_LOG_FILE, TECH_LIST_JSON_PATH, TECH_MAPPING_JSON_PATH,
    LINE_PM_MAPPING_PATH, FAILED_LOG_FILE,
    OLLAMA_HOST, OLLAMA_GENERATE_URL, OLLAMA_PULL_URL, OLLAMA_TAGS_URL,
    MODEL_NAME, CHAT_MODEL,
    ROUTER_TIMEOUT, OLLAMA_REQUEST_TIMEOUT, OLLAMA_TAGS_TIMEOUT, OLLAMA_PULL_TIMEOUT,
    STATIC_VERSION, CHAT_FALLBACK_RESPONSE,
    TechDataStore,
)
from core.models import ChatRequest, ChatResponse, ChatErrorResponse
from core.logger_setup import setup_logging, get_logger
from core.database import get_work_db, get_work_db_readonly, get_pm_db_readonly, get_source_db_readonly
from pipelines.llm_router import route_message_with_llm
from pipelines.sql_generator import build_sql_prompt, call_llm_for_sql, extract_clean_sql
from utils.data_postprocessor import apply_business_logic_filters, generate_friendly_response
from utils.observability import log_event
from services.entity_matching import get_entity_engine
from services.tts_service import tts_manager

# =====================================================
# OPTIMIZATION MODULES (Integrated)
# =====================================================

# --- DateContext: Centralized Date Handling ---
class DateContext:
    """
    จัดการวันที่ทั้งหมดในระบบแบบรวมศูนย์
    คำนวณครั้งเดียว ใช้ได้ทั้งในการสร้าง SQL, rewrite, และ validation
    """
    
    def __init__(self):
        """คำนวณวันที่ทั้งหมดที่ใช้ในระบบ"""
        import pandas as pd
        self.now = pd.Timestamp.now()
        
        # วันที่พื้นฐาน
        self.today = self.now.strftime('%Y-%m-%d')
        self.yesterday = (self.now - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (self.now + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        
        # สัปดาห์ปัจจุบัน (จันทร์-อาทิตย์)
        self.week_start = (self.now - pd.Timedelta(days=self.now.dayofweek)).strftime('%Y-%m-%d')
        self.week_end = (self.now + pd.Timedelta(days=6 - self.now.dayofweek)).strftime('%Y-%m-%d')
        
        # สัปดาห์ที่แล้ว (จันทร์-อาทิตย์)
        self.last_week_start = (self.now - pd.Timedelta(days=self.now.dayofweek + 7)).strftime('%Y-%m-%d')
        self.last_week_end = (self.now - pd.Timedelta(days=self.now.dayofweek + 1)).strftime('%Y-%m-%d')
        
        # สัปดาห์หน้า (จันทร์-อาทิตย์)
        self.next_week_start = (self.now + pd.Timedelta(days=7 - self.now.dayofweek)).strftime('%Y-%m-%d')
        self.next_week_end = (self.now + pd.Timedelta(days=13 - self.now.dayofweek)).strftime('%Y-%m-%d')
        
        # เดือนปัจจุบัน (วันที่ 1 - วันสุดท้าย)
        self.month_start = self.now.replace(day=1).strftime('%Y-%m-%d')
        self.month_end = (self.now.replace(day=1) + pd.offsets.MonthEnd(0)).strftime('%Y-%m-%d')
        
        # เดือนที่แล้ว
        self.last_month_start = (self.now.replace(day=1) - pd.Timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
        self.last_month_end = (self.now.replace(day=1) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        
        # เดือนหน้า (วันที่ 1 - วันสุดท้าย)
        if self.now.month == 12:
            _first_next = self.now.replace(year=self.now.year + 1, month=1, day=1)
        else:
            _first_next = self.now.replace(month=self.now.month + 1, day=1)
        self.next_month_start = _first_next.strftime('%Y-%m-%d')
        self.next_month_end = (_first_next + pd.offsets.MonthEnd(0)).strftime('%Y-%m-%d')
        
        # ปี
        self.year_start = self.now.replace(month=1, day=1).strftime('%Y-%m-%d')
        self.year_end = self.now.replace(month=12, day=31).strftime('%Y-%m-%d')
        self.current_year = self.now.year

_date_context_instance = None

def get_date_context():
    """Get or create global DateContext instance"""
    global _date_context_instance
    if _date_context_instance is None:
        _date_context_instance = DateContext()
    return _date_context_instance

# --- SQLValidator: SQL Safety & Validation ---
class SQLValidator:
    """ตรวจสอบความปลอดภัยและความถูกต้องของ SQL"""
    
    FORBIDDEN_OPERATIONS = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE', 'REPLACE', 'MERGE']
    DANGEROUS_PATTERNS = [r';\s*DROP', r';\s*DELETE', r'--', r'/\*', r'\bEXEC\b', r'\bEXECUTE\b']
    
    def __init__(self, allowed_tables=None):
        self.allowed_tables = allowed_tables or ['repairs_enriched', 'PM']
    
    def validate(self, sql):
        """ตรวจสอบ SQL ทั้งหมด Returns: (is_valid: bool, error_message: str)"""
        if not sql or not isinstance(sql, str):
            return False, "SQL is empty or invalid type"
        
        for op in self.FORBIDDEN_OPERATIONS:
            if re.search(rf'\b{op}\b', sql.upper()):
                return False, f"Forbidden operation: {op}"
        
        for pattern in self.DANGEROUS_PATTERNS:
            if re.search(pattern, sql, re.IGNORECASE):
                return False, f"Dangerous pattern detected"
        
        if not re.search(r'\bSELECT\b', sql, re.IGNORECASE):
            return False, "SQL must contain SELECT statement"
        
        if not re.search(r'\bFROM\b', sql, re.IGNORECASE):
            return False, "SQL must contain FROM clause"
        
        return True, ""
    
    def sanitize(self, sql):
        """ทำความสะอาด SQL"""
        sql = re.sub(r'\s+', ' ', sql).strip()
        sql = sql.rstrip(';')
        return sql
    
    def optimize(self, sql):
        """Optimize SQL query"""
        sql = re.sub(r'\bWHERE\s+1\s*=\s*1\s+AND\s+', 'WHERE ', sql, flags=re.IGNORECASE)
        return sql

_validator_instance = None

def get_validator(allowed_tables=None):
    """Get or create global SQLValidator instance"""
    global _validator_instance
    if _validator_instance is None or allowed_tables is not None:
        _validator_instance = SQLValidator(allowed_tables)
    return _validator_instance

# --- IntentDetector: Understanding User Intent ---
class IntentDetector:
    """ตรวจหาเจตนาจากคำถาม"""
    
    def __init__(self):
        self.intents = {
            'list': ['มีอะไรบ้าง', 'แสดง', 'ดู', 'list', 'show'],
            'count': ['กี่', 'จำนวน', 'นับ', 'count', 'how many'],
            'top': ['มากที่สุด', 'บ่อย', 'สูงสุด', 'top', 'most', 'highest'],
            'bottom': ['น้อยที่สุด', 'ต่ำสุด', 'น้อย', 'bottom', 'least', 'lowest'],
            'best': ['ดีที่สุด', 'เก่ง', 'ไวที่สุด', 'เร็วที่สุด', 'best', 'fastest'],
            'worst': ['แย่ที่สุด', 'ช้าที่สุด', 'นานที่สุด', 'worst', 'slowest'],
        }
        
        self.metrics = {
            'response_time': ['ไปซ่อม', 'ตอบสนอง', 'response', 'เรียก', 'มาถึง', 'ไปถึง', 'ใช้เวลาในการเรียก'],
            'repair_time': ['ซ่อม', 'แก้ไข', 'repair', 'fix', 'ซ่อมนาน', 'ซ่อมไว'],
        }
    
    def detect_line_name(self, user_msg: str, valid_lines: List[str]) -> Optional[str]:
        """ตรวจหาชื่อ Line จากคำถาม (case-insensitive)"""
        msg_lower = user_msg.lower()
        
        for line in valid_lines:
            if line.lower() in msg_lower:
                return line
        
        for line in valid_lines:
            line_clean = re.sub(r'[_\-]', '', line.lower())
            msg_clean = re.sub(r'[_\-]', '', msg_lower)
            if line_clean in msg_clean:
                return line
        
        return None

_detector_instance = None

def get_intent_detector():
    """Get or create global IntentDetector instance"""
    global _detector_instance
    if _detector_instance is None:
        _detector_instance = IntentDetector()
    return _detector_instance

# =====================================================
# END OPTIMIZATION MODULES
# =====================================================

# --- Entity Matching Sync State ---
LAST_SYNC_ROW_COUNT = 0

def _get_table_row_count(table_name: str) -> int:
    """Gets the current row count of a table."""
    try:
        conn = sqlite3.connect(WORK_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        logger.error(f"Error getting row count for {table_name}: {e}")
        return 0

def now_bangkok_str() -> str:
    """คืนเวลาปัจจุบันในโซนไทย (Asia/Bangkok) เป็นสตริง YYYY-MM-DD HH:MM:SS"""
    return datetime.now(BANGKOK_TZ).strftime("%Y-%m-%d %H:%M:%S")

def timestamp_to_bangkok_str(utc_timestamp: float) -> str:
    """แปลง Unix timestamp เป็นเวลาประเทศไทย (สำหรับแสดงให้ผู้ใช้)"""
    return datetime.fromtimestamp(utc_timestamp, tz=BANGKOK_TZ).strftime("%Y-%m-%d %H:%M:%S")

# =====================================================
# FastAPI Application Setup
# =====================================================

# NEW IMPORTS FOR VECTOR SEARCH
try:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    VECTOR_ENABLED = True
except ImportError:
    VECTOR_ENABLED = False
    print("Warning: sentence-transformers not found. Falling back to Fuzzy match.")

# --- 1. CONFIGURATION (constants imported from config.py) ---
setup_logging()
logger = get_logger("MAIN")


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


APP_ROLE = (os.getenv("APP_ROLE", "api") or "api").strip().lower()
STARTUP_DATA_SYNC_ENABLED = _env_flag("STARTUP_DATA_SYNC_ENABLED", True)
REALTIME_SYNC_ENABLED = _env_flag("REALTIME_SYNC_ENABLED", True)
STARTUP_EMBEDDING_CHECK_ENABLED = _env_flag("STARTUP_EMBEDDING_CHECK_ENABLED", True)
API_BACKGROUND_REFRESH_ENABLED = _env_flag("API_BACKGROUND_REFRESH_ENABLED", APP_ROLE == "api")

@asynccontextmanager
async def lifespan(app_instance):
    """Startup: โหลดข้อมูลและเช็ค model; Shutdown: ไม่ทำพิเศษ"""
    logger.info("🚀 System Starting (Ultimate AI+BI Edition)...")
    load_and_enrich_data(force=False)
    ensure_pm_synced()  # ให้ตาราง PM มีใน work DB ตั้งแต่เริ่มต้น
    threading.Thread(target=check_model).start()
    
    # เริ่ม Real-time Data Sync
    try:
        from services.realtime_data_sync import setup_realtime_sync
        setup_realtime_sync()
        logger.info("🔄 Real-time data sync enabled")
    except ImportError:
        logger.warning("⚠️  Real-time sync not available (missing watchdog package)")
    except Exception as e:
        logger.error(f"❌ Failed to setup real-time sync: {e}")
    
    # Model Manager ถูกลบแล้ว - ให้ Ollama จัดการเอง
    logger.info("🚀 Using direct Ollama API calls - no model management")
    
    # Auto-generate embeddings if needed (threshold: 500 new rows) - NON-BLOCKING
    try:
        from services.embeddings import auto_generate_embeddings_if_needed
        logger.info("🔍 Checking for new repair logs...")
        
        # ใช้ daemon thread เพื่อไม่ block startup
        embedding_thread = threading.Thread(
            target=auto_generate_embeddings_if_needed,
            kwargs={"threshold": 10},  # ลด threshold เป็น 10 rows
            daemon=True  # ไม่ block shutdown
        )
        embedding_thread.start()
        logger.info("🚀 Embedding generation started in background (non-blocking)")
    except Exception as e:
        logger.warning(f"⚠️  Auto-embedding check failed: {e}")
    
    yield
    # Shutdown: ทำความสะอาด real-time sync
    try:
        from services.realtime_data_sync import cleanup_realtime_sync
        cleanup_realtime_sync()
        logger.info("🛑 Real-time sync cleaned up")
    except Exception as e:
        logger.error(f"❌ Failed to cleanup real-time sync: {e}")

@asynccontextmanager
async def configured_lifespan(app_instance):
    """Role-aware startup/shutdown so API and sync worker can be split safely."""
    logger.info("System Starting (Ultimate AI+BI Edition)... role=%s", APP_ROLE)
    if STARTUP_DATA_SYNC_ENABLED:
        load_and_enrich_data(force=False)
        ensure_pm_synced()
    else:
        logger.info("Startup data sync disabled by environment")

    threading.Thread(target=check_model).start()

    if REALTIME_SYNC_ENABLED:
        try:
            from services.realtime_data_sync import setup_realtime_sync

            setup_realtime_sync()
            logger.info("Real-time data sync enabled")
        except ImportError:
            logger.warning("Real-time sync not available (missing watchdog package)")
        except Exception as e:
            logger.error("Failed to setup real-time sync: %s", e)
    else:
        logger.info("Real-time data sync disabled by environment")

    logger.info("Using direct Ollama API calls - no model management")

    if STARTUP_EMBEDDING_CHECK_ENABLED:
        try:
            from services.embeddings import auto_generate_embeddings_if_needed

            logger.info("Checking for new repair logs...")
            embedding_thread = threading.Thread(
                target=auto_generate_embeddings_if_needed,
                kwargs={"threshold": 10},
                daemon=True,
            )
            embedding_thread.start()
            logger.info("Embedding generation started in background (non-blocking)")
        except Exception as e:
            logger.warning("Auto-embedding check failed: %s", e)
    else:
        logger.info("Startup embedding check disabled by environment")

    yield

    if REALTIME_SYNC_ENABLED:
        try:
            from services.realtime_data_sync import cleanup_realtime_sync

            cleanup_realtime_sync()
            logger.info("Real-time sync cleaned up")
        except Exception as e:
            logger.error("Failed to cleanup real-time sync: %s", e)


app = FastAPI(
    lifespan=configured_lifespan,
    title="Repair Chatbot API",
    description="AI-powered repair assistance system",
    version="1.0.0"
)

# Configure proper UTF-8 JSON response
from fastapi.responses import Response
from fastapi.encoders import jsonable_encoder

class UTF8JSONResponse(Response):
    media_type = "application/json; charset=utf-8"
    
    def __init__(self, content=None, status_code=200, headers=None, **kwargs):
        if content is not None:
            content = json.dumps(
                jsonable_encoder(content),
                ensure_ascii=False,
                allow_nan=False,
                indent=None,
                separators=(",", ":"),
            ).encode("utf-8")
        super().__init__(content, status_code, headers, **kwargs)

# Override default response class
app.default_response_class = UTF8JSONResponse

# Replace all JSONResponse imports
from fastapi.responses import JSONResponse as _OriginalJSONResponse
JSONResponse = UTF8JSONResponse

# Short-lived in-memory cache for expensive dashboard endpoints
_TECH_DASH_CACHE: Dict[str, Any] = {"ts": 0.0, "payload": None}
_TECH_DASH_CACHE_TTL_SEC = 45
_TECH_DASH_CACHE_LOCK = threading.Lock()

# Configure logging to reduce noise from health checks
from fastapi import Request

# Custom logging filter to reduce health check noise
class HealthCheckFilter(logging.Filter):
    def filter(self, record):
        # Skip logging for health check and data-status endpoints
        if hasattr(record, 'args') and record.args:
            message = str(record.args[0]) if record.args else str(record.getMessage())
            if any(endpoint in message for endpoint in ['/health', '/api/data-status']):
                return False
        return True

# Apply filter to uvicorn access logger
uvicorn_logger = logging.getLogger("uvicorn.access")
uvicorn_logger.addFilter(HealthCheckFilter())

# CORS: ให้ frontend เรียก API ได้แม้เปิดจาก origin อื่น (หรือ port อื่น)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Type"],
)

# Force UTF-8 encoding for all responses
@app.middleware("http")
async def force_utf8_encoding(request: Request, call_next):
    response = await call_next(request)
    
    # Force UTF-8 for all JSON responses
    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type:
        response.headers["content-type"] = "application/json; charset=utf-8"
    
    # Add UTF-8 headers for text responses
    if "text/" in content_type and "charset" not in content_type:
        response.headers["content-type"] = f"{content_type}; charset=utf-8"
    
    return response


# --- All constants (DB paths, Ollama, Timeouts, Models) → imported from config.py ---
# FAILED_LOG_FILE, OLLAMA_HOST, OLLAMA_GENERATE_URL, OLLAMA_PULL_URL, OLLAMA_TAGS_URL,
# MODEL_NAME, CHAT_MODEL, ROUTER_TIMEOUT, _BASE_DIR, STATIC_VERSION,
# SOURCE_DB_PATH, WORK_DB_PATH, PM2025_DB_PATH, QA_LOG_FILE,
# TECH_LIST_JSON_PATH, BANGKOK_TZ, OLLAMA_REQUEST_TIMEOUT, OLLAMA_TAGS_TIMEOUT,
# OLLAMA_PULL_TIMEOUT, CHAT_FALLBACK_RESPONSE → all from config.py

# Frontend static files (served from ../frontend/static in development, /app/frontend/static in Docker)
_FRONTEND_DIR = os.path.join("/app", "frontend", "static") if os.path.exists("/app/frontend/static") else os.path.join(os.path.dirname(_BASE_DIR), "frontend", "static")
app.mount("/static", StaticFiles(directory=_FRONTEND_DIR), name="static")

# Add middleware to handle proxy paths for static files
@app.middleware("http")
async def proxy_static_middleware(request: Request, call_next):
    """Handle proxy paths like /ai/static/* by redirecting to /static/*"""
    path = request.url.path
    original_path = path
    
    # Handle /ai/ai-static/* -> /static/*
    if path.startswith("/ai/ai-static/"):
        new_path = path.replace("/ai/ai-static/", "/static/")
        request.scope["path"] = new_path
        request.scope["raw_path"] = new_path.encode()
        logger.info(f"Proxy redirect: {original_path} -> {new_path}")

    # Handle /ai-static/* -> /static/*
    elif path.startswith("/ai-static/"):
        new_path = path.replace("/ai-static/", "/static/")
        request.scope["path"] = new_path
        request.scope["raw_path"] = new_path.encode()
        logger.info(f"Proxy redirect: {original_path} -> {new_path}")

    # Handle /ai/static/* -> /static/*
    elif path.startswith("/ai/static/"):
        new_path = path.replace("/ai/static/", "/static/")
        request.scope["path"] = new_path
        request.scope["raw_path"] = new_path.encode()
        logger.info(f"Proxy redirect: {original_path} -> {new_path}")
    
    # Handle /ai/api/* -> /api/*
    elif path.startswith("/ai/api/"):
        new_path = path.replace("/ai/api/", "/api/")
        request.scope["path"] = new_path
        request.scope["raw_path"] = new_path.encode()
        logger.info(f"Proxy redirect: {original_path} -> {new_path}")
    
    # Handle /ai/ -> /
    elif path == "/ai/" or path == "/ai":
        request.scope["path"] = "/"
        request.scope["raw_path"] = b"/"
        logger.info(f"Proxy redirect: {original_path} -> /")
    
    response = await call_next(request)
    return response

# --- TTS Endpoint ---

@app.get("/api/tts")
def get_tts_audio(text: str):
    if not text:
        return Response(status_code=400)
    
    try:
        audio_rel_path = tts_manager().generate_speech(text)
        if not audio_rel_path:
            return JSONResponse({"status": "error", "message": "TTS generation failed"}, status_code=500)
        
        # Return physical file
        # audio_rel_path is something like "/static/tts_cache/hash.wav"
        filename = audio_rel_path.split("/")[-1]
        full_path = os.path.join(_FRONTEND_DIR, "tts_cache", filename)
        
        if os.path.exists(full_path):
            return FileResponse(full_path, media_type="audio/wav")
        return JSONResponse({"status": "error", "message": f"File not found at {full_path}"}, status_code=404)
    except Exception as e:
        logger.error(f"TTS API Error: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.get("/api/tts/speakers")
def get_tts_speakers():
    """Get available TTS speakers"""
    try:
        speakers = tts_manager().get_available_speakers()
        return {"speakers": speakers, "model": tts_manager().model_name}
    except Exception as e:
        logger.error(f"TTS Speakers API Error: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

_assets_dir = os.path.join(_BASE_DIR, "assets")
if os.path.isdir(_assets_dir):
    app.mount("/assets", StaticFiles(directory=_assets_dir), name="assets")

# Global Cache
db_context: Dict[str, Any] = {
    "suggestions": [],
    "lines": [],
    "processes": [],
    "techs": [],
    "pm_task_names": [],
    "shared_line_pm": [],
    "line_pm_pairs": [],   # [(line_value, pm_value), ...] จาก line_pm_mapping.json — ชื่อต่างกันแต่ความหมายเดียวกัน
    "lines_sample": "",
    "all_teams": "",
    "schema_str": ""
}
last_source_mtime = 0.0
last_pm_sync_mtime = 0.0
_DATA_REFRESH_LOCK = threading.Lock()
_BACKGROUND_REFRESH_LOCK = threading.Lock()
_background_refresh_thread: Optional[threading.Thread] = None

# AI Model Cache - ไม่ใช้แล้ว เปลี่ยนเป็นใช้ services/embeddings.py

# ChatRequest → imported from models.py

# ❌ ลบ FeedbackRequest และ API Feedback ออกเพื่อความปลอดภัย
# เหตุผล: ป้องกัน Bad Data ปนเปื้อนเข้าไปใน Memory ทำให้บอทเพี้ยน
# 
# 🔒 PRODUCTION SAFETY MODE:
# - ระบบจะใช้เฉพาะ Memory ที่ Developer เป็นคนป้อนผ่าน sql_memory.json เท่านั้น
# - ไม่มีการเรียนรู้อัตโนมัติจาก User feedback
# - ป้องกันการปนเปื้อนของ Bad Data ในระบบ AI

# --- 2. DATA PROCESSING (ENRICHMENT & VIEW CREATION) ---

def get_shift(dt_obj) -> str:
    if pd.isnull(dt_obj): return "Overtime"
    t = dt_obj.time()
    # กะเช้า: 08:00 - 17:05
    if dt_time(8, 0) <= t <= dt_time(17, 5): return "Day"
    # Bridge/Transition: 17:06 - 19:59 -> classify as Day (ongoing work)
    elif dt_time(17, 6) <= t <= dt_time(19, 59): return "Day"
    # กะดึก: 20:00 - 07:59
    elif t >= dt_time(20, 0) or t <= dt_time(7, 59): return "Night"
    return "Overtime"

def get_shift_date(dt_obj) -> str:
    """
    คำนวณวันที่ของกะงาน (Shift_Date) สำหรับแก้ไขปัญหากะดึกข้ามวัน
    
    Logic:
    - กะเช้า/กะบ่าย: ใช้วันที่เดียวกัน
    - กะดึก 20:00-23:59: ใช้วันที่เดียวกัน  
    - กะดึก 00:00-07:59: ใช้วันก่อนหน้า (เพราะเป็นกะดึกของวันก่อน)
    """
    if pd.isnull(dt_obj): return None
    
    t = dt_obj.time()
    current_date = dt_obj.date()
    
    # กะดึกช่วงหลังเที่ยงคืน (00:00-07:59) → เป็นกะดึกของวันก่อนหน้า
    if t <= dt_time(7, 59):
        shift_date = current_date - timedelta(days=1)
        return shift_date.strftime('%Y-%m-%d')
    
    # กรณีอื่นๆ ใช้วันที่เดียวกัน
    return current_date.strftime('%Y-%m-%d')

def create_summary_view(conn: sqlite3.Connection) -> None:
    # Create View to reduce Query complexity
    try:
        conn.execute("DROP VIEW IF EXISTS daily_summary")
        sql_view = """
        CREATE VIEW daily_summary AS
        SELECT 
            Date, 
            strftime('%Y-%m', Date) as Month,
            strftime('%Y', Date) as Year,
            Line, 
            Process, 
            Team, 
            Shift,
            COUNT(*) as RepairCount,
            SUM(RepairMinutes) as TotalDowntime
        FROM repairs_enriched
        GROUP BY Date, Line, Process, Team, Shift;
        """
        conn.execute(sql_view)
        logger.info("View 'daily_summary' created successfully.")
    except Exception as e:
        logger.error(f"Failed to create View: {e}")


def _file_mtime_or_zero(path: str) -> float:
    try:
        if not path or not os.path.exists(path):
            return 0.0
        return float(os.path.getmtime(path))
    except OSError:
        return 0.0


def _normalize_duration_minutes(series: pd.Series, *, label: str, convert_hours: bool = False) -> pd.Series:
    minutes = pd.to_numeric(series, errors="coerce")
    if convert_hours:
        minutes = minutes * 60

    negative_mask = minutes < 0
    if bool(negative_mask.any()):
        logger.warning(
            "%s contained %s negative values; keeping those rows as NULL",
            label,
            int(negative_mask.sum()),
        )
        minutes = minutes.mask(negative_mask)

    return minutes


def _series_sum_or_zero(series: pd.Series) -> float:
    total = series.sum(min_count=1)
    if pd.isna(total):
        return 0.0
    return float(total)


def _round_or_zero(value: Any, digits: int = 1) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return round(float(value), digits)


def _needs_source_refresh() -> bool:
    current_mtime = _file_mtime_or_zero(SOURCE_DB_PATH)
    if current_mtime <= 0:
        return False
    if not os.path.exists(WORK_DB_PATH):
        return True
    return current_mtime > last_source_mtime


def _needs_pm_sync() -> bool:
    current_mtime = _file_mtime_or_zero(PM2025_DB_PATH)
    if current_mtime <= 0:
        return False
    if not os.path.exists(WORK_DB_PATH):
        return True
    return current_mtime > last_pm_sync_mtime


def schedule_background_data_refresh_if_stale() -> bool:
    global _background_refresh_thread

    if not API_BACKGROUND_REFRESH_ENABLED:
        return False

    if not os.path.exists(WORK_DB_PATH):
        return False

    refresh_repairs = _needs_source_refresh()
    refresh_pm = _needs_pm_sync()
    if not (refresh_repairs or refresh_pm):
        return False

    with _BACKGROUND_REFRESH_LOCK:
        if _background_refresh_thread is not None and _background_refresh_thread.is_alive():
            return False

        def _run_refresh(should_refresh_repairs: bool, should_refresh_pm: bool) -> None:
            try:
                if should_refresh_repairs:
                    load_and_enrich_data(force=True)
                if should_refresh_pm:
                    ensure_pm_synced(force=True)
            except Exception:
                logger.exception("Background data refresh failed")

        _background_refresh_thread = threading.Thread(
            target=_run_refresh,
            args=(refresh_repairs, refresh_pm),
            name="background-data-refresh",
            daemon=True,
        )
        _background_refresh_thread.start()

    logger.info(
        "Scheduled background data refresh (repairs=%s, pm=%s)",
        refresh_repairs,
        refresh_pm,
    )
    return True

def load_and_enrich_data(force: bool = False) -> bool:
    global last_source_mtime
    if not os.path.exists(SOURCE_DB_PATH):
        return False

    try:
        current_mtime = _file_mtime_or_zero(SOURCE_DB_PATH)
        # ⚡ OPTIMIZATION: ไม่ต้องโหลดใหม่ถ้าไฟล์ไม่เปลี่ยน
        if not force and current_mtime <= last_source_mtime and os.path.exists(WORK_DB_PATH):
            logger.info("📊 Data already up-to-date, skipping reload")
            return True

        logger.info(f"Processing Data from {SOURCE_DB_PATH}...")
        conn_src = sqlite3.connect(f"file:{SOURCE_DB_PATH}?mode=ro", uri=True)
        try:
            cursor = conn_src.cursor()
            table_name = cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1").fetchone()
            if not table_name:
                return False
            df = pd.read_sql_query(f"SELECT * FROM {table_name[0]}", conn_src)
        finally:
            conn_src.close()

        # Data Cleaning & Enhancement
        date_col = next((c for c in df.columns if 'time' in c.lower() or 'date' in c.lower()), None)
        if date_col:
            df['Date_Obj'] = pd.to_datetime(df[date_col], errors='coerce')
            df['Date'] = df['Date_Obj'].dt.strftime('%Y-%m-%d')
            df['Shift'] = df['Date_Obj'].apply(get_shift)
            # เพิ่ม Shift_Date สำหรับแก้ไขปัญหากะดึกข้ามวัน
            df['Shift_Date'] = df['Date_Obj'].apply(get_shift_date)

        # 🕐 Smart Time Column Detection & Response Time Calculation
        # หาคอลัมน์เวลาต่างๆ
        call_time_col = None
        start_time_col = None
        end_time_col = None
        
        # หาคอลัมน์เวลาเรียก (Call Time)
        call_keywords = ['call', 'request', 'report', 'notify', 'alert']
        for keyword in call_keywords:
            found = next((c for c in df.columns if keyword in c.lower() and ('time' in c.lower() or 'date' in c.lower())), None)
            if found:
                call_time_col = found
                break
        
        # หาคอลัมน์เวลาเริ่ม (Start Time)
        start_keywords = ['start', 'begin', 'arrive', 'response']
        for keyword in start_keywords:
            found = next((c for c in df.columns if keyword in c.lower() and ('time' in c.lower() or 'date' in c.lower())), None)
            if found:
                start_time_col = found
                break
        
        # หาคอลัมน์เวลาเสร็จ (End Time)
        end_keywords = ['end', 'finish', 'complete', 'done', 'close']
        for keyword in end_keywords:
            found = next((c for c in df.columns if keyword in c.lower() and ('time' in c.lower() or 'date' in c.lower())), None)
            if found:
                end_time_col = found
                break

        # Calculate Response Time (เวลาตอบสนอง)
        df['ResponseMinutes'] = np.nan
        if call_time_col and start_time_col:
            try:
                df['CallTime'] = pd.to_datetime(df[call_time_col], errors='coerce')
                df['StartTime'] = pd.to_datetime(df[start_time_col], errors='coerce')
                
                # คำนวณ Response Time (นาที)
                response_minutes = (df['StartTime'] - df['CallTime']).dt.total_seconds() / 60
                df['ResponseMinutes'] = response_minutes
                
                df['ResponseMinutes'] = _normalize_duration_minutes(
                    response_minutes,
                    label=f"ResponseMinutes<{call_time_col}->{start_time_col}>",
                )
                logger.info(
                    "Calculated ResponseMinutes from %s -> %s (%s/%s usable rows)",
                    call_time_col,
                    start_time_col,
                    int(df['ResponseMinutes'].notna().sum()),
                    len(df),
                )
            except Exception as e:
                logger.warning(f"Failed to calculate ResponseMinutes: {e}")
                df['ResponseMinutes'] = np.nan
        else:
            logger.warning("No call/start time columns found; keeping ResponseMinutes as NULL")

        # Smart Repair Minutes (ปรับปรุงใหม่)
        keywords_priority = [
            ['repair', 'min'], ['time', 'min'], ['repair', 'time'], 
            ['downtime'], ['total', 'time'], ['usage'], ['min'], ['hour'],
            ['duration'], ['elapsed'], ['spent']
        ]
        repair_col = None
        for kws in keywords_priority:
            found = next((c for c in df.columns if all(k in c.lower() for k in kws)), None)
            if found:
                repair_col = found
                break
        
        df['RepairMinutes'] = np.nan
        if repair_col:
            repair_uses_hours = 'hour' in repair_col.lower() and 'min' not in repair_col.lower()
            df['RepairMinutes'] = _normalize_duration_minutes(
                df[repair_col],
                label=f"RepairMinutes<{repair_col}>",
                convert_hours=repair_uses_hours,
            )
            logger.info(
                "Found repair time column: %s (%s/%s usable rows)",
                repair_col,
                int(df['RepairMinutes'].notna().sum()),
                len(df),
            )
        elif start_time_col and end_time_col:
            # คำนวณจากเวลาเริ่ม-เสร็จ
            try:
                df['StartTime'] = pd.to_datetime(df[start_time_col], errors='coerce')
                df['EndTime'] = pd.to_datetime(df[end_time_col], errors='coerce')
                repair_minutes = (df['EndTime'] - df['StartTime']).dt.total_seconds() / 60
                df['RepairMinutes'] = _normalize_duration_minutes(
                    repair_minutes,
                    label=f"RepairMinutes<{start_time_col}->{end_time_col}>",
                )
                logger.info(
                    "Calculated RepairMinutes from %s -> %s (%s/%s usable rows)",
                    start_time_col,
                    end_time_col,
                    int(df['RepairMinutes'].notna().sum()),
                    len(df),
                )
            except Exception as e:
                logger.warning(f"Failed to calculate RepairMinutes: {e}")
                df['RepairMinutes'] = np.nan
        else:
            logger.warning("No repair time column found; keeping RepairMinutes as NULL")

        # Normalize Tech column: split comma-separated techs into separate rows
        tech_col = next((c for c in df.columns if 'tech' in c.lower()), None)
        if tech_col:
            logger.info(f"Normalizing Tech column: {tech_col}")
            df_expanded = []
            for idx, row in df.iterrows():
                tech_value = row[tech_col]
                if pd.isna(tech_value) or str(tech_value).strip() == '':
                    df_expanded.append(row)
                    continue
                techs = [t.strip() for t in str(tech_value).split(',') if t.strip()]
                if len(techs) <= 1:
                    df_expanded.append(row)
                    continue
                for tech in techs:
                    new_row = row.copy()
                    new_row[tech_col] = tech
                    df_expanded.append(new_row)
            df = pd.DataFrame(df_expanded).reset_index(drop=True)
            logger.info(f"Expanded to {len(df)} rows from Tech normalization")

        conn_work = sqlite3.connect(WORK_DB_PATH)
        try:
            df.to_sql('repairs_enriched', conn_work, if_exists='replace', index=False)
            
            # สร้าง View ทันทีหลังโหลดข้อมูล
            create_summary_view(conn_work)
            
        finally:
            conn_work.close()
        
        last_source_mtime = current_mtime
        load_metadata()
        return True
    except Exception as e:
        logger.error(f"Enrich Error: {e}")
        return False

def ensure_pm_synced(force: bool = False) -> bool:
    """ sync ตาราง PM จาก PM2025.db เข้า work DB (อ่าน PM2025 แบบ read-only เมื่อข้อมูล PM เปลี่ยน) """
    global last_pm_sync_mtime
    if not os.path.exists(PM2025_DB_PATH):
        return False
    current_mtime = _file_mtime_or_zero(PM2025_DB_PATH)
    if not force and current_mtime <= last_pm_sync_mtime and os.path.exists(WORK_DB_PATH):
        return True
    # ถ้า work.db ยังไม่ถูกสร้าง ให้สร้างไฟล์เปล่าไว้ก่อน (เพื่อให้ sync ได้เสมอ)
    if not os.path.exists(WORK_DB_PATH):
        try:
            sqlite3.connect(WORK_DB_PATH).close()
        except Exception:
            return False
    try:
        with _pm_db_connection_readonly(PM2025_DB_PATH) as conn_pm:
            df_pm = pd.read_sql_query("SELECT * FROM PM", conn_pm)
        if df_pm.empty:
            return False
        
        # 🔥 สร้าง column PM_real_date (วันที่จริงที่จะทำ PM - เข้มงวดที่สุด)
        # Logic: 
        # 1. ถ้ามีการย้าย (Description มี "ย้าย"/"เลื่อน") → ใช้วันที่ที่ย้ายไป
        # 2. ถ้าไม่มีการย้าย → ใช้ Due date
        # 3. แปลงเป็น YYYY-MM-DD เสมอ
        # 4. ตัดแถวซ้ำออก (dedupe)
        
        if "Due date" in df_pm.columns:
            try:
                has_description = "Description" in df_pm.columns

                def _normalize_due_date_to_ymd(val):
                    """แปลง Due date เป็นรูปแบบ YYYY-MM-DD เดียวกันทั้งหมด (สำหรับ column Due_date_ymd)"""
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        return None
                    s = str(val).strip()
                    if not s:
                        return None
                    for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y']:
                        try:
                            parsed = pd.to_datetime(s, format=fmt, errors='raise')
                            return parsed.strftime('%Y-%m-%d')
                        except Exception:
                            continue
                    try:
                        parsed = pd.to_datetime(s, errors='coerce')
                        if pd.notna(parsed):
                            return parsed.strftime('%Y-%m-%d')
                    except Exception:
                        pass
                    return None

                df_pm["Due_date_ymd"] = df_pm["Due date"].apply(_normalize_due_date_to_ymd)
                
                def extract_pm_real_date(row):
                    """
                    สร้าง PM_real_date = วันที่จริงที่จะทำ PM
                    - ถ้ามีการย้าย: ใช้วันที่ที่ย้ายไป (จาก Description)
                    - ถ้าไม่มีการย้าย: ใช้ Due date
                    - แปลงเป็น YYYY-MM-DD เสมอ
                    """
                    desc = str(row.get("Description", "")) if has_description else ""
                    due_date = row.get("Due date")
                    
                    # เช็คว่ามีการย้ายหรือไม่
                    is_postponed = "ย้าย" in desc or "เลื่อน" in desc
                    
                    # ถ้ามีการย้าย ให้หาวันที่ที่ย้ายไป
                    if is_postponed:
                        # รูปแบบ: "ย้ายจากวันที่ 17-01-2026 เป็น 14-02-2026"
                        # หรือ "เลื่อนไป 18-02-2026"
                        
                        # Pattern 1: "เป็น DD-MM-YYYY"
                        match = re.search(r'เป็น\s*(\d{1,2}-\d{1,2}-\d{4})', desc)
                        if match:
                            postponed_date_str = match.group(1)
                            parts = postponed_date_str.split('-')
                            if len(parts) == 3:
                                day, month, year = parts
                                return f"{year}-{month.zfill(2)}-{day.zfill(2)}", True
                        
                        # Pattern 2: "ไป DD-MM-YYYY"
                        match = re.search(r'ไป\s*(\d{1,2}-\d{1,2}-\d{4})', desc)
                        if match:
                            postponed_date_str = match.group(1)
                            parts = postponed_date_str.split('-')
                            if len(parts) == 3:
                                day, month, year = parts
                                return f"{year}-{month.zfill(2)}-{day.zfill(2)}", True
                    
                    # ถ้าไม่มีการย้ายหรือหาไม่เจอ ใช้ Due date
                    if not due_date or pd.isna(due_date):
                        return None, False
                    
                    due_str = str(due_date).strip()
                    
                    # ลองแปลงหลายรูปแบบ (เรียงตามความน่าจะเป็น)
                    formats = [
                        '%m/%d/%Y',      # 02/14/2026 (รูปแบบที่เห็นในตาราง)
                        '%Y-%m-%d',      # 2026-02-14 (รูปแบบมาตรฐาน)
                        '%d-%m-%Y',      # 14-02-2026
                        '%d/%m/%Y',      # 14/02/2026
                    ]
                    
                    for fmt in formats:
                        try:
                            parsed = pd.to_datetime(due_str, format=fmt, errors='raise')
                            return parsed.strftime('%Y-%m-%d'), False
                        except:
                            continue
                    
                    # ถ้าไม่ตรงรูปแบบไหนเลย ใช้ pandas auto-parse
                    try:
                        parsed = pd.to_datetime(due_str, errors='coerce')
                        if pd.notna(parsed):
                            return parsed.strftime('%Y-%m-%d'), False
                    except:
                        pass
                    
                    # ถ้าแปลงไม่ได้เลย เก็บค่าเดิม
                    return due_str, False
                
                # Apply ฟังก์ชัน
                df_pm[["PM_real_date", "IsPostponed"]] = df_pm.apply(
                    lambda row: pd.Series(extract_pm_real_date(row)), 
                    axis=1
                )
                
                # 🔥 Dedupe: ตัดแถวซ้ำออก (ถ้า Task Name + PM_real_date ซ้ำ)
                original_count = len(df_pm)
                df_pm = df_pm.drop_duplicates(subset=["Task Name", "PM_real_date"], keep="first")
                deduped_count = original_count - len(df_pm)
                
                if deduped_count > 0:
                    logger.info(f"📋 Deduped PM table: removed {deduped_count} duplicate rows")
                
                logger.info(f"📋 PM_real_date created: {len(df_pm)} rows, {df_pm['IsPostponed'].sum()} postponed")
                
            except Exception as e:
                logger.warning(f"PM_real_date creation failed: {e}")
                df_pm["PM_real_date"] = pd.to_datetime(df_pm["Due date"], errors='coerce').dt.strftime('%Y-%m-%d')
                df_pm["Due_date_ymd"] = pd.to_datetime(df_pm["Due date"], errors='coerce').dt.strftime('%Y-%m-%d')
                df_pm["IsPostponed"] = False
        else:
            # ถ้าไม่มี Due date ก็สร้าง PM_real_date / Due_date_ymd เป็น null
            df_pm["PM_real_date"] = None
            df_pm["Due_date_ymd"] = None
            df_pm["IsPostponed"] = False

        # 🔥 สร้าง column Line จาก Task Name (canonical line สำหรับกรอง ไลน์ไหน ไลน์นั้น)
        _mapping_path = os.path.join(_BASE_DIR, "data", "line_pm_mapping.json")
        pairs = []
        if os.path.isfile(_mapping_path):
            try:
                with open(_mapping_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                pairs = [tuple(p) for p in data.get("pairs", []) if len(p) >= 2]
            except Exception as e:
                logger.warning(f"line_pm_mapping for PM Line column: {e}")
        # เรียงตามความยาว pm_name (ยาวก่อน) เพื่อ match แบบเฉพาะก่อน
        pairs_sorted = sorted(set((a, b) for a, b in pairs if b), key=lambda x: -len(x[1]))

        def _norm_line(s):
            return re.sub(r"\s+", " ", re.sub(r"[_\-]+", " ", (s or "").lower())).strip()

        def _derive_line(task_name):
            """กลุ่มเดียวกันใน line_pm_mapping = อันเดียวกัน (PCB-C, PCB C, PCB_C_PM11 → PCB LINE C)"""
            if not task_name or pd.isna(task_name):
                return ""
            t = str(task_name).strip()
            tn = _norm_line(t)
            for line_alias, pm_name in pairs_sorted:
                # ถ้า Task Name ตรงกับ alias ฝั่งซ้ายหรือฝั่งขวา ในกลุ่มเดียวกัน → ใช้ค่าฝั่ง PM (อันเดียวกัน)
                if tn == _norm_line(line_alias) or tn == _norm_line(pm_name):
                    return pm_name
                if t == pm_name:
                    return pm_name
                if pm_name in t or t in pm_name:
                    return pm_name
            return t

        if "Task Name" in df_pm.columns:
            df_pm["Line"] = df_pm["Task Name"].apply(_derive_line)
            logger.info("📋 PM column Line created from Task Name (line_pm_mapping)")
        else:
            df_pm["Line"] = ""
        
        conn = sqlite3.connect(WORK_DB_PATH)
        try:
            df_pm.to_sql("PM", conn, if_exists="replace", index=False)
            logger.info("📋 PM table synced with PM_date column (read-only source)")
        finally:
            conn.close()
        last_pm_sync_mtime = current_mtime
        return True
    except Exception as e:
        logger.warning(f"ensure_pm_synced: {e}")
        return False

def load_metadata() -> None:
    if not os.path.exists(WORK_DB_PATH): return
    try:
        # โหลดคู่ Line–PM (ชื่อต่างกันแต่ความหมายเดียวกัน เช่น PCB-E ↔ PCB LINE E)
        _mapping_path = os.path.join(_BASE_DIR, "data", "line_pm_mapping.json")
        if os.path.isfile(_mapping_path):
            try:
                with open(_mapping_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                db_context["line_pm_pairs"] = [tuple(p) for p in data.get("pairs", []) if len(p) >= 2]
            except Exception as e:
                logger.warning(f"line_pm_mapping load: {e}")
                db_context["line_pm_pairs"] = []
        else:
            db_context["line_pm_pairs"] = []

        conn = sqlite3.connect(WORK_DB_PATH)
        try:
            cols = [c[1] for c in conn.execute("PRAGMA table_info(repairs_enriched)").fetchall()]
            db_context["schema_str"] = "\n".join([f"- {c} (TEXT/NUMERIC)" for c in cols])
            db_context["columns"] = cols

            # เพิ่มคำสำคัญใหม่เกี่ยวกับเวลา และ Ghost Text
            manual_keywords = [
                "Process", "Problem", "Repair", "Tech", "Line", "Shift", "Day", "Night", 
                "Count", "Total", "Team", "Month", "Year", "Summary",
                "Response", "ResponseMinutes", "RepairMinutes", "เริ่มซ่อม", "ซ่อมเสร็จ",
                "ตอบสนอง", "เข้าหน้างาน", "มาไว", "มาช้า", "เร็ว", "ช้า",
                "ถูกเรียกแล้วไป", "เรียกแล้วมา", "ไปไว", "ไปช้า",
                # Ghost Text Keywords
                "LED Manual", "LED Auto", "SDA", "led manual", "led auto", "sda",
                "LED_M_ASSY", "LED_M_PCB", "LED_A_INS", "SDA2_Insp", "SDA2_Assy",
                "PM"
            ]
            suggestions = set(manual_keywords)
            
            line_col = next((c for c in cols if 'line' in c.lower()), None)
            if line_col:
                lines = pd.read_sql_query(f'SELECT DISTINCT "{line_col}" FROM repairs_enriched', conn)[line_col].dropna().unique()
                lines_list = [str(x) for x in lines if len(str(x)) > 1]
                db_context["lines"] = sorted(lines_list, key=str.lower)
                db_context["lines_sample"] = ", ".join(lines_list[:5])
                suggestions.update(lines_list)
            process_col = next((c for c in cols if c.lower() == 'process'), None)
            if process_col:
                procs = pd.read_sql_query(f'SELECT DISTINCT "{process_col}" FROM repairs_enriched', conn)[process_col].dropna().unique()
                processes_list = [str(x) for x in procs if len(str(x)) > 1]
                db_context["processes"] = sorted(processes_list, key=str.lower)
                suggestions.update(processes_list)
            else:
                db_context["processes"] = []
            tech_col = next((c for c in cols if c.lower() == 'tech'), None)
            if tech_col:
                techs_raw = pd.read_sql_query(f'SELECT DISTINCT "{tech_col}" FROM repairs_enriched WHERE "{tech_col}" IS NOT NULL AND "{tech_col}" != \'\' AND "{tech_col}" != \'Unknown\'', conn)[tech_col].dropna().astype(str)
                TECH_EXCLUDE = {"KUSOL", "POOLAWAT Suphakson", "Support", "SupportA", "อนุวัฒน์ ลบออก", "อนุวัฒน์"}
                techs_list = [t.strip() for t in techs_raw if t.strip() and t.strip() not in TECH_EXCLUDE and not _is_english_only_name(t.strip())]
                # ไม่แสดงชื่อที่ซ้ำแบบ prefix (เช่น มีแค่ "อนุวัฒน์ เพียรหล้า" ไม่แสดง "อนุวัฒน์" แยก)
                def _drop_prefix_dupes(names):
                    names = list(names)
                    return [n for n in names if not any(n != o and o.startswith(n + " ") for o in names)]
                techs_list = _drop_prefix_dupes(techs_list)
                db_context["techs"] = sorted(techs_list, key=str.lower)
            else:
                db_context["techs"] = []
            
            team_col = next((c for c in cols if 'team' in c.lower()), None)
            if team_col:
                teams = pd.read_sql_query(f'SELECT DISTINCT "{team_col}" FROM repairs_enriched', conn)[team_col].dropna().unique()
                db_context["all_teams"] = ", ".join([str(x) for x in teams])
                suggestions.update([str(x) for x in teams if len(str(x)) > 0])

            for col_name in cols:
                if any(k in col_name.lower() for k in ['process', 'problem', 'cause', 'team', 'tech']):
                    items = pd.read_sql_query(f'SELECT DISTINCT "{col_name}" FROM repairs_enriched', conn)[col_name].dropna().unique()
                    suggestions.update([str(x) for x in items if len(str(x)) > 1])

            db_context["suggestions"] = sorted(list(suggestions), key=str.lower)
            # โหลด Task Name จากตาราง PM
            try:
                pm_names = pd.read_sql_query('SELECT DISTINCT "Task Name" FROM PM', conn)["Task Name"].dropna().astype(str)
                db_context["pm_task_names"] = sorted([x.strip() for x in pm_names if len(x.strip()) > 0], key=str.lower)
                suggestions.update(db_context["pm_task_names"])
                # จับคู่: คำที่อยู่ทั้ง Line และ PM = ความหมายเดียวกัน (ระบบจะใช้ตามบริบทคำถาม)
                line_set = set(db_context["lines"])
                pm_set = set(db_context["pm_task_names"])
                db_context["shared_line_pm"] = sorted(line_set & pm_set, key=str.lower)
            except Exception:
                db_context["pm_task_names"] = []
                db_context["shared_line_pm"] = []
        finally:
            conn.close()
        
        logger.info(f"Metadata loaded: {len(cols)} columns, {len(suggestions)} suggestions, {len(db_context.get('pm_task_names', []))} PM task names")
    except Exception as e:
        logger.error(f"Metadata Error: {e}")

# --- HELPER FUNCTIONS FOR TECH MAPPING ---

def load_tech_mapping() -> Dict[str, Any]:
    """Load tech mapping — ใช้ TechDataStore singleton (โหลดไฟล์ครั้งเดียว ไม่อ่านซ้ำทุก request)"""
    store = TechDataStore.instance()
    return {"tech_mapping": store.tech_mapping, "team_assignment": store.team_assignment}

def _normalize_tech_name(name: Optional[str]) -> str:
    """ทำให้ชื่อเทียบกันได้: ตัดช่องว่างหัวท้าย, รวมช่องว่างหลายตัว, Unicode normalize"""
    if not name:
        return ""
    s = str(name).strip()
    s = " ".join(s.split())  # collapse multiple spaces
    try:
        import unicodedata
        s = unicodedata.normalize("NFC", s)
    except Exception:
        pass
    return s

def get_tech_id_from_name(tech_name: Optional[str]) -> Optional[str]:
    """Get Tech ID from technician name. ใช้ exact match ก่อน แล้วค่อย fuzzy match ถ้าไม่ตรง (ชื่อ DB อาจต่างจากในไฟล์เล็กน้อย)"""
    if not tech_name or not str(tech_name).strip():
        return None
    mapping = load_tech_mapping()
    name_to_id = mapping.get("tech_mapping", {})
    raw = str(tech_name).strip()
    normalized = _normalize_tech_name(raw)

    # 1) Exact match (raw และ normalized)
    if raw in name_to_id:
        return name_to_id[raw]
    if normalized in name_to_id:
        return name_to_id[normalized]

    # 2) Normalize keys แล้วเทียบ (เทียบกับ key ที่ normalize แล้ว)
    for key, tech_id in name_to_id.items():
        if _normalize_tech_name(key) == normalized:
            return tech_id

    # 3) Fuzzy match เฉพาะเมื่อชื่อต้นตรงกัน (กันรูปผิดคน เช่น วิชัย ไปขึ้นรูปกุศล)
    try:
        choices = list(name_to_id.keys())
        if not choices:
            return None
        db_first = (normalized.split() or [""])[0]
        match, score = fuzz_process.extractOne(normalized, choices)
        if score >= 92:  # คะแนนสูงมาก
            key_first = (match.split() or [""])[0]
            if db_first == key_first:  # ชื่อต้นต้องตรงกัน
                return name_to_id[match]
    except Exception:
        pass
    return None

def get_tech_display_name(tech_name):
    """คืน 'ชื่อ-สกุล' สำหรับใช้แสดงใน Tech Status (ถ้ามีใน id_to_display_name) ไม่ก็คืนชื่อเดิม"""
    if not tech_name or not str(tech_name).strip():
        return tech_name
    tech_id = get_tech_id_from_name(tech_name)
    if not tech_id:
        return str(tech_name).strip()
    mapping = load_tech_mapping()
    id_to_display = mapping.get("id_to_display_name") or {}
    return id_to_display.get(str(tech_id), str(tech_name).strip())

def get_team_from_tech_id(tech_id):
    """Get team name from Tech ID. คืน None ถ้า tech อยู่ใน excluded_from_teams (ลบออกจากทีมแล้ว).
    เปรียบเทียบเป็น string เสมอ เพื่อกันกรณี tech_id เป็น int แล้วไม่ match กับ list ใน JSON (string) แล้วไป fallback Team A ผิด"""
    if tech_id is None:
        return None
    tech_id_str = str(tech_id).strip()
    if not tech_id_str:
        return None
    mapping = load_tech_mapping()
    excluded_ids = {str(x).strip() for x in mapping.get("excluded_team_ids", [])}
    if tech_id_str in excluded_ids:
        return None
    team_assignment = mapping.get("team_assignment", {})
    for team, tech_ids in team_assignment.items():
        if tech_id_str in {str(x).strip() for x in tech_ids}:
            return team
    return "Team A"  # Default fallback (ไม่ควรเกิดถ้า tech อยู่ใน team_assignment)

def _is_english_only_name(name: Optional[str]) -> bool:
    """ชื่อที่เป็นภาษาอังกฤษล้วน (ไม่มีตัวอักษรไทย) ให้ตัดออก"""
    if not name or not str(name).strip():
        return True
    s = str(name).strip()
    # Thai script range
    for c in s:
        if "\u0E00" <= c <= "\u0E7F":
            return False
    return True







def normalize_line_process_in_sql(sql, user_msg):
    """
    แก้ case-sensitive issue: ถ้า SQL ใช้ Line = 'truck' แต่ใน DB เป็น 'TRUCK'
    จะแก้เป็น LOWER(Line) = LOWER('truck') หรือแทนที่ด้วยชื่อจริงจาก DB
    """
    if not sql:
        return sql
    
    # ดึง valid Line และ Process จาก DB
    try:
        with sqlite3.connect(WORK_DB_PATH) as conn:
            lines_df = pd.read_sql("SELECT DISTINCT Line FROM repairs_enriched WHERE Line IS NOT NULL", conn)
            valid_lines = [str(x).strip() for x in lines_df['Line'].tolist() if x]
            
            processes_df = pd.read_sql("SELECT DISTINCT Process FROM repairs_enriched WHERE Process IS NOT NULL", conn)
            valid_processes = [str(x).strip() for x in processes_df['Process'].tolist() if x]
    except Exception as e:
        logger.warning(f"Could not load valid lines/processes: {e}")
        return sql
    
    # หา Line ที่ใช้ใน SQL
    line_pattern = r"(?:Line|LINE)\s*=\s*['\"]([^'\"]+)['\"]"
    line_matches = re.findall(line_pattern, sql, re.IGNORECASE)
    
    for line_in_sql in line_matches:
        # หา Line ที่ตรงกันจาก DB (case-insensitive)
        correct_line = None
        for valid_line in valid_lines:
            if valid_line.lower() == line_in_sql.lower():
                correct_line = valid_line
                break
        
        # ถ้าหาเจอและต่างกัน → แทนที่ด้วยชื่อจริง
        if correct_line and correct_line != line_in_sql:
            sql = re.sub(
                rf"(Line\s*=\s*['\"]){line_in_sql}(['\"])",
                rf"\1{correct_line}\2",
                sql,
                flags=re.IGNORECASE
            )
            logger.info(f"Normalized Line: '{line_in_sql}' → '{correct_line}'")
    
    # หา Process ที่ใช้ใน SQL
    process_pattern = r"(?:Process|PROCESS)\s*=\s*['\"]([^'\"]+)['\"]"
    process_matches = re.findall(process_pattern, sql, re.IGNORECASE)
    
    for process_in_sql in process_matches:
        # หา Process ที่ตรงกันจาก DB (case-insensitive)
        correct_process = None
        for valid_process in valid_processes:
            if valid_process.lower() == process_in_sql.lower():
                correct_process = valid_process
                break
        
        # ถ้าหาเจอและต่างกัน → แทนที่ด้วยชื่อจริง
        if correct_process and correct_process != process_in_sql:
            sql = re.sub(
                rf"(Process\s*=\s*['\"]){process_in_sql}(['\"])",
                rf"\1{correct_process}\2",
                sql,
                flags=re.IGNORECASE
            )
            logger.info(f"Normalized Process: '{process_in_sql}' → '{correct_process}'")
    
    return sql

def normalize_team_in_sql(sql):
    """
    แมปชื่อทีมใน SQL: Team A / ทีม A / ทีมเอ / A คืออันเดียวกัน → ใช้ Team = 'A' (DB เก็บ A, B, C)
    """
    if not sql:
        return sql
    # รูปแบบ: Team = 'ทีม A' | Team = 'Team A' | Team = 'ทีมเอ' ฯลฯ → Team = 'A'
    team_map = [
        (r"Team\s*=\s*['\"](?:ทีม\s*A|Team\s*A|ทีมเอ|ทีม\s*เอ)['\"]", "Team = 'A'"),
        (r"Team\s*=\s*['\"](?:ทีม\s*B|Team\s*B|ทีมบี|ทีม\s*บี)['\"]", "Team = 'B'"),
        (r"Team\s*=\s*['\"](?:ทีม\s*C|Team\s*C|ทีมซี|ทีม\s*ซี)['\"]", "Team = 'C'"),
    ]
    for pattern, replacement in team_map:
        if re.search(pattern, sql, re.IGNORECASE):
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
            logger.info(f"Normalized Team in SQL → {replacement}")
    return sql

# คำกลุ่มอาการเสีย: ถ้าผู้ใช้ถามคำหนึ่ง ให้ค้นทุกคำในกลุ่ม (ความหมายเดียวกัน)
SYMPTOM_SYNONYMS = ['ดรอป', 'เสีย', 'พัง', 'breakdown']

def expand_literal_symptom_to_like_in_sql(sql):
    """
    In-gen correction: หลังได้ SQL จาก LLM ถ้ามีเงื่อนไข literal อาการ (เช่น = 'เสีย', = 'ดรอป')
    ในคอลัมน์ ปัญหา/สาเหตุ/การแก้ไข ให้ขยายเป็น LIKE %val% และกลุ่มคำพ้อง (เสีย/พัง/ดรอป/breakdown)
    """
    if not sql or 'repairs_enriched' not in sql:
        return sql
    for col in ['ปัญหา', 'สาเหตุ', 'การแก้ไข']:
        esc = re.escape(col)
        # จับ "col" = 'value' หรือ LOWER("col") = 'value' เมื่อ value เป็นคำใน SYMPTOM_SYNONYMS
        for sym in SYMPTOM_SYNONYMS:
            # แทนที่ (AND ) "col" = 'sym' หรือ LOWER("col") = 'sym' ด้วยกลุ่ม LIKE + synonyms
            or_parts = ' OR '.join([f'LOWER("{col}") LIKE \'%{s}%\'' for s in SYMPTOM_SYNONYMS])
            replacement = f'({or_parts})'
            for pat in [
                rf'(\s+AND\s+)(["\']?{esc}["\']?\s*=\s*[\'"]{re.escape(sym)}[\'"])',
                rf'(\s+AND\s+)(LOWER\s*\(\s*["\']?{esc}["\']?\s*\)\s*=\s*[\'"]{re.escape(sym)}[\'"])',
            ]:
                sql = re.sub(pat, r'\1' + replacement, sql, flags=re.IGNORECASE)
    if re.search(r'WHERE\s+AND', sql, re.IGNORECASE):
        sql = re.sub(r'WHERE\s+AND', 'WHERE', sql, flags=re.IGNORECASE)
    return sql

def expand_symptom_synonyms_in_sql(sql):
    """
    เมื่อ SQL มีการค้น LOWER(ปัญหา/สาเหตุ/การแก้ไข) LIKE '%ดรอป%' (หรือ เสีย/พัง/breakdown)
    ให้ขยายเป็น OR ทุกคำในกลุ่ม (ดรอป OR เสีย OR พัง OR breakdown) เพื่อดึงรายการที่เกี่ยวข้องกับอาการเสียทั้งหมด
    """
    if not sql:
        return sql
    symptom_synonyms = SYMPTOM_SYNONYMS
    # จับคู่ LOWER("ปัญหา") LIKE '%ดรอป%' หรือ LOWER(ปัญหา) LIKE '%เสีย%' ฯลฯ
    pattern = r'LOWER\s*\(\s*["\']?(ปัญหา|สาเหตุ|การแก้ไข)["\']?\s*\)\s*LIKE\s*[\'"]([^\'"]*)[\'"]'
    def repl(m):
        col = m.group(1)
        raw = (m.group(2) or "").strip().lower()
        val = raw.replace("%", "").strip()
        if val in symptom_synonyms:
            or_parts = ' OR '.join([f'LOWER("{col}") LIKE \'%{s}%\'' for s in symptom_synonyms])
            return f'({or_parts})'
        return m.group(0)
    new_sql = re.sub(pattern, repl, sql, flags=re.IGNORECASE)
    if new_sql != sql:
        logger.info("Expanded symptom synonyms in SQL (ดรอป/เสีย/พัง/breakdown)")
    return new_sql

def remove_symptom_filter_when_asking_line_process_who(user_msg, sql):
    """
    เมื่อเห็นคำกลุ่ม ดรอป/เสีย/พัง/breakdown และถามแบบ "กี่นาที / อันดับ / ใครซ่อม" = ต้องการรู้ line ไหน process ไหน ใช้เวลากี่นาที ใครเป็นคนซ่อม
    ไม่ใช้คำกลุ่มนี้ไปกรองใน ปัญหา/สาเหตุ/การแก้ไข — ลบเงื่อนไข LIKE ออก ให้ตอบจากงานซ่อมของทีม (แสดง Line, Process, Tech, RepairMinutes)
    """
    if not sql or not user_msg:
        return sql
    msg_lower = user_msg.lower()
    has_symptom_word = any(k in msg_lower for k in SYMPTOM_SYNONYMS)
    ranking_who_keywords = ['กี่นาที', 'อันดับ', 'เยอะสุด', 'ใครซ่อม', 'ใครเป็นคนซ่อม', 'ใครเป็นนซ่อม', 'ใช้เวลา', 'นานสุด']
    is_asking_ranking_or_who = any(k in msg_lower for k in ranking_who_keywords)
    if not has_symptom_word or not is_asking_ranking_or_who:
        return sql
    # ลบเงื่อนไข (LOWER("ปัญหา") LIKE '%...%' OR ... ) และเทียบเท่าสำหรับ สาเหตุ, การแก้ไข
    for col in ['ปัญหา', 'สาเหตุ', 'การแก้ไข']:
        # รูปแบบเก่า: AND LOWER(col) LIKE '%ดรอป%' (หรือคำอื่นในกลุ่ม) — ลบก่อน
        for sym in SYMPTOM_SYNONYMS:
            pat2 = r'\s*AND\s*LOWER\s*\(\s*["\']?' + re.escape(col) + r'["\']?\s*\)\s*LIKE\s*[\'"]%' + re.escape(sym) + r'%[\'"]'
            sql = re.sub(pat2, '', sql, flags=re.IGNORECASE)
        # รูปแบบที่ expand_symptom_synonyms สร้าง: (LOWER("col") LIKE '%ดรอป%' OR ... OR '%breakdown%') — ลบทั้งก้อน
        like_block = r'LOWER\s*\(\s*["\']?' + re.escape(col) + r'["\']?\s*\)\s*LIKE\s*[\'"]%ดรอป%[\'"]\s*OR\s*LOWER\s*\(\s*["\']?' + re.escape(col) + r'["\']?\s*\)\s*LIKE\s*[\'"]%เสีย%[\'"]\s*OR\s*LOWER\s*\(\s*["\']?' + re.escape(col) + r'["\']?\s*\)\s*LIKE\s*[\'"]%พัง%[\'"]\s*OR\s*LOWER\s*\(\s*["\']?' + re.escape(col) + r'["\']?\s*\)\s*LIKE\s*[\'"]%breakdown%[\'"]'
        pattern = r'\s*AND\s*\(' + like_block + r'\s*\)'
        sql = re.sub(pattern, '', sql, flags=re.IGNORECASE)
    if re.search(r'WHERE\s+AND', sql, re.IGNORECASE):
        sql = re.sub(r'WHERE\s+AND', 'WHERE', sql, flags=re.IGNORECASE)
    logger.info("Removed symptom LIKE filter (ถามกี่นาที/อันดับ/ใครซ่อม → แสดง Line, Process, Tech, RepairMinutes)")
    return sql



def ensure_detail_columns_for_cause_question(user_msg, sql):
    """
    ถ้าคำถามมีคำว่า อาการ, สาเหตุ, เพราะอะไร, ปัญหา, แก้ไข → ให้คำตอบมี Line, Process, ปัญหา, สาเหตุ, การแก้ไข, Date
    """
    if not sql or not user_msg:
        return sql
    msg_lower = user_msg.lower()
    detail_keywords = ['อาการ', 'สาเหตุ', 'เพราะอะไร', 'เหตุผล', 'ปัญหา', 'แก้ไข']
    if not any(k in msg_lower for k in detail_keywords):
        return sql
    if 'FROM REPAIRS_ENRICHED' not in sql.upper() and 'FROM repairs_enriched' not in sql:
        return sql
    from_match = re.search(r'\s+FROM\s+repairs_enriched\s+', sql, re.IGNORECASE)
    select_part = sql[: from_match.start()] if from_match else sql
    has_line = re.search(r'\bLine\b', select_part, re.I)
    has_process = re.search(r'\bProcess\b', select_part, re.I)
    has_p = re.search(r'["\']?ปัญหา["\']?', select_part, re.I)
    has_s = re.search(r'["\']?สาเหตุ["\']?', select_part, re.I)
    has_ga = re.search(r'["\']?การแก้ไข["\']?', select_part, re.I)
    has_date = re.search(r'\bDate\b', select_part, re.I)
    has_team = re.search(r'\bTeam\b', select_part, re.I)  # เฉพาะใน SELECT ไม่นับ Team ใน WHERE
    filter_by_team = re.search(r"Team\s*=\s*['\"][^'\"]+['\"]", sql, re.I)
    team_in_msg = 'ทีม' in msg_lower
    # ถ้าถามแบบทีม (ทีม A เครื่องเสียอาการไหนบ้าง) ให้มี column Team ในคำตอบด้วย
    need_team = (bool(filter_by_team) or team_in_msg) and not has_team
    # ถ้ามีครบแล้ว (อย่างน้อย ปัญหา, สาเหตุ และ Process หรือ Line) อาจข้ามได้ แต่ยังเช็ค การแก้ไข กับ Date
    need_more = (not has_p or not has_s or not has_ga or not has_date or (not has_line and not has_process))
    if not need_more and has_p and has_s and has_ga and has_date and not need_team:
        return sql
    sql_upper = sql.upper()
    has_group = 'GROUP BY' in sql_upper
    if not from_match:
        return sql
    insert_pos = from_match.start()
    to_add = []
    if need_team:
        to_add.append('MAX(Team) AS Team' if has_group else 'Team')
    if not has_line:
        to_add.append('MAX(Line) AS Line' if has_group else 'Line')
    if not has_process:
        to_add.append('MAX(Process) AS Process' if has_group else 'Process')
    if not has_p:
        to_add.append('MAX("ปัญหา") AS "ปัญหา"' if has_group else '"ปัญหา"')
    if not has_s:
        to_add.append('MAX("สาเหตุ") AS "สาเหตุ"' if has_group else '"สาเหตุ"')
    if not has_ga:
        to_add.append('MAX("การแก้ไข") AS "การแก้ไข"' if has_group else '"การแก้ไข"')
    if not has_date:
        to_add.append('MAX(Date) AS Date' if has_group else 'Date')
    if not to_add:
        return sql
    # ถ้าเพิ่มแค่ Team (คำถามทีม + column อื่นมีครบ) → ใส่ Team หน้า SELECT เพื่อให้แสดง Team คอลัมน์แรก
    if need_team and len(to_add) == 1 and to_add[0] in ('Team', 'MAX(Team) AS Team'):
        sel_match = re.match(r'^(\s*SELECT\s+(?:DISTINCT\s+)?)(.+?)(\s+FROM\s+repairs_enriched\s+)', sql, re.IGNORECASE | re.DOTALL)
        if sel_match:
            prefix, cols, rest = sel_match.group(1), sel_match.group(2).strip(), sel_match.group(3)
            sql = prefix + 'Team, ' + cols + rest + sql[sel_match.end():]
            logger.info("Injected Team into SELECT (คำถามทีม → แสดง Team ด้วย)")
            return sql
    insert_str = ', ' + ', '.join(to_add)
    sql = sql[:insert_pos] + insert_str + ' ' + sql[insert_pos:]
    logger.info("Injected Line, Process, ปัญหา, สาเหตุ, การแก้ไข, Date into SELECT (คำถามอาการ/ปัญหา/สาเหตุ)" + (" + Team (ถามทีม)" if need_team else ""))
    return sql

# คอลัมน์ที่แสดงสำหรับคำถามหมวด "อาการ/สาเหตุ" (Process หรือ Line เป็นหลัก)
DETAIL_SELECT_COLS = 'Process, Line, "ปัญหา", "สาเหตุ", "การแก้ไข", Date'
# คอลัมน์สำหรับคำถาม "ประวัติการซ่อม" — Date, Line, Process, ปัญหา, สาเหตุ, การแก้ไข, บันทึกเพิ่มเติม และเอาเฉพาะ row ที่อย่างน้อย 1 ใน 4 คอลัมน์สุดท้ายมีข้อมูล
HISTORY_SELECT_COLS = 'Date, Line, Process, "ปัญหา", "สาเหตุ", "การแก้ไข", "บันทึกเพิ่มเติม"'
# เงื่อนไข SQL: แสดงเฉพาะแถวที่ ปัญหา หรือ สาเหตุ หรือ การแก้ไข หรือ บันทึกเพิ่มเติม มีข้อมูลอย่างน้อย 1 คอลัมน์
HISTORY_HAS_DETAIL_COND = '''(TRIM(COALESCE("ปัญหา",'')) <> '' OR TRIM(COALESCE("สาเหตุ",'')) <> '' OR TRIM(COALESCE("การแก้ไข",'')) <> '' OR TRIM(COALESCE("บันทึกเพิ่มเติม",'')) <> '')'''

HISTORY_QUERY_KEYWORDS = ["ประวัติการซ่อม", "ขอประวัติ", "ประวัติของ"]
CAUSE_QUERY_KEYWORDS = ["ปัญหา", "สาเหตุ", "การแก้ไข", "อาการ", "เพราะอะไร", "เหตุผล", "error", "ng", "alarm", "fault", "problem", "issue"]

def _is_history_query_text(msg_lower: str) -> bool:
    return any(k in (msg_lower or "") for k in HISTORY_QUERY_KEYWORDS)

def _is_cause_or_symptom_query_text(msg_lower: str) -> bool:
    msg = (msg_lower or "")
    if any(k in msg for k in CAUSE_QUERY_KEYWORDS):
        return True
    extra_keywords = [
        "\u0e41\u0e01\u0e49",  # แก้
        "\u0e41\u0e01\u0e49\u0e22\u0e31\u0e07\u0e44\u0e07",  # แก้ยังไง
        "\u0e41\u0e01\u0e49\u0e44\u0e07",  # แก้ไง
        "\u0e22\u0e31\u0e07\u0e44\u0e07",  # ยังไง
        "\u0e2d\u0e22\u0e48\u0e32\u0e07\u0e44\u0e23",  # อย่างไร
        "\u0e17\u0e33\u0e44\u0e07",  # ทำไง
        "\u0e17\u0e33\u0e22\u0e31\u0e07\u0e44\u0e07",  # ทำยังไง
        "\u0e27\u0e34\u0e18\u0e35",  # วิธี
        "\u0e27\u0e34\u0e18\u0e35\u0e41\u0e01\u0e49",  # วิธีแก้
        "\u0e27\u0e34\u0e18\u0e35\u0e41\u0e01\u0e49\u0e44\u0e02",  # วิธีแก้ไข
        "\u0e27\u0e34\u0e18\u0e35\u0e0b\u0e48\u0e2d\u0e21",  # วิธีซ่อม
        "how to", "howto", "fix", "solution", "resolve", "troubleshoot",
    ]
    return any(k in msg for k in extra_keywords)

def _normalize_text_for_match(value: str) -> str:
    """
    Normalize text for matching while handling punctuation carefully.
    Replaces dashes and underscores with spaces for loose matching,
    but keeps the original length and characters in mind.
    """
    text = str(value or "").strip().lower()
    # Replace multiple spaces with one
    text = re.sub(r"\s+", " ", text)
    # Note: We don't replace -/_ with space here anymore to keep it for exact matching
    # But for loose matching we can use a secondary normalization
    return text

def _normalize_loose(text: str) -> str:
    """Replaces punctuation with space for loose matching."""
    return re.sub(r"[_\-]+", " ", text).strip()

def _tokenize_search_terms(text: str, max_terms: int = 6) -> List[str]:
    if not text:
        return []
    stopwords = {
        "ขอ", "ข้อมูล", "ของ", "ที่", "มี", "ไหม", "อะไร", "บ้าง", "ด้วย", "และ", "หรือ",
        "ครับ", "ค่ะ", "คะ", "นะ", "หน่อย", "ประวัติ", "ซ่อม", "การซ่อม", "เครื่อง", "ไลน์",
        "line", "process", "tech", "pm", "repair", "history", "show", "list", "please",
    }
    raw_terms = re.findall(r"[A-Za-z0-9][A-Za-z0-9_\-]{1,}|[ก-๙]{2,}", str(text))
    out: List[str] = []
    seen = set()
    for raw in raw_terms:
        term = str(raw).strip().lower()
        if not term or term in stopwords:
            continue
        if re.fullmatch(r"\d+", term) and len(term) < 2:
            continue
        if term not in seen:
            out.append(term)
            seen.add(term)
        for part in re.split(r"[_\-]+", term):
            if len(part) < 2 or part in stopwords:
                continue
            if re.fullmatch(r"\d+", part) and len(part) < 2:
                continue
            if part not in seen:
                out.append(part)
                seen.add(part)
        if len(out) >= max_terms:
            break
    return out[:max_terms]

def _is_maintenance_domain_message(user_msg: str) -> bool:
    msg = (user_msg or "").strip()
    if not msg:
        return False
    msg_lower = msg.lower()
    
    # 1. Check maintenance-related keywords
    domain_keywords = [
        "ซ่อม", "เสีย", "เครื่อง", "ไลน์", "line", "process", "ช่าง", "เทค",
        "repair", "breakdown", "downtime", "pm", "บำรุง", "ประวัติ", "ดรอป", "พัง",
        "สาเหตุ", "การแก้ไข", "อาการ", "repairminutes", "responseminutes", "ng", "error",
        "problem", "issue", "machine", "cid", "led", "sda",
        # เพิ่ม production line keywords
        "pcb", "mopf", "toyota", "airbag", "ecu", "grease", "assy", "solder"
    ]
    domain_keywords = [
        "ซ่อม", "เสีย", "เครื่อง", "ไลน์", "line", "process", "ช่าง", "เทค",
        "repair", "breakdown", "downtime", "pm", "บำรุง", "ประวัติ", "ดรอป", "พัง",
        "สาเหตุ", "การแก้ไข", "อาการ", "repairminutes", "responseminutes", "ng", "error",
        "problem", "issue", "machine", "cid", "led", "sda",
        "pcb", "mopf", "toyota", "airbag", "ecu", "grease", "assy", "solder",
    ]
    if any(k in msg_lower for k in domain_keywords):
        return True

    # 2. Substring matching against known Lines and Processes
    msg_norm = _normalize_text_for_match(msg_lower)
    msg_loose = _normalize_loose(msg_norm)
    msg_compact = msg_loose.replace(" ", "")
    
    # ดึงค่า Lines และ Processes จาก db_context
    known_entities = []
    known_entities.extend(db_context.get("lines") or [])
    known_entities.extend(db_context.get("processes") or [])
    
    for raw_value in known_entities:
        candidate_norm = _normalize_text_for_match(raw_value)
        candidate_loose = _normalize_loose(candidate_norm)
        candidate_compact = candidate_loose.replace(" ", "")
        
        # 3. Handle substring matches
        # If user message is part of Line/Process (e.g. "CID" -> "CID1")
        if (msg_norm in candidate_norm and len(msg_norm) >= 3) or (msg_loose in candidate_loose and len(msg_loose) >= 3):
            return True
        
        # If Line/Process is part of user message
        if (candidate_norm in msg_norm or candidate_loose in msg_loose or 
            (len(candidate_compact) >= 4 and (candidate_compact in msg_compact or msg_compact in candidate_compact))):
            return True

    return False

def _extract_limit_from_sql(sql: str, default_limit: int = 10) -> int:
    if not sql:
        return default_limit
    m = re.search(r"\bLIMIT\s+(\d+)\b", sql, re.IGNORECASE)
    if not m:
        return default_limit
    try:
        return max(1, int(m.group(1)))
    except Exception:
        return default_limit

def _extract_date_condition_from_sql(sql: str) -> str:
    if not sql:
        return ""
    patterns = [
        r"Date\s+BETWEEN\s+'[^']+'\s+AND\s+'[^']+'",
        r"Date\s*=\s*'[^']+'",
        r"Date\s*>=\s*'[^']+'\s*AND\s*Date\s*<=\s*'[^']+'",
        r"Date\s*>=\s*'[^']+'",
    ]
    for pattern in patterns:
        m = re.search(pattern, sql, re.IGNORECASE)
        if m:
            return m.group(0).strip()
    return ""

def _remove_simple_filter_condition(sql: str, column_name: str) -> str:
    if not sql:
        return sql
    col = re.escape(column_name)
    cond = rf"{col}\s*(?:=|LIKE)\s*'[^']+'"
    out = sql
    out = re.sub(rf"\s+AND\s+{cond}\s*", " ", out, flags=re.IGNORECASE)
    out = re.sub(rf"WHERE\s+{cond}\s+AND\s+", "WHERE ", out, flags=re.IGNORECASE)
    out = re.sub(rf"WHERE\s+{cond}\s*(?=ORDER\s+BY|GROUP\s+BY|LIMIT|;|$)", "WHERE 1=1 ", out, flags=re.IGNORECASE)
    out = re.sub(r"\bWHERE\s+1=1\s+AND\s+", "WHERE ", out, flags=re.IGNORECASE)
    out = re.sub(r"\bWHERE\s+1=1\s*(?=ORDER\s+BY|GROUP\s+BY|LIMIT|;|$)", " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+", " ", out).strip()
    return out

def _build_detail_search_fallback_sql(user_msg: str, original_sql: str, history_mode: bool = False) -> Optional[str]:
    if not original_sql or "repairs_enriched" not in (original_sql or "").lower():
        return None
    msg = str(user_msg or "").strip()
    if not msg:
        return None

    tail_terms: List[str] = []
    m_of = re.search(r"ของ\s+(.+)$", msg, re.IGNORECASE)
    if m_of:
        tail_terms = _tokenize_search_terms(m_of.group(1), max_terms=6)
    terms = tail_terms + _tokenize_search_terms(msg, max_terms=8)
    dedup_terms: List[str] = []
    seen = set()
    for t in terms:
        if t not in seen:
            dedup_terms.append(t)
            seen.add(t)
        if len(dedup_terms) >= 6:
            break
    if not dedup_terms:
        return None

    limit_n = _extract_limit_from_sql(original_sql, default_limit=10)
    date_cond = _extract_date_condition_from_sql(original_sql)
    select_cols = HISTORY_SELECT_COLS if history_mode else 'Date, Line, Process, Tech, RepairMinutes, "ปัญหา", "สาเหตุ", "การแก้ไข", "บันทึกเพิ่มเติม"'

    token_clauses: List[str] = []
    for term in dedup_terms:
        term_esc = term.replace("'", "''")
        token_clauses.append(
            "("
            f"LOWER(COALESCE(Line,'')) LIKE '%{term_esc}%' OR "
            f"LOWER(COALESCE(Process,'')) LIKE '%{term_esc}%' OR "
            f"LOWER(COALESCE(\"ปัญหา\",'')) LIKE '%{term_esc}%' OR "
            f"LOWER(COALESCE(\"สาเหตุ\",'')) LIKE '%{term_esc}%' OR "
            f"LOWER(COALESCE(\"การแก้ไข\",'')) LIKE '%{term_esc}%' OR "
            f"LOWER(COALESCE(\"บันทึกเพิ่มเติม\",'')) LIKE '%{term_esc}%'"
            ")"
        )

    if not token_clauses:
        return None

    where_parts: List[str] = []
    if history_mode:
        where_parts.append(HISTORY_HAS_DETAIL_COND)
    if date_cond:
        where_parts.append(date_cond)
    where_parts.append("(" + " OR ".join(token_clauses) + ")")

    score_expr = " + ".join([f"(CASE WHEN {clause} THEN 1 ELSE 0 END)" for clause in token_clauses])
    where_sql = " AND ".join(where_parts)
    sql = (
        f"SELECT {select_cols} FROM repairs_enriched "
        f"WHERE {where_sql} "
        f"ORDER BY {score_expr} DESC, Date DESC LIMIT {limit_n}"
    )
    return sql

def _build_repair_no_data_fallback_sqls(user_msg: str, original_sql: str) -> List[str]:
    if not original_sql or "repairs_enriched" not in (original_sql or "").lower():
        return []

    variants: List[str] = []
    base_sql = original_sql
    relaxed_sql = base_sql

    def _relax_eq_to_like(sql_text: str, column_name: str) -> str:
        # Example: "ปัญหา" = 'ASSY SCREW' -> LOWER(COALESCE("ปัญหา",'')) LIKE '%assy screw%'
        pattern = rf"\"?{re.escape(column_name)}\"?\s*=\s*'([^']+)'"
        def _replace(match_obj):
            value = (match_obj.group(1) or "").replace("'", "''").lower()
            return f"LOWER(COALESCE(\"{column_name}\",'')) LIKE '%{value}%'"
        return re.sub(
            pattern,
            _replace,
            sql_text,
            flags=re.IGNORECASE,
        )

    relaxed_sql = re.sub(
        r"\bLine\s*=\s*'([^']+)'",
        lambda m: f"Line LIKE '%{m.group(1)}%'",
        relaxed_sql,
        flags=re.IGNORECASE,
    )
    relaxed_sql = re.sub(
        r"\bProcess\s*=\s*'([^']+)'",
        lambda m: f"Process LIKE '%{m.group(1)}%'",
        relaxed_sql,
        flags=re.IGNORECASE,
    )
    for detail_col in ["ปัญหา", "สาเหตุ", "การแก้ไข", "บันทึกเพิ่มเติม"]:
        relaxed_sql = _relax_eq_to_like(relaxed_sql, detail_col)

    if relaxed_sql != base_sql:
        variants.append(relaxed_sql)

    no_process_sql = _remove_simple_filter_condition(relaxed_sql, "Process")
    if no_process_sql and no_process_sql != relaxed_sql:
        variants.append(no_process_sql)

    no_line_sql = _remove_simple_filter_condition(relaxed_sql, "Line")
    if no_line_sql and no_line_sql != relaxed_sql:
        variants.append(no_line_sql)

    msg_lower = (user_msg or "").lower()
    sql_lower = (original_sql or "").lower()
    wants_detail_answer = any(k in sql_lower for k in ['"การแก้ไข"', '"สาเหตุ"', '"ปัญหา"', " as solution", " solution"])
    if _is_history_query_text(msg_lower) or _is_cause_or_symptom_query_text(msg_lower) or wants_detail_answer:
        detail_sql = _build_detail_search_fallback_sql(
            user_msg,
            original_sql,
            history_mode=_is_history_query_text(msg_lower),
        )
        if detail_sql:
            variants.append(detail_sql)

    dedup: List[str] = []
    seen = set()
    base_norm = re.sub(r"\s+", " ", (base_sql or "")).strip().rstrip(";")
    for variant in variants:
        norm = re.sub(r"\s+", " ", variant or "").strip().rstrip(";")
        if not norm or norm == base_norm or norm in seen:
            continue
        dedup.append(variant)
        seen.add(norm)
    return dedup





def enforce_process_or_line_filter_for_symptom_question(user_msg, sql):
    """
    คำถามแบบ "GREASE มีอาการอะไรบ้าง" → ทุก row ต้องเป็น Process = GREASE.
    คำถามแบบ "Line LED_M_PCB มีอาการอะไรบ้าง" หรือ "LED_M_PCB มีอาการอะไรบ้าง" → ทุก row ต้องเป็น Line = LED_M_PCB.
    และแสดงแค่ Process, Line, ปัญหา, สาเหตุ, การแก้ไข, Date (ไม่เอา Tech, RepairMinutes).
    """
    msg = str(user_msg or "")
    msg_lower = msg.lower()

    try:
        valid_lines = db_context.get("lines", []) or []
        valid_processes = db_context.get("processes", []) or []
    except Exception as e:
        logger.warning(f"enforce_process_or_line_filter: could not load lines/processes: {e}")
        return sql

    def _norm(s):
        return re.sub(r"\s+", " ", (s or "").strip()).lower()

    filter_by_line = None
    filter_by_process = None

    # เช็ค "Line XXX" หรือ "ไลน์ XXX" (ระบุ Line ชัดเจน)
    m_line = re.search(r'(?:line|ไลน์)\s+([^\s,?.]+)', msg_lower)
    if m_line:
        name = (m_line.group(1) or "").strip()
        for v in valid_lines:
            if _norm(v) == _norm(name) or name.upper() in v.upper() or _norm(name) in _norm(v):
                filter_by_line = v
                break
        if not filter_by_line and name:
            for v in valid_lines:
                if _norm(name) in _norm(v) or _norm(v) in _norm(name):
                    filter_by_line = v
                    break

    # ถ้าไม่มี "line/ไลน์" ชัดเจน เช็คว่าคำในคำถามตรงกับ Process หรือ Line (เช่น GREASE, LED_M_PCB)
    if not filter_by_line:
        words = re.findall(r'[A-Za-z0-9_\-]+', msg)
        for w in words:
            if len(w) < 2:
                continue
            w_norm = _norm(w)
            for p in valid_processes:
                if _norm(p) == w_norm or w_norm in _norm(p) or _norm(p) in w_norm:
                    filter_by_process = p
                    break
            if filter_by_process:
                break
        if not filter_by_process:
            for w in words:
                if len(w) < 2:
                    continue
                w_norm = _norm(w)
                for v in valid_lines:
                    if _norm(v) == w_norm or w_norm in _norm(v) or _norm(v) in w_norm:
                        filter_by_line = v
                        break
                if filter_by_line:
                    break

    if not filter_by_line and not filter_by_process:
        return sql

    # แทนที่ SELECT ... ด้วย SELECT Process, Line, "ปัญหา", "สาเหตุ", "การแก้ไข", Date
    from_match = re.search(r'\s+FROM\s+repairs_enriched\s+', sql, re.IGNORECASE)
    if not from_match:
        return sql
    select_end = from_match.start()
    before_from = sql[:select_end].strip()
    after_from = sql[from_match.start():]
    if not re.search(r'^\s*SELECT\s+', before_from, re.IGNORECASE):
        return sql
    new_select = "SELECT " + DETAIL_SELECT_COLS + " "
    sql = new_select + after_from

    # เพิ่มหรือบังคับ WHERE Process = '...' หรือ Line = '...'
    cond_value = (filter_by_process or filter_by_line).replace("'", "''")
    if filter_by_process:
        filter_cond = f"Process = '{cond_value}'"
    else:
        filter_cond = f"Line = '{cond_value}'"

    sql_upper = sql.upper()
    where_pos = re.search(r'\bWHERE\s+', sql_upper)
    if where_pos:
        insert_pos = where_pos.end()
        # ใส่ AND filter_cond หลัง WHERE (ต้องไม่ซ้ำกับที่มีอยู่แล้ว)
        rest = sql[insert_pos:]
        if filter_by_process and re.search(r"Process\s*=\s*['\"]", rest, re.IGNORECASE):
            pass
        elif filter_by_line and re.search(r"Line\s*=\s*['\"]", rest, re.IGNORECASE):
            pass
        else:
            sql = sql[:insert_pos] + filter_cond + " AND " + rest
    else:
        # ไม่มี WHERE → แทรกก่อน ORDER BY / GROUP BY / LIMIT / ;
        for anchor in [r'\bORDER\s+BY\b', r'\bGROUP\s+BY\b', r'\bLIMIT\s+', r';\s*$']:
            m = re.search(anchor, sql, re.IGNORECASE)
            if m:
                sql = sql[:m.start()] + " WHERE " + filter_cond + " " + sql[m.start():]
                break
        else:
            sql = sql.rstrip().rstrip(';') + " WHERE " + filter_cond
            if not sql.strip().endswith(';'):
                sql += ";"

    # ถ้าไม่ระบุ วัน เดือน ปี หรือคำบอกวันในคำถาม → เอามาทั้งหมด (ลบเงื่อนไข Date ออกจาก WHERE)
    date_keywords = [
        'วันนี้', 'เมื่อวาน', 'พรุ่งนี้', 'สัปดาห์', 'อาทิตย์', 'เดือน', 'ปี',
        'today', 'yesterday', 'week', 'month', 'year', 'date'
    ]
    has_date_in_question = any(k in msg_lower for k in date_keywords) or re.search(r'20\d{2}', msg)
    year_only = re.search(r'\b(20\d{2})\b', msg)
    # ระบุแค่ปี (เช่น 2026) ไม่ระบุเดือน/วัน → เอามาทั้งปี
    month_day_indicators = re.search(r'เดือน\s*(ที่)?\s*\d{1,2}|\d{1,2}\s*เดือน|ม\.?\s*?\d{1,2}|month\s*\d', msg_lower or "")
    if year_only and not month_day_indicators:
        yyyy = year_only.group(1)
        year_range = f"Date BETWEEN '{yyyy}-01-01' AND '{yyyy}-12-31'"
        sql = re.sub(r"Date\s*=\s*'[^']*'", year_range, sql, flags=re.IGNORECASE)
        sql = re.sub(r"Date\s+BETWEEN\s+'[^']+'\s+AND\s+'[^']+'", year_range, sql, count=1, flags=re.IGNORECASE)
        logger.info(f"Normalized to whole year: {year_range}")
    elif not has_date_in_question:
        # ลบ Date = '...' และ Date BETWEEN ... AND ... ออกจาก WHERE
        sql = re.sub(r"\s*AND\s+Date\s*=\s*'[^']*'", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\s*AND\s+Date\s+BETWEEN\s+'[^']+'\s+AND\s+'[^']+'", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"Date\s*=\s*'[^']*'\s+AND\s+", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"Date\s+BETWEEN\s+'[^']+'\s+AND\s+'[^']+'\s+AND\s+", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bWHERE\s+AND\s+", "WHERE ", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\s+", " ", sql).strip()
        logger.info("Removed Date filter (user did not specify date) → return all records")
    logger.info(f"Enforced symptom question filter: {filter_cond} and SELECT {DETAIL_SELECT_COLS}")
    return sql

# (REMOVED) generate_sql_simple + generate_sql — replaced by build_sql_prompt + call_llm_for_sql pipeline

def _team_from_message(user_msg):
    """ดึงชื่อทีมจากคำถาม เช่น ทีม A, ทีม B → คืน 'A', 'B' หรือ None"""
    if not user_msg:
        return None
    msg = (user_msg or "").strip()
    m = re.search(r"ทีม\s*([A-Za-z](?:\s*[A-Za-z])?)\b", msg, re.IGNORECASE)
    if m:
        return m.group(1).strip().upper()
    return None

def _date_where_from_message(user_msg):
    """ดึงเงื่อนไข Date (WHERE ...) จากคำถาม เช่น 2026 เดือน 1, เมื่อวาน, วันนี้, เดือนนี้, สัปดาห์ที่แล้ว. คืน '' ถ้าไม่ระบุช่วง."""
    if not user_msg:
        return ""
    msg = (user_msg or "").strip()
    msg_lower = msg.lower()
    now = pd.Timestamp.now()
    today = now.strftime("%Y-%m-%d")
    
    # เมื่อวาน / วันนี้
    if any(w in msg_lower for w in ["เมื่อวาน", "yesterday"]):
        yesterday = (now - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        return f" WHERE Date = '{yesterday}'"
    if any(w in msg_lower for w in ["วันนี้", "today"]):
        return f" WHERE Date = '{today}'"
    
    # เปรียบเทียบระหว่างเดือน (เช่น "เดือน 1-2", "ระหว่างเดือน 1 กับ 2", "เดือน 1 และ 2")
    month_range_patterns = [
        r"เดือน\s*(\d{1,2})\s*-\s*(\d{1,2})",  # เดือน 1-2
        r"ระหว่างเดือน\s*(\d{1,2})\s*(?:กับ|และ|ถึง)\s*(\d{1,2})",  # ระหว่างเดือน 1 กับ 2
        r"เดือน\s*(\d{1,2})\s*(?:กับ|และ|ถึง)\s*(\d{1,2})",  # เดือน 1 และ 2
    ]
    
    for pattern in month_range_patterns:
        range_m = re.search(pattern, msg)
        if range_m:
            month1 = int(range_m.group(1))
            month2 = int(range_m.group(2))
            if 1 <= month1 <= 12 and 1 <= month2 <= 12:
                # หาปี
                y = now.year
                year_m = re.search(r"ปี\s*(\d{4})", msg)
                if year_m:
                    y = int(year_m.group(1))
                year_standalone = re.search(r"\b(20\d{2})\s*(?:เดือน|ม\.?)", msg)
                if year_standalone:
                    y = int(year_standalone.group(1))
                
                # สร้างช่วงวันที่จากเดือนแรกถึงเดือนสุดท้าย
                start_month = min(month1, month2)
                end_month = max(month1, month2)
                
                start = f"{y}-{start_month:02d}-01"
                if end_month < 12:
                    end = (datetime(y, end_month + 1, 1).date() - timedelta(days=1)).strftime("%Y-%m-%d")
                else:
                    end = f"{y}-12-31"
                
                return f" WHERE Date BETWEEN '{start}' AND '{end}'"
    
    # 2026 เดือน 1 / ปี 2026 เดือน 1 / เดือน 1
    month_m = re.search(r"เดือนที่\s*(\d{1,2})|เดือน\s*(\d{1,2})\b", msg)
    if month_m:
        num = int(month_m.group(1) or month_m.group(2))
        if 1 <= num <= 12:
            y = now.year
            year_m = re.search(r"ปี\s*(\d{4})", msg)
            if year_m:
                y = int(year_m.group(1))
            year_standalone = re.search(r"\b(20\d{2})\s*(?:เดือน|ม\.?)", msg)
            if year_standalone:
                y = int(year_standalone.group(1))
            year_after = re.search(r"เดือน\s*(\d{1,2})\s*(20\d{2})\b", msg)
            if year_after:
                y = int(year_after.group(2))
            start = f"{y}-{num:02d}-01"
            end = (datetime(y, num + 1, 1).date() - timedelta(days=1)).strftime("%Y-%m-%d") if num < 12 else f"{y}-12-31"
            return f" WHERE Date BETWEEN '{start}' AND '{end}'"
    
    # เดือนนี้
    if any(w in msg_lower for w in ["เดือนนี้"]):
        month_start = now.replace(day=1).strftime("%Y-%m-%d")
        return f" WHERE Date BETWEEN '{month_start}' AND '{today}'"
    
    # เดือนก่อน / สัปดาห์ที่แล้ว (optional)
    if any(w in msg_lower for w in ["เดือนก่อน", "เดือนที่แล้ว"]):
        last = (now.replace(day=1) - pd.Timedelta(days=1))
        last_month_start = last.replace(day=1).strftime("%Y-%m-%d")
        last_month_end = last.strftime("%Y-%m-%d")
        return f" WHERE Date BETWEEN '{last_month_start}' AND '{last_month_end}'"
    if any(w in msg_lower for w in ["สัปดาห์ที่แล้ว", "อาทิตย์ที่แล้ว", "สัปดาห์ก่อน"]):
        last_week_start = (now - pd.Timedelta(days=now.dayofweek + 7)).strftime("%Y-%m-%d")
        last_week_end = (now - pd.Timedelta(days=now.dayofweek + 1)).strftime("%Y-%m-%d")
        return f" WHERE Date BETWEEN '{last_week_start}' AND '{last_week_end}'"
    if any(w in msg_lower for w in ["สัปดาห์นี้", "อาทิตย์นี้"]):
        this_week_start = (now - pd.Timedelta(days=now.dayofweek)).strftime("%Y-%m-%d")
        return f" WHERE Date BETWEEN '{this_week_start}' AND '{today}'"
    
    # ปี 2026 / ปี 2025 = ทั้งปีนั้น (1 ม.ค. - 31 ธ.ค.)
    year_match = re.search(r"ปี\s*(20\d{2})\b", msg, re.IGNORECASE)
    if year_match:
        yyyy = year_match.group(1)
        return f" WHERE Date BETWEEN '{yyyy}-01-01' AND '{yyyy}-12-31'"
    if "ปี" in msg_lower or "year" in msg_lower:
        year_match = re.search(r"\b(20\d{2})\b", msg)
        if year_match:
            yyyy = year_match.group(1)
            return f" WHERE Date BETWEEN '{yyyy}-01-01' AND '{yyyy}-12-31'"
    return ""

# ช่างที่ไม่เอามาแสดงในคำตอบ — ใช้ TechDataStore singleton (โหลดครั้งเดียว ไม่อ่านไฟล์ซ้ำทุก request)

def get_tech_exclude_for_answer() -> Tuple[str, ...]:
    """รายชื่อ Tech ที่ไม่แสดงในคำตอบ — ใช้ TechDataStore singleton"""
    return TechDataStore.instance().exclude_from_answer

def _extract_where_clause(sql: str, sql_upper: str) -> str:
    """ดึง WHERE clause จาก SQL เดิม (ไม่รวม GROUP BY/ORDER BY/LIMIT)"""
    if "WHERE" not in sql_upper:
        return ""
    where_start = sql_upper.find("WHERE")
    where_end = len(sql_upper)
    for keyword in ["GROUP BY", "ORDER BY", "LIMIT"]:
        idx = sql_upper.find(keyword, where_start)
        if idx != -1 and idx < where_end:
            where_end = idx
    return sql[where_start:where_end].strip()

def _detect_query_intent(user_msg: str) -> str:
    """
    ตรวจจับ intent จากคำถาม
    
    Returns:
        - TOP: มากที่สุด, สูงสุด, อันดับ, top
        - SUM: รวม, ทั้งหมด (เวลา/นาที)
        - COUNT: กี่ครั้ง, จำนวน
        - AGGREGATION: มี GROUP BY, SUM, COUNT, AVG
        - HISTORY: ประวัติ, ขอประวัติ
        - OVERVIEW: มีอะไรบ้าง, แสดง, ดู
        - PM: คำถาม PM
        - DETAIL: รายละเอียด, อาการ, สาเหตุ
        - CHAT: อื่นๆ
    """
    msg_lower = user_msg.lower()
    
    # TOP (ranking)
    if any(k in msg_lower for k in ["มากที่สุด", "สูงสุด", "อันดับ", "top", "เยอะที่สุด", "นานที่สุด", "เร็วที่สุด", "ช้าที่สุด"]):
        return "TOP"
    
    # HISTORY
    if any(k in msg_lower for k in ["ประวัติการซ่อม", "ขอประวัติ", "ประวัติของ"]):
        return "HISTORY"
    
    # PM
    if any(k in msg_lower for k in ["pm", "พีเอ็ม", "แผน", "บำรุงรักษา"]):
        return "PM"
    
    # OVERVIEW (มีอะไรบ้าง)
    if any(k in msg_lower for k in ["มีอะไรบ้าง", "มีอะไรเสียบ้าง", "เสียอะไรบ้าง", "แสดง", "ดู", "list"]):
        return "OVERVIEW"
    
    # DETAIL (อาการ/สาเหตุ)
    if any(k in msg_lower for k in ["อาการ", "สาเหตุ", "ปัญหา", "การแก้ไข", "เพราะอะไร"]):
        return "DETAIL"
    
    # SUM (รวมเวลา)
    if any(k in msg_lower for k in ["รวม", "ทั้งหมด"]) and any(k in msg_lower for k in ["นาที", "เวลา", "ชั่วโมง"]):
        return "SUM"
    
    # COUNT (จำนวนครั้ง)
    if any(k in msg_lower for k in ["กี่ครั้ง", "จำนวน", "นับ", "count"]):
        return "COUNT"
    
    return "CHAT"


def _should_hide_total_count_for_top_query(user_msg: str, sql: str, total_count: Optional[int]) -> bool:
    """ซ่อน total_count สำหรับคำถามจัดอันดับแบบ top-1 เพื่อไม่ให้สรุปผลชวนสับสน"""
    if total_count is None or not sql:
        return False

    sql_upper = sql.upper()
    limit_n = _extract_limit_from_sql(sql, default_limit=0)
    has_aggregation = any(token in sql_upper for token in ["GROUP BY", "COUNT(", "SUM(", "AVG(", "MIN(", "MAX("])

    if limit_n != 1 or not has_aggregation:
        return False
    if "FROM PM" in sql_upper or "JOIN PM" in sql_upper:
        return False

    return _detect_query_intent(user_msg or "") == "TOP"





def _fix_repair_specific_day_from_user_intent(user_msg, sql):
    """2026 วันที่ 3 / ปี 2026 วันที่ 3 = วันที่ 3 ม.ค. ของปีนั้น (วันที่ 3 ของปี); วันที่ 3 = วันที่ 3 ของเดือนปัจจุบัน (repairs_enriched)"""
    if not sql or ("repairs_enriched" not in sql and "REPAIRS_ENRICHED" not in sql.upper()):
        return sql
    msg = (user_msg or "").strip()
    if any(w in msg.lower() for w in ["วันนี้", "เมื่อวาน", "yesterday", "today"]):
        return sql  # วันนี้/เมื่อวาน จัดการโดย _normalize_repair_date
    day_m = re.search(r"วันที่\s*(\d{1,2})\b", msg)
    if not day_m:
        return sql
    day = int(day_m.group(1))
    if day < 1 or day > 31:
        return sql
    now = pd.Timestamp.now()
    year = now.year
    month = now.month
    year_m = re.search(r"(?:ปี\s*)?(20\d{2})\s*วันที่", msg)
    if year_m:
        year = int(year_m.group(1))
        month = 1  # 2026 วันที่ 3 = วันที่ 3 ของปี = 3 ม.ค. (day 3 of year)
    try:
        target = pd.Timestamp(year, month, day).strftime("%Y-%m-%d")
    except Exception:
        return sql
    new_cond = f"Date = '{target}'"
    sql_before = sql
    if re.search(r"Date\s*=\s*['\"]\d{4}-\d{2}-\d{2}['\"]", sql, re.IGNORECASE):
        sql = re.sub(r"Date\s*=\s*['\"]\d{4}-\d{2}-\d{2}['\"]", new_cond, sql, count=1, flags=re.IGNORECASE)
    elif re.search(r"Date\s+BETWEEN\s+['\"]\d{4}-\d{2}-\d{2}['\"]\s+AND\s+['\"]\d{4}-\d{2}-\d{2}['\"]", sql, re.IGNORECASE):
        sql = re.sub(
            r"Date\s+BETWEEN\s+['\"]\d{4}-\d{2}-\d{2}['\"]\s+AND\s+['\"]\d{4}-\d{2}-\d{2}['\"]",
            new_cond,
            sql,
            count=1,
            flags=re.IGNORECASE,
        )
    else:
        has_date = bool(re.search(r"\bDate\s*(?:=|BETWEEN|>=|<=)", sql, re.IGNORECASE))
        if not has_date:
            has_where = " WHERE " in sql.upper()
            prefix = " AND " if has_where else " WHERE "
            for pattern, repl in [
                (r"(\s+ORDER\s+BY\s+)", rf"{prefix}{new_cond} \1"),
                (r"(\s+GROUP\s+BY\s+)", rf"{prefix}{new_cond} \1"),
                (r"(\s+LIMIT\s+\d+)", rf"{prefix}{new_cond} \1"),
                (r"(\s*;\s*)$", rf"{prefix}{new_cond} \1"),
            ]:
                if re.search(pattern, sql, re.IGNORECASE):
                    sql = re.sub(pattern, repl, sql, count=1, flags=re.IGNORECASE)
                    break
            else:
                sql = f"{sql.rstrip().rstrip(';')}{prefix}{new_cond}"
                if not sql.strip().endswith(";"):
                    sql += ";"
            sql = re.sub(r"WHERE\s+AND\s+", "WHERE ", sql, flags=re.IGNORECASE)
    if sql != sql_before:
        logger.info(f"Fixed repair SQL to specific day: {target} (จาก วันที่ {day})")
    return sql

# ความสมบูรณ์ขั้นต่ำ (0–100) ในการถือว่า "ข้อความก่อน ประวัติ" เป็นชื่อช่างจริง จาก DB
TECH_NAME_MIN_RATIO = 95

def _resolve_tech_name_for_history(name_before):
    """คืน (tech_value_from_db, ratio) ถ้า name_before ตรงกับชื่อช่างใน DB ที่ความสมบูรณ์ >= TECH_NAME_MIN_RATIO มิฉะนั้นคืน (None, 0)."""
    if not name_before or not name_before.strip():
        return None, 0
    name_clean = name_before.strip()
    if not os.path.exists(WORK_DB_PATH):
        return None, 0
    try:
        with sqlite3.connect(WORK_DB_PATH) as conn:
            tech_df = pd.read_sql("SELECT DISTINCT Tech FROM repairs_enriched WHERE Tech IS NOT NULL AND TRIM(Tech) != ''", conn)
        valid_techs = [str(t).strip() for t in tech_df["Tech"].tolist() if t]
    except Exception as e:
        logger.warning(f"_resolve_tech_name_for_history: {e}")
        return None, 0
    if not valid_techs:
        return None, 0
    try:
        best = fuzz_process.extractOne(name_clean, valid_techs, scorer=fuzz.ratio)
        if best and len(best) >= 2 and best[1] >= TECH_NAME_MIN_RATIO:
            return best[0], best[1]
    except Exception as e:
        logger.warning(f"_resolve_tech_name_for_history extractOne: {e}")
    return None, 0

def _repair_sqlite_date_syntax_repairs(sql):
    """
    แปลงไวยากรณ์วันที่แบบ MySQL/PostgreSQL ที่เหลืออยู่ให้เป็น SQLite (repairs_enriched เท่านั้น).
    ใช้หลัง context-specific fixes (last week, last month) แล้ว — แก้เฉพาะที่ยังเหลือ CURDATE/DATE_SUB/INTERVAL ฯลฯ
    รองรับ: CURDATE(), NOW(), DATE_SUB(..., INTERVAL n DAY/MONTH/WEEK), DATE_ADD(..., INTERVAL n DAY), DATE(CURDATE()-INTERVAL n WEEK).
    ข้อจำกัด: นิพจน์ซับซ้อนมาก (ซ้อนหลายชั้น) อาจไม่ถูกแทนที่ครบ; ฟังก์ชันอื่นเช่น LAST_DAY, CONCAT ยังไม่แปลง.
    """
    if not sql or ("repairs_enriched" not in sql and "REPAIRS_ENRICHED" not in sql.upper()):
        return sql
    out = sql

    # CURDATE() → date('now')
    out = re.sub(r"\bCURDATE\s*\(\s*\)", "date('now')", out, flags=re.IGNORECASE)
    # NOW() ในบริบทวันที่ → date('now')
    out = re.sub(r"\bNOW\s*\(\s*\)", "date('now')", out, flags=re.IGNORECASE)

    # DATE_SUB(CURDATE(), INTERVAL n DAY) → date('now', '-n days')
    def _days_repl(m):
        n = m.group(1)
        return f"date('now', '-{n} days')"
    out = re.sub(
        r"DATE_SUB\s*\(\s*CURDATE\s*\(\s*\)\s*,\s*INTERVAL\s*(\d+)\s*DAY\s*\)",
        _days_repl,
        out,
        flags=re.IGNORECASE,
    )
    # DATE_SUB(CURDATE(), INTERVAL n MONTH) → date('now', '-n month')
    def _month_repl(m):
        n = m.group(1)
        return f"date('now', '-{n} month')"
    out = re.sub(
        r"DATE_SUB\s*\(\s*CURDATE\s*\(\s*\)\s*,\s*INTERVAL\s*(\d+)\s*MONTH\s*\)",
        _month_repl,
        out,
        flags=re.IGNORECASE,
    )
    # DATE_SUB(CURDATE(), INTERVAL n WEEK) → date('now', '-n*7 days')
    def _week_repl(m):
        n = int(m.group(1))
        return f"date('now', '-{n * 7} days')"
    out = re.sub(
        r"DATE_SUB\s*\(\s*CURDATE\s*\(\s*\)\s*,\s*INTERVAL\s*(\d+)\s*WEEK\s*\)",
        _week_repl,
        out,
        flags=re.IGNORECASE,
    )
    # DATE(CURDATE() - INTERVAL n WEEK) → date('now', '-n*7 days')
    out = re.sub(
        r"DATE\s*\(\s*CURDATE\s*\(\s*\)\s*-\s*INTERVAL\s*(\d+)\s*WEEK\s*\)",
        _week_repl,
        out,
        flags=re.IGNORECASE,
    )
    # DATE_ADD(CURDATE(), INTERVAL n DAY) → date('now', '+n days')
    def _add_days_repl(m):
        n = m.group(1)
        return f"date('now', '+{n} days')"
    out = re.sub(
        r"DATE_ADD\s*\(\s*CURDATE\s*\(\s*\)\s*,\s*INTERVAL\s*(\d+)\s*DAY\s*\)",
        _add_days_repl,
        out,
        flags=re.IGNORECASE,
    )

    if out != sql:
        logger.info("📋 แปลงไวยากรณ์วันที่ MySQL/PostgreSQL → SQLite (repairs_enriched)")
    return out


def _remove_spurious_line_process_like(user_msg, sql):
    """ลบเงื่อนไข LOWER(Line) LIKE '%x%' / LOWER(Process) LIKE '%y%' ที่เป็น placeholder หรือตัวอักษรเดียว (ไม่ได้มาจากคำถาม)"""
    if not sql or ("repairs_enriched" not in sql and "REPAIRS_ENRICHED" not in sql.upper()):
        return sql
    msg_lower = (user_msg or "").lower()
    out = sql
    # จับ AND LOWER(Line) LIKE '%...%' หรือ AND LOWER(Process) LIKE '%...%'
    for col in ["Line", "Process"]:
        pattern = rf"\s+AND\s+LOWER\s*\(\s*{col}\s*\)\s+LIKE\s+['\"]%([^'\"]*)%['\"]"
        for m in list(re.finditer(pattern, out, re.IGNORECASE)):
            value = (m.group(1) or "").strip()
            # ลบถ้า value เป็นตัวอักษรเดียว หรือไม่ปรากฏในคำถาม (และไม่ใช่คำที่มีความหมายเช่น ispu, led)
            if (len(value) <= 1 or 
                (str(value).lower() not in str(msg_lower)) and 
                not _word_overlap(value, msg_lower)):
                out = re.sub(
                    rf"\s+AND\s+LOWER\s*\(\s*{col}\s*\)\s+LIKE\s+['\"]%" + re.escape(value) + r"%['\"]\s*",
                    " ",
                    out,
                    count=1,
                    flags=re.IGNORECASE,
                )
                logger.info(f"Removed spurious LOWER({col}) LIKE '%{value}%' from SQL")
                break
    out = re.sub(r"\s+", " ", out).strip()
    return out

def _fix_repair_line_filter_from_user_intent(user_msg, sql):
    """
    ถ้าผู้ใช้ระบุ Line ชัดเจน (เช่น PCB E) แต่ SQL ใส่ Line filter คนละค่า (เช่น PCB-C)
    ให้ rewrite WHERE Line ให้ตรงกับที่ user ระบุ เพื่อกัน LLM เลือก line ผิด
    
    ⚠️ Guards:
    - ถ้า SQL มี Line IN (...) อยู่แล้ว → skip (จาก line expansion)
    - ถ้า SQL มี GROUP BY Line → skip (user ไม่ได้ระบุ Line เฉพาะ)
    - ถ้า SQL มี aggregation (COUNT, SUM, AVG, GROUP BY) → skip
    """
    if not sql or ("repairs_enriched" not in sql and "REPAIRS_ENRICHED" not in sql.upper()):
        return sql
    msg = (user_msg or "").strip()
    if not msg:
        return sql

    sql_upper = sql.upper()
    
    # Guard 1: ถ้า SQL มี Line IN (...) อยู่แล้ว → skip
    if re.search(r'\bLine\s+IN\s*\([^)]+\)', sql, re.IGNORECASE):
        logger.info("Line IN (...) already exists, skipping _fix_repair_line_filter_from_user_intent")
        return sql
    
    # Guard 2: ถ้า SQL มี GROUP BY Line → skip
    if re.search(r'\bGROUP\s+BY\s+Line\b', sql, re.IGNORECASE):
        logger.info("GROUP BY Line detected, skipping _fix_repair_line_filter_from_user_intent")
        return sql
    
    # Guard 3: ถ้า SQL มี aggregation → skip
    if any(k in sql_upper for k in ["COUNT(", "SUM(", "AVG(", "GROUP BY"]):
        logger.info("Aggregation detected, skipping _fix_repair_line_filter_from_user_intent")
        return sql

    # ถ้าเป็นคำถามแบบ all line / ทั้งหมด ไม่ควรบังคับ Line เดียว
    msg_lower = msg.lower()
    if any(k in msg_lower for k in ["all line", "ทุกไลน์", "ทุก line", "ทั้งหมด", "ทั้งโรงงาน"]):
        return sql

    def _norm_key(s: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[_\-]+", " ", (s or "").lower())).strip()

    def _resolve_line_from_msg(m: str) -> str:
        """
        หา Line name จาก message
        ⚠️ ต้องเป็น pattern ของ Line จริง ไม่ใช่คำว่า "line" ทั่วไป
        """
        m_norm = _norm_key(m)
        
        # Pattern 1: PCB-X (PCB A, PCB-B, etc.)
        m_pcb = re.search(r"\bpcb[\s\-]?([a-z])\b", m_norm, re.IGNORECASE)
        if m_pcb:
            letter = m_pcb.group(1).upper()
            preferred = [f"PCB-{letter}", f"PCB {letter}"]
            candidates = db_context.get("lines", []) or []
            for p in preferred:
                for v in candidates:
                    if _norm_key(v) == _norm_key(p):
                        return v
            return f"PCB-{letter}"
        
        # Pattern 2: Match กับ Line names ที่มีใน DB
        # แต่ต้องมีความยาวมากกว่า 3 ตัวอักษร เพื่อไม่ให้จับคำว่า "line" เดี่ยวๆ
        candidates = list(db_context.get("lines", []) or [])
        candidates.extend([a for a, _b in (db_context.get("line_pm_pairs") or []) if a])
        seen = set()
        uniq = []
        for c in candidates:
            k = _norm_key(str(c))
            if not k or k in seen or len(k) < 3:  # ต้องยาวกว่า 3 ตัวอักษร
                continue
            seen.add(k)
            uniq.append(str(c))
        
        best = ""
        best_len = 0
        for c in uniq:
            ck = _norm_key(c)
            if ck and ck in m_norm and len(ck) > best_len:
                best = c
                best_len = len(ck)
        return best

    line_value = _resolve_line_from_msg(msg)
    if not line_value:
        return sql

    # ถ้า SQL ไม่มีเงื่อนไข Line เลย ก็ไม่ต้อง rewrite
    if not re.search(r"\bLine\b", sql, re.IGNORECASE):
        return sql

    line_safe = str(line_value).replace("'", "''")
    desired = f"Line = '{line_safe}'"

    # remove existing line filters in WHERE (Line =, Line LIKE, LOWER(Line) LIKE, Line IN (...))
    where_match = re.search(r"\bWHERE\b", sql, re.IGNORECASE)
    if not where_match:
        return sql
    sql_before = sql
    u = sql.upper()
    # determine end of WHERE clause
    after_where = u[where_match.end():]
    m_end = re.search(r"\b(ORDER\s+BY|GROUP\s+BY|LIMIT)\b|;", after_where)
    where_end = (where_match.end() + m_end.start()) if m_end else len(sql)
    where_body = sql[where_match.end():where_end]

    # strip existing Line constraints
    patterns = [
        r"\s*(AND\s+)?Line\s*=\s*['\"][^'\"]+['\"]\s*",
        r"\s*(AND\s+)?Line\s+LIKE\s*['\"][^'\"]+['\"]\s*",
        r"\s*(AND\s+)?LOWER\s*\(\s*Line\s*\)\s+LIKE\s*['\"][^'\"]+['\"]\s*",
        r"\s*(AND\s+)?Line\s+IN\s*\([^)]+\)\s*",
    ]
    for pat in patterns:
        where_body = re.sub(pat, " ", where_body, flags=re.IGNORECASE)
    # cleanup ANDs
    where_body = re.sub(r"\s+AND\s+AND\s+", " AND ", where_body, flags=re.IGNORECASE)
    where_body = re.sub(r"^\s*AND\s+", "", where_body, flags=re.IGNORECASE)
    where_body = re.sub(r"\s*AND\s*$", "", where_body, flags=re.IGNORECASE)
    where_body = where_body.strip()

    new_where = f" WHERE {desired}"
    if where_body:
        new_where += f" AND {where_body}"
    new_sql = sql[:where_match.start()] + new_where + " " + sql[where_end:].lstrip()

    if new_sql != sql_before:
        logger.info(f"Rewrote Line filter from user intent → {desired}")
    return new_sql

def _ensure_repair_entity_filter_from_user_intent(user_msg, sql):
    """
    ถ้าผู้ใช้ระบุชื่อ Process/Line ชัดเจน (เช่น AACT) แต่ SQL ไม่มี filter เลย
    ให้ inject filter เพื่อกันผลลัพธ์กว้างเกินจริง
    """
    if not sql or ("repairs_enriched" not in sql and "REPAIRS_ENRICHED" not in sql.upper()):
        return sql

    msg = str(user_msg or "").strip()
    if not msg:
        return sql
    msg_lower = msg.lower()
    sql_upper = sql.upper()

    # คำถามภาพรวมทั้งโรงงาน/จัดอันดับ ไม่ควรถูกบังคับเป็น entity เดียว
    broad_query_keywords = [
        "all line", "all process", "ทุกไลน์", "ทุก line", "ทุก process",
        "ทั้งโรงงาน", "ทั้งหมด", "อันดับ", "มากที่สุด", "top", "ranking", "compare",
    ]
    if any(k in msg_lower for k in broad_query_keywords):
        return sql

    # Query รวมค่า/จัดกลุ่ม ไม่ควร inject filter เพิ่มอัตโนมัติ
    if any(k in sql_upper for k in ["COUNT(", "SUM(", "AVG(", "GROUP BY"]):
        return sql

    is_detail_like_query = (
        _is_history_query_text(msg_lower)
        or _is_cause_or_symptom_query_text(msg_lower)
        or any(k in msg_lower for k in ["เสีย", "error", "alarm", "fault", "problem", "issue"])
    )
    if not is_detail_like_query:
        return sql

    has_line_filter = bool(
        re.search(r"\bLine\s*(=|LIKE|IN\s*\()", sql, re.IGNORECASE)
        or re.search(r"LOWER\s*\(\s*Line\s*\)\s+LIKE", sql, re.IGNORECASE)
    )
    has_process_filter = bool(
        re.search(r"\bProcess\s*(=|LIKE|IN\s*\()", sql, re.IGNORECASE)
        or re.search(r"LOWER\s*\(\s*Process\s*\)\s+LIKE", sql, re.IGNORECASE)
    )
    if has_line_filter or has_process_filter:
        return sql

    valid_lines = [str(x).strip() for x in (db_context.get("lines") or []) if str(x).strip()]
    valid_processes = [str(x).strip() for x in (db_context.get("processes") or []) if str(x).strip()]
    if (not valid_lines or not valid_processes) and os.path.exists(WORK_DB_PATH):
        # fallback เมื่อ metadata cache ยังไม่พร้อม/ว่าง
        try:
            with sqlite3.connect(WORK_DB_PATH) as conn:
                if not valid_lines:
                    lines_df = pd.read_sql("SELECT DISTINCT Line FROM repairs_enriched WHERE Line IS NOT NULL", conn)
                    valid_lines = [str(x).strip() for x in lines_df["Line"].tolist() if str(x).strip()]
                if not valid_processes:
                    proc_df = pd.read_sql("SELECT DISTINCT Process FROM repairs_enriched WHERE Process IS NOT NULL", conn)
                    valid_processes = [str(x).strip() for x in proc_df["Process"].tolist() if str(x).strip()]
        except Exception as e:
            logger.warning(f"_ensure_repair_entity_filter_from_user_intent: fallback metadata load failed: {e}")
    if not valid_lines and not valid_processes:
        return sql

    msg_norm = _normalize_text_for_match(msg)
    msg_loose = _normalize_loose(msg_norm)
    msg_compact = msg_loose.replace(" ", "")
    words = re.findall(r"[A-Za-z0-9\u0e00-\u0e7f\-_.]+", msg)
    word_norm_set = {_normalize_text_for_match(w) for w in words if len(w) >= 2}
    word_loose_set = {_normalize_loose(w) for w in word_norm_set}

    def _find_best_entity(candidates):
        best = ""
        best_len = 0
        for raw in candidates:
            c = str(raw or "").strip()
            if not c:
                continue
            c_norm = _normalize_text_for_match(c)
            c_loose = _normalize_loose(c_norm)
            if len(c_norm) < 3:
                continue
            if c_norm in {"line", "process", "machine", "repair", "history"}:
                continue
            c_compact = c_loose.replace(" ", "")
            matched = (
                c_norm in msg_norm
                or c_loose in msg_loose
                or (len(c_compact) >= 4 and c_compact in msg_compact)
                or c_norm in word_norm_set
                or c_loose in word_loose_set
            )
            if matched and len(c_norm) > best_len:
                best = c
                best_len = len(c_norm)
        return best

    line_match = _find_best_entity(valid_lines)
    process_match = _find_best_entity(valid_processes)
    explicit_line_hint = bool(re.search(r"\bline\b", msg_lower))

    target_kind = None
    target_value = ""
    if explicit_line_hint and line_match:
        target_kind = "line"
        target_value = line_match
    elif process_match and not line_match:
        target_kind = "process"
        target_value = process_match
    elif line_match and not process_match:
        target_kind = "line"
        target_value = line_match
    elif process_match and line_match:
        # ถ้าชนทั้งคู่ให้เลือกตัวที่เฉพาะเจาะจงกว่า (ชื่อยาวกว่า)
        if len(_normalize_text_for_match(process_match)) >= len(_normalize_text_for_match(line_match)):
            target_kind = "process"
            target_value = process_match
        else:
            target_kind = "line"
            target_value = line_match
    else:
        return sql

    safe_value = target_value.replace("'", "''")
    filter_cond = f"Process = '{safe_value}'" if target_kind == "process" else f"Line = '{safe_value}'"

    if re.search(r"\bWHERE\b", sql, re.IGNORECASE):
        sql = re.sub(r"\bWHERE\b", f"WHERE {filter_cond} AND", sql, count=1, flags=re.IGNORECASE)
    else:
        inserted = False
        for anchor in [r"\bORDER\s+BY\b", r"\bGROUP\s+BY\b", r"\bLIMIT\s+\d+\b", r";\s*$"]:
            m = re.search(anchor, sql, re.IGNORECASE)
            if m:
                sql = sql[:m.start()] + f" WHERE {filter_cond} " + sql[m.start():]
                inserted = True
                break
        if not inserted:
            sql = sql.rstrip().rstrip(";") + f" WHERE {filter_cond}"
            if not sql.endswith(";"):
                sql += ";"

    sql = re.sub(r"\s+", " ", sql).strip()
    logger.info(f"Injected entity filter from user intent: {filter_cond}")
    return sql

def _word_overlap(value, msg_lower):
    """ตรวจว่า value กับ msg มีคำซ้อนกันพอถือว่า user ระบุ (เช่น value=LED_A_INS, msg มี led)"""
    v_lower = value.lower()
    if v_lower in msg_lower:
        return True
    # แยกตาม _ และเว้นวรรค ว่ามี token ใดใน msg ไหม (อย่างน้อย 2 ตัวอักษร)
    tokens = [t for t in re.split(r"[\s_\-]+", v_lower) if len(t) >= 2]
    return any(t in msg_lower for t in tokens)

# --- NEW ENHANCEMENT FUNCTIONS ---

def resolve_entities(user_msg):
    """
    Enhanced entity resolution with database-aware categorization.
    ปัญหา: ผู้ใช้งานมักพิมพ์ชื่อผิด หรือพิมพ์ชื่อเล่น เช่น "Line A" อาจจะพิมพ์เป็น "Lne A", "สมชาย" พิมพ์เป็น "สมชัย"
    วิธีแก้: ใช้ Fuzzy Match เทียบคำในประโยคกับค่าจริงใน Database แล้วแก้ให้ถูกก่อน
    """
    logger.info(f"[ENTITY] resolve_entities called with: '{user_msg}'")
    logger.info(f"[ENTITY] db_context suggestions count: {len(db_context.get('suggestions', []))}")
    
    if not db_context.get("suggestions"):
        logger.warning("[ENTITY] No suggestions available, returning original message")
        return user_msg
        
    # ตรวจสอบและแทนที่คำที่คล้ายกัน
    words = user_msg.split()
    corrected_msg = user_msg
    entity_constraints = []
    
    # === PCB Pattern Matching (Enhanced) ===
    pcb_match_found = False
    if "PCB" in user_msg.upper() or "pcb" in user_msg.lower():
        # Check if a specific PCB line is already mentioned
        pcb_lines = [
            "C-PCB", "C.PCB", "LED_A_PCB", "LED_M_PCB", "PCB-A", "PCB-B", "PCB-B1", 
            "PCB-C", "PCB-D", "PCB-D2", "PCB-E", "PCB-F", "PCB-I", "PCB-K", 
            "PCB-K1", "PCB-K2", "PCB_R1L2", "PCB_R1L3"
        ]
        
        specific_pcb = [line for line in pcb_lines if line.upper() in user_msg.upper()]
        if specific_pcb:
            for sp in specific_pcb:
                entity_constraints.append(f"Line Entity: {sp}")
                logger.info(f"[ENTITY] Detected specific PCB line: {sp}")
            pcb_match_found = True
        else:
            logger.info("[ENTITY] General PCB keyword detected - adding broad PCB line constraint")
            entity_constraints.append(f"Line Entity: PCB (ใช้ WHERE Line LIKE '%PCB%' เพื่อครอบคลุม {len(pcb_lines)} lines)")
            pcb_match_found = True
    
    # === MOPF Pattern Matching (Enhanced) ===
    if "MOPF" in user_msg.upper():
        logger.info("[ENTITY] MOPF keyword detected - adding MOPF line constraint")
        if "MOPF1" in user_msg.upper() or "MOPF 1" in user_msg.upper():
            entity_constraints.append("Line Entity: MOPF1 (ใช้ WHERE Line LIKE '%MOPF1%' OR Line LIKE '%MOPF#1%')")
        elif "MOPF2" in user_msg.upper() or "MOPF 2" in user_msg.upper():
            entity_constraints.append("Line Entity: MOPF2 (ใช้ WHERE Line LIKE '%MOPF2%' OR Line LIKE '%MOPF#2%')")
        else:
            entity_constraints.append("Line Entity: MOPF (ใช้ WHERE Line LIKE '%MOPF%')")
        logger.info("[ENTITY] Added MOPF constraint")
    
    # === Line Name Matching (New) ===
    lines_list = db_context.get("lines", [])
    for line_name in lines_list:
        if line_name.upper() in user_msg.upper() and line_name.upper() not in ["PCB", "MOPF"]:
            entity_constraints.append(f"Line Entity: {line_name}")
            logger.info(f"[ENTITY] Detected line: {line_name}")
            break
    
    # === Process Name Matching (New) ===
    processes_list = db_context.get("processes", [])
    for process_name in processes_list:
        if process_name.upper() in user_msg.upper():
            entity_constraints.append(f"Process Entity: {process_name}")
            logger.info(f"[ENTITY] Detected process: {process_name}")
            break
    
    # === Tech Name Matching (Enhanced) ===
    tech_names = db_context.get("techs", [])
    logger.info(f"[ENTITY] Available techs: {len(tech_names)} names")
    
    # ค้นหาชื่อช่างที่ตรงกัน (improved matching)
    for word in words:
        if len(word) >= 2:
            # ค้นหาแบบ exact match ก่อน
            exact_matches = [tech for tech in tech_names if word.lower() in tech.lower()]
            if exact_matches:
                best_match = exact_matches[0]  # เอาตัวแรก
                entity_constraints.append(f"Tech Entity: {best_match}")
                logger.info(f"[ENTITY] Tech exact match: '{word}' -> '{best_match}'")
                break
    
    # === Fuzzy Matching สำหรับคำอื่นๆ (Enhanced) ===
    for word in words:
        # ข้ามคำสั้นๆ หรือคำเชื่อม
        if len(word) < 3: 
            continue
            
        # เช็คกับฐานข้อมูล (ลดเกณฑ์เป็น 80% เพื่อให้จับได้มากขึ้น)
        match = fuzz_process.extractOne(word, db_context["suggestions"], scorer=fuzz.ratio)
        if match and match[1] >= 80 and match[1] < 100:
            logger.info(f"[ENTITY] Fuzzy match: {word} -> {match[0]} (Score: {match[1]})")
            corrected_msg = corrected_msg.replace(word, match[0])
    
    # เช็คคำผสม เช่น "PCB C" -> "PCB-C"
    for suggestion in db_context["suggestions"]:
        if "-" in suggestion:
            # แยกคำด้วย dash
            parts = suggestion.split("-")
            if len(parts) == 2:
                # สร้างรูปแบบที่อาจพิมพ์ผิด เช่น "PCB C" แทน "PCB-C"
                wrong_format = f"{parts[0]} {parts[1]}"
                if wrong_format in corrected_msg:
                    logger.info(f"[ENTITY] Compound match: {wrong_format} -> {suggestion}")
                    corrected_msg = corrected_msg.replace(wrong_format, suggestion)
    
    # === Add Entity Constraints to Message ===
    if entity_constraints:
        constraint_text = "\n\nข้อมูล Entity ที่พบในฐานข้อมูล (โปรดใช้ชื่อเหล่านี้ใน SQL):\n"
        constraint_text += "\n".join([f"- {constraint}" for constraint in entity_constraints])
        corrected_msg = f"{corrected_msg}{constraint_text}"
        logger.info(f"[ENTITY] Added {len(entity_constraints)} entity constraints")
    
    logger.info(f"[ENTITY] Final result: '{corrected_msg[:200]}...'")
    return corrected_msg

def _needs_pm_attach(sql):
    """เช็คว่า SQL ใช้ตาราง pm.* หรือไม่ (ต้อง ATTACH PM2025.db ก่อนรัน)"""
    return sql and "pm." in sql.upper()

def _restore_pm_prefix(sql):
    """ถ้า AI แก้ SQL แล้วเอา pm. ออก (กลายเป็น FROM PM) ให้ใส่ pm. กลับ — ใช้หลัง AI fix"""
    if not sql or "pm." in sql:
        return sql
    # ตารางใน PM2025 ที่รู้จัก
    for table in ["PM", "ReportPM_CID1", "ReportPM_CID2", "ReportPM_TEST", "Checksheet_CID1", "Checksheet_CID2", "Checksheet_CID3", "Checksheet_TEST", "Checksheet_TEST2", "Checksheet_MOPF2", "Checksheet_LED_AUTO", "PMTest", "PM_SMT"]:
        # FROM PM / JOIN PM ฯลฯ → FROM pm.PM
        sql = re.sub(rf"\b(FROM|JOIN)\s+{re.escape(table)}\b", rf"\1 pm.{table}", sql, flags=re.IGNORECASE)
    return sql

def _normalize_pm_date_column(sql):
    """ตาราง PM: ใช้ Due_date_ymd (รูปแบบ YYYY-MM-DD เดียวกัน) แทน Due date/Date — รันบน work DB"""
    if not sql:
        return sql
    # "Due date" และ Date → Due_date_ymd (คอลัมน์ที่จัด format แล้ว อยู่ใน work DB)
    sql = re.sub(r'"Due date"', "Due_date_ymd", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\b(WHERE|AND|OR)\s+Date\b", r'\1 Due_date_ymd', sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bORDER\s+BY\s+Date\b", r'ORDER BY Due_date_ymd', sql, flags=re.IGNORECASE)
    return sql

def _resolve_pm_db_path():
    """หา path จริงของ PM2025.db (รองรับรันจากโฟลเดอร์อื่นหรือ Docker volume)"""
    candidates = [
        PM2025_DB_PATH,
        os.path.join(os.getcwd(), "data", "PM2025.db"),
        os.path.join(os.getcwd(), "backend", "data", "PM2025.db"),
        os.path.join(_BASE_DIR, "PM2025.db"),
    ]
    for p in candidates:
        if p and os.path.isfile(p):
            return p
    return None

def _normalize_pm_columns(sql):
    """ตาราง pm.PM: Status→Progress, Remark→Description, Date/\"Due date\"→Due_date_ymd, TaskName→\"Task Name\"; Progress ใช้ Not started/Completed ไม่มี Pending"""
    if not sql:
        return sql
    sql = _normalize_pm_date_column(sql)
    sql = re.sub(r"\bStatus\b", "Progress", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bRemark\b", "Description", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bTaskName\b", '"Task Name"', sql, flags=re.IGNORECASE)
    # ค่า Progress ใน PM คือ Not started (ยังไม่เสร็จ), Completed (เสร็จ) — ไม่มี Pending; แก้เงื่อนไขผิด
    sql = re.sub(
        r"Progress\s*=\s*['\"]Pending['\"]",
        "(TRIM(LOWER(COALESCE(Progress,''))) != 'completed')",
        sql,
        flags=re.IGNORECASE,
    )
    return sql

def _is_pm_only_query(sql):
    """เช็คว่า SQL ใช้เฉพาะตาราง pm.* (ไม่มี repairs_enriched) — รันกับ PM2025.db อย่างเดียวได้"""
    if not sql:
        return False
    u = sql.upper()
    return "PM." in u and "REPAIRS_ENRICHED" not in u

def _is_pm_only_sql(sql):
    """เช็คว่า SQL ใช้เฉพาะตาราง PM (ไม่มี repairs_enriched และไม่มีตารางอื่นเช่น ReportPM, Checksheet) — รันกับ PM2025.db ได้เลย"""
    if not sql:
        return False
    u = sql.upper()
    if "REPAIRS_ENRICHED" in u:
        return False
    if "FROM PM" not in u and "JOIN PM" not in u:
        return False
    # ถ้ามีตารางอื่นจาก PM2025 (ReportPM, Checksheet, PMTest, PM_SMT) ไม่ถือว่าเป็น PM-only
    other_pm_tables = ["REPORTPM", "CHECKSHEET", "PMTEST", "PM_SMT"]
    for t in other_pm_tables:
        if t in u and ("FROM " + t in u or "JOIN " + t in u):
            return False
    return True

def _force_pm_table_only(sql):
    """คำถาม PM: บังคับให้ใช้เฉพาะตาราง PM — แทนที่ FROM/JOIN ตารางอื่น (ReportPM, Checksheet ฯลฯ) เป็น PM"""
    if not sql:
        return sql
    other_tables = [
        "ReportPM_CID1", "ReportPM_CID2", "ReportPM_TEST",
        "Checksheet_CID1", "Checksheet_CID2", "Checksheet_CID3", "Checksheet_TEST",
        "Checksheet_TEST2", "Checksheet_MOPF2", "Checksheet_LED_AUTO",
        "PMTest", "PM_SMT"
    ]
    out = sql
    for t in other_tables:
        out = re.sub(rf"\bFROM\s+{re.escape(t)}\b", "FROM PM", out, flags=re.IGNORECASE)
        out = re.sub(rf"\bJOIN\s+{re.escape(t)}\b", "JOIN PM", out, flags=re.IGNORECASE)
    if out != sql:
        logger.info("📋 PM: forced SQL to use only table PM (replaced other table names)")
    return out

def _get_pm_period_range(user_msg):
    """คำถาม PM เกี่ยวกับช่วงเวลา (เดือนนี้/ปีนี้/ปี X/ปีหน้า/สัปดาห์นี้) → คืน (start_date, end_date). คืน None ถ้าไม่ใช่ช่วงเวลาที่กำหนด"""
    if not user_msg or not str(user_msg).strip():
        return None
    msg = str(user_msg).strip()
    now = pd.Timestamp.now()
    today = now.strftime("%Y-%m-%d")
    cur_year = now.year
    # ปีที่ระบุเป็นตัวเลข (ปี 2025, ปี 2026)
    m_year = re.search(r"ปี\s*(\d{4})", msg)
    if m_year:
        y = int(m_year.group(1))
        return (f"{y}-01-01", f"{y}-12-31")
    # ปีหน้า
    if "ปีหน้า" in msg or "next year" in msg.lower():
        return (f"{cur_year + 1}-01-01", f"{cur_year + 1}-12-31")
    # ปีนี้ = ทั้งปี (1 ม.ค. – 31 ธ.ค. ของปีปัจจุบัน)
    if "ปีนี้" in msg or "this year" in msg.lower():
        return (f"{cur_year}-01-01", f"{cur_year}-12-31")
    # เดือนหน้า (เฉพาะเดือนถัดไป)
    if "เดือนหน้า" in msg or "next month" in msg.lower():
        if now.month == 12:
            first_next = now.replace(year=now.year + 1, month=1, day=1)
        else:
            first_next = now.replace(month=now.month + 1, day=1)
        next_month_start = first_next.strftime("%Y-%m-%d")
        next_month_end = (first_next + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")
        return (next_month_start, next_month_end)
    # สัปดาห์นี้ / อาทิตย์นี้ (จันทร์–อาทิตย์ ของสัปดาห์ปัจจุบัน)
    if "สัปดาห์นี้" in msg or "อาทิตย์นี้" in msg or "this week" in msg.lower():
        week_start = (now - pd.Timedelta(days=now.dayofweek)).strftime("%Y-%m-%d")
        week_end = (now + pd.Timedelta(days=6 - now.dayofweek)).strftime("%Y-%m-%d")
        return (week_start, week_end)
    # สัปดาห์หน้า / อาทิตย์หน้า (จันทร์–อาทิตย์ ของสัปดาห์ถัดไป)
    if "สัปดาห์หน้า" in msg or "อาทิตย์หน้า" in msg or "next week" in msg.lower():
        next_monday = now + pd.Timedelta(days=7 - now.dayofweek)
        next_week_start = next_monday.strftime("%Y-%m-%d")
        next_week_end = (next_monday + pd.Timedelta(days=6)).strftime("%Y-%m-%d")
        return (next_week_start, next_week_end)
    # เดือนนี้ (วันที่ 1 ถึง วันสุดท้ายของเดือน)
    if "เดือนนี้" in msg or "this month" in msg.lower():
        month_start = now.replace(day=1).strftime("%Y-%m-%d")
        month_end = (now.replace(day=1) + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")
        return (month_start, month_end)
    # เดือนที่เท่าไหร่: เดือน 12, เดือน 1, เดือนมกราคม ฯลฯ
    month_names = {
        "มกราคม": 1, "กุมภาพันธ์": 2, "มีนาคม": 3, "เมษายน": 4, "พฤษภาคม": 5, "มิถุนายน": 6,
        "กรกฎาคม": 7, "สิงหาคม": 8, "กันยายน": 9, "ตุลาคม": 10, "พฤศจิกายน": 11, "ธันวาคม": 12,
    }
    for th_name, num in month_names.items():
        if th_name in msg or f"เดือน {num}" in msg or f"เดือน {num} " in msg:
            y = cur_year
            my = re.search(r"ปี\s*(\d{4})", msg)
            if my:
                y = int(my.group(1))
            start = f"{y}-{num:02d}-01"
            if num == 12:
                end = f"{y}-12-31"
            else:
                end_d = datetime(y, num + 1, 1).date() - timedelta(days=1)
                end = end_d.strftime("%Y-%m-%d")
            return (start, end)
    m_month_num = re.search(r"เดือน\s*(\d{1,2})", msg)
    if m_month_num:
        num = int(m_month_num.group(1))
        if 1 <= num <= 12:
            y = cur_year
            my = re.search(r"ปี\s*(\d{4})", msg)
            if my:
                y = int(my.group(1))
            start = f"{y}-{num:02d}-01"
            if num == 12:
                end = f"{y}-12-31"
            else:
                end_d = datetime(y, num + 1, 1).date() - timedelta(days=1)
                end = end_d.strftime("%Y-%m-%d")
            return (start, end)
    return None

def _fix_pm_sqlite_compat(sql):
    """ลบ/แปลง syntax แบบ PostgreSQL (DATE_TRUNC, INTERVAL, DueDate) ในคำถาม PM ให้ SQLite รันได้; ลบ "; AND" ที่ผิด"""
    if not sql or ("FROM PM" not in sql.upper() and "JOIN PM" not in sql.upper()):
        return sql
    out = sql
    # ลบ semicolon กลางประโยค (WHERE ... ; AND ... → WHERE ... AND ...)
    out = re.sub(r";\s*AND\s+", " AND ", out, flags=re.IGNORECASE)
    # DueDate → Due_date_ymd (คอลัมน์วันที่จัด format แล้ว ใน work DB)
    out = re.sub(r"\bDueDate\b", "Due_date_ymd", out, flags=re.IGNORECASE)
    # ลบ condition ทั้งก้อนที่ใช้ BETWEEN ... INTERVAL (SQLite ไม่รองรับ) — ใช้ .+ แบบ greedy ให้ถึง INTERVAL ตัวสุดท้าย
    out = re.sub(
            r"\s*AND\s+(?:Due_date_ymd|\"Due date\"|DueDate)\s+BETWEEN\s+.+INTERVAL\s*\'[^\']*\'[^;]*?(?=\s+AND\s+|\s*;\s*$|\s+ORDER\s+BY|\s+GROUP\s+BY|\s+LIMIT)",
        " ",
        out,
        flags=re.IGNORECASE | re.DOTALL,
    )
    # ลบ expression อื่นที่เหลือ DATE_TRUNC(...) หรือ INTERVAL '...' (เผื่อยังค้าง)
    out = re.sub(r"DATE_TRUNC\s*\([^)]+\)", "", out, flags=re.IGNORECASE)
    out = re.sub(r"INTERVAL\s*\'[^\']*\'", "", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+AND\s+AND\s+", " AND ", out, flags=re.IGNORECASE)
    out = re.sub(r"^\s*AND\s+", "", out, flags=re.IGNORECASE)
    if out != sql:
        logger.info("📋 PM: removed PostgreSQL-style date (DATE_TRUNC/INTERVAL) for SQLite")
    return out

def _fix_pm_year_from_user_message(user_msg, sql):
    """ถ้าผู้ใช้ถาม ปี YYYY (เช่น ปี 2026) และ SQL เป็น PM → แก้ช่วง Due_date_ymd เป็น 1 ม.ค.–31 ธ.ค. ของปีนั้น (ถามปีไหนตอบปีนั้น)"""
    if not sql or not user_msg:
        return sql
    u = sql.upper()
    if "FROM PM" not in u and "JOIN PM" not in u:
        return sql
    if ("\"DUE DATE\"" not in u) and ("PM_REAL_DATE" not in u):
        return sql
    year_m = re.search(r"ปี\s*(\d{4})", user_msg)
    if not year_m:
        return sql
    y = int(year_m.group(1))
    if not (2000 <= y <= 2100):
        return sql
    start_ymd = f"{y}-01-01"
    end_ymd = f"{y}-12-31"
    new_cond = f"Due_date_ymd >= '{start_ymd}' AND Due_date_ymd <= '{end_ymd}'"
    # แทนที่รูปแบบ Due_date_ymd / "Due date" BETWEEN ... หรือ >= ... AND <= ...
    sql_out = re.sub(
        r'(?:Due_date_ymd|"Due date")\s*BETWEEN\s*[\'"]\d{4}-\d{2}-\d{2}[\'"]\s*AND\s*[\'"]\d{4}-\d{2}-\d{2}[\'"]',
        new_cond,
        sql,
        flags=re.IGNORECASE,
    )
    sql_out = re.sub(
        r'(?:Due_date_ymd|"Due date")\s*>=\s*[\'"]\d{4}-\d{2}-\d{2}[\'"]\s*AND\s*(?:Due_date_ymd|"Due date")\s*<=\s*[\'"]\d{4}-\d{2}-\d{2}[\'"]',
        new_cond,
        sql_out,
        flags=re.IGNORECASE,
    )
    if sql_out != sql:
        logger.info(f"📋 แก้ช่วงปี PM ตามคำถาม: {y}-01-01 ถึง {y}-12-31")
    return sql_out

def _pm_unify_date_column(df):
    """รวม Start date และ Due date เป็น column ชื่อ date: ขยายแถวตามวันที่ (Start/Due), วันเดียวกันและข้อมูลเหมือนกันแสดง 1 แถว, วันเดียวกันแต่ข้อมูลไม่เหมือนเอามาทั้งหมด"""
    if df.empty:
        return df
    start_col = None
    due_col = None
    for c in df.columns:
        if re.match(r"Start\s*date|Start date", str(c), re.I):
            start_col = c
        if re.match(r"Due\s*date|Due date|Due_date_ymd", str(c), re.I):
            due_col = c
    if not start_col and not due_col:
        return df
    out = []
    for _, row in df.iterrows():
        dates = []
        if start_col and pd.notna(row.get(start_col)) and str(row.get(start_col)).strip():
            d = str(row.get(start_col)).strip()[:10]
            if re.match(r"\d{4}-\d{2}-\d{2}", d):
                dates.append(d)
        if due_col and pd.notna(row.get(due_col)) and str(row.get(due_col)).strip():
            d = str(row.get(due_col)).strip()[:10]
            if re.match(r"\d{4}-\d{2}-\d{2}", d) and d not in dates:
                dates.append(d)
        if not dates:
            new_row = row.to_dict()
            new_row["date"] = None
            out.append(new_row)
            continue
        for d in dates:
            new_row = row.to_dict()
            new_row["date"] = d
            out.append(new_row)
    if not out:
        return df
    result = pd.DataFrame(out)
    # ลบ Start date / Due date ออก (เหลือแค่ date)
    for col in list(result.columns):
        if col in (start_col, due_col) or (col != "date" and re.match(r"Start\s*date|Due\s*date|Due_date_ymd", str(col), re.I)):
            result = result.drop(columns=[col], errors="ignore")
    # วันเดียวกันและข้อมูลเหมือนกัน → แสดง 1; วันเดียวกันแต่ข้อมูลไม่เหมือน → เอามาทั้งหมด
    result = result.drop_duplicates()
    logger.info(f"📋 PM: รวม Start date + Due date เป็น column date, dedupe แล้ว {len(result)} แถว")
    return result

def _is_readonly_sql(sql):
    """เช็คว่า SQL เป็นแค่ SELECT (ไม่อนุญาต DROP/DELETE/UPDATE ฯลฯ) — ใช้ก่อนรันกับ PM2025.db เพื่อป้องกันตารางหาย"""
    if not sql or not sql.strip():
        return False
    u = sql.upper().strip()
    # อนุญาตแค่ SELECT (อาจมี WITH ... SELECT)
    if u.startswith("SELECT") or u.startswith("WITH"):
        for kw in ("DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "REPLACE", "CREATE "):
            if kw in u:
                return False
        return True
    return False

def _pm_db_connection_readonly(pm_db_path):
    """เปิด PM2025.db แบบ read-only (กันไม่ให้คำสั่งเขียน/ลบทำลายตาราง)"""
    abs_path = os.path.abspath(pm_db_path).replace("\\", "/")
    # file:///C:/path (Windows) หรือ file:///path (Unix)
    if abs_path.startswith("/"):
        uri = f"file:{abs_path}?mode=ro"
    else:
        uri = f"file:///{abs_path}?mode=ro"
    return sqlite3.connect(uri, uri=True)

class SqlExecutionError(Exception):
    """เกิดเมื่อรัน SQL ไม่สำเร็จหลัง retry + LLM fix — ใช้ส่ง error message กลับให้ handler ทำ Auto-Retry ได้"""
    def __init__(self, message, sql_used=""):
        self.sql_used = sql_used
        super().__init__(message)

def execute_sql_safe(
    sql: str,
    retries: int = 2,
    skip_pm_column_normalize: bool = False,
    user_msg: Optional[str] = None,
    limit_override: Optional[int] = None,
    skip_limit_enforcement: bool = False,
) -> Tuple[pd.DataFrame, str]:
    """
    รัน SQL กับตารางที่ถูกต้อง: PM2025.db สำหรับ PM (FROM PM เท่านั้น), work DB (repair_enriched.db) สำหรับซ่อม (repairs_enriched).
    ถ้า Execute Error จะส่ง error ให้ LLM แก้ SQL (Auto-Retry ในฟังก์ชัน); หลัง retry หมดแล้วจะ raise SqlExecutionError.
    skip_pm_column_normalize: True = โหมดตอบอีกครั้ง ไม่แปลง Progress/Pending ด้วย regex
    """
    attempt = 0
    current_sql = sql
    # แก้คอลัมน์ PM (โหมดตอบอีกครั้งไม่แก้ — บังคับใน Prompt ว่า Progress มีแค่ Not started/Completed)
    if current_sql and ("FROM PM" in current_sql.upper() or "JOIN PM" in current_sql.upper()) and not skip_pm_column_normalize:
        current_sql = _normalize_pm_columns(current_sql)
    
    if not skip_limit_enforcement:
        current_sql = ensure_limit_5(current_sql, user_msg=user_msg, limit_override=limit_override)

    # คำถาม PM → รันกับ PM2025.db โดยตรง (ตาราง PM อยู่ในไฟล์นี้เสมอ ไม่พึ่ง work DB)
    # เปิด PM2025.db แบบ read-only และรันแค่ SELECT เพื่อป้องกันตาราง PM หาย (จาก AI ส่ง DROP/DELETE ฯลฯ)
    pm_db = _resolve_pm_db_path()
    # ถ้า SQL อ้างถึงคอลัมน์ที่สร้างใน work DB (PM_real_date/IsPostponed/Due_date_ymd) ให้ข้ามการรันบน PM2025.db
    # เพราะ PM2025.db ไม่มีคอลัมน์เหล่านี้ — จะให้ไป fallback work DB เลยเพื่อเร็วและไม่ spam warning
    has_work_only_pm_cols = current_sql and any(k in current_sql.upper() for k in ["PM_REAL_DATE", "ISPOSTPONED", "DUE_DATE_YMD"])
    if _is_pm_only_sql(current_sql) and pm_db and _is_readonly_sql(current_sql) and not has_work_only_pm_cols:
        try:
            with _pm_db_connection_readonly(pm_db) as conn:
                df = pd.read_sql_query(current_sql, conn)
            logger.info(f"📋 PM query ran on PM2025.db directly (read-only): {pm_db}")
            return df, current_sql
        except Exception as e_pm:
            logger.warning(f"PM2025.db query failed (will try work DB): {e_pm}")
    elif _is_pm_only_sql(current_sql) and not _is_readonly_sql(current_sql):
        logger.warning("📋 PM query rejected: SQL is not read-only (PM2025.db is protected)")
    elif _is_pm_only_sql(current_sql) and not pm_db:
        logger.warning("📋 PM query but PM2025.db not found at any candidate path")
    
    while attempt <= retries:
        try:
            with sqlite3.connect(WORK_DB_PATH) as conn:
                df = pd.read_sql_query(current_sql, conn)
            return df, current_sql
        except Exception as e:
            err_str = str(e)
            logger.warning(f"SQL Failed (Attempt {attempt+1}): {e}")
            # ถ้า no such table: PM และมีไฟล์ PM2025 → รันกับ PM2025.db โดยตรง (read-only)
            if ("no such table: PM" in err_str or "no such table: pm" in err_str.lower()) and pm_db and _is_pm_only_sql(current_sql) and _is_readonly_sql(current_sql):
                try:
                    with _pm_db_connection_readonly(pm_db) as conn:
                        df = pd.read_sql_query(current_sql, conn)
                    logger.info("📋 PM query succeeded on PM2025.db (fallback, read-only)")
                    return df, current_sql
                except Exception as e2:
                    logger.warning(f"PM2025 fallback also failed: {e2}")
                    raise e2
            attempt += 1
            error_msg = str(e)
            if attempt > retries:
                raise SqlExecutionError(error_msg, current_sql or sql)
            is_pm_fix = current_sql and ("FROM PM" in current_sql.upper() or "JOIN PM" in current_sql.upper())
            pm_note = ""
            if is_pm_fix:
                pm_note = ' Table PM: "Task Name", Due_date_ymd, Progress, Description (Line, Due_date_ymd exist in work DB). Progress values: Not started, Completed — do NOT use Pending. Do NOT use ปัญหา, สาเหตุ, Process, Tech, RepairMinutes, Date, Status (use Progress). Use "Task Name" not TaskName.'
            fix_prompt = f"""The following SQLite query failed:
SQL: {current_sql}
Error: {error_msg}
Schema: {db_context.get('schema_str', '')}
{pm_note}

Task: Fix the SQL query. Use table PM (not pm.PM) for PM data; use repairs_enriched for repair data. Output ONLY the fixed SQL."""
            try:
                res = requests.post(OLLAMA_GENERATE_URL, json={
                    "model": MODEL_NAME,
                    "prompt": fix_prompt,
                    "stream": False,
                    "options": {"temperature": 0.1, "num_predict": 256}
                }, timeout=100)
                current_sql = clean_sql(res.json().get("response", ""))
                if current_sql and ("FROM PM" in current_sql.upper() or "JOIN PM" in current_sql.upper()) and not skip_pm_column_normalize:
                    current_sql = _normalize_pm_columns(current_sql)
                if not skip_limit_enforcement:
                    current_sql = ensure_limit_5(current_sql, user_msg=user_msg, limit_override=limit_override)
                logger.info(f"AI Fixed SQL (Auto-Retry): {current_sql}")
            except Exception:
                break
    # หลัง retry หมดแล้ว — raise เพื่อให้ handler ส่ง error กลับให้ Qwen/LLM ลองแก้ SQL ใหม่ได้ (Auto-Retry ระดับ handler)
    raise SqlExecutionError(error_msg, current_sql or sql)

def validate_logic(user_msg, sql):
    """
    ฟังก์ชัน validate_logic (ตรวจสอบตรรกะ SQL ก่อนรัน)
    ปัญหา: AI ชอบมั่วตรรกะ เช่น ถามว่า "ใครซ่อมเร็วสุด" แต่ดันเขียน ORDER BY RepairMinutes DESC (กลายเป็นซ่อมนานสุด)
    วิธีแก้: ใช้ Rule-based เช็ค keyword ในคำถามเทียบกับ SQL
    """
    msg_lower = user_msg.lower()
    sql_upper = sql.upper()
    
    # กฎข้อที่ 1: ถามหาความเร็ว (Fast/Response) ต้องเรียง ASC
    if any(w in msg_lower for w in ['เร็วที่สุด', 'ไวที่สุด', 'fastest', 'quickest', 'เริ่มซ่อมไว', 'ตอบสนองเร็ว', 'มาไว', 'ซ่อมเสร็จไว']):
        if 'DESC' in sql_upper and 'LIMIT' in sql_upper:
            logger.warning("Logic Mismatch: User asked for FASTEST but SQL uses DESC. Switching to ASC.")
            return sql.replace('DESC', 'ASC')
    
    # กฎข้อที่ 2: ถามหาความช้า (Slow/Longest) ต้องเรียง DESC
    if any(w in msg_lower for w in ['นานที่สุด', 'ช้าที่สุด', 'slowest', 'longest', 'เริ่มซ่อมช้า', 'ตอบสนองช้า', 'มาช้า', 'ซ่อมนาน']):
        if 'ASC' in sql_upper and 'LIMIT' in sql_upper:
            logger.warning("Logic Mismatch: User asked for SLOWEST but SQL uses ASC. Switching to DESC.")
            return sql.replace('ASC', 'DESC')
    
    return sql

# --- 4 NEW CRITICAL ENHANCEMENT FUNCTIONS ---

def verify_sql_columns(sql):
    """
    ฟังก์ชัน verify_sql_columns (กันมั่วชื่อคอลัมน์/ค่าข้อมูล) 🛡️ สำคัญมาก
    ปัญหา: AI ชอบเผลอเติมคำเอง เช่น ใน DB มี Machine_A แต่ AI เขียน WHERE Line = 'Machine A' (มีเว้นวรรค) 
    หรือเขียนชื่อคอลัมน์ผิด 
    หน้าที่: สแกน SQL ก่อนรัน ถ้าเจอชื่อตารางหรือคอลัมน์ที่ไม่มีอยู่จริง ให้แก้หรือ Error ทันที
    """
    if not db_context.get("columns"):
        return sql  # ถ้าไม่มี schema ให้ผ่านไป
    # SQL ที่ใช้ตาราง PM (หรือ pm.) — อย่าเอา schema ของ repairs_enriched ไปแก้
    if _needs_pm_attach(sql) or (sql and ("FROM PM" in sql.upper() or "JOIN PM" in sql.upper())):
        return sql

    valid_columns = [c.lower() for c in db_context["columns"]]
    
    # ดึงชื่อคอลัมน์ที่ถูกเรียกใช้ใน SQL (แบบคร่าวๆ)
    # หาคำหลัง SELECT, WHERE, ORDER BY, GROUP BY
    words = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', sql)
    
    # คำสงวน SQL (ไม่ต้องเช็ค) — รวมชื่อตาราง/คอลัมน์จาก pm (PM2025) และ alias ที่ใช้ใน aggregate
    sql_keywords = {
        'select', 'from', 'where', 'group', 'by', 'order', 'limit', 'desc', 'asc', 
        'count', 'sum', 'avg', 'and', 'or', 'like', 'in', 'between', 'as', 'date', 
        'strftime', 'repairs_enriched', 'distinct', 'not', 'null', 'is', 'max', 'min',
        'having', 'case', 'when', 'then', 'else', 'end', 'inner', 'left', 'right',
        'join', 'on', 'union', 'all', 'exists', 'coalesce', 'cast', 'substr', 'total',
        'daily', 'count', 'freq', 'amount', 'value', 'rate', 'ratio',
        'pm', 'attach', 'database',
        'checksheet_cid1', 'checksheet_cid2', 'checksheet_cid3', 'checksheet_test', 'checksheet_test2',
        'checksheet_mopf2', 'checksheet_led_auto', 'reportpm_cid1', 'reportpm_cid2', 'reportpm_test',
        'pmtest', 'pm_smt',
        'line', 'machine', 'status', 'problem', 'remark', 'due', 'actual', 'plan',
        'equipment', 'spec', 'document', 'method', 'items', 'progress', 'priority', 'assigned', 'bucket',
        'recurring', 'completed', 'checklist', 'labels', 'description', 'in-charge', 'checked', 'approved',
        # alias / นิพจน์ที่ใช้ใน SELECT aggregate (ห้าม autocorrect เป็นชื่อคอลัมน์จริง)
        'totalrepairminutes', 'totalresponseminutes', 'repaircount', 'callcount', 'float',
        'avgresponsetime', 'totalrepairs',
        # SQLite string functions (ป้องกัน false positive warning)
        'trim', 'ltrim', 'rtrim', 'upper', 'lower', 'length', 'replace', 'instr'
    }
    
    suspicious = []
    corrections = {}
    
    for w in words:
        w_lower = w.lower()
        if w_lower not in sql_keywords and not w.isdigit():
            # ถ้าไม่ใช่ Keyword และไม่ใช่ตัวเลข -> ต้องเป็นชื่อคอลัมน์ หรือ ค่าข้อมูล
            if w_lower in valid_columns: 
                continue  # ผ่าน
            
            # ตรวจสอบว่าอยู่ใน quote หรือไม่ (ถ้าอยู่ใน quote คือค่าข้อมูล ปล่อยผ่าน)
            if f"'{w}'" in sql or f'"{w}"' in sql:
                continue  # เป็นค่าข้อมูล ปล่อยผ่าน
            
            # หาคำที่คล้ายกันในคอลัมน์จริง
            match = fuzz_process.extractOne(w_lower, valid_columns, scorer=fuzz.ratio)
            if match and match[1] >= 80:  # ความเหมือน 80% ขึ้นไป
                corrections[w] = match[0]
                logger.warning(f"Column autocorrect: {w} -> {match[0]}")
            else:
                suspicious.append(w)
    
    # แก้ไข SQL ด้วยคำที่ถูกต้อง
    corrected_sql = sql
    for wrong, correct in corrections.items():
        # แทนที่แบบระวัง (ไม่แทนที่ถ้าอยู่ใน quote)
        pattern = r'\b' + re.escape(wrong) + r'\b(?![\'"])'
        corrected_sql = re.sub(pattern, correct, corrected_sql, flags=re.IGNORECASE)
    
    # ถ้ามีคำที่น่าสงสัยมาก ให้เตือน
    if suspicious:
        logger.warning(f"Suspicious words in SQL (might be typos): {suspicious}")
    
    return corrected_sql

def disambiguate_question(user_msg):
    """
    Guardrail: ถามกลับเมื่อคำถามคลุมเครือ (เช่น ไม่ระบุชื่อ Line ชัดเจน, เครื่อง 1 มีหลายตัวเลือก).
    ใช้ก่อนสร้าง SQL; ข้ามเมื่อโหมด ai_100.
    """
    if not user_msg or not str(user_msg).strip():
        return None
    if not db_context.get("suggestions"):
        return None
    
    msg_lower = str(user_msg).strip().lower()
    msg = str(user_msg).strip()
    words = msg.split()
    
    # 🔥 เช็คคำถามที่คลุมเครือมากๆ (มีแค่คำอุทานโดยไม่มีบริบท)
    vague_questions = [
        'อะไร', 'ไร', 'ยังไง', 'ทำไง', 'ช่วย', 'บอก', 'หน่อย'
    ]
    # ต้องเป็นคำถามสั้น (1-2 คำ) และมีแค่คำคลุมเครือ ไม่มีบริบท
    if len(words) <= 2 and any(vq in msg_lower for vq in vague_questions):
        # มีคำว่า สาเหตุ / ปัญหา / เพราะอะไร = คำถามชัดเจน (ต้องการดูคอลัมน์ ปัญหา, สาเหตุ ในผลลัพธ์) ไม่ถือว่าเคลียร์
        specific_repair = ['line', 'pm', 'ช่าง', 'เครื่อง', 'ซ่อม', 'เสีย', 'วันนี้', 'เดือน', 'ทีม', 'ไลน์', 'ปัญหา', 'สาเหตุ', 'เพราะอะไร', 'อาการ', 'เหตุผล', 'แก้ไข']
        if not any(specific in msg_lower for specific in specific_repair):
            return {
                "type": "too_vague",
                "text": "ขอคำถามละเอียดกว่านี้หน่อยได้ไหมคะ? ลองถามว่าต้องการรู้เรื่องอะไร เช่น:\n- การซ่อม: 'วันนี้มีอะไรเสียบ้าง', 'ช่างไหนซ่อมเยอะที่สุด'\n- PM: 'เดือนนี้มี PM อะไรบ้าง', 'PM ที่เลื่อนไปในปีนี้'"
            }
    
    # ตัวอย่าง: เช็คเรื่อง Line/Machine ที่กำกวม
    ambiguous_patterns = [
        (r'เครื่อง\s*(\d+)', 'เครื่อง'),
        (r'line\s*(\d+)', 'line'),
        (r'machine\s*(\d+)', 'machine'),
        (r'ช่าง\s*(\d+)', 'ช่าง'),
        (r'ทีม\s*([a-zA-Z])', 'ทีม')
    ]
    
    for pattern, category in ambiguous_patterns:
        match = re.search(pattern, msg_lower)
        if match:
            search_term = match.group(1)
            # ทีม A / ทีม B / ทีม C = ชัดเจน (ตัวอักษรเดียว = ชื่อทีม) ไม่ถามกลับ
            if category == 'ทีม' and len(search_term) == 1:
                continue
            # หาตัวเลือกที่เป็นไปได้ใน DB
            matches = []
            for suggestion in db_context["suggestions"]:
                if search_term in str(suggestion).lower():
                    matches.append(suggestion)
            # ถ้ามีตัวเลือกมากกว่า 1 ให้ถามกลับ
                matches_str = [str(m) for m in matches]
                # Avoid slicing for Pyre2 strictness
                subset = []
                for i in range(min(len(matches_str), 5)):
                    subset.append(matches_str[i])
                display_matches = ", ".join(subset)
                suffix = "..." if len(matches) > 5 else ""
                
                return {
                    "type": "ambiguous_entity",
                    "text": f"หมายถึง{category}ไหนคะ? มี {display_matches}{suffix}"
                }
                suggestions_list = list(db_context.get("suggestions", []))
                similar_suggestions = []
                count = 0
                for s in suggestions_list:
                    s_str = str(s)
                    if any(char.isdigit() for char in s_str) and len(s_str) < 20:
                        similar_suggestions.append(s_str)
                        count += 1
                        if count >= 5: break
                
                if similar_suggestions:
                    return {
                        "type": "not_found", 
                        "text": f"ไม่พบ{category} {search_term} ในระบบค่ะ มีตัวเลือก: {', '.join(similar_suggestions)}"
                    }
    
    # เช็คคำถามที่กำกวมเรื่องเวลา
    time_ambiguous = [
        'เมื่อไหร่', 'ช่วงไหน', 'วันไหน', 'เวลาไหน'
    ]
    
    if any(word in msg_lower for word in time_ambiguous) and not any(word in msg_lower for word in ['วันนี้', 'เมื่อวาน', 'อาทิตย์', 'เดือน', 'ปี', 'สัปดาห์']):
        return {
            "type": "question",
            "text": "ต้องการข้อมูลช่วงเวลาไหนคะ? เช่น วันนี้, เมื่อวาน, อาทิตย์ที่แล้ว, เดือนนี้"
        }
    
    return None  # ถ้าชัดเจนดีแล้ว ให้ผ่านไปทำ SQL ต่อ

def _parse_pm_description_parts(desc):
    """แยก Description (เลื่อน) เป็น: สร้างเมื่อ, ย้ายจากวันที่, ย้ายไปวันไหน, เนื่องจาก, ผู้แจ้ง — ทุกวันที่มาจาก Description (มีคำว่าย้ายจาก) ไม่ใช้ Due date"""
    if not desc or not str(desc).strip():
        return {}
    s = str(desc).strip()
    out = {}
    # สร้างเมื่อ ... (ถ้ามีใน Description)
    m = re.search(r"สร้างเมื่อ\s*([^|]+?)(?=\s*ย้ายจาก|\s*เนื่องจาก|\s*ผู้แจ้ง|$)", s, re.DOTALL | re.IGNORECASE)
    if m:
        out["สร้างเมื่อ"] = m.group(1).strip()[:50]
    # ย้ายจากวันที่ X เป็น Y — แยกทั้งสองวันที่จาก Description (ไม่ใช้ Due date)
    m = re.search(r"ย้ายจากวันที่\s*([^\s]+)\s+เป็น\s+([^\s]+)", s)
    if m:
        out["ย้ายจากวันที่"] = m.group(1).strip()[:30]
        out["ย้ายไปวันไหน"] = m.group(2).strip()[:30]
    else:
        # รูปแบบเก่า: มีแค่ ย้ายจากวันที่ ... (ไม่มี " เป็น ")
        m = re.search(r"ย้ายจากวันที่\s*([^|]+?)(?=\s*เนื่องจาก|\s*ผู้แจ้ง|\s*ต้องการให้|$)", s, re.DOTALL)
        if m:
            out["ย้ายจากวันที่"] = m.group(1).strip()[:50]
    m = re.search(r"เนื่องจาก\s*([^|]+?)(?=\s*ผู้แจ้ง|\s*ต้องการให้|$)", s, re.DOTALL)
    if m:
        out["เนื่องจาก"] = m.group(1).strip()[:120]
    m = re.search(r"ผู้แจ้ง\s*([^|]+?)(?=\s*ต้องการให้|$)", s, re.DOTALL)
    if m:
        out["ผู้แจ้ง"] = m.group(1).strip()[:50]
    m = re.search(r"ต้องการให้\s*([^|]+)", s, re.DOTALL)
    if m:
        out["ต้องการให้"] = m.group(1).strip()[:80]
    return out

def format_pm_description_for_display(desc):
    """แบ่งข้อความ Description (เลื่อน) ตามคำว่า ย้ายจากวันที่, เนื่องจาก, ผู้แจ้ง, ต้องการให้ — สำหรับแสดงผล"""
    if not desc or not str(desc).strip():
        return ""
    s = str(desc).strip()
    seps = ("ย้ายจากวันที่", "เนื่องจาก", "ผู้แจ้ง", "ต้องการให้")
    parts = re.split(r'(' + '|'.join(re.escape(x) for x in seps) + r')', s)
    out = []
    i = 0
    while i < len(parts):
        if parts[i] in seps and i + 1 < len(parts):
            txt = parts[i + 1].strip()[:80]
            if len(parts[i + 1].strip()) > 80:
                txt += "..."
            out.append(parts[i] + ": " + txt)
            i += 2
        else:
            if parts[i].strip():
                out.append(parts[i].strip()[:80])
            i += 1
    return " | ".join(out) if out else (s[:150] + ("..." if len(s) > 150 else ""))

def _parse_year_from_yay_from_date(s):
    """จากข้อความ ย้ายจากวันที่ เช่น 19-12-2025 หรือ 27-12-2025 คืนปี (int) หรือ None ถ้า parse ไม่ได้"""
    if not s or not str(s).strip():
        return None
    s = str(s).strip()
    # รูปแบบ dd-mm-yyyy
    m = re.match(r"(\d{1,2})-(\d{1,2})-(\d{4})$", s)
    if m:
        return int(m.group(3))
    # รูปแบบ yyyy-mm-dd
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m:
        return int(m.group(1))
    return None

def _filter_pm_postpone_by_year(df, user_msg):
    """ถ้าคำถามมี 'ปี YYYY' และ df มีคอลัมน์ ย้ายจากวันที่ → กรองเฉพาะแถวที่ ย้ายจากวันที่ อยู่ในปีนั้น (01-01-YYYY ถึง 31-12-YYYY)"""
    if df.empty or "ย้ายจากวันที่" not in df.columns:
        return df
    year_match = re.search(r"ปี\s*(\d{4})", user_msg.strip())
    if not year_match:
        return df
    year = int(year_match.group(1))
    def in_year(val):
        y = _parse_year_from_yay_from_date(val)
        return y is not None and y == year
    mask = df["ย้ายจากวันที่"].apply(in_year)
    out = df.loc[mask].copy()
    logger.info(f"📋 Filtered PM เลื่อน by ย้ายจากวันที่ in year {year}: {len(df)} -> {len(out)} rows")
    return out

def enrich_pm_postpone_columns(df):
    """เพิ่มคอลัมน์แยกสำหรับ PM เลื่อน จาก Description เท่านั้น: ย้ายจากวันที่, ย้ายไปวันไหน, เนื่องจาก, ผู้แจ้ง (ทุกวันที่มาจากคำว่า ย้ายจาก/เป็น ใน Description ไม่ใช้ Due date)"""
    if df.empty or "Description" not in df.columns:
        return df
    if not any(df["Description"].astype(str).str.contains("ย้าย|เลื่อน", na=False, regex=True)):
        return df
    out = df.copy()
    parsed = out["Description"].astype(str).apply(lambda d: _parse_pm_description_parts(d) if d else {})
    create_vals = parsed.apply(lambda p: p.get("สร้างเมื่อ", ""))
    if (create_vals != "").any():
        out["สร้างเมื่อ"] = create_vals
    out["ย้ายจากวันที่"] = parsed.apply(lambda p: p.get("ย้ายจากวันที่", ""))
    out["ย้ายไปวันไหน"] = parsed.apply(lambda p: p.get("ย้ายไปวันไหน", ""))  # จาก Description เท่านั้น (คำว่า " เป็น " ใน Description)
    out["เนื่องจาก"] = parsed.apply(lambda p: p.get("เนื่องจาก", ""))
    # เหตุผล = ข้อความหลัง "เนื่องจาก......" ใน Description (แยกโดย _parse_pm_description_parts)
    out["เหตุผล"] = out["เนื่องจาก"]
    out["ผู้แจ้ง"] = parsed.apply(lambda p: p.get("ผู้แจ้ง", ""))
    return out

def filter_important_columns(df, sql):
    """
    🔥 กรองเฉพาะ columns ที่สำคัญ ซ่อน columns ที่ไม่จำเป็น (id, Tech_ID, col15-17, extracted_at ฯลฯ)
    """
    if df.empty:
        return df
    
    sql_upper = sql.upper() if sql else ""
    is_pm_query = "FROM PM" in sql_upper
    
    # Columns ที่ไม่ต้องการแสดง (Technical/Internal columns)
    hidden_columns = [
        'id', 'Tech_ID', 'col15', 'col16', 'col17', 'extracted_at', 
        'Date_Obj', 'Call_Time', 'Start_Time', 'End_Time',
        'Call_Time_m', 'Repair_Time_m',  # มี RepairMinutes, ResponseMinutes อยู่แล้ว
        'IsPostponed',  # 🔥 ใช้แค่ใน backend ไม่แสดงในตาราง
        'date'  # 🔥 ซ่อน column "date" ที่สร้างจาก _pm_unify_date_column (ใช้ "Due date" แทน)
    ]
    if is_pm_query:
        # PM: ไม่เอา columns เหล่านี้มาตอบ/แสดง
        hidden_columns = list(hidden_columns) + [
            'Priority', 'Assigned To', 'Bucket Name', 'Labels', 'Task ID',
            'Created By', 'Created Date', 'Start date', 'Is Recurring', 'Late',
            'Completed By', 'Completed Checklist Items', 'Checklist Items', 'File Path'
        ]
    
    # Columns ที่ควรแสดง (ตามลำดับความสำคัญ)
    if is_pm_query:
        # สำหรับ PM — แสดงเฉพาะที่จำเป็น (Task Name, Line, Due date, Progress, Description, Time Slot ฯลฯ)
        preferred_order = [
            'Task Name', 'Due date', 'Progress', 'Description',
            'Completed Date', 'Time Slot', 'ย้ายจากวันที่', 'ย้ายไปวันไหน', 'เนื่องจาก', 'ผู้แจ้ง', 'Remark'
        ]
    else:
        # สำหรับการซ่อม — บริบทเวลาแยกเป็น ช่วงวันที่ + กะ (กะดึกเดือนนี้, กะดึกสัปดาห์ก่อน, เมื่อวานกะดึก ฯลฯ)
        preferred_order = [
            'Date', 'Shift', 'Team', 'Tech', 'Line', 'Process', 'ปัญหา',
            'TotalRepairMinutes', 'TotalResponseMinutes', 'RepairCount', 'CallCount',
            'สาเหตุ', 'การแก้ไข', 'บันทึกเพิ่มเติม',
            'ResponseMinutes', 'RepairMinutes',
            'CallTime', 'StartTime'  # เก็บไว้กรณีต้องการเห็นเวลาที่แท้จริง
        ]
    
    # กรอง columns
    available_cols = [col for col in df.columns if col not in hidden_columns]
    
    # จัดเรียงตามลำดับความสำคัญ
    ordered_cols = []
    for col in preferred_order:
        if col in available_cols:
            ordered_cols.append(col)
            available_cols.remove(col)
    
    # เพิ่ม columns ที่เหลือ (ที่ไม่ได้อยู่ใน preferred_order) ต่อท้าย
    ordered_cols.extend(available_cols)
    
    # สร้าง DataFrame ใหม่ตามลำดับที่ต้องการ
    result_df = df[ordered_cols].copy()
    
    col_brief = ", ".join([str(c) for i, c in enumerate(ordered_cols) if i < 10])
    logger.info(f"Filtered columns: {len(df.columns)} -> {len(result_df.columns)} ({col_brief}...)")
    return result_df

def generate_helpful_no_data_message(user_msg, sql):
    """
    สร้างคำตอบเมื่อไม่พบข้อมูลแบบเป็นมิตรและใช้งานได้จริง
    """
    msg_lower = (user_msg or "").lower()
    sql_upper = sql.upper() if sql else ""
    is_pm_query = "FROM PM" in sql_upper
    is_solution_query = _is_cause_or_symptom_query_text(msg_lower)

    base_message = "ยังไม่พบข้อมูลที่ตรงกับเงื่อนไขที่ระบุค่ะ"

    try:
        with sqlite3.connect(WORK_DB_PATH) as conn:
            if is_pm_query:
                latest = pd.read_sql_query('SELECT MAX(Due_date_ymd) as latest FROM PM WHERE Due_date_ymd IS NOT NULL', conn)
                if not latest.empty and latest['latest'].iloc[0]:
                    latest_date = latest['latest'].iloc[0]
                    base_message += f"\n\nข้อมูล PM ล่าสุดคือ {latest_date}"
                base_message += "\n\nลองถามใหม่ เช่น:\n- 'เดือนหน้ามี PM อะไรบ้าง'\n- 'ปีนี้มี PM อะไรบ้าง'\n- 'PM ที่เลื่อนในเดือนนี้'"
            else:
                latest = pd.read_sql_query('SELECT MAX(Date) as latest FROM repairs_enriched WHERE Date IS NOT NULL', conn)
                if not latest.empty and latest['latest'].iloc[0]:
                    latest_date = latest['latest'].iloc[0]
                    if 'วันนี้' in msg_lower or 'today' in msg_lower:
                        base_message += f"\n\nข้อมูลล่าสุดคือ {latest_date} (วันนี้อาจยังไม่มีรายการใหม่)"
                        base_message += "\n\nลองถามใหม่ เช่น:\n- 'เมื่อวานมีอะไรเสียบ้าง'\n- 'สัปดาห์นี้มีอะไรเสียบ้าง'"
                    elif is_solution_query:
                        base_message += f"\n\nข้อมูลล่าสุดคือ {latest_date}"
                        base_message += "\n\nลองระบุคำค้นและช่วงเวลาเพิ่ม เช่น:\n- 'ASSY SCREW สาเหตุและการแก้ไข 30 วันล่าสุด'\n- 'ประวัติซ่อม ASSY SCREW เดือนนี้'"
                    elif any(kw in msg_lower for kw in ['line', 'ไลน์', 'process', 'เครื่อง']):
                        base_message += f"\n\nข้อมูลล่าสุดคือ {latest_date}"
                        base_message += "\n\nลองถามใหม่ เช่น:\n- 'เมื่อวานมีอะไรเสียบ้าง'\n- 'แสดง Line ทั้งหมด'\n- 'สัปดาห์นี้เครื่องเสียอะไรบ้าง'"
                    elif 'ช่าง' in msg_lower or 'tech' in msg_lower:
                        base_message += f"\n\nข้อมูลล่าสุดคือ {latest_date}"
                        base_message += "\n\nลองถามใหม่ เช่น:\n- 'แสดงรายชื่อช่างทั้งหมด'\n- 'เมื่อวานช่างไหนซ่อมเยอะที่สุด'"
                    else:
                        base_message += f"\n\nข้อมูลล่าสุดคือ {latest_date}"
                        base_message += "\n\nลองถามใหม่ เช่น:\n- 'เมื่อวานมีอะไรเสียบ้าง'\n- 'สัปดาห์นี้สรุปงานซ่อม'"
    except Exception as e:
        logger.warning(f"generate_helpful_no_data_message error: {e}")
        base_message += "\n\nลองถามใหม่ เช่น:\n- 'เมื่อวานมีอะไรเสียบ้าง'\n- 'เดือนนี้มี PM อะไรบ้าง'"

    return base_message

def explain_sql_result(user_msg: str, sql: str, df: pd.DataFrame, total_count: Optional[int] = None) -> str:
    """
    ฟังก์ชัน explain_sql_result (อธิบายผลลัพธ์เป็นภาษาคน) 🧠 Enhanced
    ปัญหา: ได้ตารางตัวเลขมา User ดูไม่รู้เรื่อง หรือไม่รู้ว่า AI ดึงมาถูกไหม 
    หน้าที่: ให้ AI อ่าน SQL และผลลัพธ์ แล้วสรุปให้ฟังแบบข้อความเต็ม
    total_count: จำนวนทั้งหมดก่อน LIMIT (ถ้ามี)
    """
    if df.empty:
        return "ไม่พบข้อมูลครับ (0 รายการ)"
    # ป้องกัน total_count เป็น string (จาก GROUP BY count ผิด) — ให้เป็น int หรือ None
    if total_count is not None and not isinstance(total_count, (int, float)):
        try:
            total_count = int(float(total_count))
        except (TypeError, ValueError):
            total_count = None
    elif total_count is not None:
        total_count = int(total_count)
    
    summary = ""
    row_count = len(df)
    msg_lower = user_msg.lower()
    is_asking_tech = any(k in msg_lower for k in ["ช่าง", "tech", "ใครซ่อม", "ใครเป็นคนซ่อม", "คนซ่อม", "technician"])
    is_asking_team = "ทีม" in msg_lower and any(k in msg_lower for k in ["มากที่สุด", "เวลาซ่อม", "เวลาเรียก", "ซ่อมรวม", "เรียกรวม"])
    
    # 1. อ่าน SQL เพื่อบอกว่าดึงอะไรมา
    sql_upper = sql.upper()
    is_pm_sql = "FROM PM" in sql_upper or "JOIN PM" in sql_upper
    is_pm_list_question = is_pm_sql and any(kw in msg_lower for kw in [
        "มี pm อะไรบ้าง",
        "pm อะไรบ้าง",
        "เดือนนี้มี pm",
        "เดือนหน้ามี pm",
        "ปีนี้มี pm",
        "สัปดาห์นี้มี pm",
        "อาทิตย์นี้มี pm",
        "pm เดือน",
        "pm ปี",
        "pm สัปดาห์",
        "pm อาทิตย์",
    ])
    
    # 🔥 กรณีพิเศษ: คำถามแนว "มี PM อะไรบ้าง" ให้ตอบสั้นและไปดูรายละเอียดในตาราง
    if is_pm_list_question:
        shown_count = int(total_count) if (total_count and total_count > 0) else row_count
        return f"จากข้อมูล PM แสดง {shown_count} รายการ กด 📋 ดูรายละเอียดเป็นตาราง เพื่อดูรายการค่ะ"

    # 🔥 กรณีพิเศษ: ถามแบบ "มีอะไรเสียบ้าง" หรือ "มีอะไรบ้าง" = ควรสรุปภาพรวม
    # ดรอป/เสีย/breakdown/พัง = กลุ่มคำเดียวกัน (อาการเสีย)
    is_overview_question = any(kw in msg_lower for kw in ['มีอะไรเสีย', 'มีอะไรบ้าง', 'เสียอะไรบ้าง', 'มีเครื่องเสีย', 'มีอะไรดรอป', 'มีอะไรพัง', 'ดรอปอะไรบ้าง', 'พังอะไรบ้าง'])
    
    # แยกประเภทคำถาม: PM vs Repair
    if is_overview_question and not ("ORDER BY" in sql_upper and "LIMIT" in sql_upper):
        if is_pm_sql:
            # สำหรับ PM - ดูจากคำถามว่าถามช่วงเวลาไหน
            time_context = "เดือนนี้"
            if 'วันนี้' in msg_lower:
                time_context = "วันนี้"
            elif 'ปีนี้' in msg_lower:
                time_context = "ปีนี้"
            elif 'สัปดาห์' in msg_lower or 'อาทิตย์' in msg_lower:
                time_context = "สัปดาห์นี้"
            elif 'เดือนหน้า' in msg_lower:
                time_context = "เดือนหน้า"
            
            if total_count and total_count > row_count:
                summary = f"📅 {time_context} มี PM {total_count} รายการค่ะ (แสดง {row_count} รายการแรก)"
            else:
                summary = f"📅 {time_context} มี PM {row_count} รายการค่ะ"
        else:
            # สำหรับการซ่อม — บริบทเวลาแยกเป็น ช่วงวันที่ + กะ (กะดึกเดือนนี้, กะดึกสัปดาห์ก่อน, เมื่อวานกะดึก ฯลฯ)
            period = ""
            if "เมื่อวาน" in msg_lower or "yesterday" in msg_lower:
                period = "เมื่อวาน"
            elif "วันนี้" in msg_lower or "today" in msg_lower:
                period = "วันนี้"
            elif "เดือนนี้" in msg_lower or "this month" in msg_lower:
                period = "เดือนนี้"
            elif "เดือนก่อน" in msg_lower or "เดือนที่แล้ว" in msg_lower or "last month" in msg_lower:
                period = "เดือนก่อน"
            elif any(w in msg_lower for w in ["สัปดาห์ก่อน", "สัปดาห์ที่แล้ว", "อาทิตย์ก่อน", "อาทิตย์ที่แล้ว"]):
                period = "สัปดาห์ก่อน"
            elif any(w in msg_lower for w in ["สัปดาห์นี้", "อาทิตย์นี้", "this week"]):
                period = "สัปดาห์นี้"
            shift_label = ""
            if "กะดึก" in msg_lower or "night" in msg_lower:
                shift_label = "กะดึก"
            elif "กะเช้า" in msg_lower or ("day" in msg_lower and "shift" in msg_lower):
                shift_label = "กะเช้า"
            time_shift = (period + shift_label) if (period and shift_label) else (period or shift_label or "วันนี้")
            if time_shift and not time_shift.endswith(" "):
                time_shift = time_shift + " "
            if total_count and total_count > row_count:
                summary = f"🔧 {time_shift}มีเครื่องเสีย {total_count} รายการค่ะ (แสดง {row_count} รายการแรก)"
            else:
                summary = f"🔧 {time_shift}มีเครื่องเสีย {row_count} รายการค่ะ"

        # ไม่ยก list ขึ้นมาบรรยายในข้อความ (ให้ดูในตารางแทน)
        summary += "\n\nกด 📋 ดูรายละเอียดเป็นตาราง เพื่อดูรายการค่ะ"
        return summary.strip()
    
    # 2. ตรวจสอบกรณีนับแถวเดียว (SELECT COUNT(*) ทั้งตาราง)
    if len(df) > 0:
        top_row = df.iloc[0]
        if len(df.columns) == 1 and df.columns[0].upper() in ['COUNT(*)', 'TOTALREPAIRS', 'C']:
            total = int(top_row.iloc[0])
            summary = f"มีงานซ่อมทั้งหมด {total} ใบ"
    
    # 2b. ถ้าผลลัพธ์จากตาราง PM ให้ใช้คำอธิบายแบบ PM (แม้จะ match pattern repair ไปแล้ว)
    is_pm_result = len(df) > 0 and (
        "Machine" in df.columns or "Status" in df.columns or "Due Date" in df.columns or "Due date" in df.columns
        or ("LINE" in df.columns and "RepairMinutes" not in df.columns)
        or "Task Name" in df.columns
        or ("Progress" in df.columns and is_pm_sql)  # สรุป PM ปีนี้ → Progress, COUNT(*)
    )
    if is_pm_result:
        top_row = df.iloc[0]
        # กรณีสรุปสถานะ PM (Progress + COUNT(*)) เช่น "สรุป PM ปีนี้"
        if "Progress" in df.columns and ("COUNT(*)" in [str(c) for c in df.columns] or "COUNT(*)" in sql_upper):
            parts = []
            cnt_col = next((c for c in df.columns if "COUNT" in str(c).upper()), None)
            for _, row in df.iterrows():
                prog = row.get("Progress")
                cnt = row.get(cnt_col) if cnt_col else None
                if pd.notna(prog) and pd.notna(cnt):
                    parts.append(f"{prog} {int(cnt)} รายการ")
            summary = "สรุปสถานะ PM: " + ", ".join(parts) if parts else summary
        else:
            # กรณีรายการ PM (Task Name, Due date, เลื่อน ฯลฯ) — แสดง Task Name, Due date; อิโมจิ 🌜 = กะดึก
            summary = "จากข้อมูล PM/Checksheet: "
            if "Task Name" in top_row.index and pd.notna(top_row.get("Task Name")):
                tn = str(top_row["Task Name"])[:80]
                if "🌜" in tn:
                    summary += f"งาน {tn} (กะดึก). "
                else:
                    summary += f"งาน {tn}. "
            # สร้างเมื่อไหร่ (จากคอลัมน์หรือจาก Description)
            created_val = None
            for created_col in ("Created", "Created At", "Created at", "Start Date", "สร้างเมื่อ"):
                if created_col in top_row.index and pd.notna(top_row.get(created_col)):
                    created_val = top_row[created_col]
                    break
            if created_val is None and "Description" in top_row.index:
                parts = _parse_pm_description_parts(str(top_row.get("Description", "") or ""))
                created_val = parts.get("สร้างเมื่อ")
            if created_val:
                summary += f"สร้างเมื่อ: {created_val}. "
            # ย้ายไปวันไหน = จาก Description (คำว่า " เป็น ") — ไม่ใช้ Due date
            if "ย้ายไปวันไหน" in top_row.index and pd.notna(top_row.get("ย้ายไปวันไหน")) and str(top_row.get("ย้ายไปวันไหน", "") or "").strip():
                summary += f"ย้ายไปวันไหน: {top_row['ย้ายไปวันไหน']}. "
            elif "Due date" in top_row.index and pd.notna(top_row.get("Due date")):
                summary += f"ย้ายไปวันไหน: {top_row['Due date']}. "
            elif "Due Date" in top_row.index and pd.notna(top_row.get("Due Date")):
                summary += f"ย้ายไปวันไหน: {top_row['Due Date']}. "
            # แยก Description (เลื่อน): ย้ายจากวันที่, เนื่องจาก, ผู้แจ้ง (จาก Description)
            if "Description" in top_row.index and pd.notna(top_row.get("Description")) and str(top_row.get("Description", "") or "").strip():
                desc_str = str(top_row.get("Description")).strip()
                if "ย้าย" in desc_str or "เลื่อน" in desc_str:
                    parts = _parse_pm_description_parts(desc_str)
                    if parts.get("ย้ายจากวันที่"):
                        summary += f"ย้ายจากวันที่: {parts['ย้ายจากวันที่']}. "
                    if parts.get("เนื่องจาก"):
                        summary += f"เนื่องจาก: {parts['เนื่องจาก']}. "
                    if parts.get("ผู้แจ้ง"):
                        summary += f"ผู้แจ้ง: {parts['ผู้แจ้ง']}. "
                else:
                    desc_formatted = format_pm_description_for_display(desc_str)
                    summary += f"รายละเอียด: {desc_formatted} "
            if "LINE" in top_row.index and pd.notna(top_row.get("LINE")):
                summary += f"Line {top_row['LINE']} "
            if "Machine" in top_row.index and pd.notna(top_row.get("Machine")):
                summary += f"เครื่อง {top_row['Machine']} "
            if "Status" in top_row.index and pd.notna(top_row.get("Status")):
                summary += f"สถานะ {top_row['Status']} "
            if "Progress" in top_row.index and pd.notna(top_row.get("Progress")):
                summary += f"สถานะ {top_row['Progress']} "
            if "Remark" in top_row.index and pd.notna(top_row.get("Remark")) and str(top_row.get("Remark", "") or "").strip():
                summary += f"หมายเหตุ: {str(top_row['Remark'])[:120]} "
            if "Problem" in top_row.index and pd.notna(top_row.get("Problem")) and str(top_row.get("Problem", "") or "").strip():
                summary += f"ปัญหา {str(top_row['Problem'])[:80]} "
            summary = summary.strip()
    # (fallback text removed — section 4 footer แสดงจำนวนรายการแทน)
    
    # 4. สรุปจำนวนรายการ (ไม่ยก list แถวขึ้นมาบรรยาย — ให้ดูในตารางแทน)
    if row_count > 0:
        if total_count and total_count > row_count:
            summary += f"\n\n มีทั้งหมด {total_count} รายการ (แสดง {row_count} รายการแรก)"
        else:
            summary += f"\n\n แสดง {row_count} รายการ"
        summary += "\nกด 📋 ดูรายละเอียดเป็นตาราง เพื่อดูรายการค่ะ"

    return summary.strip()

def get_trend_analysis(line_name, days=7):
    """
    ฟังก์ชัน get_trend_analysis (วิเคราะห์แนวโน้ม) 📈
    ปัญหา: User ถาม "ช่วงนี้เครื่องไหนมีปัญหา" ถ้าตอบแค่ยอดรวมอาจไม่เห็นภาพ 
    หน้าที่: ดึงข้อมูลย้อนหลัง 7-30 วัน แล้วดู Trend ว่ากราฟมัน "ขาขึ้น" หรือ "ขาลง"
    """
    if not os.path.exists(WORK_DB_PATH):
        return {"trend": "unknown", "message": "ไม่มีข้อมูล"}
    
    try:
        with sqlite3.connect(WORK_DB_PATH) as conn:
            # ดึงข้อมูล N วันย้อนหลัง
            sql = f"""
            SELECT Date, COUNT(*) as daily_count 
            FROM repairs_enriched 
            WHERE Line = '{line_name}' 
            AND Date >= DATE('now', '-{days} days')
            GROUP BY Date 
            ORDER BY Date
            """
            
            df = pd.read_sql_query(sql, conn)
            
            if df.empty:
                return {"trend": "no_data", "message": f"ไม่มีข้อมูลเครื่อง {line_name} ใน {days} วันที่ผ่านมา"}
            
            # วิเคราะห์แนวโน้ม
            counts = df['daily_count'].values
            dates = df['Date'].values
            
            if len(counts) < 3:
                avg_count = counts.mean()
                return {
                    "trend": "insufficient_data", 
                    "message": f"เครื่อง {line_name} มีงานซ่อมเฉลี่ย {avg_count:.1f} ครั้ง/วัน (ข้อมูลน้อย)",
                    "data": df.to_dict('records')
                }
            
            # คำนวณแนวโน้ม (เปรียบเทียบครึ่งแรกกับครึ่งหลัง)
            mid_point = len(counts) // 2
            first_half_avg = counts[:mid_point].mean()
            second_half_avg = counts[mid_point:].mean()
            
            trend_change = second_half_avg - first_half_avg
            trend_percent = (trend_change / first_half_avg * 100) if first_half_avg > 0 else 0
            
            # จัดหมวดหมู่แนวโน้ม
            if abs(trend_percent) < 10:
                trend_status = "stable"
                trend_message = f"เครื่อง {line_name} มีแนวโน้มคงที่ (เฉลี่ย {counts.mean():.1f} ครั้ง/วัน)"
            elif trend_percent > 20:
                trend_status = "increasing_high"
                trend_message = f"⚠️ เครื่อง {line_name} มีแนวโน้มเสียบ่อยขึ้นมาก! (+{trend_percent:.0f}%)"
            elif trend_percent > 0:
                trend_status = "increasing"
                trend_message = f"📈 เครื่อง {line_name} มีแนวโน้มเสียบ่อยขึ้น (+{trend_percent:.0f}%)"
            elif trend_percent < -20:
                trend_status = "decreasing_high"
                trend_message = f"✅ เครื่อง {line_name} มีแนวโน้มเสียน้อยลงมาก! ({trend_percent:.0f}%)"
            else:
                trend_status = "decreasing"
                trend_message = f"📉 เครื่อง {line_name} มีแนวโน้มเสียน้อยลง ({trend_percent:.0f}%)"
            
            # เพิ่มข้อมูลวันที่มีปัญหามากที่สุด
            max_day = df.loc[df['daily_count'].idxmax()]
            trend_message += f" วันที่เสียมากสุดคือ {max_day['Date']} ({max_day['daily_count']} ครั้ง)"
            
            return {
                "trend": trend_status,
                "message": trend_message,
                "trend_percent": trend_percent,
                "avg_daily": counts.mean(),
                "max_day": max_day.to_dict(),
                "data": df.to_dict('records')
            }
            
    except Exception as e:
        logger.error(f"Trend analysis error: {e}")
        return {"trend": "error", "message": f"เกิดข้อผิดพลาดในการวิเคราะห์แนวโน้ม: {e}"}

# --- 4. API & LOGIC ---

def check_model():
    # ⚡ OPTIMIZATION: เช็คว่า model มีอยู่แล้วหรือไม่ก่อน pull
    try:
        # ลองเช็คว่า model มีอยู่แล้วไหม
        response = requests.get(OLLAMA_TAGS_URL, timeout=OLLAMA_TAGS_TIMEOUT)
        if response.status_code == 200:
            models = response.json().get('models', [])
            model_exists = any(model.get('name', '').startswith(MODEL_NAME) for model in models)

            if model_exists:
                logger.info(f"⚡ Model {MODEL_NAME} already exists, skipping pull")
                return

        logger.info(f"📥 Pulling model {MODEL_NAME}...")
        # Try multiple times if needed
        for attempt in range(3):
            try:
                pull_response = requests.post(OLLAMA_PULL_URL, json={"name": MODEL_NAME}, timeout=OLLAMA_PULL_TIMEOUT)
                if pull_response.status_code == 200:
                    logger.info(f"✅ Model {MODEL_NAME} ready")
                    return
                else:
                    logger.warning(f"Pull attempt {attempt+1} failed: {pull_response.text}")
            except Exception as e:
                logger.warning(f"Pull attempt {attempt+1} error: {e}")

        logger.error(f"❌ Failed to pull model {MODEL_NAME} after 3 attempts")

    except Exception as e:
        logger.warning(f"⚠️ Model check/pull failed: {e} (will try on first use)")

# ❌ DISABLED: Feedback API endpoint for production safety
# The following endpoint has been disabled to prevent bad data injection
# @app.post("/feedback")
# def save_feedback(req: FeedbackRequest):
#     """
#     Enhanced Feedback Loop API - ปุ่ม 👍/👎 สอนบอท 🎓 Enterprise Grade
#     ปัญหา: วันนี้ AI อาจจะตอบผิด หรือ SQL ไม่สมบูรณ์
#     วิธีแก้: สร้าง API ให้ User กด Like/Dislike คำตอบ
#     """
#     pass  # Function disabled for safety

@app.post("/api/reload")
def force_reload_data():
    """
    🔄 Force Reload API - บังคับให้ระบบโหลดข้อมูลใหม่
    ใช้เมื่อ: มีการอัปเดตข้อมูลใหม่และต้องการให้ระบบรู้ทันที
    """
    try:
        logger.info("🔄 Force reload requested by user")
        
        # บังคับโหลดข้อมูลใหม่
        success = load_and_enrich_data(force=True)
        
        if success:
            return JSONResponse(content={
                "status": "success",
                "message": "✅ ข้อมูลอัปเดตเรียบร้อยแล้วค่ะ!",
                "timestamp": now_bangkok_str()
            })
        else:
            return JSONResponse(content={
                "status": "error", 
                "message": "❌ ไม่สามารถโหลดข้อมูลได้ กรุณาตรวจสอบไฟล์ข้อมูล"
            })
            
    except Exception as e:
        logger.error(f"Force reload error: {e}")
        return JSONResponse(content={
            "status": "error",
            "message": f"❌ เกิดข้อผิดพลาด: {str(e)}"
        })

@app.get("/health")
def health_check():
    """
    🏥 Health Check Endpoint - For Docker healthcheck and monitoring
    """
    return JSONResponse(content={
        "status": "healthy",
        "service": "repair-chatbot",
        "timestamp": now_bangkok_str()
    })

@app.get("/api/data-status")
def get_data_status():
    """
    📊 Data Status API - ตรวจสอบสถานะข้อมูลล่าสุด
    """
    try:
        # ตรวจสอบไฟล์ต้นฉบับ
        source_exists = os.path.exists(SOURCE_DB_PATH)
        source_mtime = os.path.getmtime(SOURCE_DB_PATH) if source_exists else 0
        source_size = os.path.getsize(SOURCE_DB_PATH) if source_exists else 0
        
        # ตรวจสอบไฟล์ที่ประมวลผลแล้ว
        work_exists = os.path.exists(WORK_DB_PATH)
        work_mtime = os.path.getmtime(WORK_DB_PATH) if work_exists else 0
        work_size = os.path.getsize(WORK_DB_PATH) if work_exists else 0
        
        # นับจำนวน records
        total_records = 0
        if work_exists:
            try:
                with sqlite3.connect(WORK_DB_PATH) as conn:
                    total_records = conn.execute("SELECT COUNT(*) FROM repairs_enriched").fetchone()[0]
            except Exception:
                pass
        
        status = {
            "source_db_exists": source_exists,
            "work_db_exists": work_exists,
            "source_db": {
                "path": SOURCE_DB_PATH,
                "mtime": source_mtime,
                "size_mb": round(source_size / 1024 / 1024, 2),
                "last_modified": timestamp_to_bangkok_str(source_mtime) if source_mtime > 0 else "N/A"
            },
            "work_db": {
                "path": WORK_DB_PATH,
                "mtime": work_mtime,
                "size_mb": round(work_size / 1024 / 1024, 2),
                "last_modified": timestamp_to_bangkok_str(work_mtime) if work_mtime > 0 else "N/A",
                "total_records": total_records
            },
            "sync_status": {
                "up_to_date": work_mtime >= source_mtime if (source_mtime > 0 and work_mtime > 0) else False,
                "time_diff_seconds": abs(work_mtime - source_mtime) if (source_mtime > 0 and work_mtime > 0) else 0
            }
        }
        return JSONResponse(content=status)
        
    except Exception as e:
        logger.error(f"Data status error: {e}")
        return JSONResponse(content={"error": str(e)})


@app.post("/api/force-sync")
def force_data_sync():
    """
    🔄 Force Data Sync API - บังคับ sync ข้อมูลจาก source → work database
    """
    try:
        logger.info("[FORCE_SYNC] Manual data sync triggered")
        
        # Force reload ข้อมูล
        success = load_and_enrich_data(force=True)
        
        if success:
            # Trigger embedding regeneration
            try:
                from services.embeddings import auto_generate_embeddings_if_needed
                import threading
                threading.Thread(
                    target=auto_generate_embeddings_if_needed,
                    kwargs={"threshold": 1},  # Force generate
                    daemon=True
                ).start()
                logger.info("[FORCE_SYNC] Embedding regeneration triggered")
            except Exception as e:
                logger.warning(f"[FORCE_SYNC] Embedding trigger failed: {e}")
            
            return JSONResponse(content={
                "status": "success",
                "message": "Data sync completed successfully",
                "timestamp": now_bangkok_str()
            })
        else:
            return JSONResponse(content={
                "status": "error", 
                "message": "Data sync failed"
            }, status_code=500)
            
    except Exception as e:
        logger.error(f"[FORCE_SYNC] Error: {e}")
        return JSONResponse(content={
            "status": "error",
            "message": str(e)
        }, status_code=500)


@app.get("/api/embedding-status")
def get_embedding_status():
    """
    🔍 Embedding Status API - ตรวจสอบสถานะ embeddings
    """
    try:
        from services.embeddings import check_embedding_status
        status = check_embedding_status()
        return JSONResponse(content=status)
    except Exception as e:
        logger.error(f"Embedding status error: {e}")
        return JSONResponse(content={"error": str(e), "status": "error"})


# ============================================================
# Meta Mode API Endpoints
# Handles concurrent writes safely via meta_database.py (WAL + Lock)
# ============================================================

class MetaDataRequest(BaseModel):
    name: str
    topic: str  # แนะนำ: ไม่เกิน 1000 characters
    answer: str  # แนะนำ: ไม่เกิน 50000 characters


def _reload_meta_index():
    """Reload the in-memory FAISS index after a write so queries see fresh data."""
    try:
        from services.meta_vector import get_meta_engine
        engine = get_meta_engine()
        engine.reload_index()
    except Exception as exc:
        logger.warning(f"meta index reload failed (non-critical): {exc}")


@app.post("/api/meta/add", tags=["Meta Mode"])
def add_meta_data(req: MetaDataRequest):
    """Add new knowledge to Meta Database (thread-safe, supports 30+ concurrent writers)."""
    from services.meta_database import insert_meta_knowledge
    try:
        name   = req.name.strip()
        topic  = req.topic.strip()
        answer = req.answer.strip()

        if not name or not topic or not answer:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "กรุณากรอกข้อมูลให้ครบถ้วน"},
            )
        
        # Validate length (optional - for performance)
        if len(topic) > 1000:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "หัวข้อยาวเกินไป (สูงสุด 1000 ตัวอักษร)"},
            )
        
        if len(answer) > 50000:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "คำตอบยาวเกินไป (สูงสุด 50,000 ตัวอักษร)"},
            )

        inserted_id = insert_meta_knowledge(name, topic, answer)

        if inserted_id == -1:
            return JSONResponse(
                status_code=500,
                content={"status": "error", "message": "ไม่สามารถบันทึกได้ ลองใหม่อีกครั้ง"},
            )

        # Rebuild vector index in a background thread to keep latency low
        threading.Thread(target=_reload_meta_index, daemon=True).start()

        return JSONResponse(content={
            "status": "success",
            "message": "บันทึกความรู้สำเร็จ",
            "id": inserted_id,
        })

    except Exception as exc:
        logger.error(f"add_meta_data error: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"เกิดข้อผิดพลาด: {str(exc)}"},
        )


@app.delete("/api/meta/delete/{meta_id}", tags=["Meta Mode"])
def delete_meta_data(meta_id: int):
    """Delete a knowledge entry from Meta Database by id."""
    from services.meta_database import delete_meta_knowledge
    try:
        success = delete_meta_knowledge(meta_id)
        if success:
            threading.Thread(target=_reload_meta_index, daemon=True).start()
            return JSONResponse(content={"status": "success", "message": "ลบสำเร็จ"})
        else:
            return JSONResponse(
                status_code=404,
                content={"status": "error", "message": f"ไม่พบ id={meta_id}"},
            )
    except Exception as exc:
        logger.error(f"delete_meta_data error: {exc}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"เกิดข้อผิดพลาด: {str(exc)}"},
        )


@app.get("/api/meta/list", tags=["Meta Mode"])
def list_meta_data():
    """Return all knowledge entries in Meta Database (for admin view)."""
    from services.meta_database import get_all_meta_knowledge, get_meta_knowledge_count
    try:
        items = get_all_meta_knowledge()
        return JSONResponse(content={
            "status": "success",
            "count": get_meta_knowledge_count(),
            "items": items,
        })
    except Exception as exc:
        logger.error(f"list_meta_data error: {exc}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(exc)},
        )

@app.post("/api/generate-embeddings")
def trigger_embedding_generation():
    """
    ⚡ Generate Embeddings API - สร้าง embeddings สำหรับ rows ใหม่
    """
    try:
        from services.embeddings import auto_generate_embeddings_if_needed
        
        # Run in background thread
        def run_embedding():
            try:
                result = auto_generate_embeddings_if_needed(threshold=1)  # Force generate even 1 new row
                logger.info(f"Embedding generation completed: {result}")
            except Exception as e:
                logger.error(f"Embedding generation error: {e}")
        
        threading.Thread(target=run_embedding).start()
        
        return JSONResponse(content={
            "status": "started",
            "message": "Embedding generation started in background"
        })
    except Exception as e:
        logger.error(f"Trigger embedding error: {e}")
        return JSONResponse(content={"error": str(e), "status": "error"})


@app.get("/api/trend/{line_name}")
def get_line_trend(line_name: str, days: int = 7):
    """
    API endpoint สำหรับดูแนวโน้มของเครื่องจักรเฉพาะ
    """
    # ⚡ OPTIMIZATION: โหลด data เฉพาะเมื่อจำเป็น
    load_and_enrich_data()  # จะ skip ถ้าไฟล์ไม่เปลี่ยน
    trend_info = get_trend_analysis(line_name, days)
    return JSONResponse(content=trend_info)

@app.get("/api/suggestions")
def get_suggestions():
    """Ghost text: words, lines, processes, pm_task_names, shared = คำที่อยู่ทั้ง Line และ PM (ความหมายเดียวกัน ใช้ตามคำถาม)"""
    return JSONResponse(content={
        "words": db_context["suggestions"],
        "lines": db_context.get("lines", []),
        "processes": db_context.get("processes", []),
        "techs": db_context.get("techs", []),
        "pm_task_names": db_context.get("pm_task_names", []),
        "shared": db_context.get("shared_line_pm", []),
        "line_pm_pairs": db_context.get("line_pm_pairs", []),
    })

@app.get("/api/ai-stats")
def get_ai_stats():
    """
    API endpoint for viewing AI usage statistics from observability logs
    Returns: total queries, average latency, pipeline usage distribution
    """
    try:
        from utils.observability import get_log_stats
        
        # Get statistics from observability module
        stats = get_log_stats()
        
        # Calculate overall average latency
        total_latency = 0
        total_count = 0
        for pipeline, latencies in stats.get('avg_latency', {}).items():
            count = stats.get('by_pipeline', {}).get(pipeline, 0)
            total_latency += latencies * count
            total_count += count
        
        avg_latency_ms = int(total_latency / total_count) if total_count > 0 else 0
        
        # Build response
        response = {
            "total_queries": stats.get('total_queries', 0),
            "avg_latency_ms": avg_latency_ms,
            "pipeline_usage": stats.get('by_pipeline', {}),
            "avg_latency_by_pipeline": {
                pipeline: int(latency) 
                for pipeline, latency in stats.get('avg_latency', {}).items()
            },
            "error_rate": stats.get('error_rate', {}),
            "timestamp": datetime.now().isoformat()
        }
        
        return JSONResponse(content=response)
        
    except ImportError:
        # Observability module not available
        return JSONResponse(content={
            "error": "Observability module not available",
            "total_queries": 0,
            "avg_latency_ms": 0,
            "pipeline_usage": {}
        }, status_code=503)
        
    except Exception as e:
        logger.error(f"Failed to get AI stats: {e}")
        return JSONResponse(content={
            "error": str(e),
            "total_queries": 0,
            "avg_latency_ms": 0,
            "pipeline_usage": {}
        }, status_code=500)

@app.get("/api/system-stats")
def get_system_stats():
    """
    API endpoint สำหรับดูสถิติระบบ Enterprise Grade
    """
    try:
        # นับจำนวน Error Logs
        error_count = 0
        try:
            if os.path.exists(FAILED_LOG_FILE):
                with open(FAILED_LOG_FILE, "r", encoding="utf-8") as f:
                    error_count = len(f.readlines())
        except:
            pass
        
        # ตรวจสอบสถานะ Database
        db_status = "connected" if os.path.exists(WORK_DB_PATH) else "disconnected"
        
        # ตรวจสอบ Vector Model - ใช้ services/embeddings.py แทน
        vector_status = "enabled" if VECTOR_ENABLED else "disabled"
        
        # ดึงข้อมูลพื้นฐานจาก DB
        total_records = 0
        unique_lines = 0
        unique_processes = 0
        unique_techs = 0
        
        if os.path.exists(WORK_DB_PATH):
            try:
                with sqlite3.connect(WORK_DB_PATH) as conn:
                    # นับจำนวนรายการทั้งหมด
                    total_result = pd.read_sql("SELECT COUNT(*) as total FROM repairs_enriched", conn)
                    total_records = int(total_result['total'].iloc[0]) if not total_result.empty else 0
                    
                    # นับ Line ที่ไม่ซ้ำ
                    lines_result = pd.read_sql("SELECT COUNT(DISTINCT Line) as count FROM repairs_enriched WHERE Line IS NOT NULL", conn)
                    unique_lines = int(lines_result['count'].iloc[0]) if not lines_result.empty else 0
                    
                    # นับ Process ที่ไม่ซ้ำ
                    processes_result = pd.read_sql("SELECT COUNT(DISTINCT Process) as count FROM repairs_enriched WHERE Process IS NOT NULL", conn)
                    unique_processes = int(processes_result['count'].iloc[0]) if not processes_result.empty else 0
                    
                    # นับ Tech ที่ไม่ซ้ำ
                    techs_result = pd.read_sql("SELECT COUNT(DISTINCT Tech) as count FROM repairs_enriched WHERE Tech IS NOT NULL AND Tech != 'Unknown'", conn)
                    unique_techs = int(techs_result['count'].iloc[0]) if not techs_result.empty else 0
                    
            except Exception as e:
                logger.error(f"Database stats error: {e}")
        
        return JSONResponse(content={
            "system_status": "operational",
            "timestamp": now_bangkok_str(),
            "ai_features": {
                "fallback_relax": "✅ Active",
                "sql_explainer": "✅ Active",
                "correlation_analysis": "✅ Active"
            },
            "error_tracking": {
                "total_errors": error_count,
                "log_file": FAILED_LOG_FILE
            },
            "database": {
                "status": db_status,
                "total_records": total_records,
                "unique_lines": unique_lines,
                "unique_processes": unique_processes,
                "unique_technicians": unique_techs
            },
            "performance": {
                "model_name": MODEL_NAME,
                "ollama_host": OLLAMA_HOST,
                "response_timeout": f"{OLLAMA_REQUEST_TIMEOUT}s"
            }
        })
        
    except Exception as e:
        logger.error(f"System stats error: {e}")
        return JSONResponse(content={
            "system_status": "error",
            "error": str(e),
            "timestamp": now_bangkok_str()
        })

@app.get("/api/dashboard")
def get_dashboard_data():
    # ⚡ OPTIMIZATION: โหลด data เฉพาะเมื่อจำเป็น
    load_and_enrich_data()  # จะ skip ถ้าไฟล์ไม่เปลี่ยน
    if not os.path.exists(WORK_DB_PATH): return JSONResponse(content={"lines": {}})
    try:
        with sqlite3.connect(WORK_DB_PATH) as conn:
            # หาวันที่ล่าสุดในฐานข้อมูล
            max_date_query = "SELECT MAX(Date) as max_date FROM repairs_enriched"
            max_date_result = pd.read_sql_query(max_date_query, conn)
            max_date = max_date_result['max_date'].iloc[0] if not max_date_result.empty else pd.Timestamp.now().strftime('%Y-%m-%d')
            
            # ดึงข้อมูลการซ่อมแยกตาม Line และ Process
            sql = f"""
            SELECT 
                COALESCE(Line, 'Unknown') as machine, 
                Process, 
                COUNT(*) as RepairCount 
            FROM repairs_enriched 
            WHERE Date = '{max_date}' 
            GROUP BY COALESCE(Line, 'Unknown'), Process
            ORDER BY machine, Process
            """
            
            df = pd.read_sql_query(sql, conn)
            
            # จัดโครงสร้างข้อมูลใหม่: จัดกลุ่มตาม Line
            dashboard_data = {}
            
            for _, row in df.iterrows():
                line_name = row['machine']
                process_name = row['Process']
                repair_count = int(row['RepairCount'])
                
                # สร้าง Line ใหม่ถ้ายังไม่มี
                if line_name not in dashboard_data:
                    dashboard_data[line_name] = {
                        "total_repairs": 0,
                        "processes": [],
                        "status": "good"
                    }
                
                # กำหนดสถานะของ Process
                process_status = "good"
                if repair_count >= 5: process_status = "critical"
                elif repair_count >= 2: process_status = "warning"
                
                # เพิ่ม Process เข้าไปใน Line
                dashboard_data[line_name]["processes"].append({
                    "name": process_name,
                    "count": int(repair_count),
                    "status": process_status
                })
                
                # รวมยอดซ่อมทั้งหมดของ Line
                dashboard_data[line_name]["total_repairs"] += repair_count
            
            # กำหนดสถานะของ Line ตามยอดรวม
            for line_name in dashboard_data:
                total = dashboard_data[line_name]["total_repairs"]
                if total >= 15: dashboard_data[line_name]["status"] = "critical"
                elif total >= 5: dashboard_data[line_name]["status"] = "warning"
                else: dashboard_data[line_name]["status"] = "good"
            
            return JSONResponse(content={"lines": dashboard_data, "date": max_date})
            
    except Exception as e:
        logger.error(f"Dashboard Error: {e}")
        return JSONResponse(content={"lines": {}, "error": str(e)})

@app.get("/api/tech-dashboard")
def get_tech_dashboard():
    now_ts = time.time()
    with _TECH_DASH_CACHE_LOCK:
        cached_payload = _TECH_DASH_CACHE.get("payload")
        cached_ts = float(_TECH_DASH_CACHE.get("ts") or 0.0)
    if cached_payload is not None and (now_ts - cached_ts) < _TECH_DASH_CACHE_TTL_SEC:
        resp = JSONResponse(content=cached_payload)
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["X-Cache"] = "HIT"
        return resp
    try:
        if not os.path.exists(WORK_DB_PATH):
            return JSONResponse(content={
                "teams": {},
                "error": "Database file not found"
            })
        db_path = WORK_DB_PATH
        
        # Direct SQLite connection (context manager ensures close)
        with sqlite3.connect(db_path) as conn:
            # Get data last 365 days เพื่อคำนวณเวลาซ่อมรวม ต่อ วัน/สัปดาห์/เดือน/ปี
            query = """
            SELECT Date, Tech, Line, Process, 
                   RepairMinutes, ResponseMinutes, Shift, Team
            FROM repairs_enriched 
            WHERE Date >= date('now', '-365 days')
            AND Tech IS NOT NULL AND Tech != 'Unknown' AND Tech != ''
            ORDER BY Date DESC
            """
            
            df = pd.read_sql_query(query, conn)
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        if 'RepairMinutes' in df.columns:
            df['RepairMinutes'] = pd.to_numeric(df['RepairMinutes'], errors='coerce')
        if 'ResponseMinutes' in df.columns:
            df['ResponseMinutes'] = pd.to_numeric(df['ResponseMinutes'], errors='coerce')
        
        # ตัดชื่อที่ไม่อยากแสดงออก: ใช้ ExcludeFromAnswer จาก tech_list_from_db.json
        # ชื่อที่เป็นภาษาอังกฤษล้วน (เช่น TEST) ให้ตัด ยกเว้นถ้าอยู่ใน tech_mapping (ชื่อเล่นอย่าง WORRACHART, MANUS)
        exclude_set = set(get_tech_exclude_for_answer() or ())
        df = df[~df["Tech"].astype(str).str.strip().str.upper().isin({str(x).strip().upper() for x in exclude_set})]
        def _drop_english_unless_official(name):
            if not _is_english_only_name(name):
                return False
            return get_tech_id_from_name(name) is None
        df = df[~df["Tech"].astype(str).apply(_drop_english_unless_official)]
        
        if df.empty:
            return JSONResponse(content={
                "teams": {},
                "error": "No recent data found"
            })
        
        # ใช้ Team จาก repair_data/repair_enriched ให้ตรง (มีอยู่แล้วใน df) — เติมเฉพาะแถวที่ไม่มีทีม
        def assign_team_with_mapping(tech_name):
            try:
                tech_id = get_tech_id_from_name(tech_name)
                if tech_id:
                    team = get_team_from_tech_id(tech_id)
                    if team is None:
                        return "ลบออกแล้ว"  # อยู่ใน excluded_team_ids (ชาตรี สุดเนตร, ชินวัตร มีบ้านเกิ้ง, กิตติพันธ์ อาจหาญ ฯลฯ)
                    if team:
                        return team
            except:
                pass
            hash_val = hash(tech_name) % 3
            return f'Team {["A", "B", "C"][hash_val]}'
        
        # แสดงเฉพาะช่าง 22 คนตามรายการทางการ — ตัดชื่อที่ไม่อยู่ใน tech_mapping ออก
        df = df[df["Tech"].astype(str).apply(lambda n: get_tech_id_from_name(n) is not None)].copy()
        _tm = load_tech_mapping()
        team_assignment = _tm.get("team_assignment") or {}
        _allowed_ids = set()
        for _tid_list in team_assignment.values():
            _allowed_ids.update(str(x).strip() for x in _tid_list)
        # เพิ่มคอลัมน์ tech_id เป็น string (strip ให้ตรงกับ team_assignment)
        def _tid(tech_name):
            tid = get_tech_id_from_name(tech_name)
            return str(tid).strip() if tid is not None else ''
        df['tech_id'] = df['Tech'].astype(str).apply(_tid)
        df = df[df['tech_id'] != '']
        df = df[df['tech_id'].isin(_allowed_ids)].copy()
        if df.empty:
            return JSONResponse(content={"teams": {}, "total_records": 0, "status": "success", "message": "No data for official tech list"})
        
        teams = ['Team A', 'Team B', 'Team C']
        hide_in_tech_status_by_team = _tm.get("hide_in_tech_status_by_team") or {}
        hide_tech_ids_in_team = _tm.get("hide_tech_ids_in_team") or {}
        
        # แต่ละทีมดึงรายชื่อจาก team_assignment เท่านั้น — ไม่ใช้ค่า Team จาก DB (กันคนหลุดไปโผล่ทีมผิด)
        teams_data = {}
        for team in teams:
            team_ids = set(str(x).strip() for x in team_assignment.get(team, []))
            team_df = df[df['tech_id'].astype(str).str.strip().isin(team_ids)].copy()
            hide_names = set(hide_in_tech_status_by_team.get(team, []))
            if hide_names:
                team_df = team_df[~team_df['Tech'].astype(str).str.strip().isin(hide_names)]
            hide_ids = set(str(x).strip() for x in hide_tech_ids_in_team.get(team, []))
            if hide_ids:
                team_df = team_df[~team_df['tech_id'].str.strip().isin(hide_ids)]
            
            if team_df.empty:
                teams_data[team] = {
                    "total_jobs": 0,
                    "avg_response": 0,
                    "avg_repair": 0,
                    "response_samples": 0,
                    "repair_samples": 0,
                    "mvp": "ไม่มีข้อมูล",
                    "mvp_score": 0,
                    "mvp_most_jobs": "ไม่มีข้อมูล",
                    "mvp_most_repair_minutes": "ไม่มีข้อมูล",
                    "mvp_fastest_response": "ไม่มีข้อมูล",
                    "technicians": []
                }
                continue
            
            # Team-level stats
            total_jobs = len(team_df)
            response_samples = int(team_df['ResponseMinutes'].notna().sum()) if 'ResponseMinutes' in team_df.columns else 0
            repair_samples = int(team_df['RepairMinutes'].notna().sum()) if 'RepairMinutes' in team_df.columns else 0
            avg_response = _round_or_zero(team_df['ResponseMinutes'].mean()) if 'ResponseMinutes' in team_df.columns else 0
            avg_repair = _round_or_zero(team_df['RepairMinutes'].mean()) if 'RepairMinutes' in team_df.columns else 0
            
            # Individual technician stats — group by tech_id เพื่อรวมคนเดียวกัน (ตัดชื่อซ้ำ 2 รอบ)
            tech_stats = team_df.groupby('tech_id').agg(
                Tech=('Tech', 'first'),
                JobCount=('Tech', 'count'),
                AvgResponseTime=('ResponseMinutes', 'mean'),
                StdResponseTime=('ResponseMinutes', 'std'),
                AvgRepairTime=('RepairMinutes', 'mean'),
                StdRepairTime=('RepairMinutes', 'std'),
                ResponseSamples=('ResponseMinutes', 'count'),
                RepairSamples=('RepairMinutes', 'count'),
                LineCount=('Line', 'nunique'),
                ProcessCount=('Process', 'nunique')
            ).reset_index()
            
            # ช่วงเวลาสำหรับรวมเวลาซ่อม (วัน/สัปดาห์/เดือน/ปี)
            now = pd.Timestamp.now().normalize()
            today_str = now.strftime('%Y-%m-%d')
            week_start = (now - pd.Timedelta(days=7))
            month_start = (now - pd.Timedelta(days=30))
            year_start = (now - pd.Timedelta(days=365))
            
            # Calculate performance scores and repair minutes by period for each technician
            technicians_detail = []
            for _, tech_row in tech_stats.iterrows():
                tech_name = tech_row['Tech']
                tech_id = tech_row['tech_id']
                if tech_name and str(tech_name).strip() in hide_names:
                    continue  # ไม่แสดงในทีมนี้ตาม hide_in_tech_status_by_team
                
                # เวลาซ่อมรวม (นาที) ต่อ วัน / สัปดาห์ / เดือน / ปี (ใช้ tech_id เพื่อรวมทุกแถวของคนเดียวกัน)
                tech_mask = team_df['tech_id'] == tech_id
                day_mask = (pd.to_datetime(team_df['Date']).dt.normalize() == pd.Timestamp(today_str).normalize()) if team_df['Date'].dtype != 'object' else (team_df['Date'].astype(str).str[:10] == today_str)
                repair_day = _series_sum_or_zero(team_df.loc[tech_mask & day_mask, 'RepairMinutes'])
                repair_week = _series_sum_or_zero(team_df.loc[tech_mask & (pd.to_datetime(team_df['Date']) >= week_start), 'RepairMinutes'])
                repair_month = _series_sum_or_zero(team_df.loc[tech_mask & (pd.to_datetime(team_df['Date']) >= month_start), 'RepairMinutes'])
                repair_year = _series_sum_or_zero(team_df.loc[tech_mask & (pd.to_datetime(team_df['Date']) >= year_start), 'RepairMinutes'])
                
                # Calculate 4-pillar scores
                speed_score = max(0, 100 - (tech_row['AvgResponseTime'] * 2)) if tech_row['ResponseSamples'] > 0 and tech_row['AvgResponseTime'] > 0 else 50
                skill_score = max(0, 100 - (tech_row['AvgRepairTime'] / 2)) if tech_row['RepairSamples'] > 0 and tech_row['AvgRepairTime'] > 0 else 50
                versatility_score = min(100, (tech_row['LineCount'] * 20) + (tech_row['ProcessCount'] * 10))
                workload_percent = (tech_row['JobCount'] / total_jobs * 100) if total_jobs > 0 else 0
                
                technicians_detail.append({
                    "name": get_tech_display_name(tech_name),
                    "tech_id": tech_id,
                    "job_count": int(tech_row['JobCount']),
                    "avg_response": _round_or_zero(tech_row['AvgResponseTime']),
                    "avg_repair": _round_or_zero(tech_row['AvgRepairTime']),
                    "response_samples": int(tech_row['ResponseSamples']),
                    "repair_samples": int(tech_row['RepairSamples']),
                    "line_count": int(tech_row['LineCount']),
                    "process_count": int(tech_row['ProcessCount']),
                    "speed_score": round(speed_score, 1),
                    "skill_score": round(skill_score, 1),
                    "versatility_score": round(versatility_score, 1),
                    "workload_percent": round(workload_percent, 1),
                    "repair_min_day": round(repair_day, 0),
                    "repair_min_week": round(repair_week, 0),
                    "repair_min_month": round(repair_month, 0),
                    "repair_min_year": round(repair_year, 0)
                })
            
            # MVP รายวัน: เลือกจากข้อมูลวันนี้เท่านั้น (group by tech_id เพื่อไม่ซ้ำคน)
            team_df_today = team_df[(pd.to_datetime(team_df['Date']).dt.normalize() == pd.Timestamp(today_str).normalize()) if team_df['Date'].dtype != 'object' else (team_df['Date'].astype(str).str[:10] == today_str)]
            if not team_df_today.empty:
                tech_today = team_df_today.groupby('tech_id').agg(
                    Tech=('Tech', 'first'),
                    ResponseMinutes=('ResponseMinutes', 'mean'),
                    RepairMinutes=('RepairMinutes', 'mean'),
                    Line=('Line', 'nunique'),
                    Process=('Process', 'nunique')
                ).reset_index()
                tech_today.columns = ['tech_id', 'Tech', 'AvgResponse', 'AvgRepair', 'LineCount', 'ProcessCount']
                tech_today['speed_score'] = tech_today['AvgResponse'].apply(lambda x: max(0, 100 - (x * 2)) if x > 0 else 50)
                tech_today['skill_score'] = tech_today['AvgRepair'].apply(lambda x: max(0, 100 - (x / 2)) if x > 0 else 50)
                tech_today['versatility_score'] = (tech_today['LineCount'] * 20 + tech_today['ProcessCount'] * 10).clip(upper=100)
                tech_today['combined'] = (tech_today['speed_score'] + tech_today['skill_score'] + tech_today['versatility_score']) / 3
                best = tech_today.loc[tech_today['combined'].idxmax()]
                mvp_name = get_tech_display_name(best['Tech'])
                mvp_score = round(float(best['combined']), 1)
            else:
                # MVP รายวัน: ไม่มีงานวันนี้ = ไม่มี MVP
                mvp_name = "ไม่มีข้อมูล"
                mvp_score = 0
            
            # MVP 3 อันของทีม: ซ่อมจำนวนครั้งมากที่สุด, เวลาซ่อมมากที่สุด, โดนเรียกแล้วไปเร็วที่สุด
            mvp_most_jobs = "ไม่มีข้อมูล"
            mvp_most_repair_minutes = "ไม่มีข้อมูล"
            mvp_fastest_response = "ไม่มีข้อมูล"
            if technicians_detail:
                by_jobs = max(technicians_detail, key=lambda x: x["job_count"])
                mvp_most_jobs = by_jobs["name"]
                repair_candidates = [item for item in technicians_detail if item.get("repair_samples", 0) > 0]
                if repair_candidates:
                    by_repair_min = max(repair_candidates, key=lambda x: x["repair_min_month"])
                    mvp_most_repair_minutes = by_repair_min["name"]
                response_candidates = [item for item in technicians_detail if item.get("response_samples", 0) > 0]
                if response_candidates:
                    by_fastest = min(response_candidates, key=lambda x: x["avg_response"] if x["avg_response"] > 0 else 9999)
                    mvp_fastest_response = by_fastest["name"] if by_fastest["avg_response"] > 0 else "ไม่มีข้อมูล"
            
            team_info: Dict[str, Any] = {
                "total_jobs": total_jobs,
                "avg_response": avg_response,
                "avg_repair": avg_repair,
                "response_samples": response_samples,
                "repair_samples": repair_samples,
                "mvp": mvp_name,
                "mvp_score": mvp_score,
                "mvp_most_jobs": mvp_most_jobs,
                "mvp_most_repair_minutes": mvp_most_repair_minutes,
                "mvp_fastest_response": mvp_fastest_response,
                "technicians": technicians_detail
            }
            teams_data[team] = team_info
        
        payload = {
            "teams": teams_data,
            "total_records": len(df),
            "date_range": f"{df['Date'].min()} to {df['Date'].max()}" if not df.empty else "No data",
            "status": "success"
        }
        with _TECH_DASH_CACHE_LOCK:
            _TECH_DASH_CACHE["payload"] = payload
            _TECH_DASH_CACHE["ts"] = now_ts

        resp = JSONResponse(content=payload)
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["X-Cache"] = "MISS"
        return resp
        
    except Exception as e:
        logger.error(f"Tech Dashboard API error: {e}")
        return JSONResponse(content={
            "teams": {},
            "error": str(e)
        })

@app.get("/api/tech-detail/{tech_name}")
def get_tech_detail(tech_name: str, team_name: str = None):
    """API endpoint สำหรับรายละเอียดช่างรายคน"""
    try:
        if not os.path.exists(WORK_DB_PATH):
            return JSONResponse(content={"error": "Database not found"})
        with sqlite3.connect(WORK_DB_PATH) as conn:
            # Get detailed data for specific technician
            query = """
            SELECT Date, Line, Process, ปัญหา as Problem,
                   RepairMinutes, ResponseMinutes, Shift
            FROM repairs_enriched 
            WHERE Tech = ? AND Date >= date('now', '-30 days')
            ORDER BY Date DESC
            """
            
            df = pd.read_sql_query(query, conn, params=[tech_name])
        
        if df.empty:
            return JSONResponse(content={"error": "No data found for this technician"})
        
        # Calculate detailed statistics
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        if 'RepairMinutes' in df.columns:
            df['RepairMinutes'] = pd.to_numeric(df['RepairMinutes'], errors='coerce')
        if 'ResponseMinutes' in df.columns:
            df['ResponseMinutes'] = pd.to_numeric(df['ResponseMinutes'], errors='coerce')
        total_jobs = len(df)
        response_samples = int(df['ResponseMinutes'].notna().sum()) if 'ResponseMinutes' in df.columns else 0
        repair_samples = int(df['RepairMinutes'].notna().sum()) if 'RepairMinutes' in df.columns else 0
        avg_response = _round_or_zero(df['ResponseMinutes'].mean()) if 'ResponseMinutes' in df.columns else 0
        avg_repair = _round_or_zero(df['RepairMinutes'].mean()) if 'RepairMinutes' in df.columns else 0
        success_rate = round(95 + (5 * (1 - avg_repair / 100)), 1)  # Mock calculation
        total_repair_minutes = round(_series_sum_or_zero(df['RepairMinutes']), 0) if 'RepairMinutes' in df.columns else 0
        today_str = pd.Timestamp.now().strftime('%Y-%m-%d')
        df['DateStr'] = df['Date'].dt.strftime('%Y-%m-%d')
        jobs_today = int((df['DateStr'] == today_str).sum())
        today_mask = df['DateStr'] == today_str
        repair_minutes_today = round(_series_sum_or_zero(df.loc[today_mask, 'RepairMinutes']), 0) if 'RepairMinutes' in df.columns and today_mask.any() else 0
        
        # ต่อเดือน (ข้อมูลนี้เป็น 30 วันล่าสุดอยู่แล้ว)
        jobs_per_month = total_jobs
        repair_minutes_per_month = total_repair_minutes
        avg_minutes_per_repair_month = round(repair_minutes_per_month / repair_samples, 1) if repair_samples > 0 else 0
        total_response_minutes_per_month = round(_series_sum_or_zero(df['ResponseMinutes']), 0) if 'ResponseMinutes' in df.columns else 0
        avg_response_minutes_per_month = round(total_response_minutes_per_month / response_samples, 1) if response_samples > 0 else 0
        
        tech_id = get_tech_id_from_name(tech_name)
        
        # Performance scores
        speed_score = max(0, 100 - (avg_response * 2)) if response_samples > 0 and avg_response > 0 else 50
        skill_score = max(0, 100 - (avg_repair / 2)) if repair_samples > 0 and avg_repair > 0 else 50
        versatility_score = min(100, df['Line'].nunique() * 20 + df['Process'].nunique() * 10)
        workload_score = min(100, total_jobs * 2)
        
        # Problem breakdown
        problem_breakdown = []
        if 'Problem' in df.columns:
            problem_stats = df.groupby('Problem')['RepairMinutes'].agg(['mean', 'count']).reset_index()
            problem_stats.columns = ['problem', 'avg_time', 'count']
            problem_stats = problem_stats[problem_stats['count'] > 0]
            problem_stats = problem_stats.sort_values('count', ascending=False).head(5)
            
            for _, row in problem_stats.iterrows():
                score = max(0, 100 - (row['avg_time'] / 2))
                problem_breakdown.append({
                    "problem": row['problem'],
                    "avg_time": _round_or_zero(row['avg_time']),
                    "count": int(row['count']),
                    "score": round(score, 1)
                })
        
        # Versatility data
        versatility_data = []
        line_stats = df.groupby('Line').size().reset_index(name='count')
        for _, row in line_stats.iterrows():
            versatility_data.append({
                "line": row['Line'],
                "count": int(row['count'])
            })
        
        return JSONResponse(content={
            "tech_name": tech_name,
            "tech_id": tech_id,
            "summary": {
                "total_jobs": total_jobs,
                "avg_response": avg_response,
                "avg_repair": avg_repair,
                "response_samples": response_samples,
                "repair_samples": repair_samples,
                "success_rate": success_rate,
                "jobs_today": jobs_today,
                "repair_minutes_today": repair_minutes_today,
                "total_repair_minutes": total_repair_minutes,
                "jobs_per_month": jobs_per_month,
                "repair_minutes_per_month": repair_minutes_per_month,
                "avg_minutes_per_repair_month": avg_minutes_per_repair_month,
                "total_response_minutes_per_month": total_response_minutes_per_month,
                "avg_response_minutes_per_month": avg_response_minutes_per_month
            },
            "performance_scores": {
                "speed": round(speed_score, 1),
                "skill": round(skill_score, 1),
                "versatility": round(versatility_score, 1),
                "workload": round(workload_score, 1)
            },
            "skill_breakdown": problem_breakdown,
            "versatility_data": versatility_data,
            "date_range": f"{df['Date'].min()} to {df['Date'].max()}"
        })
        
    except Exception as e:
        logger.error(f"Tech Detail API error: {e}")
        return JSONResponse(content={"error": str(e)})

@app.get("/api/tech-detail/{tech_name}/trend")
def get_tech_trend(tech_name: str, period: str = "week"):
    """API สำหรับ Repair Time Trend = Total repair time (นาทีรวม) ต่อ วัน/สัปดาห์/เดือน ใช้ใน Modal รายละเอียดช่าง"""
    try:
        if not os.path.exists(WORK_DB_PATH):
            return _mock_trend(period, 6.3)
        # ปี ใช้ 365 วัน; เดือน 90 วัน; สัปดาห์/วัน 90 วัน
        days_back = 365 if period == "year" else 90
        with sqlite3.connect(WORK_DB_PATH) as conn:
            query = """
            SELECT Date, RepairMinutes
            FROM repairs_enriched
            WHERE Tech = ? AND Date >= date('now', ?) AND RepairMinutes IS NOT NULL
            ORDER BY Date
            """
            df = pd.read_sql_query(query, conn, params=[tech_name, f"-{days_back} days"])
        if df.empty:
            return _mock_trend(period, 6.3)
        df["Date"] = pd.to_datetime(df["Date"])
        target_total = float(df["RepairMinutes"].sum())
        # รวมเวลาซ่อม (SUM) ต่อช่วง — ไม่ใช่ค่าเฉลี่ย
        if period == "day":
            cutoff = df["Date"].max() - pd.Timedelta(hours=24)
            df_day = df[df["Date"] >= cutoff]
            if df_day.empty:
                return _mock_trend(period, 50.0)
            df_day = df_day.copy()
            df_day["Hour"] = df_day["Date"].dt.strftime("%Y-%m-%d %H:00")
            agg = df_day.groupby("Hour")["RepairMinutes"].sum().reset_index()
            agg.columns = ["date", "repair_minutes"]
        elif period == "year":
            df = df[df["Date"] >= (df["Date"].max() - pd.Timedelta(days=365))]
            if df.empty:
                return _mock_trend(period, 50.0)
            df = df.copy()
            df["Month"] = df["Date"].dt.to_period("M").astype(str)
            agg = df.groupby("Month")["RepairMinutes"].sum().reset_index()
            agg.columns = ["date", "repair_minutes"]
        else:
            days = 7 if period == "week" else 30
            df = df[df["Date"] >= (df["Date"].max() - pd.Timedelta(days=days))]
            if df.empty:
                return _mock_trend(period, 50.0)
            agg = df.groupby(df["Date"].dt.date.astype(str))["RepairMinutes"].sum().reset_index()
            agg.columns = ["date", "repair_minutes"]
        out = [{"date": row["date"], "repair_minutes": round(float(row["repair_minutes"]), 1)} for _, row in agg.iterrows()]
        if not out:
            return _mock_trend(period, 6.3)
        return JSONResponse(content={"period": period, "data": out})
    except Exception as e:
        logger.warning(f"Tech trend error: {e}")
        return _mock_trend(period, 6.3)

def _mock_trend(period: str, avg_min: float):
    """สร้างข้อมูล mock สำหรับ Repair Time Trend"""
    np.random.seed(42)
    if period == "day":
        n = 24
        base = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        dates = [(base + timedelta(hours=i)).strftime("%Y-%m-%d %H:00") for i in range(n)]
    elif period == "year":
        n = 12
        base = datetime.now().replace(day=1) - timedelta(days=365)
        dates = [(base + timedelta(days=30 * i)).strftime("%Y-%m") for i in range(n)]
    else:
        n = 7 if period == "week" else 30
        base = datetime.now() - timedelta(days=n)
        dates = [(base + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n)]
    mins = np.clip(np.random.normal(avg_min, 1.5, n), 1, 20).tolist()
    data = [{"date": d, "repair_minutes": round(m, 1)} for d, m in zip(dates, mins)]
    return JSONResponse(content={"period": period, "data": data})

# ค่า LIMIT เริ่มต้น (ถ้าถาม "ทุก/ทั้งหมด/ทุกอัน" ใช้ limit สูง)
DEFAULT_QUERY_LIMIT = 10  # 🔥 เพิ่มจาก 5 เป็น 10 เพื่อให้เห็นข้อมูลมากขึ้น
FULL_LIST_LIMIT = 100  # เมื่อถามแบบ "ทุก" / "ทั้งหมด" / "ทุกอัน"

def ensure_limit_5(sql, user_msg=None, limit_override=None):
    """ใส่ LIMIT ให้ query — คำถาม PM (FROM PM) ไม่ใส่ LIMIT; limit_override (จากปุ่มกี่รายการ) มีลำดับสูงสุด; ถ้าถาม 'ทุก'/ทั้งหมด ใช้ limit สูง; ถ้าถาม N อันดับ/รายการ ใช้ LIMIT N; ไม่งั้น 10 (default)"""
    if not sql or not sql.strip():
        return sql
    s = sql.strip()
    # คำถาม PM: ไม่ใส่ LIMIT (แสดงทุกรายการ)
    if re.search(r'\bFROM\s+PM\b', s, re.IGNORECASE):
        s = re.sub(r'\bLIMIT\s+\d+\s*', '', s, flags=re.IGNORECASE)
        s = re.sub(r'\s*;\s*$', ';', s)
        return s.strip()
    has_existing_limit = bool(re.search(r'\bLIMIT\s+\d+\b', s, re.IGNORECASE))
    requested_limit = None

    if limit_override is not None and 1 <= limit_override <= 500:
        requested_limit = limit_override
    elif user_msg:
        msg = (user_msg or "").strip()
        msg_lower = msg.lower()

        # ถาม "3 อันดับ" / "5 อันดับ" / "top 3" / "20 รายการ" → ใช้เลขนั้นเป็น LIMIT
        m = re.search(r"(?:top\s+)?(\d+)\s*อันดับ|อันดับ\s*(\d+)|(\d+)\s*อันดับ", msg_lower)
        if m:
            n = next((int(x) for x in m.groups() if x), None)
            if n and 1 <= n <= 500:
                requested_limit = n

        if requested_limit is None:
            m2 = re.search(r"(\d+)\s*รายการ", msg_lower)
            if m2:
                n = int(m2.group(1))
                if 1 <= n <= 500:
                    requested_limit = n

        if requested_limit is None:
            wants_full_list = any(k in msg_lower for k in ("ทุก", "ทั้งหมด", "ทุกอัน", "ทั้งระบบ", "all"))
            if wants_full_list and _detect_query_intent(msg) != "TOP":
                requested_limit = FULL_LIST_LIMIT

    if requested_limit is None:
        if has_existing_limit:
            return s.strip()
        requested_limit = DEFAULT_QUERY_LIMIT

    s = re.sub(r'\bLIMIT\s+\d+\s*', f'LIMIT {requested_limit} ', s, flags=re.IGNORECASE)
    if 'LIMIT' not in s.upper():
        s = s.rstrip().rstrip(';').strip()
        s = f"{s} LIMIT {requested_limit};"
    return s.strip()

def clean_sql(text):
    # Clean SQL Response from AI
    if not text:
        return ""
    
    # ลบ markdown code blocks
    text = re.sub(r'```sql|```', '', text, flags=re.I).strip()
    
    # หา SQL statement ที่สมบูรณ์
    sql = ""
    match = re.search(r'(SELECT\s+.*?;)', text, re.DOTALL | re.IGNORECASE)
    if match: 
        sql = match.group(1).strip()
    else:
        # หา SQL statement ที่ไม่มี semicolon
        match_loose = re.search(r'(SELECT\s+.*)', text, re.DOTALL | re.IGNORECASE)
        if match_loose:
            sql = match_loose.group(1).strip()
            # เพิ่ม semicolon ถ้าไม่มี
            if not sql.endswith(';'):
                sql += ';'
    
    if not sql:
        # ถ้าไม่เจอ SELECT ให้ return text ที่ทำความสะอาดแล้ว
        cleaned = text.strip()
        if cleaned and not cleaned.endswith(';'):
            cleaned += ';'
        sql = cleaned

    # 🔥 Robust Date Placeholder Replacement (Mirroring extract_clean_sql)
    try:
        from pipelines.sql_generator import _compute_date_context
        dates = _compute_date_context()
        placeholders = {
            "today_date": dates['today'],
            "yesterday_date": dates['yesterday'],
            "tomorrow_date": dates['tomorrow']
        }
        for placeholder, actual_date in placeholders.items():
            sql = re.sub(rf"'{placeholder}'", f"'{actual_date}'", sql, flags=re.IGNORECASE)
            sql = re.sub(rf"\"{placeholder}\"", f"'{actual_date}'", sql, flags=re.IGNORECASE)
            sql = re.sub(rf"\b{placeholder}\b", f"'{actual_date}'", sql, flags=re.IGNORECASE)
    except Exception as e:
        logger.warning(f"[clean_sql] Date replacement failed: {e}")
    
    return sql

_qa_log_lock = None

def _get_qa_log_lock():
    global _qa_log_lock
    if _qa_log_lock is None:
        import threading
        _qa_log_lock = threading.Lock()
    return _qa_log_lock

def save_qa_log(question: str, answer: str, sql: str = "", debug: dict = None):
    """เก็บคำถาม คำตอบ และ debug ทุกครั้งที่มีคนถาม-ตอบ ไว้ใน data/qa_log.jsonl (หนึ่งบรรทัดต่อหนึ่ง record)"""
    try:
        log_dir = os.path.dirname(QA_LOG_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        entry = {
            "timestamp": now_bangkok_str(),
            "question": (question or "").strip(),
            "answer": (answer or "").strip(),
            "sql": (sql or "").strip(),
            "debug": debug if isinstance(debug, dict) else {},
        }
        line = json.dumps(entry, ensure_ascii=False) + "\n"
        lock = _get_qa_log_lock()
        with lock:
            with open(QA_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
    except Exception as e:
        logger.warning(f"save_qa_log failed: {e}")

@app.get("/ollama-status")
def ollama_status():
    """ตรวจสอบว่า Ollama พร้อมหรือไม่ และ model ที่ใช้มีอยู่หรือไม่ (ใช้ debug เมื่อตอบคำถามไม่ได้)"""
    try:
        res = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=OLLAMA_TAGS_TIMEOUT)
        if res.status_code != 200:
            return JSONResponse(
                status_code=200,
                content={
                    "ok": False,
                    "ollama_host": OLLAMA_HOST,
                    "model_required": MODEL_NAME,
                    "error": f"Ollama API returned HTTP {res.status_code}",
                    "hint": "ตรวจว่า container ollama_service รันอยู่ และมี NVIDIA GPU (หรือรอ pull model)",
                },
            )
        data = res.json()
        models = [m.get("name", "") for m in data.get("models", [])]
        has_model = any(MODEL_NAME in n or n == MODEL_NAME for n in models)
        has_chat_model = any(CHAT_MODEL in n or n == CHAT_MODEL for n in models)
        return {
            "ok": has_model,
            "ollama_host": OLLAMA_HOST,
            "model_required": MODEL_NAME,
            "chat_model": CHAT_MODEL,
            "chat_model_available": has_chat_model,
            "models": models,
            "error": None if has_model else f"Model '{MODEL_NAME}' ยังไม่มีใน Ollama - รัน: ollama pull {MODEL_NAME}",
        }
    except requests.exceptions.ConnectionError as e:
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "ollama_host": OLLAMA_HOST,
                "model_required": MODEL_NAME,
                "error": f"เชื่อมต่อ Ollama ไม่ได้: {e}",
                "hint": "รัน docker compose up -d แล้วรอให้ ollama_service พร้อม หรือรัน Ollama บน host แล้วตั้ง OLLAMA_HOST",
            },
        )
    except Exception as e:
        return JSONResponse(
            status_code=200,
            content={"ok": False, "ollama_host": OLLAMA_HOST, "model_required": MODEL_NAME, "error": str(e)},
        )


# --- Async Polling Job Status Storage ---
# dictionary เพื่อเก็บสถานะของแต่ละ job: chat_jobs[job_id] = {"status": "processing"|"completed"|"error", "result": None, "timestamp": time.time()}
chat_jobs: Dict[str, Any] = {}

def _cleanup_old_jobs():
    """ลบ jobs ที่ค้างเกิน 10 นาทีเพื่อคืนหน่วยความจำ"""
    current_time = time.time()
    stale_jobs = [jid for jid, job in chat_jobs.items() if current_time - job.get("timestamp", current_time) > 600]
    for jid in stale_jobs:
        chat_jobs.pop(jid, None)

def _process_chat_job(job_id: str, req: ChatRequest):
    """Background worker สำหรับประมวลผลคำถามและอัปเดตสถานะกลับเข้า dict"""
    req_start_time = time.time()
    try:
        # Step: Routing
        chat_jobs[job_id]["stage"] = "ROUTING"
        
        # We'll modify _chat_impl later if needed to report more granular stages.
        # For now, let's wrap the calls to report high-level stages.
        # NOTE: _chat_impl is synchronous, so we can't easily report stages *during* its execution 
        # unless we pass the job_id into it or use a thread-local.
        # Since _chat_impl is huge, let's just set stages before/after key phases if possible.
        
        resp_data = _chat_impl(req, job_id=job_id)
        
        chat_jobs[job_id] = {
            "status": "completed",
            "result": resp_data,
            "timestamp": time.time(),
            "stage": "SUMMARY"
        }
    except Exception as e:
        logger.exception(f"Async Job {job_id} error: {e}")
        _user_msg = (getattr(req, "message", "") or "").strip()
        _fallback_text = None
        try:
            _fallback_text = _get_chat_model_response(_user_msg) if _user_msg else None
        except Exception:
            pass
            
        if _fallback_text and _fallback_text != CHAT_FALLBACK_RESPONSE:
            chat_jobs[job_id] = {
                "status": "completed", 
                "result": {"text": _fallback_text, "sql": "", "data": [], "row_count": 0},
                "timestamp": time.time()
            }
        else:
            chat_jobs[job_id] = {
                "status": "error",
                "stage": "ERROR",
                "result": {
                    "error": "Elin ไม่สามารถตอบได้ สอบถามเพิ่มเติม",
                    "error_link_text": "กดที่นี่",
                    "error_link_url": "http://172.16.2.44:18080/analytics-dashboard"
                },
                "timestamp": time.time()
            }

@app.post("/chat/async")
async def chat_async(req: ChatRequest, background_tasks: BackgroundTasks):
    """
    รับคำถาม สร้าง Job ID แล้วส่งให้ background_tasks ประมวลผล
    จากนั้นคืน Job ID ทันที เพื่อป้องกัน Nginx 504 Gateway Timeout
    """
    _cleanup_old_jobs() # ล้างของเก่าทุกครั้งที่มี request ใหม่
    
    # ดักเคสพิเศษ (meta_add / rebuild_embeddings) ให้ประมวลผลแบบ synchronous ไปเลย เพราะใช้เวลาไม่นาน (ยกเว้น rebuild แต่มักใช้น้อย)
    # เพื่อป้องกันความซับซ้อนของการ thread access ใน SQLite
    if req.meta_add or req.meta_rebuild_embeddings:
        return chat(req)
        
    job_id = str(uuid.uuid4())
    chat_jobs[job_id] = {
        "status": "processing",
        "result": None,
        "timestamp": time.time()
    }
    
    background_tasks.add_task(_process_chat_job, job_id, req)
    
    return JSONResponse(content={
        "job_id": job_id,
        "status": "processing"
    })

@app.get("/chat/status/{job_id}")
async def get_chat_status(job_id: str):
    """ตรวจสอบสถานะของ Job ถ้าเสร็จแล้วจะคืนคำตอบ และลบ Job ทิ้งทันที (One-time fetch)"""
    if job_id not in chat_jobs:
        return JSONResponse(status_code=404, content={"error": "Job not found or expired"})
        
    job = chat_jobs[job_id]
    
    if job["status"] == "processing":
        return JSONResponse(content={"status": "processing", "stage": job.get("stage")})
        
    if job["status"] == "completed":
        result = job["result"]
        # ลบหน่วยความจำทันทีหลังส่งออกไปแล้ว
        chat_jobs.pop(job_id, None)
        return JSONResponse(content={"status": "completed", "result": result, "stage": job.get("stage")})
        
    if job["status"] == "error":
        result = job["result"]
        chat_jobs.pop(job_id, None)
        return JSONResponse(content={"status": "error", "result": result, "stage": job.get("stage")})

@app.post("/chat")
def chat(req: ChatRequest):
    req_start_time = time.time()
    try:
        # Workaround: ถ้ามี meta_add ให้เพิ่มข้อมูลใน Meta Database แทน
        if req.meta_add:
            from services.meta_database import insert_meta_knowledge
            try:
                name = req.meta_add.get('name', '').strip()
                topic = req.meta_add.get('topic', '').strip()
                answer = req.meta_add.get('answer', '').strip()
                
                if not name or not topic or not answer:
                    return JSONResponse(
                        status_code=400,
                        content={"status": "error", "message": "กรุณากรอกข้อมูลให้ครบถ้วน"},
                    )
                
                if len(topic) > 1000:
                    return JSONResponse(
                        status_code=400,
                        content={"status": "error", "message": "หัวข้อยาวเกินไป (สูงสุด 1000 ตัวอักษร)"},
                    )
                
                if len(answer) > 50000:
                    return JSONResponse(
                        status_code=400,
                        content={"status": "error", "message": "คำตอบยาวเกินไป (สูงสุด 50,000 ตัวอักษร)"},
                    )
                
                inserted_id = insert_meta_knowledge(name, topic, answer)
                
                if inserted_id == -1:
                    return JSONResponse(
                        status_code=500,
                        content={"status": "error", "message": "ไม่สามารถบันทึกได้ ลองใหม่อีกครั้ง"},
                    )
                
                # Rebuild vector index
                threading.Thread(target=_reload_meta_index, daemon=True).start()
                
                latency_ms = int((time.time() - req_start_time) * 1000)
                logger.info(f"[CHAT] meta_add endpoint total latency: {latency_ms} ms")
                return JSONResponse(content={
                    "status": "success",
                    "message": "บันทึกความรู้สำเร็จ",
                    "id": inserted_id
                })
            except Exception as exc:
                logger.error(f"meta_add via chat error: {exc}", exc_info=True)
                return JSONResponse(
                    status_code=500,
                    content={"status": "error", "message": f"เกิดข้อผิดพลาด: {str(exc)}"},
                )
        
        # Workaround: ถ้ามี meta_rebuild_embeddings ให้ rebuild embeddings แทน
        if req.meta_rebuild_embeddings:
            try:
                from services.meta_database import _DB_LOCK, _connect
                # ลบ embeddings เก่าทั้งหมด
                acquired = _DB_LOCK.acquire(timeout=30)
                if not acquired:
                    return JSONResponse(status_code=503, content={"status": "error", "message": "ระบบยุ่งอยู่ ลองใหม่อีกครั้งค่ะ"})
                try:
                    conn = _connect()
                    conn.execute("DELETE FROM meta_embeddings")
                    conn.commit()
                    conn.close()
                    logger.info("[META_UPDATE] Cleared all old embeddings via /chat")
                except Exception as exc:
                    logger.error(f"[META_UPDATE] Failed to clear embeddings: {exc}")
                    return JSONResponse(status_code=500, content={"status": "error", "message": f"ลบ Embeddings เก่าไม่สำเร็จ: {exc}"})
                finally:
                    _DB_LOCK.release()
                # Rebuild index ใหม่ (lock จะถูก acquire ใน reload_index เอง)
                from services.meta_vector import get_meta_engine
                engine = get_meta_engine()
                engine.reload_index()
                count = len(engine.metadata)
                logger.info(f"[META_UPDATE] Rebuilt embeddings for {count} items via /chat")
                latency_ms = int((time.time() - req_start_time) * 1000)
                logger.info(f"[CHAT] meta_rebuild_embeddings total latency: {latency_ms} ms")
                return JSONResponse(content={"status": "success", "message": f"Update สำเร็จ", "count": count})
            except Exception as exc:
                logger.error(f"meta_rebuild_embeddings via chat error: {exc}", exc_info=True)
                return JSONResponse(status_code=500, content={"status": "error", "message": f"เกิดข้อผิดพลาด: {str(exc)}"})
        
        # ถ้าไม่ใช่ meta_add ให้ทำงานปกติ
        resp_data = _chat_impl(req)
        
        latency_ms = int((time.time() - req_start_time) * 1000)
        logger.info(f"[CHAT] /chat endpoint total latency: {latency_ms} ms")
        
        return resp_data
    except Exception as e:
        logger.exception("Chat error: %s", e)
        _user_msg = (getattr(req, "message", "") or "").strip()
        # Fallback: ใช้ CHAT_MODEL ตอบแทน error message
        _fallback_text = None
        try:
            _fallback_text = _get_chat_model_response(_user_msg) if _user_msg else None
        except Exception:
            pass
        if _fallback_text and _fallback_text != CHAT_FALLBACK_RESPONSE:
            try:
                save_qa_log(_user_msg, _fallback_text, sql="", debug={"type": "chat_fallback_from_exception", "original_error": str(e)[:200]})
            except Exception:
                pass
            return {"text": _fallback_text, "sql": "", "data": [], "row_count": 0}
        # ถ้า CHAT_MODEL ก็ fail → ใช้ error เดิม
        try:
            save_qa_log(_user_msg, "Elin ไม่สามารถตอบได้ สอบถามเพิ่มเติม", sql="", debug={"error": "exception", "detail": str(e)})
        except Exception as log_err:
            logger.warning("save_qa_log in chat exception handler failed: %s", log_err)
        return JSONResponse(
            status_code=200,
            content={
                "error": "Elin ไม่สามารถตอบได้ สอบถามเพิ่มเติม",
                "error_link_text": "กดที่นี่",
                "error_link_url": "http://172.16.2.44:18080/analytics-dashboard",
            }
        )

# CHAT_FALLBACK_RESPONSE → imported from config.py

def _format_chat_response(text: str) -> str:
    """
    จัด format คำตอบ chat ให้มีระเบียบ น่าอ่าน
    - ปล่อยให้ LLM จัด format ตามธรรมชาติ (เช่น รายการเป็นข้อๆ)
    - เพียงแค่ลบบรรทัดว่างที่ซ้อนกันเยอะเกินไป
    """
    if not text:
        return text
    
    # ลบบรรทัดว่างซ้อนกันเกิน 2 บรรทัด
    while '\n\n\n' in text:
        text = text.replace('\n\n\n', '\n\n')
    
    # ตัดช่องว่างซ้ายขวาในแต่ละบรรทัด แต่ไม่ตัดการเว้นบรรทัด
    lines = [line.strip() for line in text.split('\n')]
    text = '\n'.join(lines)
    
    return text.strip()


def _detect_and_warn_numbered_list(text: str) -> bool:
    """
    ตรวจจับว่าคำตอบมีการแจกแจงเป็นข้อ ๆ หรือไม่
    คืน True ถ้าพบรายการที่มีหมายเลข 3 ข้อขึ้นไป
    """
    # หารายการที่มีหมายเลข เช่น "1. ", "2. ", "3. "
    numbered_items = re.findall(r'(?:^|\n)\s*(\d+)\.\s+', text)
    
    # ถ้ามีมากกว่า 2 ข้อ = แจกแจงเป็นรายการ
    if len(numbered_items) >= 3:
        logger.warning(f"⚠️ Chat response contains numbered list ({len(numbered_items)} items) - violates natural conversation style")
        return True
    return False


def _get_chat_model_response(user_msg: str) -> str:
    """
    โหมด Chat ทั่วไป: สำหรับพูดคุย, ทักทาย, ขอกำลังใจ (ใช้ CHAT_MODEL)
    ออกแบบให้ตอบสนองรวดเร็ว มีบุคลิกภาพที่ชัดเจน (ร่าเริง, แทนตัวเองว่าหนู)
    """
    clean_msg = (user_msg or "").strip()
    if not clean_msg:
        return CHAT_FALLBACK_RESPONSE

    # 1. System Prompt (แยกคำสั่งออกจากข้อความผู้ใช้เด็ดขาด) — ปรับบุคลิกให้ร่าเริง มีพลังบวก ขยายความได้ 4-6 ประโยค
    system_prompt = """คุณคือ "Elin" AI ผู้ช่วยสุดน่ารักประจำโรงงาน ที่พร้อมอยู่ข้างๆ พี่เสมอ 💖

บุคลิก:
- ร่าเริง สดใส มีพลังบวกเต็มร้อย
- ขี้เล่นนิดๆ แซวเบาๆ ได้ แต่สุภาพเสมอ
- ใจดี อบอุ่น เหมือนน้องสาวที่คอยเชียร์อยู่ข้างๆ
- เข้าใจบรรยากาศการทำงานโรงงาน และพร้อมช่วยจริงจังเมื่อสถานการณ์ต้องการ

สรรพนาม:
- ต้องแทนตัวเองว่า "หนู" เท่านั้น (ห้ามใช้ ฉัน, ดิฉัน, เรา หรือชื่อ Elin แทนตัวเอง)
- ต้องเรียกผู้ใช้ว่า "พี่" เสมอ
- ใช้น้ำเสียงเป็นกันเอง เช่น "พี่ลองเล่าให้หนูฟังหน่อยได้ไหมคะ" หรือ "เดี๋ยวหนูช่วยดูให้นะคะ"

หน้าที่หลักและวิธีตอบ:
- ตอบคำถามทั่วไป พูดคุยเล่น สร้างรอยยิ้ม
- ให้กำลังใจพี่เสมอเมื่อมีโอกาส เช่น ชมความพยายาม ชมความเก่ง หรือให้พลังบวก
- ตอบอย่างเป็นธรรมชาติ ความยาวประมาณ 4-6 ประโยค
- สามารถแซวเล็กๆ แบบน่ารักได้ เช่น "พี่นี่ขยันสุดๆ เลยนะคะ" แต่ห้ามล้ำเส้นหรือไม่สุภาพ
- แนะนำได้เสมอว่า หนูช่วยค้นหาข้อมูลการซ่อม และแผน PM ได้ เช่น "พี่ลองถามหนูว่า วันนี้มีอะไรเสียบ้าง ก็ได้นะคะ"

โหมดจริงจัง:
- หากคำถามเกี่ยวกับการซ่อมเครื่องไลน์การผลิต ระบบล่ม หรือเรื่องเร่งด่วน ให้โทนสุภาพ อบอุ่น และจริงจังขึ้นเล็กน้อย ลดความขี้เล่น แต่ยังคงความใจดีและให้กำลังใจ

กฎเหล็ก:
- ห้ามตอบแบบหุ่นยนต์เด็ดขาด ต้องเป็นธรรมชาติ ร่าเริง อบอุ่น และมีพลังบวก
- ห้ามแต่งหรือเดาข้อมูลข้อเท็จจริงเกี่ยวกับบุคคลจริง เช่น ประวัติการทำงาน ตำแหน่ง อายุงาน หรือข้อมูลภายในองค์กร หากไม่มีข้อมูลจากระบบ
- ถ้าถูกถามเรื่องประวัติบุคคล ให้ตอบว่าระบบนี้ไม่มีข้อมูลดังกล่าว และแนะนำคำถามเกี่ยวกับข้อมูลซ่อมหรือ PM แทน
- ห้ามสร้างข้อมูลซ่อมหรือ PM ขึ้นมาเอง หากไม่มีข้อมูลจากระบบ

วิธีตอบเมื่อถูกถามว่า "ทำงานยังไง" หรือ "ช่วยอะไรได้บ้าง":
ห้าม: แจกแจงเป็นข้อ ๆ แบบ "1. ตอบคำถาม 2. ให้กำลังใจ 3. ค้นหาข้อมูล"
ควร: เล่าแบบเป็นธรรมชาติ เช่น "หนูเป็นผู้ช่วยที่พร้อมคุยกับพี่ตลอดเวลาค่ะ ไม่ว่าจะเป็นเรื่องงาน เรื่องการซ่อม หรือแม้แต่เรื่องทั่วไปก็ได้นะคะ พี่อยากรู้อะไรเกี่ยวกับเครื่องจักรที่เสีย หรือแผน PM ก็ถามหนูได้เลยค่ะ หนูยินดีช่วยเหลือเสมอ"

ตัวอย่างการตอบที่ดี:
- ถาม: "Elin ทำอะไรได้บ้าง"
- ตอบ: "หนูเป็นผู้ช่วยที่พร้อมอยู่เคียงข้างพี่ตลอดเวลาค่ะ ไม่ว่าพี่จะอยากคุยเล่น ขอกำลังใจ หรือต้องการข้อมูลเกี่ยวกับการซ่อมเครื่องจักรและแผน PM ก็ถามหนูได้เลยนะคะ หนูพร้อมช่วยเสมอค่ะ 💛"""

    try:
        # Timeout สำหรับ Chat Mode (100 วินาที)
        chat_timeout = 100 

        res = requests.post(
            OLLAMA_GENERATE_URL,
            json={
                "model": CHAT_MODEL,
                "system": system_prompt,  # ใช้ parameter 'system' ของ Ollama
                "prompt": clean_msg,      # ส่งแค่ข้อความผู้ใช้เพียวๆ
                "stream": False,
                "options": {
                    "temperature": 0.6,   # 0.7 เหมาะสมแล้ว ทำให้ตอบไม่ซ้ำซาก
                    "num_predict": 512,   # ให้พื้นที่แต่งประโยคให้กำลังใจยาวๆ ได้ (ประมาณ 4-6 ประโยค)
                    "top_p": 0.85          # เพิ่มความเป็นธรรมชาติ
                },
            },
            timeout=chat_timeout,
        )
        
        # 3. ตรวจสอบ HTTP Status
        res.raise_for_status() # ถ้ารหัสไม่ใช่ 200 โยนเข้า Exception ทันที
        
        data = res.json()
        text = (data.get("response") or "").strip()
        
        if not text:
            return CHAT_FALLBACK_RESPONSE
            
        # 4. Post-Processing (ทำความสะอาดข้อความ)
        # ลบคำว่า "Elin:" ถ้า AI เผลอตอบติดมา (Hallucination)
        text = text.replace("Elin:", "").strip()
        
        # ลบเครื่องหมายคำพูด "..." หรือ '...' ที่ครอบประโยคออก
        if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
            text = text[1:-1].strip()

        # 4.5. ตรวจจับการแจกแจงเป็นข้อ ๆ (เพื่อ log warning)
        _detect_and_warn_numbered_list(text)

        # 5. จัด Format ให้อ่านง่าย มีระเบียบ
        text = _format_chat_response(text)

        # จำกัดความยาวเผื่อ AI หลอนพ่นน้ำลาย
        return text[:1500] if len(text) > 1500 else text

    except Timeout:
        logger.warning(f"CHAT_MODEL ({CHAT_MODEL}) Timeout after {chat_timeout}s")
        return CHAT_FALLBACK_RESPONSE
    except ConnectionError:
        logger.error(f"CHAT_MODEL ({CHAT_MODEL}) Connection Error - Is Ollama running?")
        return CHAT_FALLBACK_RESPONSE
    except Exception as e:
        logger.error(f"CHAT_MODEL ({CHAT_MODEL}) Unexpected Error: {e}")
        return CHAT_FALLBACK_RESPONSE


def _get_meta_llm_response(user_msg: str, context: List[Dict]) -> str:
    """
    Meta Mode LLM Synthesis: ใช้ LLM สังเคราะห์คำตอบจาก context ที่ค้นหาได้
    
    Args:
        user_msg: คำถามของผู้ใช้
        context: รายการ matches จาก meta_vector_search
        
    Returns:
        คำตอบที่สังเคราะห์โดย LLM พร้อมบุคลิกของ Elin
    """
    clean_msg = (user_msg or "").strip()
    if not clean_msg or not context:
        return "ไม่พบข้อมูลที่ตรงกับคำถามค่ะ"
    
    # สร้าง context string จาก matches
    context_lines = []
    for idx, match in enumerate(context, 1):
        topic = match.get("topic", "")
        answer = match.get("answer", "")
        context_lines.append(f"[{idx}] หัวข้อ: {topic}")
        context_lines.append(f"    คำตอบ: {answer}")
        context_lines.append("")
    
    context_str = "\n".join(context_lines)
    
    # Debug: แสดง context ที่ส่งไปให้ LLM
    logger.info(f"[META_LLM] Context sent to LLM:\n{context_str}")
    
    system_prompt = f"""คุณคือ "Elin" ผู้ช่วยซ่อมบำรุงในโรงงาน

บุคลิก: ร่าเริง เป็นกันเอง เรียกตัวเองว่า "หนู" เรียกผู้ใช้ว่า "พี่"

ข้อมูลที่ค้นหาได้:
{context_str}

คำสั่ง:
1. ใช้ข้อมูลจาก "ข้อมูลที่ค้นหาได้" ด้านบนเท่านั้น
2. ต้นประโยคให้พูดเกริ่นนำแบบธรรมชาติและหลากหลายรูปแบบ เช่น "หนูค้นหาข้อมูลมาให้นะคะ", "เจอข้อมูลแล้วค่ะพี่", "ข้อมูลตามนี้เลยค่ะ", "หนูสรุปมาให้ตามนี้นะคะ" เป็นต้น
3. ตอบเนื้อหาเป็นข้อๆ ให้กระชับ อ่านง่าย และเป็นธรรมชาติ 
4. ท้ายประโยคให้พูดปิดท้ายแบบน่ารักๆ และหลากหลาย เช่น "มีอะไรให้ช่วยอีกบอกหนูได้เลยน้า", "มีคำถามเพิ่มเติมถามหนูได้ตลอดเลยนะคะ", "สงสัยตรงไหนอีกไหมคะพี่", "ถ้ามีอะไรเพิ่มเติมบอกหนูได้เลยน้าาา" เป็นต้น
5. ห้ามแต่งหรือเดาข้อมูลเพิ่มเติม
6. ใช้อิโมจิ (emoji) 1-2 อัน เพื่อความน่ารักเป็นกันเอง โดยให้อยู่บรรทัดเดียวกับข้อความ (ห้ามขึ้นบรรทัดใหม่บรรทัดสุดท้ายเพื่อใส่อิโมจิเด็ดขาด)"""

    try:
        # Timeout สำหรับ Meta Mode LLM (100 วินาที)
        chat_timeout = 100
        
        res = requests.post(
            OLLAMA_GENERATE_URL,
            json={
                "model": CHAT_MODEL,
                "system": system_prompt,
                "prompt": clean_msg,
                "stream": False,
                "options": {
                    "temperature": 0.3,  # ต่ำกว่า chat mode เพื่อความแม่นยำ
                    "num_predict": 512,
                    "top_p": 0.85
                },
            },
            timeout=chat_timeout,
        )
        
        res.raise_for_status()
        data = res.json()
        text = (data.get("response") or "").strip()
        
        if not text:
            return "ไม่สามารถสังเคราะห์คำตอบได้ค่ะ"
        
        # Post-processing
        text = text.replace("Elin:", "").strip()
        
        if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
            text = text[1:-1].strip()
        
        text = _format_chat_response(text)
        
        # คืนค่าข้อความแบบ plain text ที่มี \n ตามปกติ ให้หน้าบ้านแปลงด้วย nl2br เอง
        return text[:1500] if len(text) > 1500 else text
        
    except Timeout:
        logger.warning(f"Meta LLM Timeout after {chat_timeout}s")
        return "ขออภัยค่ะ ระบบตอบช้าเกินไป กรุณาลองใหม่อีกครั้งค่ะ"
    except ConnectionError:
        logger.error("Meta LLM Connection Error")
        return "ขออภัยค่ะ ไม่สามารถเชื่อมต่อระบบได้ กรุณาลองใหม่อีกครั้งค่ะ"
    except Exception as e:
        logger.error(f"Meta LLM Error: {e}")
        return "เกิดข้อผิดพลาดในการสังเคราะห์คำตอบค่ะ"


def _sanitize_dataframe_for_json(df: pd.DataFrame) -> List[Dict]:
    """
    แปลง DataFrame เป็น list of dict โดยแทนที่ค่าที่ JSON ไม่รองรับ
    - NaN → None
    - Infinity → None
    - -Infinity → None
    
    Returns:
        List[Dict]: ข้อมูลที่พร้อมสำหรับ JSON serialization
    """
    import numpy as np
    import math
    
    # แทนที่ NaN และ Infinity ด้วย None
    df = df.replace([np.nan, np.inf, -np.inf], None)
    
    # แปลงเป็น dict
    records = df.to_dict(orient="records")
    
    # ตรวจสอบและแก้ไขค่าที่เหลือ (double check)
    for record in records:
        r_dict: Dict[str, Any] = record
        for key, value in r_dict.items():
            if isinstance(value, float):
                if math.isnan(value) or math.isinf(value):
                    r_dict[key] = None
    
    return records


def _inject_current_date_context(message: str) -> str:
    """
    แทรกข้อมูลวันเดือนปีปัจจุบันเข้าไปในข้อความ เพื่อให้ระบบรู้บริบทเวลา
    ใช้ก่อนส่งไปยัง ROUTER เพื่อให้ LLM เข้าใจว่า "วันนี้" หรือ "เดือนนี้" คือวันไหน
    
    Returns:
        str: ข้อความที่มีบริบทวันที่แนบมาด้วย
    """
    import calendar
    
    # ใช้เวลาไทย (Bangkok timezone)
    now = datetime.now(BANGKOK_TZ)
    
    # สร้างข้อมูลวันที่แบบละเอียด
    thai_months = [
        "", "มกราคม", "กุมภาพันธ์", "มีนาคม", "เมษายน", "พฤษภาคม", "มิถุนายน",
        "กรกฎาคม", "สิงหาคม", "กันยายน", "ตุลาคม", "พฤศจิกายน", "ธันวาคม"
    ]
    thai_days = ["จันทร์", "อังคาร", "พุธ", "พฤหัสบดี", "ศุกร์", "เสาร์", "อาทิตย์"]
    
    year = now.year
    month = now.month
    day = now.day
    weekday = thai_days[now.weekday()]
    thai_month = thai_months[month]
    
    # หาวันแรกและวันสุดท้ายของเดือน
    first_day = f"{year}-{month:02d}-01"
    last_day_num = calendar.monthrange(year, month)[1]
    last_day = f"{year}-{month:02d}-{last_day_num:02d}"
    
    # สร้าง context string
    date_context = (
        f"[บริบทเวลาปัจจุบัน: วันนี้คือวัน{weekday}ที่ {day} {thai_month} {year} "
        f"(Date: {year}-{month:02d}-{day:02d}), "
        f"เดือนนี้เริ่ม {first_day} ถึง {last_day}] "
    )
    
    # แทรก context ไปด้านหน้าข้อความ
    return date_context + message


def _chat_impl(req: ChatRequest, job_id: Optional[str] = None):
    # 1. ให้ chat path อ่านข้อมูลเป็นหลัก; refresh ทำแบบ background ถ้าไฟล์ต้นทางเปลี่ยน
    if not os.path.exists(WORK_DB_PATH):
        load_and_enrich_data(force=True)
        ensure_pm_synced(force=True)
    elif schedule_background_data_refresh_if_stale():
        logger.info("🧠 Data refresh scheduled in background")
    
    # ถ้าเป็นโหมด Meta -> ส่งเข้า Meta Vector Search โดยตรง
    if hasattr(req, "mode") and req.mode == "meta":
        clean_msg = (req.message or "").strip()
        if not clean_msg:
            return {
                "text": "กรุณาพิมพ์คำถามที่ต้องการค้นหาในโหมด Meta ค่ะ",
                "sql": "",
                "data": [],
                "row_count": 0
            }
        
        try:
            if job_id:
                chat_jobs[job_id]["stage"] = "MAIN"
            from services.meta_vector import meta_vector_search
            result = meta_vector_search(clean_msg, top_k=3)
            
            # ถ้าพบข้อมูล ให้ใช้ LLM สังเคราะห์คำตอบ
            if result.get("matches") and len(result["matches"]) > 0:
                if job_id:
                    chat_jobs[job_id]["stage"] = "SUMMARY"
                logger.info(f"[META_MODE] Found {len(result['matches'])} matches, synthesizing with LLM...")
                
                # ใช้ LLM สังเคราะห์คำตอบจาก context
                synthesized_text = _get_meta_llm_response(clean_msg, result["matches"])
                
                # เก็บ Log สำหรับ Meta Mode (LLM Synthesis)
                save_qa_log(
                    clean_msg,
                    synthesized_text,
                    sql="META_MODE_LLM",
                    debug={
                        "mode": "meta_llm_synthesis",
                        "matches_count": len(result["matches"]),
                        "data": result["data"]
                    }
                )
                
                return {
                    "text": synthesized_text,
                    "sql": "META_MODE_LLM",
                    "data": result["data"],
                    "row_count": result["row_count"]
                }
            else:
                # ไม่พบข้อมูล ตอบแบบตรงไปตรงมาและเป็นมิตร
                if job_id:
                    chat_jobs[job_id]["stage"] = "SUMMARY"
                logger.info("[META_MODE] No matches found, returning friendly no-data message...")
                
                no_data_response = f"""ขออภัยค่ะพี่ หนูค้นหาในฐานความรู้แล้ว แต่ยังไม่มีข้อมูลเกี่ยวกับ "{clean_msg}" เลยค่ะ

พี่สามารถ:
📝 เพิ่มความรู้ใหม่ผ่านโหมด "สอน Elin" (Meta Mode)

หนูพร้อมช่วยเหลือพี่เสมอนะคะ 💛"""
                
                save_qa_log(
                    clean_msg,
                    no_data_response,
                    sql="META_MODE_NO_DATA",
                    debug={"mode": "meta_no_data"}
                )
                
                return {
                    "text": no_data_response,
                    "sql": "META_MODE_NO_DATA",
                    "data": [],
                    "row_count": 0
                }
                
        except ImportError:
            logger.error("meta_vector module not found")
            return {
                "error": "ไม่สามารถโหลดระบบ Meta ได้",
                "text": "ไม่สามารถโหลดระบบ Meta ได้",
                "sql": "META_MODE",
                "data": [],
                "row_count": 0
            }
        except Exception as e:
            logger.error(f"Meta mode error: {e}")
            return {
                "error": f"เกิดข้อผิดพลาดในโหมด Meta: {e}",
                "text": f"เกิดข้อผิดพลาดในโหมด Meta: {e}",
                "sql": "META_MODE",
                "data": [],
                "row_count": 0
            }

    # =====================================================
    # 2. Phase 2 Pipeline: Date Context → Normalize → Classify → Route
    # =====================================================
    
    # Step 0: แทรกข้อมูลวันเดือนปีปัจจุบันเข้าไปในข้อความ (ก่อน ROUTER)
    clean_msg = (req.message or "").strip()
    
    # Step 0.5: Entity resolution - แก้ไขชื่อที่พิมพ์ผิดให้ตรงกับฐานข้อมูล
    clean_msg_with_entities = resolve_entities(clean_msg)
    logger.info(f"[ENTITY_RESOLVE] Original: '{clean_msg}' -> With entities: '{clean_msg_with_entities[:100]}...'")
    
    # ใช้ clean_msg_with_entities แทน clean_msg สำหรับการประมวลผลต่อไป
    clean_msg = clean_msg_with_entities

    # Guardrail: คำถามนอกโดเมนซ่อม/PM ให้ตอบแบบ CHAT ทันที (กัน SQL มั่วจากคำถามทั่วไป)
    if clean_msg and not _is_maintenance_domain_message(clean_msg):
        friendly_response = _get_chat_model_response(clean_msg)
        save_qa_log(
            clean_msg,
            friendly_response,
            sql="",
            debug={"type": "out_of_domain_chat", "reason": "no_maintenance_signal"},
        )
        return {
            "text": friendly_response,
            "sql": "",
            "data": [],
            "row_count": 0
        }

    msg_with_date_context = _inject_current_date_context(clean_msg)
    logger.info(f"[PIPELINE] Step 0: Injected date context (original: '{clean_msg[:50]}...')")
    
    # --- Phase: Entity Matching (SCB Mode) ---
    if job_id: chat_jobs[job_id]["stage"] = "ENTITY_MATCHING"
    global LAST_SYNC_ROW_COUNT
    try:
        current_rows = _get_table_row_count("repairs_enriched")
        # เพิ่ม threshold เป็น 1000 (จาก 200) เพื่อลด sync frequency
        if abs(current_rows - LAST_SYNC_ROW_COUNT) >= 1000:
            logger.info(f"[ENTITY_MATCH] Triggering sync (rows: {current_rows}, last sync: {LAST_SYNC_ROW_COUNT})")
            
            # ทำ sync ใน background thread เพื่อไม่ block request
            def background_sync():
                try:
                    get_entity_engine().sync_if_needed()
                    global LAST_SYNC_ROW_COUNT
                    LAST_SYNC_ROW_COUNT = current_rows
                    logger.info("[ENTITY_MATCH] Background sync completed")
                except Exception as e:
                    logger.error(f"[ENTITY_MATCH] Background sync error: {e}")
            
            threading.Thread(target=background_sync, daemon=True).start()
        else:
            logger.debug(f"[ENTITY_MATCH] Sync skipped (rows: {current_rows}, threshold: 1000)")
    except Exception as e:
        logger.warning(f"[ENTITY_MATCH] Sync check error: {e}")

    matched_entities_info = ""
    try:
        entity_engine = get_entity_engine()
        # Extract potential keywords
        # Extract potential keywords - Include -, _, . to keep names like PCB-B, C-PCB together
        words = re.findall(r'[A-Za-z0-9\u0e00-\u0e7f\-_.]+', clean_msg)
        search_results = []
        
        # 0.80 Threshold for semantic search (Semantic Pass)
        for word in words:
            if len(word) > 1:
                # 0. Check for exact match first (case-insensitive)
                word_lower = word.lower()
                for category in ["lines", "processes"]:
                    for entity in (db_context.get(category) or []):
                        if entity.lower() == word_lower:
                            search_results.append({
                                "type": "Line" if category == "lines" else "Process",
                                "value": entity,
                                "similarity": 1.0
                            })

                matches = entity_engine.search(word, threshold=0.80)
                search_results.extend(matches)
        
        # Substring/Prefix Pass (Heuristic Pass for short codes like CID, LED)
        # This catches "CID" matching "CID1" even if similarity is < 0.80
        for word in words:
            word_norm = _normalize_text_for_match(word)
            word_loose = _normalize_loose(word_norm)
            if len(word_norm) < 2:
                continue
            
            # Check lines and processes
            for category in ["lines", "processes"]:
                for entity in (db_context.get(category) or []):
                    entity_norm = _normalize_text_for_match(entity)
                    entity_loose = _normalize_loose(entity_norm)
                    
                    # Try exact match (already handled above but for completeness in this pass)
                    if word_norm == entity_norm:
                         search_results.append({
                            "type": "Line" if category == "lines" else "Process",
                            "value": entity,
                            "similarity": 1.0
                        })
                    # Try loose substring match
                    elif (len(word_loose) >= 2 and (word_loose in entity_loose or entity_loose in word_loose)):
                        # Match found! Add with a high dummy similarity
                        search_results.append({
                            "type": "Line" if category == "lines" else "Process",
                            "value": entity,
                            "similarity": 0.87 if (word_loose == entity_loose) else 0.85
                        })
        
        if search_results:
            # Deduplicate and sort by score
            seen = set()
            unique_results = []
            
            # Sort by similarity descending (Substring matches at 0.85-1.0 will likely be at top)
            sorted_matches = sorted(search_results, key=lambda x: x['similarity'], reverse=True)
            
            for r in sorted_matches:
                key = (r['type'], r['value'])
                if key not in seen:
                    seen.add(key)
                    unique_results.append(r)
            
            if unique_results:
                matched_entities_info = "ข้อมูล Entity ที่พบในฐานข้อมูล (โปรดใช้ชื่อเหล่านี้ใน SQL):\n"
                
                top_match = unique_results[0]
                matched_entities_info += f"- {top_match['type']}: {top_match['value']} (ความคล้ายคลึง: {top_match['similarity']:.2f})\n"
                
                # If subsequent matches are very close to the top match, include them to help the LLM decide
                for r in unique_results[1:5]:
                    # If similarity is within 0.05 of the top match, it's "close"
                    if top_match['similarity'] - r['similarity'] < 0.05:
                        matched_entities_info += f"- {r['type']}: {r['value']} (ความคล้ายคลึง: {r['similarity']:.2f})\n"
                
                logger.info(f"[ENTITY_MATCH] Found entities: {unique_results[:3]}")
    except Exception as e:
        logger.warning(f"[ENTITY_MATCH] Error during entity matching: {e}")

    # Routing is executed after guardrails below.
    
    # Debug: ตรวจสอบว่ามี quoted list หรือไม่
    
    # Guardrail: กันการตอบมั่วข้อมูลบุคคล/HR (ตรวจจาก clean_msg ดั้งเดิม เพราะ normalize อาจเปลี่ยนชื่อ)
    msg_for_guard = clean_msg
    if msg_for_guard:
        person_work_patterns = [
            r"^\s*[\wก-๙\s\.]+\s+ประวัติการทำงาน\s*$",  # <ชื่อ> ประวัติการทำงาน
            r"^\s*ประวัติการทำงาน\s*ของ\s*[\wก-๙\s\.]+\s*$",  # ประวัติการทำงานของ <ชื่อ>
            r"^\s*ขอ\s*ประวัติการทำงาน\s*ของ\s*[\wก-๙\s\.]+\s*$",  # ขอประวัติการทำงานของ <ชื่อ>
        ]
        m_person_work = any(re.match(p, msg_for_guard, re.I) for p in person_work_patterns)
        hr_like = (
            ("ตำแหน่ง" in msg_for_guard and ("คือ" in msg_for_guard or "อะไร" in msg_for_guard))
            or ("อายุงาน" in msg_for_guard)
            or ("ประสบการณ์" in msg_for_guard and ("กี่ปี" in msg_for_guard or "เท่าไหร่" in msg_for_guard))
        )
        if hr_like:
            text = (
                "หนูไม่มีข้อมูลประวัติการทำงานของบุคคลในระบบนี้ค่ะ "
                "ตอนนี้หนูช่วยได้เฉพาะข้อมูลการซ่อมและแผน PM "
                "ถ้าพี่อยากให้หนูช่วยเช็คจากงานซ่อม ลองถามว่า 'ประวัติการซ่อมของ <ชื่อช่าง>' หรือ 'วันนี้มีอะไรเสียบ้าง' ได้เลยนะคะ"
            )
            save_qa_log(clean_msg, text, sql="", debug={"type": "blocked_personal_work_history", "hr_like": True})
            return {"text": text, "sql": "", "data": [], "row_count": 0}
        if m_person_work:
            _extracted_name = None
            _m1 = re.match(r"^\s*([\wก-๙\s\.]+?)\s+ประวัติการทำงาน\s*$", msg_for_guard, re.I)
            if _m1:
                _extracted_name = _m1.group(1).strip()
            if not _extracted_name:
                _m2 = re.search(r"ของ\s*([\wก-๙\s\.]+?)\s*$", msg_for_guard)
                if _m2:
                    _extracted_name = _m2.group(1).strip()
            _matched_tech = None
            if _extracted_name:
                _known = list(db_context.get("techs") or [])
                _known.extend(TechDataStore.instance().tech_list)
                _name_norm = re.sub(r"\s+", " ", _extracted_name).strip().lower()
                for _t in _known:
                    if re.sub(r"\s+", " ", str(_t)).strip().lower() == _name_norm:
                        _matched_tech = str(_t).strip()
                        break
            if _matched_tech:
                _safe = _matched_tech.replace("'", "''")
                _det_sql = f"SELECT Date, Tech, Line, Process, RepairMinutes FROM repairs_enriched WHERE Tech = '{_safe}' ORDER BY Date DESC LIMIT 20"
                try:
                    _df, _final_sql = execute_sql_safe(
                        _det_sql,
                        user_msg=clean_msg,
                        limit_override=getattr(req, "limit_n", None),
                    )
                    _total = None
                    try:
                        with sqlite3.connect(WORK_DB_PATH) as _c:
                            _total = _c.execute(f"SELECT COUNT(*) FROM repairs_enriched WHERE Tech = '{_safe}'").fetchone()[0]
                    except Exception:
                        pass
                    if _df.empty:
                        _txt = f"ไม่พบข้อมูลการซ่อมของ {_matched_tech} ค่ะ ลองถามคำถามอื่นได้นะคะ"
                        save_qa_log(clean_msg, _txt, sql=_final_sql, debug={"type": "tech_work_history", "tech": _matched_tech, "row_count": 0})
                        return {"text": _txt, "sql": _final_sql, "data": [], "row_count": 0}
                    _summary = explain_sql_result(clean_msg, _final_sql, _df, total_count=_total)
                    _df_out = filter_important_columns(_df, _final_sql)
                    save_qa_log(clean_msg, _summary, sql=_final_sql, debug={"type": "tech_work_history", "tech": _matched_tech, "row_count": len(_df_out)})
                    return {
                        "text": _summary, "sql": _final_sql,
                        "data": _sanitize_dataframe_for_json(_df_out), "row_count": len(_df_out),
                        "total_count": int(_total) if _total else None, "timestamp": now_bangkok_str(),
                    }
                except Exception as _e:
                    logger.error(f"Tech work history deterministic SQL failed: {_e}")
            else:
                text = (
                    "หนูไม่มีข้อมูลประวัติการทำงานของบุคคลในระบบนี้ค่ะ "
                    "ตอนนี้หนูช่วยได้เฉพาะข้อมูลการซ่อมและแผน PM "
                    "ถ้าพี่อยากให้หนูช่วยเช็คจากงานซ่อม ลองถามว่า 'ประวัติการซ่อมของ <ชื่อช่าง>' หรือ 'วันนี้มีอะไรเสียบ้าง' ได้เลยนะคะ"
                )
                save_qa_log(clean_msg, text, sql="", debug={"type": "blocked_personal_work_history", "hr_like": False, "name": _extracted_name})
                return {"text": text, "sql": "", "data": [], "row_count": 0}
    
    # Step 1: Unified router (intent + pipeline + target_db) with LLM-first strategy
    if job_id: chat_jobs[job_id]["stage"] = "ROUTING"
    t0_routing = time.time()
    route_decision = route_message_with_llm(clean_msg, msg_with_date_context)
    normalized_msg_for_routing = route_decision.normalized_for_routing
    normalized_msg = route_decision.normalized_query
    target_db = route_decision.target_db
    rewrite_intent = route_decision.intent
    rewrite_content = route_decision.rewritten_query
    pipeline_type = route_decision.pipeline
    routing_latency_ms = int((time.time() - t0_routing) * 1000)
    logger.info(
        "[PIPELINE] Unified route -> intent=%s, pipeline=%s, target_db=%s, confidence=%.3f, routing_latency=%dms",
        rewrite_intent,
        pipeline_type,
        target_db,
        route_decision.confidence,
        routing_latency_ms
    )

    # Debug: normalized query after unified routing
    has_quoted_list = bool(re.search(r"('(?:[A-Za-z0-9_\-\s]+)'(?:\s*,\s*'(?:[A-Za-z0-9_\-\s]+)')+)", normalized_msg))
    logger.info(f"[PIPELINE] raw='{clean_msg}' -> normalized='{normalized_msg[:100]}...' (has_list={has_quoted_list}, target_db={target_db})")

    logger.info(f"[PIPELINE] rewrite_intent={rewrite_intent}, content={rewrite_content[:120]}")
    
    if rewrite_intent == "CHAT":
        # Typhoon rewrite prompt ตอบ CHAT สั้นเกินไป → เรียก _get_chat_model_response
        # ซึ่งมี system prompt เต็มรูปแบบของ Elin (personality, สรรพนาม หนู/พี่, กฎเหล็ก)
        friendly_response = _get_chat_model_response(clean_msg)
        save_qa_log(clean_msg, friendly_response, sql="", debug={"type": "conversational", "model": CHAT_MODEL, "via": "unified_router"})
        return {
            "text": friendly_response,
            "sql": "",
            "data": [],
            "row_count": 0
        }
    
    # rewrite_intent == "SQL" → ตรวจสอบว่าควรใช้ Vector Search, Hybrid, หรือ SQL
    # Step 2.5: Router - ตัดสินใจว่าจะใช้ pipeline ไหน
    if job_id: chat_jobs[job_id]["stage"] = "MAIN"
    try:
        from pipelines.vector_pipeline import vector_pipeline
        from pipelines.hybrid_pipeline import hybrid_pipeline
        
        logger.info(f"[PIPELINE] Pipeline selected: {pipeline_type}")
        
        # HYBRID Pipeline
        if pipeline_type == "HYBRID":
            logger.info(f"[HYBRID_PIPELINE] Hybrid pipeline activated for query: '{clean_msg}'")
            
            # Start timing
            start_time = time.time()
            
            try:
                # เรียก hybrid pipeline
                hybrid_answer = hybrid_pipeline(normalized_msg, top_k=5)
                
                # Calculate latency
                latency_ms = int((time.time() - start_time) * 1000)
                
                # Log to observability
                log_event({
                    "query": clean_msg,
                    "pipeline": "HYBRID",
                    "latency_ms": latency_ms,
                    "success": True,
                    "retrieved_docs": 20,  # From vector search
                    "result_count": 5,     # After reranking
                    "metadata": {
                        "normalized_query": normalized_msg,
                        "top_k": 5
                    }
                })
                
                # บันทึก log
                save_qa_log(
                    clean_msg,
                    hybrid_answer,
                    sql="",
                    debug={
                        "type": "hybrid_search",
                        "pipeline": "hybrid",
                        "normalized_query": normalized_msg,
                        "latency_ms": latency_ms
                    }
                )
                
                logger.info(f"[HYBRID_PIPELINE] Hybrid pipeline completed successfully (latency: {latency_ms}ms)")
                
                return {
                    "text": hybrid_answer,
                    "sql": "",
                    "data": [],
                    "row_count": 0,
                    "pipeline": "hybrid"
                }
                
            except Exception as e:
                # Calculate latency even on error
                latency_ms = int((time.time() - start_time) * 1000)
                
                # Log error to observability
                log_event({
                    "query": clean_msg,
                    "pipeline": "HYBRID",
                    "latency_ms": latency_ms,
                    "success": False,
                    "error": str(e),
                    "metadata": {
                        "normalized_query": normalized_msg
                    }
                })
                
                logger.error(f"[HYBRID_PIPELINE] Hybrid pipeline error: {e}")
                # Fallback to SQL pipeline if hybrid search fails
                logger.info(f"[PIPELINE] Hybrid pipeline failed, falling back to SQL pipeline")
        
        # VECTOR Pipeline
        elif pipeline_type == "VECTOR":
            logger.info(f"[VECTOR_PIPELINE] Vector pipeline activated for query: '{clean_msg}'")
            
            # Start timing
            start_time = time.time()
            
            try:
                # เรียก vector pipeline
                vector_answer = vector_pipeline(normalized_msg, top_k=5, min_similarity=0.3)
                
                # Calculate latency
                latency_ms = int((time.time() - start_time) * 1000)
                
                # Log to observability
                log_event({
                    "query": clean_msg,
                    "pipeline": "VECTOR",
                    "latency_ms": latency_ms,
                    "success": True,
                    "retrieved_docs": 20,  # From vector search
                    "result_count": 5,     # After reranking
                    "metadata": {
                        "normalized_query": normalized_msg,
                        "top_k": 5,
                        "min_similarity": 0.3,
                        "reranker": True,
                        "compression": True
                    }
                })
                
                # บันทึก log
                save_qa_log(
                    clean_msg,
                    vector_answer,
                    sql="",
                    debug={
                        "type": "vector_search",
                        "pipeline": "vector",
                        "normalized_query": normalized_msg,
                        "latency_ms": latency_ms
                    }
                )
                
                logger.info(f"[VECTOR_PIPELINE] Vector pipeline completed successfully (latency: {latency_ms}ms)")
                
                return {
                    "text": vector_answer,
                    "sql": "",
                    "data": [],
                    "row_count": 0,
                    "pipeline": "vector"
                }
                
            except Exception as e:
                # Calculate latency even on error
                latency_ms = int((time.time() - start_time) * 1000)
                
                # Log error to observability
                log_event({
                    "query": clean_msg,
                    "pipeline": "VECTOR",
                    "latency_ms": latency_ms,
                    "success": False,
                    "error": str(e),
                    "metadata": {
                        "normalized_query": normalized_msg
                    }
                })
                
                logger.error(f"[VECTOR_PIPELINE] Vector pipeline error: {e}")
                # Fallback to SQL pipeline if vector search fails
                logger.info(f"[PIPELINE] Vector pipeline failed, falling back to SQL pipeline")
        
        # SQL Pipeline (default)
        else:
            logger.info(f"[PIPELINE] Using SQL pipeline")
    
    except ImportError as e:
        logger.warning(f"[PIPELINE] Advanced pipelines not available: {e}")
        logger.info(f"[PIPELINE] Pipeline selected: SQL (advanced pipelines not available)")
    except Exception as e:
        logger.error(f"[PIPELINE] Router error: {e}")
        logger.info(f"[PIPELINE] Pipeline selected: SQL (router error)")
    
    # rewrite_intent == "SQL" → ใช้ rewritten_query สำหรับ SQL generation
    rewritten_query = rewrite_content
    
    # Start timing for SQL pipeline
    sql_start_time = time.time()
    
    # Guardrail: คำถามคลุมเครือ → ถามกลับ; ข้ามเมื่อโหมด ai_100
    if not getattr(req, "ai_100", False):
        # 1. Check for general knowledge questions that leaked into SQL pipeline
        general_kb_keywords = ["คืออะไร", "what is", "แปลว่าอะไร", "หมายถึงอะไร"]
        msg_lower = clean_msg.lower()
        if any(k in msg_lower for k in general_kb_keywords) and len(clean_msg) < 50:
            logger.warning(f"[GUARD] Detected general knowledge question in SQL pipeline: {clean_msg}")
            # Try to get answer from chat model instead of running SQL
            try:
                kb_resp = _get_chat_model_response(clean_msg)
                if kb_resp and kb_resp != CHAT_FALLBACK_RESPONSE:
                    save_qa_log(clean_msg, kb_resp, sql="", debug={"type": "kb_divert", "reason": "general_knowledge"})
                    return {"text": kb_resp, "sql": "", "data": [], "row_count": 0}
            except:
                pass

        disambiguation = disambiguate_question(clean_msg or "")
        if disambiguation:
            clarification_text = disambiguation["text"]
            save_qa_log(clean_msg, clarification_text, sql="", debug={"type": "clarification", "reason": disambiguation.get("type")})
            return {
                "text": clarification_text,
                "type": "clarification",
                "sql": "",
                "data": [],
                "row_count": 0
            }
    
    # =====================================================
    # Phase 3: Build Prompt -> Call LLM -> Extract SQL (single pipeline)
    # =====================================================
    # Step 5: Build dynamic prompt (schema target_db)
    # Inject matched entities into the prompt
    # Combine original query and rewritten summary for maximum context
    combined_query = f"Original Query: {clean_msg}\nSummary: {rewritten_query}"
    final_query_with_entities = combined_query
    if matched_entities_info:
        final_query_with_entities = f"{combined_query}\n\n{matched_entities_info}"
        
    if job_id: chat_jobs[job_id]["stage"] = "SQL_GEN"
    sql_prompt = build_sql_prompt(final_query_with_entities, target_db)
    logger.info(f"[PIPELINE] Step 5: Built SQL prompt ({len(sql_prompt)} chars) for target_db={target_db}")
    
    # Debug: แสดงส่วนสำคัญของ prompt สำหรับ shift queries
    if any(keyword in clean_msg.lower() for keyword in ['กะดึก', 'กะเช้า', 'night', 'day shift']):
        logger.info(f"[DEBUG] Shift query detected: '{clean_msg}' → normalized: '{final_query_with_entities[:100]}...'")
    
    # Step 6: Call LLM (MODEL_NAME / Qwen Coder)
    t0_llm_sql = time.time()
    raw_llm_response = call_llm_for_sql(sql_prompt)
    llm_sql_latency_ms = int((time.time() - t0_llm_sql) * 1000)
    logger.info(f"[PIPELINE] Step 6: LLM raw response ({len(raw_llm_response)} chars), latency={llm_sql_latency_ms}ms")
    
    # Debug: แสดง raw response สำหรับ shift queries
    if any(keyword in clean_msg.lower() for keyword in ['กะดึก', 'กะเช้า', 'night', 'day shift']):
        logger.info(f"[DEBUG] LLM raw response for shift query: '{raw_llm_response[:200]}...'")
    
    # Step 7: Extract clean SQL
    sql = extract_clean_sql(raw_llm_response)
    logger.info(f"[PIPELINE] Step 7: Extracted SQL: {sql[:200] if sql else '(empty)'}")

    




    # ถ้าโมเดลตอบถามกลับ (CLARIFY: หรือ CLARIFY ...) — โหมด AI 100% ไม่ทำ keyword fallback; โหมดปกติให้ลองค้นคำหลักแทน
    if sql and (sql.strip().upper().startswith("CLARIFY:") or sql.strip().upper().startswith("CLARIFY ")):
        if getattr(req, "ai_100", False):
            clarification_text = sql[8:].strip().rstrip(";").strip()
            save_qa_log(clean_msg, clarification_text, sql="", debug={"type": "clarification", "ai_100": True})
            return {"text": clarification_text, "sql": "", "data": [], "row_count": 0}
        keyword = clean_msg.strip()
        # คำถามสั้นเป็นคำหลัก (คำเดียวหรือสองคำ) → สร้าง SQL ค้นแล้วรัน
        if keyword and len(keyword) <= 50 and len(keyword.split()) <= 2:
            kw = keyword.split()[0] if keyword.split() else keyword
            if kw and re.match(r"^[\w\u0e00-\u0e7f\-]+$", kw, re.U):  # คำเดียวที่ดูเหมือนคำค้น
                kw_esc = kw.replace("'", "''").lower()
                # ดรอป/เสีย/breakdown/พัง = กลุ่มอาการเสีย → ค้นใน ปัญหา, สาเหตุ, การแก้ไข
                symptom_keywords = ['ดรอป', 'เสีย', 'breakdown', 'พัง']
                if kw_esc in symptom_keywords:
                    fallback_sql_repair = f"""SELECT Line, Process, "ปัญหา", "สาเหตุ", "การแก้ไข", Date FROM repairs_enriched WHERE Date >= date('now','-30 days') ORDER BY Date DESC, Line, Process LIMIT 20"""
                else:
                    fallback_sql_repair = f"SELECT Date, Shift, Team, Tech, Line, Process, RepairMinutes, ResponseMinutes FROM repairs_enriched WHERE LOWER(Line) LIKE '%{kw_esc}%' OR LOWER(Process) LIKE '%{kw_esc}%' ORDER BY Date DESC LIMIT 20"
                fallback_sql_pm = f'SELECT "Task Name", "Due date", Progress FROM PM WHERE LOWER(COALESCE(Line,"Task Name")) LIKE \'%{kw_esc}%\' ORDER BY "Due date"'
                try:
                    df_repair, sql_repair = execute_sql_safe(
                        fallback_sql_repair,
                        user_msg=clean_msg,
                        limit_override=getattr(req, "limit_n", None),
                    )
                    if not df_repair.empty:
                        text = f"เจอ {len(df_repair)} รายการที่เกี่ยวข้องกับ \"{kw}\" ค่ะ"
                        save_qa_log(clean_msg, text, sql=sql_repair, debug={"type": "keyword_fallback", "row_count": len(df_repair)})
                        return {"text": text, "sql": sql_repair, "data": _sanitize_dataframe_for_json(df_repair), "row_count": len(df_repair)}
                    # ถ้ามีคำว่า PM ในคำถามถึงจะค้นตาราง PM ด้วย ถ้าไม่มีให้ตอบไม่พบเลย
                    msg_lower = clean_msg.lower()
                    if "pm" in msg_lower or "พีเอ็ม" in msg_lower:
                        df_pm, sql_pm = execute_sql_safe(
                            fallback_sql_pm,
                            user_msg=clean_msg,
                            limit_override=getattr(req, "limit_n", None),
                        )
                        if not df_pm.empty:
                            text = f"เจอ {len(df_pm)} รายการที่เกี่ยวข้องกับ \"{kw}\" ค่ะ"
                            save_qa_log(clean_msg, text, sql=sql_pm, debug={"type": "keyword_fallback", "row_count": len(df_pm)})
                            return {"text": text, "sql": sql_pm, "data": _sanitize_dataframe_for_json(df_pm), "row_count": len(df_pm)}
                    text = generate_helpful_no_data_message(clean_msg, fallback_sql_repair)
                    save_qa_log(clean_msg, text, sql=fallback_sql_repair, debug={"type": "keyword_fallback", "row_count": 0})
                    return {"text": text, "sql": fallback_sql_repair, "data": [], "row_count": 0}
                except Exception as e:
                    logger.warning(f"Keyword fallback failed for '{kw}': {e}")
        clarification_text = sql[8:].strip().rstrip(";").strip()
        save_qa_log(clean_msg, clarification_text, sql="", debug={"type": "clarification"})
        return {
            "text": clarification_text,
            "sql": "",
            "data": [],
            "row_count": 0,
        }
    
    if not sql:
        

        if not sql:
            # โหมด AI 100% ไม่ใช้ fallback; โหมดปกติลอง fallback คำถามสาเหตุ/ปัญหา
            if getattr(req, "ai_100", False):
                pass  # จะไปถึง return error ด้านล่าง
            else:
                cause_keywords = ['สาเหตุ', 'ปัญหา', 'เพราะอะไร', 'อาการ', 'เหตุผล', 'แก้ไข']
                msg_lower = clean_msg.lower()
                if any(k in msg_lower for k in cause_keywords) and 'pm' not in msg_lower and 'พีเอ็ม' not in msg_lower:
                    words = re.findall(r'[A-Za-z0-9_\u0e00-\u0e7f-]+', clean_msg)
                    candidates = [w for w in words if len(w) >= 2 and re.match(r'^[A-Za-z0-9_-]+$', w) and w.upper() not in ('PM', 'AND', 'OR', 'SQL')]
                    line_process_condition = ""
                    if candidates:
                        kw = candidates[0].replace("'", "''")
                        line_process_condition = f" AND (LOWER(Line) LIKE '%{kw.lower()}%' OR LOWER(Process) LIKE '%{kw.lower()}%')"
                    today_val = pd.Timestamp.now().strftime('%Y-%m-%d')
                    month_start = pd.Timestamp.now().replace(day=1).strftime('%Y-%m-%d')
                    sql = f'''SELECT Line, Process, "ปัญหา", "สาเหตุ", "การแก้ไข", Date
FROM repairs_enriched
WHERE Date BETWEEN '{month_start}' AND '{today_val}'{line_process_condition}
ORDER BY Date DESC, Line, Process
LIMIT 20'''
                    logger.info(f"Using cause/reason fallback SQL for: {clean_msg[:60]}")
    if not sql:
        logger.warning(f"No SQL generated for: {clean_msg} — trying CHAT_MODEL fallback")
        _chat_fb = None
        try:
            _chat_fb = _get_chat_model_response(clean_msg)
        except Exception:
            pass
        if _chat_fb and _chat_fb != CHAT_FALLBACK_RESPONSE:
            save_qa_log(clean_msg, _chat_fb, sql="", debug={"type": "chat_fallback_no_sql"})
            return {"text": _chat_fb, "sql": "", "data": [], "row_count": 0}
        err_msg = "Elin ไม่สามารถตอบได้ สอบถามเพิ่มเติม"
        save_qa_log(clean_msg, err_msg, sql="", debug={"error": "no_sql"})
        logger.error(f"Failed to generate SQL for: {clean_msg}")
        return {
            "error": err_msg,
            "error_link_text": "กดที่นี่",
            "error_link_url": "http://172.16.2.44:18080/analytics-dashboard",
        }

    # โหมด AI 100% (ตอบอีกครั้ง): ถ้าไม่ใช่ SQL ที่รันได้ ให้ถือเป็นข้อความตอบ ไม่แสดง error "SQL ไม่ปลอดภัย"
    if getattr(req, "ai_100", False):
        sql_stripped = (sql or "").strip().rstrip(";").strip()
        # LLM ตอบขอให้ชี้แจงหรือข้อความอื่นที่ขึ้นต้นด้วย CLARIFY → ไม่ส่งไป validator
        if sql_stripped.upper().startswith("CLARIFY"):
            _chat_fb2 = None
            try:
                _chat_fb2 = _get_chat_model_response(clean_msg)
            except Exception:
                pass
            response_text = _chat_fb2 if (_chat_fb2 and _chat_fb2 != CHAT_FALLBACK_RESPONSE) else (sql_stripped or CHAT_FALLBACK_RESPONSE)
            save_qa_log(clean_msg, response_text, sql="", debug={"ai_100": True, "non_sql_response": True})
            return {"text": response_text, "sql": "", "data": [], "row_count": 0}
        looks_like_sql = sql and re.search(r'\bSELECT\b', sql, re.IGNORECASE) and re.search(r'\bFROM\b', sql, re.IGNORECASE)
        if not looks_like_sql:
            # LLM ตอบเป็นข้อความ (เช่น CLARIFY...) — ใช้ CHAT_MODEL ตอบแทน
            _chat_fb3 = None
            try:
                _chat_fb3 = _get_chat_model_response(clean_msg)
            except Exception:
                pass
            response_text = _chat_fb3 if (_chat_fb3 and _chat_fb3 != CHAT_FALLBACK_RESPONSE) else ((sql or "").strip().rstrip(";").strip() or CHAT_FALLBACK_RESPONSE)
            save_qa_log(clean_msg, response_text, sql="", debug={"ai_100": True, "non_sql_response": True})
            return {"text": response_text, "sql": "", "data": [], "row_count": 0}
        validator = get_validator()
        valid, err = validator.validate(sql)
        if not valid:
            # โหมดตอบอีกครั้ง: ถ้า validator บอกว่าไม่มี SELECT/FROM หรือไม่ใช่ SQL ที่ปลอดภัย ให้ถือเป็นคำตอบข้อความ แทนการแสดง error (กันกรณี LLM คืน CLARIFY หรือข้อความที่ clean_sql ตัดแล้วเหลือไม่ครบ)
            _chat_fb4 = None
            try:
                _chat_fb4 = _get_chat_model_response(clean_msg)
            except Exception:
                pass
            if _chat_fb4 and _chat_fb4 != CHAT_FALLBACK_RESPONSE:
                save_qa_log(clean_msg, _chat_fb4, sql="", debug={"ai_100": True, "validator_rejected": err, "type": "chat_fallback"})
                return {"text": _chat_fb4, "sql": "", "data": [], "row_count": 0}
            save_qa_log(clean_msg, f"SQL ไม่ปลอดภัย: {err}", sql=sql, debug={"ai_100": True, "validation_error": err})
            return {
                "error": f"SQL ไม่ปลอดภัย: {err}",
                "error_link_text": "กดที่นี่",
                "error_link_url": "http://172.16.2.44:18080/analytics-dashboard",
            }
        # ------------- โหมด ai_100: validate ความปลอดภัย + SQLite compat + verify columns เท่านั้น -------------
        sql = _repair_sqlite_date_syntax_repairs(sql)
        if sql and ("FROM PM" in sql.upper() or "JOIN PM" in sql.upper()):
            sql = _fix_pm_sqlite_compat(sql)
        sql = verify_sql_columns(sql)
        logger.info("AI 100% (ตอบอีกครั้ง): safety + sqlite compat + verify_sql_columns only")
    else:
        # ------------- โหมดปกติ: อาศัย LLM-generated SQL ล้วน และเช็คความปลอดภัยเท่านั้น -------------
        sql = _repair_sqlite_date_syntax_repairs(sql)
        if sql and ("FROM PM" in sql.upper() or "JOIN PM" in sql.upper()):
            sql = _fix_pm_sqlite_compat(sql)
        sql = verify_sql_columns(sql)

    # Execute: รันกับตารางที่ถูกต้อง (PM2025.db สำหรับ PM, work DB สำหรับซ่อม); ถ้า Error ให้ Qwen แก้ SQL แล้ว retry
    try:
        t0_sql_exec = time.time()
        df, final_sql = execute_sql_safe(
            sql,
            skip_pm_column_normalize=getattr(req, "ai_100", False),
            user_msg=clean_msg,
            limit_override=getattr(req, "limit_n", None),
        )
        sql_exec_latency_ms = int((time.time() - t0_sql_exec) * 1000)
        logger.info(f"[PIPELINE] Step 8: SQL execution latency={sql_exec_latency_ms}ms")
    except SqlExecutionError as ex:
        # Auto-Retry: ส่ง error กลับให้ Qwen/LLM แก้ SQL ใหม่ แล้วรันอีกครั้ง
        logger.warning(f"SQL execution failed, retrying with LLM fix: {ex}")
        try:
            fix_prompt = f"""คำถามผู้ใช้: {clean_msg}
SQL ที่รันไม่สำเร็จ:
{ex.sql_used}
Error: {str(ex)}
กรุณาแก้ SQL ให้ถูกต้อง (ใช้ตาราง PM จาก PM2025.db สำหรับคำถาม PM, ใช้ repairs_enriched จาก work DB สำหรับซ่อม). ตอบเฉพาะ SQL เท่านั้น."""
            res = requests.post(OLLAMA_GENERATE_URL, json={
                "model": MODEL_NAME,
                "prompt": fix_prompt,
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 512}
            }, timeout=100)
            new_sql = clean_sql(res.json().get("response", ""))
            if not new_sql:
                save_qa_log(clean_msg, str(ex), sql=ex.sql_used, debug={"error": "sql_fix_empty", "exec_error": str(ex)})
                return {"error": "ขออภัย ระบบไม่สามารถแก้คำสั่ง SQL ได้ กรุณาถามใหม่หรือระบุรายละเอียดเพิ่มค่ะ"}
            df, final_sql = execute_sql_safe(
                new_sql,
                skip_pm_column_normalize=getattr(req, "ai_100", False),
                user_msg=clean_msg,
                limit_override=getattr(req, "limit_n", None),
            )
        except Exception as e2:
            logger.error(f"Retry after SqlExecutionError failed: {e2}")
            save_qa_log(clean_msg, str(e2), sql=getattr(ex, "sql_used", ""), debug={"error": "retry_failed", "exec_error": str(ex)})
            return {"error": "ขออภัย ระบบขัดข้องในการดึงข้อมูล สอบถามเพิ่มเติมหรือลองถามใหม่ค่ะ"}

    try:
        # กรอง History: ถ้าเป็นประวัติซ่อม เก็บเฉพาะแถวที่มี ปัญหา/สาเหตุ/การแก้ไข/บันทึกเพิ่มเติม อย่างน้อย 1 ช่อง
        if not df.empty and "repairs_enriched" in (final_sql or "").upper() and any(k in (clean_msg or "") for k in ["ประวัติการซ่อม", "ขอประวัติ", "ประวัติของ"]):
            detail_cols = ["ปัญหา", "สาเหตุ", "การแก้ไข", "บันทึกเพิ่มเติม"]
            existing = [c for c in df.columns if c in detail_cols]
            if not existing:
                existing = [c for c in df.columns if str(c).strip('"') in detail_cols]
            if existing:
                def _has_detail(r):
                    return any(pd.notna(r.get(c)) and str(r.get(c)).strip() != "" for c in existing)
                before = len(df)
                df = df[df.apply(_has_detail, axis=1)].copy()
                if len(df) < before:
                    logger.info(f"Filtered history rows: kept only rows with at least one of {existing} non-empty ({before} -> {len(df)})")
        
        # Exclude Tech: โหลดช่างจาก get_tech_exclude_for_answer() และลบแถวที่มีช่างเหล่านี้ออกจาก df
        exclude_techs = get_tech_exclude_for_answer()
        if not df.empty and exclude_techs:
            tech_col = next((c for c in df.columns if str(c).strip('"').lower() == 'tech'), None)
            if tech_col is not None:
                exclude_upper = [str(t).strip().upper() for t in exclude_techs]
                before_n = len(df)
                limit_match = re.search(r"\bLIMIT\s+(\d+)", final_sql or "", flags=re.IGNORECASE)
                limit_n = int(limit_match.group(1)) if limit_match else None
                df = df[~df[tech_col].astype(str).str.strip().str.upper().isin(exclude_upper)].copy()
                if len(df) < before_n:
                    logger.info(f"Filtered out TECH in ExcludeFromAnswer: {before_n} -> {len(df)} rows")
                # If LIMIT query loses rows after exclusion (e.g. top-10 contains TEST),
                # re-run without LIMIT and then re-apply exclusion to backfill valid rows.
                needs_limit_recovery = (
                    "LIMIT" in (final_sql or "").upper()
                    and limit_n is not None
                    and len(df) < max(1, limit_n)
                    and len(df) < before_n
                )
                if needs_limit_recovery:
                    sql_no_limit = re.sub(r"\bLIMIT\s+\d+\s*(?:OFFSET\s+\d+)?", "", final_sql or "", flags=re.IGNORECASE).strip().rstrip(";")
                    if sql_no_limit:
                        try:
                            df_retry, _ = execute_sql_safe(
                                sql_no_limit,
                                skip_pm_column_normalize=getattr(req, "ai_100", False),
                                skip_limit_enforcement=True,
                            )
                            retry_tech_col = next((c for c in df_retry.columns if str(c).strip('"').lower() == 'tech'), None)
                            if retry_tech_col is not None:
                                before_retry = len(df_retry)
                                df_retry = df_retry[
                                    ~df_retry[retry_tech_col].astype(str).str.strip().str.upper().isin(exclude_upper)
                                ].copy()
                                recovered_df = df_retry.head(max(1, limit_n)).copy()
                                if len(recovered_df) > len(df):
                                    df = recovered_df
                                    logger.info(
                                        f"LIMIT exclude recovery: {before_retry} -> {len(df_retry)} rows after exclude, returning {len(df)} rows"
                                    )
                        except Exception as e:
                            logger.warning(f"LIMIT exclude recovery failed: {e}")
        
        # ถ้า df ว่างเปล่า (0 แถว): ลอง fallback SQL แบบผ่อนเงื่อนไขก่อน แล้วค่อยส่งข้อความ no-data
        if df.empty:
            if "repairs_enriched" in (final_sql or "").upper():
                fallback_sql_candidates = _build_repair_no_data_fallback_sqls(clean_msg, final_sql)
                for fallback_sql in fallback_sql_candidates:
                    logger.info(f"Trying no-data fallback SQL: {fallback_sql[:180]}")
                    try:
                        df_fallback, fallback_sql_final = execute_sql_safe(
                            fallback_sql,
                            skip_pm_column_normalize=getattr(req, "ai_100", False),
                            user_msg=clean_msg,
                            limit_override=getattr(req, "limit_n", None),
                        )
                        if not df_fallback.empty:
                            df = df_fallback
                            final_sql = fallback_sql_final
                            logger.info(f"No-data fallback succeeded: {len(df)} rows")
                            break
                    except Exception as e:
                        logger.warning(f"No-data fallback failed: {e}")

        # ถ้ายังว่าง: ส่งข้อความ no-data มาตรฐาน (ไม่ fallback เป็น chat ทั่วไป)
        if df.empty:
            no_data_text = generate_helpful_no_data_message(clean_msg, final_sql)
            save_qa_log(clean_msg, no_data_text, sql=final_sql, debug={"row_count": 0, "reason": "no_data_with_suggestion"})
            return {"text": no_data_text, "sql": final_sql, "data": [], "row_count": 0}
        
        # 🔥 เก็บ IsPostponed ไว้ก่อนกรอง (สำหรับใช้ใน explain_sql_result)
        df_with_flags = df.copy()
        
        # Total Count: ถ้า SQL มี LIMIT สร้าง Query ใหม่แบบ COUNT(*) (ตัด LIMIT/OFFSET/ORDER BY ออก) รันกับ DB เดียวกับที่ใช้รัน main query
        total_count = None
        if "LIMIT" in (final_sql or "").upper():
            try:
                # ตัด LIMIT/OFFSET ออกก่อน
                base_sql = re.sub(r'\bLIMIT\s+\d+\s*', '', final_sql, flags=re.IGNORECASE)
                base_sql = re.sub(r'\bOFFSET\s+\d+\s*', '', base_sql, flags=re.IGNORECASE)
                base_sql = base_sql.rstrip().rstrip(';').strip()
                # ตัด ORDER BY ออกให้หมด (ก่อนเอาไปนับ)
                base_sql = re.sub(
                    r'\bORDER\s+BY\s+[\s\S]+?(?=\bLIMIT\b|\bOFFSET\b|\bGROUP\s+BY\b|\bHAVING\b|$)',
                    '',
                    base_sql,
                    flags=re.IGNORECASE,
                )
                base_sql = re.sub(r'\s+', ' ', base_sql).strip()
                # นับจำนวนแบบปลอดภัย: wrap เป็น subquery เสมอ (กัน edge case SELECT ซับซ้อน)
                count_sql = f"SELECT COUNT(*) FROM ({base_sql}) AS _count_sub"
                # รันกับตารางเดียวกับ main query: PM2025.db สำหรับ PM-only, work DB สำหรับซ่อม
                count_db = WORK_DB_PATH
                if _is_pm_only_sql(final_sql):
                    has_work_only = any(k in (final_sql or "").upper() for k in ["PM_REAL_DATE", "ISPOSTPONED", "DUE_DATE_YMD"])
                    if not has_work_only and _resolve_pm_db_path():
                        count_db = _resolve_pm_db_path()
                with sqlite3.connect(count_db) as conn:
                    cursor = conn.execute(count_sql)
                    raw = cursor.fetchone()[0]
                if raw is not None:
                    try:
                        total_count = int(float(raw))
                    except (TypeError, ValueError):
                        total_count = None
                logger.info(f"Total count before LIMIT: {total_count} (db={os.path.basename(count_db)})")
            except Exception as e:
                logger.warning(f"Could not get total count: {e}")
                total_count = None
        
        # กรองผลลัพธ์สำหรับคำถาม "เดือนหน้า" PM ให้เหลือเฉพาะเดือนถัดไป (backup plan ถ้า rewrite ไม่ทำงาน)
        if any(w in (clean_msg or "").lower() for w in ["เดือนหน้า", "next month"]):
            if "FROM PM" in final_sql.upper() or "JOIN PM" in final_sql.upper():
                now = pd.Timestamp.now()
                if now.month == 12:
                    first_next = now.replace(year=now.year + 1, month=1, day=1)
                else:
                    first_next = now.replace(month=now.month + 1, day=1)
                next_month_start = first_next.strftime("%Y-%m-%d")
                next_month_end = (first_next + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")
                # กรองตาม Due date หรือ date column
                date_col = None
                for col in df.columns:
                    if "due date" in str(col).lower() or col == "date":
                        date_col = col
                        break
                if date_col:
                    df = df[df[date_col].astype(str).str[:10] >= next_month_start]
                    df = df[df[date_col].astype(str).str[:10] <= next_month_end]
                    logger.info(f"Filtered PM เดือนหน้า results to {next_month_start} to {next_month_end}: {len(df)} rows")
        
        # ========== Post-Execution (ก่อน Response กลับให้ Frontend) ==========
        # 1) เพิ่มข้อมูลเลื่อน PM: แยก Description เป็น ย้ายจาก, ย้ายไป, เนื่องจาก, ผู้แจ้ง + กรองตามปี
        df = enrich_pm_postpone_columns(df)
        df_with_flags = enrich_pm_postpone_columns(df_with_flags)
        df = _filter_pm_postpone_by_year(df, clean_msg)
        df_with_flags = _filter_pm_postpone_by_year(df_with_flags, clean_msg)
        
        # 2) สร้างข้อความตอบกลับ: แปลง df เป็นภาษาไทยแบบเป็นธรรมชาติ (ข้อมูล auto-reload ทุกครั้งแล้ว ไม่ต้องแสดงข้อความ)
        display_total_count = None if _should_hide_total_count_for_top_query(clean_msg, final_sql, total_count) else total_count
        summary_text = explain_sql_result(clean_msg, final_sql, df_with_flags, total_count=display_total_count)
        
        # 3) กรองคอลัมน์: ซ่อนคอลัมน์ระบบ (id, IsPostponed ฯลฯ) และคอลัมน์ภายใน PM
        df = filter_important_columns(df, final_sql)
        sql_upper = (final_sql or "").upper()
        if "FROM PM" in sql_upper:
            for col in ["date", "PM_real_date"]:
                if col in df.columns:
                    df = df.drop(columns=[col], errors="ignore")
        if "เหตุผล" in df.columns or "ย้ายจากวันที่" in df.columns:
            df = df.drop(columns=["Description", "เนื่องจาก"], errors="ignore")
        
        row_count = len(df)
        
        # 4) Logging: เก็บคำถาม, Text สรุป, SQL, Row Count ลง qa_log.jsonl
        save_qa_log(
            clean_msg,
            summary_text,
            sql=final_sql,
            debug={"row_count": row_count, "columns": list(df.columns), "data_sample": df.head(5).to_dict(orient="records")},
        )
        
        # Calculate SQL pipeline latency
        sql_latency_ms = int((time.time() - sql_start_time) * 1000)
        
        # Log to observability
        try:
            log_event({
                "query": clean_msg,
                "pipeline": "SQL",
                "latency_ms": sql_latency_ms,
                "success": True,
                "result_count": row_count,
                "metadata": {
                    "normalized_query": normalized_msg,
                    "target_db": target_db,
                    "total_count": int(total_count) if total_count is not None else None,
                    "has_limit": "LIMIT" in (final_sql or "").upper()
                }
            })
        except Exception as e:
            logger.warning(f"[OBSERVABILITY] Failed to log SQL pipeline: {e}")
        
        # 5) Return ตรงกับ API Schema: { text, sql, data (records), row_count, total_count, timestamp }
        return {
            "text": summary_text,
            "sql": final_sql,
            "data": _sanitize_dataframe_for_json(df),
            "row_count": row_count,
            "total_count": int(display_total_count) if display_total_count is not None else None,
            "timestamp": now_bangkok_str(),
        }

    except Exception as e:
        err_msg = "ขออภัยครับ ระบบขัดข้องในการดึงข้อมูล"
        save_qa_log(clean_msg, err_msg, sql=sql, debug={"error": str(e)})
        logger.error(f"SQL Execution Error: {e}")
        return {"error": err_msg}

@app.get("/")
def root():
    index_path = os.path.join(_FRONTEND_DIR, "index.html")
    try:
        with open(index_path, "r", encoding="utf-8") as f:
            html = f.read()
        html = html.replace("__STATIC_VERSION__", STATIC_VERSION)
        return Response(
            content=html,
            media_type="text/html",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    except Exception as e:
        logger.warning(f"Serve index.html with version failed: {e}")
        return FileResponse(index_path)

@app.get("/favicon.ico")
@app.get("/ai/favicon.ico")
def favicon():
    favicon_path = os.path.join(_FRONTEND_DIR, "favicon.ico")
    if os.path.isfile(favicon_path):
        return FileResponse(
            favicon_path,
            media_type="image/x-icon",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    return Response(status_code=204)

# Service Worker (ต้องอยู่ที่ root path เพื่อให้ scope ครอบคลุมทั้งเว็บ)
@app.get("/sw.js")
def service_worker():
    return FileResponse(
        os.path.join(_FRONTEND_DIR, "sw.js"),
        media_type="application/javascript",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )

# Service Worker for proxy path
@app.get("/ai/sw.js")
def service_worker_proxy():
    return FileResponse(
        os.path.join(_FRONTEND_DIR, "sw.js"),
        media_type="application/javascript",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )

# Source map files (return 204 No Content to suppress 404 errors)
@app.get("/static/js/{filename:path}.map")
def source_maps(filename: str):
    """Source map files are not needed in production, return 204 to suppress browser warnings"""
    return Response(status_code=204)

# Chrome DevTools well-known endpoint (return 204 to suppress warnings)
@app.get("/.well-known/{path:path}")
def well_known(path: str):
    """Well-known endpoints for browser features, return 204 to suppress warnings"""
    return Response(status_code=204)

# Avatar base64: ดึง base64 จาก comment บรรทัด 1 ในไฟล์ แล้วส่งสคริปต์ที่ใส่ในตัวแปร (ทำให้รูปขึ้นโดยไม่ต้องย้ายสาย base64 เอง)
@app.get("/api/avatar-base64.js")
@app.get("/ai/api/avatar-base64.js")
def serve_avatar_base64_js():
    path = os.path.join(_FRONTEND_DIR, "avatar-base64.js")
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

        b64 = ""

        # Prefer explicit data URL in comment/documentation, fallback to AVATAR_BASE64 assignment.
        m = re.search(r"data:image/[^;]+;base64,([A-Za-z0-9+/=\s]+)", content, re.IGNORECASE | re.DOTALL)
        if not m:
            m = re.search(r"AVATAR_BASE64\s*=\s*['\"]([A-Za-z0-9+/=\s]+)['\"]", content, re.IGNORECASE)

        if m and m.group(1):
            b64 = re.sub(r"\s+", "", m.group(1))
            if b64 == "PASTE_YOUR_BASE64_STRING_HERE":
                b64 = ""

        if m and b64:
            script = (
                "(function(global){var AVATAR_BASE64=" + json.dumps(b64) + ";"
                "if(typeof AVATAR_BASE64!=='string'||!AVATAR_BASE64)AVATAR_BASE64='';"
                "var dataUrl=AVATAR_BASE64?(AVATAR_BASE64.indexOf('data:')===0?AVATAR_BASE64:'data:image/png;base64,'+AVATAR_BASE64):'';"
                "var cssUrl=AVATAR_BASE64?(AVATAR_BASE64.indexOf('url(')===0?AVATAR_BASE64:'url(data:image/png;base64,'+AVATAR_BASE64+')'):'';"
                "global.SOMR_AVATAR_IMG=dataUrl;global.SOMR_AVATAR_CSS=cssUrl;"
                "})(typeof window!=='undefined'?window:this);"
            )
            return Response(
                content=script,
                media_type="application/javascript",
                headers={
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "Pragma": "no-cache",
                    "Expires": "0",
                },
            )
    except Exception as e:
        logger.warning("avatar-base64.js extract failed: %s", e)
    if os.path.isfile(path):
        return FileResponse(
            path,
            media_type="application/javascript",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    return Response(
        content="(function(g){g.SOMR_AVATAR_IMG='';g.SOMR_AVATAR_CSS='';})(typeof window!=='undefined'?window:this);",
        media_type="application/javascript",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.post("/api/meta/update_embeddings")
def api_meta_update_embeddings():
    """
    ลบ Embeddings เก่าทั้งหมดออกจาก meta_embeddings แล้ว Rebuild ใหม่ทั้งหมด
    ใช้เมื่อข้อมูลใน meta_knowledge มีการเปลี่ยนแปลง (แก้ไข/ลบ) และต้องการ sync embeddings
    """
    try:
        from services.meta_database import _DB_LOCK, _connect
        
        # ลบ embeddings เก่าทั้งหมด (ต้องปล่อย lock ก่อนเรียก reload_index ซึ่งก็ใช้ lock เหมือนกัน)
        acquired = _DB_LOCK.acquire(timeout=30)
        if not acquired:
            return JSONResponse({"status": "error", "message": "ระบบยุ่งอยู่ ลองใหม่อีกครั้งค่ะ"}, status_code=503)
        try:
            conn = _connect()
            conn.execute("DELETE FROM meta_embeddings")
            conn.commit()
            conn.close()
            logger.info("[META_UPDATE] Cleared all old embeddings")
        except Exception as e:
            logger.error(f"[META_UPDATE] Failed to clear embeddings: {e}")
            return JSONResponse({"status": "error", "message": f"ลบ Embeddings เก่าไม่สำเร็จ: {e}"}, status_code=500)
        finally:
            _DB_LOCK.release()
        
        # Rebuild index ใหม่ (lock จะถูก acquire ใน reload_index เอง)
        from services.meta_vector import get_meta_engine
        engine = get_meta_engine()
        engine.reload_index()
        
        count = len(engine.metadata)
        logger.info(f"[META_UPDATE] Rebuilt embeddings for {count} items")
        
        return JSONResponse({
            "status": "success",
            "message": f"อัปเดต Embeddings สำเร็จแล้วค่ะ! สร้างใหม่ทั้งหมด {count} รายการ",
            "count": count
        })
    except Exception as e:
        logger.error(f"[META_UPDATE] Failed to update embeddings: {e}", exc_info=True)
        return JSONResponse(
            {"status": "error", "message": str(e)},
            status_code=500
        )

if __name__ == "__main__":
    # รันด้วย python main.py → ฟังที่ 0.0.0.0 ให้เครื่องอื่นใน LAN เข้าได้
    import uvicorn
    import socket
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "18080"))
    # แสดง URL ให้คอมอื่นเข้า (ถ้าเข้าไม่ได้ ให้รัน open_firewall_port_18080.bat แบบ Run as administrator)
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        lan_ip = s.getsockname()[0]
        s.close()
    except Exception:
        lan_ip = "(รัน ipconfig ดู IPv4)"
    print("")
    print("  Backend รันแล้ว: http://localhost:%d" % port)
    print("  คอมอื่นใน LAN ใช้: http://%s:%d" % (lan_ip, port))
    print("  ถ้าเข้าไม่ได้: รัน open_firewall_port_18080.bat (คลิกขวา → Run as administrator)")
    print("")
    uvicorn.run("main:app", host=host, port=port, reload=True)
