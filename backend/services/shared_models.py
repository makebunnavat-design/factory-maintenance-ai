#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shared Model Manager - แชร์ BGE models ระหว่าง services
=======================================================
แก้ปัญหา BGE model โหลดซ้ำ 3 ครั้ง
"""

import logging
import threading
from typing import Optional
from sentence_transformers import SentenceTransformer
from pathlib import Path

logger = logging.getLogger("[SHARED_MODELS]")

class SharedBGEManager:
    """จัดการ BGE model แบบ singleton - โหลดครั้งเดียว ใช้ร่วมกัน"""
    
    _instance: Optional['SharedBGEManager'] = None
    _lock = threading.Lock()
    
    def __init__(self):
        self.bge_model: Optional[SentenceTransformer] = None
        self.model_lock = threading.Lock()
        self.load_count = 0
        
        # Model path
        BASE_DIR = Path(__file__).parent.parent
        MODELS_DIR = BASE_DIR / "models"
        LOCAL_BGE_M3 = MODELS_DIR / "bge-m3"
        self.model_path = str(LOCAL_BGE_M3) if LOCAL_BGE_M3.exists() else "BAAI/bge-m3"
    
    @classmethod
    def instance(cls) -> 'SharedBGEManager':
        """Singleton pattern"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    def get_model(self) -> SentenceTransformer:
        """ได้ BGE model (โหลดครั้งเดียว แชร์ใช้ร่วมกัน)"""
        if self.bge_model is None:
            with self.model_lock:
                if self.bge_model is None:
                    logger.info(f"[SHARED_BGE] Loading BGE model (first time): {self.model_path}")
                    
                    # Suppress output
                    import sys
                    from io import StringIO
                    old_stdout = sys.stdout
                    old_stderr = sys.stderr
                    sys.stdout = StringIO()
                    sys.stderr = StringIO()
                    
                    try:
                        # Force CPU-only to avoid GPU conflict
                        self.bge_model = SentenceTransformer(self.model_path, device='cpu')
                        logger.info("[SHARED_BGE] BGE model loaded successfully (CPU-only)")
                    finally:
                        sys.stdout = old_stdout
                        sys.stderr = old_stderr
        
        self.load_count += 1
        logger.debug(f"[SHARED_BGE] BGE model accessed (count: {self.load_count})")
        return self.bge_model
    
    def get_stats(self) -> dict:
        """ดูสถิติการใช้งาน"""
        return {
            "model_loaded": self.bge_model is not None,
            "access_count": self.load_count,
            "model_path": self.model_path
        }

# Global instance
def get_shared_bge_model() -> SentenceTransformer:
    """ได้ shared BGE model"""
    return SharedBGEManager.instance().get_model()