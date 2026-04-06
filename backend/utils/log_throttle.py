#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Log Throttling Utility
======================
จำกัดการแสดง log messages ที่ซ้ำๆ ให้แสดงตามช่วงเวลาที่กำหนด
"""

import time
from typing import Dict, Optional
from functools import wraps

class LogThrottle:
    """จัดการการ throttle log messages"""
    
    def __init__(self):
        self.last_logged: Dict[str, float] = {}
        
    def should_log(self, key: str, interval_seconds: int = 300) -> bool:
        """
        ตรวจสอบว่าควรแสดง log หรือไม่
        
        Args:
            key: unique key สำหรับ log message
            interval_seconds: ช่วงเวลาขั้นต่ำระหว่างการแสดง log (default: 5 นาที)
            
        Returns:
            True ถ้าควรแสดง log, False ถ้าไม่ควร
        """
        now = time.time()
        last_time = self.last_logged.get(key, 0)
        
        if now - last_time >= interval_seconds:
            self.last_logged[key] = now
            return True
        return False
    
    def throttled_print(self, message: str, key: Optional[str] = None, 
                       interval_seconds: int = 300, force: bool = False):
        """
        Print message แบบ throttled
        
        Args:
            message: ข้อความที่จะแสดง
            key: unique key (ถ้าไม่ระบุจะใช้ message เป็น key)
            interval_seconds: ช่วงเวลาขั้นต่ำ (default: 5 นาที)
            force: บังคับแสดงโดยไม่สนใจ throttle
        """
        if force or self.should_log(key or message, interval_seconds):
            print(message)

# Global throttle instance
log_throttle = LogThrottle()

def throttled_log(interval_seconds: int = 300, key: Optional[str] = None):
    """
    Decorator สำหรับ throttle function calls ที่มี print statements
    
    Args:
        interval_seconds: ช่วงเวลาขั้นต่ำระหว่างการเรียก function
        key: unique key สำหรับ throttle (ถ้าไม่ระบุจะใช้ function name)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            throttle_key = key or f"{func.__module__}.{func.__name__}"
            
            if log_throttle.should_log(throttle_key, interval_seconds):
                return func(*args, **kwargs)
            # ถ้าไม่ควรแสดง log ให้ทำงานแบบเงียบๆ
            return None
            
        return wrapper
    return decorator

def throttled_print(message: str, key: Optional[str] = None, 
                   interval_seconds: int = 300, force: bool = False):
    """
    Convenience function สำหรับ throttled printing
    
    Args:
        message: ข้อความที่จะแสดง
        key: unique key (ถ้าไม่ระบุจะใช้ message เป็น key)
        interval_seconds: ช่วงเวลาขั้นต่ำ (default: 5 นาที)
        force: บังคับแสดงโดยไม่สนใจ throttle
    """
    log_throttle.throttled_print(message, key, interval_seconds, force)

# Specific throttle functions for common use cases
def throttled_realtime_log(message: str, force: bool = False):
    """Throttled logging สำหรับ real-time sync (5 นาที)"""
    throttled_print(message, "realtime_sync", 300, force)

def throttled_embed_log(message: str, force: bool = False):
    """Throttled logging สำหรับ embedding operations (5 นาที)"""
    throttled_print(message, "embedding_ops", 300, force)

def throttled_index_log(message: str, force: bool = False):
    """Throttled logging สำหรับ FAISS index operations (5 นาที)"""
    throttled_print(message, "faiss_index", 300, force)