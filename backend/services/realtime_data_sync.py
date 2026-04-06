#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Real-time Data Synchronization Service
=====================================
รองรับการอัปเดตข้อมูลแบบ real-time สำหรับระบบ repair chatbot
"""

import os
import time
import sqlite3
import threading
import logging
from typing import Optional, Dict, Any, Callable
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from core.config import SOURCE_DB_PATH, WORK_DB_PATH, PM2025_DB_PATH

logger = logging.getLogger("[REALTIME]")

class DatabaseChangeHandler(FileSystemEventHandler):
    """ตรวจจับการเปลี่ยนแปลงไฟล์ฐานข้อมูล"""
    
    def __init__(self, callback: Callable):
        self.callback = callback
        self.last_modified = {}
        self.debounce_time = 2  # รอ 2 วินาทีก่อน sync เพื่อป้องกัน multiple events
        
    def on_modified(self, event):
        if event.is_directory:
            return
            
        file_path = event.src_path
        if not file_path.endswith('.db'):
            return
            
        # Debounce: ป้องกัน multiple events ในเวลาใกล้เคียง
        now = time.time()
        if file_path in self.last_modified:
            if now - self.last_modified[file_path] < self.debounce_time:
                return
                
        self.last_modified[file_path] = now
        
        from utils.log_throttle import throttled_realtime_log
        throttled_realtime_log(f"📁 Database file changed: {file_path}")
        
        # เรียก callback หลังจาก debounce
        threading.Timer(self.debounce_time, self._delayed_callback, [file_path]).start()
        
    def _delayed_callback(self, file_path: str):
        """เรียก callback หลังจาก debounce period"""
        from utils.log_throttle import throttled_realtime_log
        
        try:
            self.callback(file_path)
        except Exception as e:
            throttled_realtime_log(f"Error in file change callback: {e}", force=True)

class RealTimeDataSync:
    """จัดการการ sync ข้อมูลแบบ scheduled check ทุก 30 วินาที"""
    
    def __init__(self):
        self.observer = None
        self.is_running = False
        self.sync_callbacks = []
        self.last_sync_time = {}
        self.check_interval = 30  # เช็คทุก 30 วินาที
        self.last_source_mtime = 0
        self.last_pm_mtime = 0
        self.scheduler_thread = None
        
    def add_sync_callback(self, callback: Callable[[str], None]):
        """เพิ่ม callback ที่จะถูกเรียกเมื่อข้อมูลเปลี่ยน"""
        self.sync_callbacks.append(callback)
        
    def start_monitoring(self):
        """เริ่มตรวจจับการเปลี่ยนแปลงไฟล์แบบ scheduled check"""
        if self.is_running:
            logger.warning("Real-time monitoring already running")
            return
            
        try:
            # เริ่ม scheduled check thread
            if os.path.exists(SOURCE_DB_PATH):
                self.last_source_mtime = os.path.getmtime(SOURCE_DB_PATH)
            if os.path.exists(PM2025_DB_PATH):
                self.last_pm_mtime = os.path.getmtime(PM2025_DB_PATH)

            self.is_running = True
            self.scheduler_thread = threading.Thread(target=self._scheduled_check, daemon=True)
            self.scheduler_thread.start()
            
            logger.info(f"🔄 Scheduled monitoring started (check every {self.check_interval}s)")
            
        except Exception as e:
            logger.error(f"Failed to start scheduled monitoring: {e}")
            
    def stop_monitoring(self):
        """หยุดตรวจจับการเปลี่ยนแปลง"""
        self.is_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        logger.info("🛑 Scheduled monitoring stopped")
        
    def _scheduled_check(self):
        """เช็คการเปลี่ยนแปลงไฟล์ทุก 30 วินาที"""
        from utils.log_throttle import throttled_realtime_log
        
        while self.is_running:
            try:
                if os.path.exists(SOURCE_DB_PATH):
                    current_mtime = os.path.getmtime(SOURCE_DB_PATH)
                    
                    # เช็คว่าไฟล์เปลี่ยนแปลงหรือไม่
                    if current_mtime > self.last_source_mtime:
                        self.last_source_mtime = current_mtime
                        throttled_realtime_log(f"📁 Database file changed: {SOURCE_DB_PATH}")
                        self._on_file_changed(SOURCE_DB_PATH)

                if os.path.exists(PM2025_DB_PATH):
                    current_pm_mtime = os.path.getmtime(PM2025_DB_PATH)
                    if current_pm_mtime > self.last_pm_mtime:
                        self.last_pm_mtime = current_pm_mtime
                        throttled_realtime_log(f"📁 Database file changed: {PM2025_DB_PATH}")
                        self._on_file_changed(PM2025_DB_PATH)
                        
                time.sleep(self.check_interval)
                
            except Exception as e:
                throttled_realtime_log(f"Error in scheduled check: {e}", force=True)
                time.sleep(self.check_interval)
            
    def _on_file_changed(self, file_path: str):
        """จัดการเมื่อไฟล์เปลี่ยนแปลง"""
        from utils.log_throttle import throttled_realtime_log
        
        # เรียก callbacks ทั้งหมด
        for callback in self.sync_callbacks:
            try:
                callback(file_path)
            except Exception as e:
                throttled_realtime_log(f"Error in sync callback: {e}", force=True)



# Global instance
realtime_sync = RealTimeDataSync()

def setup_realtime_sync():
    """ตั้งค่า real-time sync"""
    from utils.log_throttle import throttled_realtime_log
    
    def on_data_changed(file_path: str):
        """Callback เมื่อข้อมูลเปลี่ยน"""
        throttled_realtime_log(f"🔄 Data changed, syncing: {file_path}")
        
        if file_path == SOURCE_DB_PATH or SOURCE_DB_PATH in file_path:
            try:
                # Force reload ข้อมูลทั้งหมด (แทน incremental sync)
                from main import load_and_enrich_data
                success = load_and_enrich_data(force=True)
                
                if success:
                    throttled_realtime_log("✅ Real-time sync completed (full reload)")
                    
                    # Trigger embedding regeneration ถ้าจำเป็น
                    try:
                        from services.embeddings import auto_generate_embeddings_if_needed
                        import threading
                        threading.Thread(
                            target=auto_generate_embeddings_if_needed,
                            kwargs={"threshold": 10},  # ลด threshold เป็น 10 rows
                            daemon=True
                        ).start()
                        throttled_realtime_log("🔍 Embedding regeneration triggered")
                    except Exception as e:
                        throttled_realtime_log(f"⚠️ Embedding trigger failed: {e}")
                        
                else:
                    throttled_realtime_log("❌ Real-time sync failed", force=True)
                
            except Exception as e:
                throttled_realtime_log(f"❌ Real-time sync error: {e}", force=True)
        elif file_path == PM2025_DB_PATH or PM2025_DB_PATH in file_path:
            try:
                from main import ensure_pm_synced
                success = ensure_pm_synced(force=True)
                if success:
                    throttled_realtime_log("✅ PM sync completed")
                else:
                    throttled_realtime_log("❌ PM sync failed", force=True)
            except Exception as e:
                throttled_realtime_log(f"❌ PM sync error: {e}", force=True)
                
    # เพิ่ม callback
    realtime_sync.add_sync_callback(on_data_changed)
    
    # เริ่มตรวจจับ
    realtime_sync.start_monitoring()

def cleanup_realtime_sync():
    """ทำความสะอาด real-time sync"""
    realtime_sync.stop_monitoring()

# ตัวอย่างการใช้งาน
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("🔄 Starting real-time data sync test...")
    setup_realtime_sync()
    
    try:
        # รอการเปลี่ยนแปลงไฟล์
        print("Monitoring for file changes... (Press Ctrl+C to stop)")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping real-time sync...")
        cleanup_realtime_sync()
