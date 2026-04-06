#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pydantic Models for Repair Chatbot API
=======================================
Request/Response models สำหรับทุก endpoint — บังคับโครงสร้างข้อมูลที่ชัดเจน
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# =====================================================
# REQUEST MODELS
# =====================================================

class ChatRequest(BaseModel):
    """Request body สำหรับ /chat endpoint"""
    message: str
    topic: Optional[str] = Field(
        default=None,
        description="'repair' | 'pm' | null — เลือกจากปุ่มหน้าถามคำถาม แยก database"
    )
    ai_100: bool = Field(
        default=False,
        description="True = ให้ AI คิด 100% (ใช้แค่ความปลอดภัย + แก้วันที่ + ตรวจคอลัมน์)"
    )
    mode: Optional[str] = Field(
        default="normal",
        description="'normal' | 'meta' — เลือกโหมดการทำงาน (meta = ถามตอบจาก Meta Database)"
    )
    limit_n: Optional[int] = Field(
        default=None,
        ge=1,
        le=500,
        description="จำนวนรายการที่แสดง (1–500), ไม่ส่ง = ใช้ค่าเริ่มต้น 10 หรือจากคำถาม"
    )
    # Workaround สำหรับ proxy ที่ไม่ forward /api/meta/add
    meta_add: Optional[Dict[str, str]] = Field(
        default=None,
        description="{'name': str, 'topic': str, 'answer': str} — เพิ่มความรู้ใน Meta Database"
    )
    # Workaround สำหรับ proxy ที่ไม่ forward /api/meta/update_embeddings
    meta_rebuild_embeddings: Optional[bool] = Field(
        default=None,
        description="True = ลบ embeddings เก่าทั้งหมดแล้ว rebuild ใหม่"
    )


# =====================================================
# RESPONSE MODELS
# =====================================================

class ChatResponse(BaseModel):
    """Response body สำหรับ /chat endpoint (กรณีสำเร็จ)"""
    text: str = Field(description="คำตอบที่แสดงให้ผู้ใช้")
    sql: str = Field(default="", description="SQL ที่ใช้ (ว่างถ้าเป็นโหมดคุยเล่น)")
    data: List[Dict[str, Any]] = Field(default_factory=list, description="ข้อมูลตาราง")
    row_count: int = Field(default=0, description="จำนวนแถวที่แสดง")
    total_count: Optional[int] = Field(default=None, description="จำนวนแถวทั้งหมดใน DB (ก่อน LIMIT)")
    timestamp: Optional[str] = Field(default=None, description="เวลาตอบ (Bangkok timezone)")


class ChatErrorResponse(BaseModel):
    """Response body สำหรับ /chat endpoint (กรณี error)"""
    error: str = Field(description="ข้อความ error")
    error_link_text: str = Field(default="กดที่นี่")
    error_link_url: str = Field(default="http://172.16.2.44:18080/analytics-dashboard")


class DataStatusResponse(BaseModel):
    """Response body สำหรับ /api/data-status"""
    total_records: int = 0
    latest_date: Optional[str] = None
    last_processed: Optional[str] = None
    source_db: Optional[str] = None
    work_db: Optional[str] = None
    pm_db: Optional[str] = None
    source_db_exists: bool = False
    work_db_exists: bool = False
    pm_db_exists: bool = False


class ReloadResponse(BaseModel):
    """Response body สำหรับ /api/reload"""
    success: bool
    message: str


class SuggestionsResponse(BaseModel):
    """Response body สำหรับ /api/suggestions (Ghost text)"""
    words: List[str] = Field(default_factory=list)
    lines: List[str] = Field(default_factory=list)
    processes: List[str] = Field(default_factory=list)
    techs: List[str] = Field(default_factory=list)
    pm_task_names: List[str] = Field(default_factory=list)
    shared_line_pm: List[str] = Field(default_factory=list)
    line_pm_pairs: List = Field(default_factory=list)


class DashboardLineData(BaseModel):
    """ข้อมูลสรุปรายไลน์สำหรับ Dashboard"""
    total: int = 0
    avg_repair: float = 0.0
    top_process: Optional[str] = None


class DashboardResponse(BaseModel):
    """Response body สำหรับ /api/dashboard"""
    lines: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None


class TechDashboardTeam(BaseModel):
    """ข้อมูลทีมช่างในหน้า Tech Dashboard"""
    team_name: str
    techs: List[Dict[str, Any]] = Field(default_factory=list)
    total_repairs: int = 0
    avg_repair_time: float = 0.0


class TechDashboardResponse(BaseModel):
    """Response body สำหรับ /api/tech-dashboard"""
    teams: Dict[str, Any] = Field(default_factory=dict)
    summary: Dict[str, Any] = Field(default_factory=dict)
    timestamp: Optional[str] = None
    error: Optional[str] = None


class SystemStatsResponse(BaseModel):
    """Response body สำหรับ /api/system-stats"""
    total_repairs: int = 0
    total_techs: int = 0
    total_lines: int = 0
    total_pm: int = 0
    db_size_mb: float = 0.0
    uptime: Optional[str] = None
    timestamp: Optional[str] = None
