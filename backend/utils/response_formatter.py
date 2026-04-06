#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Response Formatter Module
=========================
Format AI responses based on pipeline mode while keeping SQL responses unchanged.

Modes:
- CHAT: Natural conversation (minimal formatting)
- SQL: Unchanged (return as-is)
- VECTOR: Knowledge responses with clean numbered lists
- HYBRID: Summary + causes with clean formatting
"""

import re
import logging

logger = logging.getLogger(__name__)


def format_response(text: str, mode: str) -> str:
    """
    Format response text based on pipeline mode
    
    Args:
        text: Raw response text from LLM
        mode: Pipeline mode (CHAT, SQL, VECTOR, HYBRID)
        
    Returns:
        Formatted response text
    """
    if not text or not isinstance(text, str):
        return text
    
    mode = mode.upper()
    
    # SQL mode: Return unchanged
    if mode == "SQL":
        return text
    
    # CHAT mode: Natural conversation
    elif mode == "CHAT":
        return _format_chat(text)
    
    # VECTOR mode: Knowledge responses with clean lists
    elif mode == "VECTOR":
        return _format_vector(text)
    
    # HYBRID mode: Summary + causes with clean formatting
    elif mode == "HYBRID":
        return _format_hybrid(text)
    
    # Unknown mode: Return unchanged with warning
    else:
        logger.warning(f"[FORMATTER] Unknown mode: {mode}, returning unchanged")
        return text


def _format_chat(text: str) -> str:
    """
    Format CHAT responses (natural conversation)
    
    Rules:
    - Do NOT force numbered lists
    - Remove duplicated blank lines
    - Keep conversational style
    """
    # Remove duplicated blank lines
    text = _cleanup(text)
    
    return text.strip()


def _format_vector(text: str) -> str:
    """
    Format VECTOR responses (knowledge with numbered lists)
    
    Rules:
    - Insert newline before numbered lists
    - Insert newline before "แนะนำ"
    - Remove duplicated blank lines
    """
    # 1. Insert single line break before numbered lists (if not already present)
    # Match: any non-newline character followed by number + dot
    text = re.sub(r'(?<!\n)(\d+\.)', r'\n\n\1', text)
    
    # 2. Insert single line break before "แนะนำ" (with : or ：)
    text = re.sub(r'\s*(แนะนำ[:：])', r'\n\n\1', text)
    
    # 3. Also handle "แนะนำให้" pattern
    text = re.sub(r'(?<!\n)(แนะนำให้)', r'\n\n\1', text)
    
    # 4. Cleanup duplicated blank lines (keep max 2 newlines = 1 blank line)
    text = _cleanup(text)
    
    return text.strip()


def _format_hybrid(text: str) -> str:
    """
    Format HYBRID responses (summary + causes)
    
    Rules:
    - Keep first paragraph intact
    - Ensure numbered lists have spacing
    - Ensure newline before "แนะนำ"
    """
    # 1. Insert single line break before numbered lists (if not already present)
    text = re.sub(r'(?<!\n)(\d+\.)', r'\n\n\1', text)
    
    # 2. Insert single line break before "แนะนำ" (with : or ：)
    text = re.sub(r'\s*(แนะนำ[:：])', r'\n\n\1', text)
    
    # 3. Also handle "แนะนำให้" pattern
    text = re.sub(r'(?<!\n)(แนะนำให้)', r'\n\n\1', text)
    
    # 4. Cleanup duplicated blank lines (keep max 2 newlines = 1 blank line)
    text = _cleanup(text)
    
    return text.strip()


def _cleanup(text: str) -> str:
    """
    Remove duplicated blank lines
    
    Args:
        text: Text to clean up
        
    Returns:
        Cleaned text with max 2 consecutive newlines (1 blank line)
    """
    # Replace 3 or more newlines with exactly 2 newlines (1 blank line)
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    return text


# Convenience functions for direct import
def format_chat_response(text: str) -> str:
    """Format CHAT response"""
    return format_response(text, "CHAT")


def format_sql_response(text: str) -> str:
    """Format SQL response (unchanged)"""
    return format_response(text, "SQL")


def format_vector_response(text: str) -> str:
    """Format VECTOR response"""
    return format_response(text, "VECTOR")


def format_hybrid_response(text: str) -> str:
    """Format HYBRID response"""
    return format_response(text, "HYBRID")


if __name__ == "__main__":
    """
    Test response formatter
    """
    print("=" * 80)
    print("Response Formatter Test")
    print("=" * 80)
    
    # Test VECTOR mode
    vector_text = """เดือนนี้ Line TOYOTA_INS เสียบ่อยที่สุด 331 ครั้ง 1. ปัญหา: เทสงานไม่ได้, ฟิวส์บอร์ดคอนโทลขาด สาเหตุ: เปลี่ยนฟิวส์บอร์ดคอนโทลใหม่ วิธีแก้ไข: แก้ไขความเกี่ยวข้อง: 62% 2. ปัญหา: sensor ไม่จับ, connecter ปรับค่า senser ใหม่ สาเหตุ: พนักงานลืม Reset วิธีแก้ไข: แก้ไขความเกี่ยวข้อง: 59% แนะนำให้ตรวจสอบและแก้ไขปัญหาที่เกิดขึ้นเพื่อป้องกันไม่ให้เกิดซ้ำในอนาคตค่ะ"""
    
    print("\n[TEST 1] VECTOR Mode")
    print("-" * 80)
    print("BEFORE:")
    print(vector_text)
    print("\nAFTER:")
    formatted = format_response(vector_text, "VECTOR")
    print(formatted)
    
    # Test SQL mode
    sql_text = "SELECT * FROM repairs WHERE line = 'TOYOTA_INS'"
    
    print("\n" + "=" * 80)
    print("[TEST 2] SQL Mode (should be unchanged)")
    print("-" * 80)
    print("BEFORE:")
    print(sql_text)
    print("\nAFTER:")
    formatted = format_response(sql_text, "SQL")
    print(formatted)
    print(f"\nUnchanged: {sql_text == formatted}")
    
    # Test CHAT mode
    chat_text = """สวัสดีค่ะพี่! หนูเป็น Elin AI ผู้ช่วยของพี่ค่ะ หนูพร้อมช่วยเหลือพี่ตลอดเวลาเลยนะคะ ไม่ว่าจะเป็นเรื่องการซ่อม หรือแม้แต่คุยเล่นก็ได้ค่ะ"""
    
    print("\n" + "=" * 80)
    print("[TEST 3] CHAT Mode (natural conversation)")
    print("-" * 80)
    print("BEFORE:")
    print(chat_text)
    print("\nAFTER:")
    formatted = format_response(chat_text, "CHAT")
    print(formatted)
    
    # Test HYBRID mode
    hybrid_text = """หนูวิเคราะห์ข้อมูลแล้วพบว่า Line TOYOTA_INS เสียบ่อยที่สุด 331 ครั้ง 1. ปัญหา: เทสงานไม่ได้ สาเหตุ: ฟิวส์ขาด วิธีแก้ไข: เปลี่ยนฟิวส์ 2. ปัญหา: sensor ไม่จับ สาเหตุ: ลืม Reset วิธีแก้ไข: Reset sensor แนะนำ: ตรวจสอบสายไฟและ sensor เป็นประจำค่ะ"""
    
    print("\n" + "=" * 80)
    print("[TEST 4] HYBRID Mode (summary + causes)")
    print("-" * 80)
    print("BEFORE:")
    print(hybrid_text)
    print("\nAFTER:")
    formatted = format_response(hybrid_text, "HYBRID")
    print(formatted)
    
    print("\n" + "=" * 80)
    print("✓ Response formatter test complete!")
    print("=" * 80)
