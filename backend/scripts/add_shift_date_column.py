#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script สำหรับเพิ่ม Shift_Date column ในฐานข้อมูล
==============================================
แก้ไขปัญหากะดึกข้ามวัน โดยเพิ่ม column ที่เก็บวันที่ของกะงานจริง
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sqlite3
import pandas as pd
from datetime import datetime, timedelta, time as dt_time
from core.database import get_work_db
from core.config import WORK_DB_PATH

def get_shift_date(dt_obj):
    """
    คำนวณวันที่ของกะงาน (Shift_Date)
    
    NOTE: ฟังก์ชันนี้ซ้ำกับ get_shift_date() ใน main.py 
    เก็บไว้ในนี้เพื่อให้ script ทำงานได้อิสระ
    
    Logic:
    - กะเช้า/กะบ่าย: ใช้วันที่เดียวกัน
    - กะดึก 20:00-23:59: ใช้วันที่เดียวกัน  
    - กะดึก 00:00-07:59: ใช้วันก่อนหน้า (เพราะเป็นกะดึกของวันก่อน)
    """
    if pd.isnull(dt_obj):
        return None
    
    t = dt_obj.time()
    current_date = dt_obj.date()
    
    # กะดึกช่วงหลังเที่ยงคืน (00:00-07:59) → เป็นกะดึกของวันก่อนหน้า
    if t <= dt_time(7, 59):
        shift_date = current_date - timedelta(days=1)
        return shift_date.strftime('%Y-%m-%d')
    
    # กรณีอื่นๆ ใช้วันที่เดียวกัน
    return current_date.strftime('%Y-%m-%d')

def add_shift_date_column():
    """เพิ่ม Shift_Date column ในตาราง repairs_enriched"""
    
    print("🔧 เริ่มเพิ่ม Shift_Date column...")
    
    with get_work_db() as conn:
        cursor = conn.cursor()
        
        # 1. ตรวจสอบว่ามีตาราง repairs_enriched หรือไม่
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='repairs_enriched'
        """)
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            print("❌ Table repairs_enriched not found. Please run load_and_enrich_data() first.")
            return False
        
        # 2. ตรวจสอบว่ามี column Shift_Date อยู่แล้วหรือไม่
        cursor.execute("PRAGMA table_info(repairs_enriched)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'Shift_Date' in columns:
            print("⚠️  Column Shift_Date มีอยู่แล้ว จะอัพเดทข้อมูล...")
        else:
            print("➕ เพิ่ม column Shift_Date...")
            cursor.execute("ALTER TABLE repairs_enriched ADD COLUMN Shift_Date TEXT")
            conn.commit()
        
        # 3. เช็คว่าข้อมูล Shift_Date ครบแล้วหรือไม่
        print("📊 เช็คสถานะข้อมูล Shift_Date...")
        cursor.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(Shift_Date) as shift_date_rows
            FROM repairs_enriched 
            WHERE CallTime IS NOT NULL
        """)
        total_rows, shift_date_rows = cursor.fetchone()
        
        if total_rows == shift_date_rows and total_rows > 0:
            print(f"✅ Shift_Date ครบแล้ว ({shift_date_rows}/{total_rows} rows) - ข้าม")
            return True
        
        print(f"📈 ต้องอัพเดท Shift_Date: {total_rows - shift_date_rows} rows")
        
        # 4. โหลดเฉพาะข้อมูลที่ยังไม่มี Shift_Date
        print("📊 โหลดข้อมูลที่ต้องอัพเดท...")
        df = pd.read_sql_query("""
            SELECT id, CallTime, Date, Shift 
            FROM repairs_enriched 
            WHERE CallTime IS NOT NULL AND Shift_Date IS NULL
        """, conn)
        
        if df.empty:
            print("✅ ไม่มีข้อมูลที่ต้องอัพเดท")
            return True
        
        print(f"📈 พบข้อมูลที่ต้องอัพเดท {len(df)} รายการ")
        
        # 5. แปลง CallTime เป็น datetime และคำนวณ Shift_Date
        df['CallTime'] = pd.to_datetime(df['CallTime'], errors='coerce')
        df['Shift_Date'] = df['CallTime'].apply(get_shift_date)
        
        # 6. อัพเดทข้อมูลกลับเข้าฐานข้อมูลแบบ bulk update
        print("💾 อัพเดทข้อมูล Shift_Date...")
        
        # ใช้ bulk update แทนการ update ทีละ row
        update_data = []
        for _, row in df.iterrows():
            if pd.notna(row['Shift_Date']):
                update_data.append((row['Shift_Date'], row['id']))
        
        if update_data:
            cursor.executemany("""
                UPDATE repairs_enriched 
                SET Shift_Date = ? 
                WHERE id = ?
            """, update_data)
            
            conn.commit()
            print(f"✅ อัพเดทสำเร็จ {len(update_data)} รายการ (bulk update)")
        else:
            print("⚠️  ไม่มีข้อมูลที่ต้องอัพเดท")
        
        # 7. ตรวจสอบผลลัพธ์
        print("\n📋 ตัวอย่างข้อมูลหลังอัพเดท:")
        sample = pd.read_sql_query("""
            SELECT Date, Shift, Shift_Date, CallTime
            FROM repairs_enriched 
            WHERE Shift_Date IS NOT NULL 
            ORDER BY CallTime DESC 
            LIMIT 10
        """, conn)
        
        print(sample.to_string(index=False))
        
        # 8. สถิติการแบ่งกะ
        print("\n📊 สถิติการแบ่งกะ:")
        stats = pd.read_sql_query("""
            SELECT 
                Shift,
                COUNT(*) as Total_Records,
                COUNT(CASE WHEN Date != Shift_Date THEN 1 END) as Cross_Date_Records
            FROM repairs_enriched 
            WHERE Shift_Date IS NOT NULL
            GROUP BY Shift
        """, conn)
        
        print(stats.to_string(index=False))
        return True

def create_shift_date_view():
    """สร้าง view สำหรับ query ที่ใช้ Shift_Date"""
    
    print("\n🔍 สร้าง view สำหรับ query...")
    
    with get_work_db() as conn:
        cursor = conn.cursor()
        
        # ตรวจสอบว่ามีตาราง repairs_enriched หรือไม่
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='repairs_enriched'
        """)
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            print("❌ Table repairs_enriched not found, cannot create view")
            return False
        
        # ลบ view เก่า (ถ้ามี)
        cursor.execute("DROP VIEW IF EXISTS repairs_by_shift_date")
        
        # สร้าง view ใหม่
        cursor.execute("""
            CREATE VIEW repairs_by_shift_date AS
            SELECT 
                *,
                Shift_Date || '_' || Shift as Shift_Period
            FROM repairs_enriched
            WHERE Shift_Date IS NOT NULL
        """)
        
        conn.commit()
        print("✅ สร้าง view 'repairs_by_shift_date' สำเร็จ")
        
        # ตัวอย่างการใช้งาน
        print("\n📝 ตัวอย่างการใช้งาน:")
        print("-- ดูกะดึกวันที่ 18 มีนาคม (ทั้งหมด)")
        print("SELECT * FROM repairs_by_shift_date WHERE Shift_Date = '2025-03-18' AND Shift = 'Night'")
        print()
        print("-- ดูข้อมูลตาม Shift_Period")
        print("SELECT * FROM repairs_by_shift_date WHERE Shift_Period = '2025-03-18_Night'")
        return True

if __name__ == "__main__":
    try:
        success1 = add_shift_date_column()
        success2 = create_shift_date_view()
        
        if success1 and success2:
            print("\n🎉 เสร็จสิ้นการอัพเกรดระบบ!")
        else:
            print("\n⚠️  การอัพเกรดเสร็จสิ้นแต่มีข้อผิดพลาดบางส่วน")
        
    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาด: {e}")
        import traceback
        traceback.print_exc()