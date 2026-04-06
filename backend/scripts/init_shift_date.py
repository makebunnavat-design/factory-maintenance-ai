#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Initialization Script for Shift_Date Column
===========================================
รัน script นี้ครั้งเดียวหลัง docker compose up เพื่อเพิ่ม Shift_Date column
ตรวจสอบและสร้างตาราง repairs_enriched ก่อนถ้ายังไม่มี
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    """เรียกใช้ script เพิ่ม Shift_Date column"""
    print("🔧 Initializing Shift_Date column for cross-day shift support...")
    
    try:
        # ตรวจสอบว่ามีตาราง repairs_enriched หรือไม่
        from core.database import get_work_db
        
        with get_work_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='repairs_enriched'
            """)
            table_exists = cursor.fetchone() is not None
            
            if not table_exists:
                print("📊 Table repairs_enriched not found, need to load data first...")
                
                # โหลดข้อมูลเพื่อสร้างตาราง repairs_enriched
                from main import load_and_enrich_data
                success = load_and_enrich_data(force=True)
                
                if not success:
                    print("❌ Failed to load and create repairs_enriched table")
                    return False
                    
                print("✅ Table repairs_enriched created successfully")
            
            # เช็คว่า Shift_Date column มีข้อมูลครบแล้วหรือไม่
            cursor.execute("PRAGMA table_info(repairs_enriched)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'Shift_Date' not in columns:
                print("➕ Shift_Date column not found, need to add...")
            else:
                # เช็คว่ามีข้อมูล Shift_Date ครบหรือไม่
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(Shift_Date) as shift_date_rows
                    FROM repairs_enriched 
                    WHERE CallTime IS NOT NULL
                """)
                total_rows, shift_date_rows = cursor.fetchone()
                
                if total_rows == shift_date_rows and total_rows > 0:
                    print(f"✅ Shift_Date column already complete ({shift_date_rows}/{total_rows} rows)")
                    return True
                else:
                    print(f"⚠️  Shift_Date incomplete ({shift_date_rows}/{total_rows} rows), updating...")
        
        # ตอนนี้เพิ่ม/อัพเดท Shift_Date column
        from scripts.add_shift_date_column import add_shift_date_column, create_shift_date_view
        add_shift_date_column()
        create_shift_date_view()
        
        print("✅ Shift_Date initialization completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)