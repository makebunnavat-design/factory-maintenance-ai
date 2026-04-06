โฟลเดอร์ฟอนต์ (ใช้จากเครื่อง ไม่ต้องพึ่งเน็ต)
=============================================

1) ฟอนต์ Prompt (ข้อความไทย/หน้าจอหลัก)
   - ใช้ไฟล์ .ttf จาก backend/fonts/ (หรือดาวน์โหลดที่ https://fonts.google.com/specimen/Prompt)
   - ในโฟลเดอร์นี้ (static/fonts/) ควรมี:
     • Prompt-Regular.ttf   (น้ำหนัก 400)
     • Prompt-Medium.ttf    (น้ำหนัก 500)
     • Prompt-SemiBold.ttf  (น้ำหนัก 600)
     • Prompt-Light.ttf     (น้ำหนัก 300)
   - ถ้ามีแค่ Prompt-Regular.ttf ระบบก็ใช้ได้

2) ฟอนต์ Phosphor (ไอคอน)
   - ไฟล์ Phosphor.woff2 ใส่ในโฟลเดอร์ static/css/
     (ไฟล์ phosphor-regular.css อ้างอิงจากโฟลเดอร์ css)
   - หรือดาวน์โหลดชุด Phosphor Icons แล้วคัดลอก .woff2 ไปที่ static/css/

เมื่อใส่ไฟล์ครบแล้ว รีเฟรชหน้าเว็บ ฟอนต์จะโหลดจากเครื่อง ไม่ผ่านเน็ต
