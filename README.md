# 🤖 Repair Chatbot - AI-Powered Maintenance Assistant

## Portfolio Showcase

This repo now includes a portfolio-friendly static case-study page in [`portfolio/`](portfolio/README.md).

- Open `portfolio/index.html` locally to view the mockup/showcase
- Use that folder as the presentation layer if you want to publish this project with GitHub Pages

AI Chatbot สำหรับตอบคำถามเกี่ยวกับข้อมูลการซ่อมและ Preventive Maintenance (PM)

## 🚀 Quick Start

### เริ่มใช้งาน (Windows)

1. **เริ่มระบบทั้งหมด:**
   ```
   ดับเบิลคลิก START_ALL.bat
   ```

2. **เปิดเบราว์เซอร์:**
   ```
   http://localhost:18080
   ```

3. **หยุดระบบ:**
   ```
   ดับเบิลคลิก STOP_ALL.bat
   ```

📖 **คู่มือฉบับเต็ม:** [QUICK_START.md](QUICK_START.md)

---

## 📁 Project Structure

```
repair-chatbot/
├── START_ALL.bat                    # 🚀 เริ่มทุกอย่าง
├── STOP_ALL.bat                     # 🛑 หยุดทุกอย่าง
├── sync-repair-data-windows.ps1     # 🔄 Auto-sync script
├── docker-compose.yml               # 🐳 Docker configuration
├── backend/                         # 🔧 Backend (Python/FastAPI)
│   ├── main.py                      # Main application
│   ├── core/                        # Core modules
│   ├── pipelines/                   # AI pipelines
│   ├── services/                    # Services (embeddings, vector search)
│   ├── utils/                       # Utilities
│   ├── data/                        # Data directory
│   │   ├── repair_data.db           # 📂 Synced from O:\LOG\
│   │   └── PM2025.db                # 📂 Synced from O:\Database\
│   └── models/                      # AI models
└── frontend/                        # 🎨 Frontend (HTML/CSS/JS)
    └── static/
```

---

## 🔄 Database Auto-Sync

ระบบจะ sync ฐานข้อมูลจาก O:\ drive อัตโนมัติทุก 5 นาที:

- `O:\LOG\repair_data.db` → `backend/data/repair_data.db`
- `O:\Database\PM2025.db` → `backend/data/PM2025.db`

**ตรวจสอบสถานะ:** ดูหน้าต่าง "Database Auto-Sync"

📖 **สถาปัตยกรรม:** [SYNC_ARCHITECTURE.md](SYNC_ARCHITECTURE.md)

---

## 🐳 Docker Services

| Service | Port | Description |
|---------|------|-------------|
| repair-chatbot | 18080 | FastAPI Backend + AI Engine |
| ollama_service | 11434 | LLM Model Server |

**คำสั่งที่มีประโยชน์:**
```bash
# ดู logs
docker-compose logs -f repair-chatbot

# Restart
docker-compose restart repair-chatbot

# Rebuild
docker-compose up --build -d
```

---

## 🤖 Features

### 1. AI-Powered Query Understanding
- รู้จักคำถามภาษาไทยและอังกฤษ
- แยกโหมดอัตโนมัติ (Data/Chat/Meta)
- สร้าง SQL query จาก natural language

### 2. Multi-Mode Operation

**Data Mode (โหมดข้อมูล)**
- ตอบคำถามจากฐานข้อมูลการซ่อม
- สร้าง SQL query อัตโนมัติ
- แสดงผลเป็นตาราง/กราฟ

**Chat Mode (โหมดคุยเล่น)**
- คุยสบายๆ กับ AI
- ตอบคำถามทั่วไป
- บุคลิก "Elin" ร่าเริง อบอุ่น

**Meta Mode (โหมดความรู้)**
- ค้นหาจาก Knowledge Base
- ตอบคำถามเกี่ยวกับ PM, การซ่อม
- LLM สังเคราะห์คำตอบจาก context

### 3. Smart Data Processing
- Auto-enrichment (Shift, Team, Response Time)
- Tech normalization (แยก comma-separated techs)
- PM date handling (รองรับการเลื่อน PM)
- Business logic filters

### 4. Vector Search
- Semantic search ด้วย BGE-M3 embeddings
- Reranking ด้วย BGE-Reranker-Large
- Hybrid search (Vector + Fuzzy)

---

## ⚙️ Configuration

### Environment Variables

แก้ไฟล์ `docker-compose.yml`:

```yaml
environment:
  - OLLAMA_HOST=http://ollama_service:11434
  - HF_HUB_OFFLINE=0  # Set to 1 for offline mode
```

### AI Models

แก้ไฟล์ `backend/core/config.py`:

```python
# SQL Generation Model (ใหญ่, แม่นยำ)
MODEL_NAME = "hf.co/bartowski/Qwen2.5-Coder-14B-Instruct-GGUF:Q3_K_M"

# Chat Model (เล็ก, เร็ว)
CHAT_MODEL = "scb10x/llama3.1-typhoon2-8b-instruct:latest"
```

### Sync Interval

แก้ไฟล์ `START_ALL.bat`:

```batch
powershell.exe -File sync-repair-data-windows.ps1 -IntervalMinutes 10
```

---

## 🔧 Troubleshooting

### Docker ไม่เริ่ม
- ตรวจสอบ Docker Desktop เปิดอยู่
- รัน `docker info` เพื่อเช็คสถานะ

### Sync ไม่ทำงาน
- ตรวจสอบ O:\ drive accessible
- ดู logs ในหน้าต่าง "Database Auto-Sync"
- รัน sync ด้วยมือ: `powershell -File sync-repair-data-windows.ps1`

### Backend Error
- ดู logs: `docker-compose logs -f repair-chatbot`
- ตรวจสอบไฟล์ database อยู่ใน `backend/data/source/`

📖 **Troubleshooting Guide:** [QUICK_START.md](QUICK_START.md#-troubleshooting)

---

## 📊 System Requirements

- **OS:** Windows 10/11
- **Docker Desktop:** Latest version
- **RAM:** 8GB+ recommended
- **Disk:** 10GB+ free space
- **Network:** Access to O:\ drive

---

## 📚 Documentation

- [QUICK_START.md](QUICK_START.md) - คู่มือการใช้งาน
- [SYNC_ARCHITECTURE.md](SYNC_ARCHITECTURE.md) - สถาปัตยกรรมระบบ sync

---

## 🎯 Usage Examples

### ตัวอย่างคำถาม (Data Mode)

```
- วันนี้มีอะไรเสียบ้าง
- เดือนนี้ Line PCB C เสียกี่ครั้ง
- ช่าง A ซ่อมเร็วที่สุดใน Line ไหน
- สัปดาห์นี้มี PM อะไรบ้าง
- เดือนที่แล้ว Process ไหนเสียบ่อยที่สุด
```

### ตัวอย่างคำถาม (Meta Mode)

```
- PM คืออะไร
- วิธีซ่อม Motor เสีย
- Checklist PM ประจำเดือน
- ขั้นตอนการ Troubleshoot Sensor
```

---

## 📞 Support

หากมีปัญหา:
1. ตรวจสอบ [QUICK_START.md](QUICK_START.md#-troubleshooting)
2. ดู Docker logs
3. ตรวจสอบ Sync logs
4. ติดต่อทีมพัฒนา

---

**Made with ❤️ for Maintenance Teams**
