#!/bin/bash
set -e

echo "=========================================="
echo "Repair Chatbot - Starting..."
echo "=========================================="

role="${APP_ROLE:-api}"
startup_sync_enabled="${STARTUP_DATA_SYNC_ENABLED:-1}"

# Check models at startup (quick verification)
echo "[INFO] Checking models availability..."
python scripts/check_models.py

# Wait for source database to be available
echo "[INFO] Waiting for source database..."
timeout=30
counter=0
while [ ! -f "data/repair_data.db" ] && [ $counter -lt $timeout ]; do
    echo "Waiting for repair_data.db... ($counter/$timeout)"
    sleep 1
    counter=$((counter + 1))
done

if [ ! -f "data/repair_data.db" ]; then
    echo "⚠️  Source database not found, will create empty work database"
else
    echo "✅ Source database found"
fi

# Initialize Shift_Date column if needed (one-time setup)
echo "[INFO] Initializing Shift_Date column for cross-day shift support..."
# Skip Shift_Date initialization temporarily to avoid blocking startup
echo "⚠️  Skipping Shift_Date initialization to avoid startup delay"
# python scripts/init_shift_date.py || echo "⚠️  Shift_Date initialization had issues, continuing..."

echo "[INFO] Database files are synced by Windows host script"
echo "[INFO] Mounted from: backend/data/"
echo "[INFO] Container role: ${role}"

if [ "$role" = "api" ] && [ "$startup_sync_enabled" = "0" ]; then
    echo "[INFO] API startup sync disabled, waiting briefly for work database from sync worker..."
    work_timeout=60
    work_counter=0
    while [ ! -f "data/repair_enriched.db" ] && [ $work_counter -lt $work_timeout ]; do
        echo "Waiting for repair_enriched.db... ($work_counter/$work_timeout)"
        sleep 1
        work_counter=$((work_counter + 1))
    done

    if [ -f "data/repair_enriched.db" ]; then
        echo "✅ Work database found"
    else
        echo "⚠️  Work database not ready yet, API will continue with emergency fallback if needed"
    fi
fi

if [ "$role" = "sync-worker" ]; then
    echo "[START] Launching dedicated sync worker..."
    exec python scripts/run_sync_worker.py
fi

echo "[INFO] Launching API container"
echo "[START] Launching FastAPI server on port 18080..."
exec uvicorn main:app --host 0.0.0.0 --port 18080
