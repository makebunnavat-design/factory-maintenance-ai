# Override ด้วย mirror ได้: docker build --build-arg BASE_IMAGE=registry.cn-hangzhou.aliyuncs.com/library/python:3.11-slim .
# ลำดับ layer สำคัญ: COPY requirements ก่อน → ถ้า requirements.txt ไม่เปลี่ยน Docker จะใช้ cache ไม่โหลด pip ใหม่
ARG BASE_IMAGE=python:3.11-slim
FROM ${BASE_IMAGE}

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Environment variables for GPU and offline mode
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility
# Set HF_HOME to centralize models
ENV HF_HOME=/app/models/huggingface
# Offline mode flags (enable these for truly offline runtime)
# ENV HF_HUB_OFFLINE=1
# ENV TRANSFORMERS_OFFLINE=1

# ติดตั้ง dependencies สำหรับ VITS Thai TTS
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    libsndfile1 \
    ffmpeg \
    libavcodec-extra \
    && rm -rf /var/lib/apt/lists/*

# Layer นี้จะใช้ cache ถ้า requirements.txt ไม่เปลี่ยน
COPY config/requirements.txt ./requirements.txt
# 🔥 BuildKit Optimization: ใช้ mount cache เพื่อให้ pip ไม่ต้องโหลดใหม่ถ้าเคยโหลดแล้ว
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --default-timeout=1000 -r requirements.txt

# Copy source code
COPY . .

# 🔥 Skip copying models - use volume mounts instead!
# Models will be available at runtime via: ./backend/models:/app/models
# This makes docker build MUCH faster (no 2.5GB transfer)

# Create models directory structure for runtime
RUN mkdir -p models/huggingface

# Set HF_HOME to use volume-mounted models
ENV HF_HOME=/app/models/huggingface

# Make entrypoint executable
RUN chmod +x entrypoint.sh

# Create directories for volume mounts
RUN mkdir -p models frontend/static data/source

EXPOSE 18080

# Use entrypoint script for automatic setup
ENTRYPOINT ["./entrypoint.sh"]