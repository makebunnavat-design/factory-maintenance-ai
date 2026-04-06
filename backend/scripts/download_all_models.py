#!/usr/bin/env python3
"""
Download all required AI models for offline use.
This script checks for existing models and downloads missing ones.
Optimized for docker-compose with volume mounts.
"""
import os
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_models():
    # 1. Setup paths
    # In Docker with volume mounts, models are at /app/models
    # Locally, we use relative path.
    base_dir = Path("/app") if os.path.exists("/app") else Path(".").absolute()
    models_dir = base_dir / "models"
    hf_cache_dir = models_dir / "huggingface"
    
    os.makedirs(models_dir, exist_ok=True)
    os.makedirs(hf_cache_dir, exist_ok=True)
    
    # Set HuggingFace cache directory
    os.environ['HF_HOME'] = str(hf_cache_dir)
    
    # Check if we're in offline mode (docker-compose sets these)
    offline_mode = os.getenv('HF_HUB_OFFLINE') == '1' or os.getenv('TRANSFORMERS_OFFLINE') == '1'
    
    if offline_mode:
        logger.info("🔒 Running in OFFLINE mode - using existing models only")
    
    # 2. Embedding Model (BGE-M3)
    model_id = "BAAI/bge-m3"
    local_path = models_dir / "bge-m3"
    
    # Check if model exists locally
    model_exists = False
    if local_path.exists() and any(local_path.iterdir()):
        logger.info(f"✅ Embedding model found locally at {local_path}")
        model_exists = True
    
    if not model_exists and not offline_mode:
        logger.info(f"📥 Downloading embedding model: {model_id}...")
        try:
            from sentence_transformers import SentenceTransformer
            SentenceTransformer(model_id, cache_folder=str(hf_cache_dir))
            logger.info(f"✅ {model_id} downloaded successfully")
        except Exception as e:
            logger.error(f"❌ Failed to download {model_id}: {e}")
    elif not model_exists and offline_mode:
        logger.warning(f"⚠️ Model {model_id} not found locally and offline mode is enabled")

    # 3. Reranker Model (BGE-Reranker-Large)
    reranker_id = "BAAI/bge-reranker-large"
    reranker_local_path = models_dir / "bge-reranker-large"
    
    # Check if reranker exists locally
    reranker_exists = False
    if reranker_local_path.exists() and any(reranker_local_path.iterdir()):
        logger.info(f"✅ Reranker model found locally at {reranker_local_path}")
        reranker_exists = True
    
    if not reranker_exists and not offline_mode:
        logger.info(f"📥 Downloading reranker model: {reranker_id}...")
        try:
            from sentence_transformers import CrossEncoder
            CrossEncoder(reranker_id, cache_folder=str(hf_cache_dir))
            logger.info(f"✅ {reranker_id} downloaded successfully")
        except Exception as e:
            logger.error(f"❌ Failed to download {reranker_id}: {e}")
    elif not reranker_exists and offline_mode:
        logger.warning(f"⚠️ Model {reranker_id} not found locally and offline mode is enabled")

    # 4. PyThaiTTS (Vachana model)
    if not offline_mode:
        logger.info("📥 Initializing PyThaiTTS (Vachana)...")
        try:
            from pythaitts import TTS
            # This will download the vachana model if not present
            TTS(pretrained="vachana")
            logger.info("✅ PyThaiTTS models ready")
        except Exception as e:
            logger.error(f"❌ Failed to setup PyThaiTTS: {e}")
    else:
        # In offline mode, just check if PyThaiTTS can be imported
        logger.info("🔒 Checking PyThaiTTS availability in offline mode...")
        try:
            from pythaitts import TTS
            logger.info("✅ PyThaiTTS library available (models will be loaded at runtime)")
        except ImportError as e:
            logger.warning(f"⚠️ PyThaiTTS library not available: {e}")

if __name__ == "__main__":
    download_models()
    logger.info("🚀 All models processed!")