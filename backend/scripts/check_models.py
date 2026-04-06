#!/usr/bin/env python3
"""
Check if models exist locally before attempting download.
"""
import os
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_models():
    base_dir = Path("/app") if os.path.exists("/app") else Path(".").absolute()
    models_dir = base_dir / "models"
    
    logger.info(f"Checking models in: {models_dir}")
    
    if not models_dir.exists():
        logger.warning(f"Models directory does not exist: {models_dir}")
        return
    
    # List all contents
    for item in models_dir.rglob("*"):
        if item.is_file():
            size_mb = item.stat().st_size / (1024 * 1024)
            logger.info(f"Found: {item.relative_to(models_dir)} ({size_mb:.1f} MB)")
    
    # Check specific model directories
    bge_m3_path = models_dir / "bge-m3"
    bge_reranker_path = models_dir / "bge-reranker-large"
    
    if bge_m3_path.exists():
        files = list(bge_m3_path.iterdir())
        logger.info(f"BGE-M3 model: {len(files)} files found")
    else:
        logger.warning("BGE-M3 model directory not found")
    
    if bge_reranker_path.exists():
        files = list(bge_reranker_path.iterdir())
        logger.info(f"BGE-Reranker model: {len(files)} files found")
    else:
        logger.warning("BGE-Reranker model directory not found")

if __name__ == "__main__":
    check_models()