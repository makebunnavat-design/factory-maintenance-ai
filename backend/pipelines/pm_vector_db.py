import os
import sqlite3
import json
import faiss
import numpy as np
import logging
from typing import List, Optional
from sentence_transformers import SentenceTransformer

from core.config import PM2025_DB_PATH

logger = logging.getLogger("[PM_VECTOR]")

# Use a lightweight multilingual model or whatever is standard in your pipeline
# Use the local model requested by the user
MODEL_NAME = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "models", "bge-m3")


# Paths for persisting the FAISS index and task names
# Must use vector_index folder because docker-compose mounts it as a whole directory
VECTOR_INDEX_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "vector_index")
os.makedirs(VECTOR_INDEX_DIR, exist_ok=True)
FAISS_INDEX_PATH = os.path.join(VECTOR_INDEX_DIR, "pm_tasks.index")
TASK_NAMES_PATH = os.path.join(VECTOR_INDEX_DIR, "pm_tasks_meta.json")

# Global variables for caching
_model = None
_index = None
_task_names = []


def _get_model():
    # Lazy load the model to save memory if not used
    global _model
    if _model is None:
        logger.info(f"Loading SentenceTransformer model: {MODEL_NAME}")
        _model = SentenceTransformer(MODEL_NAME)
    return _model


def build_index():
    """
    ดึงรายชื่อ Task Name จาก PM2025.db แล้วสร้าง FAISS index เก็บไว้
    """
    logger.info("Building PM Vector Index...")
    if not PM2025_DB_PATH or not os.path.exists(PM2025_DB_PATH):
        logger.error(f"Cannot build index: DB not found at {PM2025_DB_PATH}")
        return False

    task_names = set()
    try:
        with sqlite3.connect(PM2025_DB_PATH) as conn:
            cursor = conn.cursor()
            # Fetch from PM table
            cursor.execute("SELECT DISTINCT \"Task Name\" FROM PM WHERE \"Task Name\" IS NOT NULL AND \"Task Name\" != ''")
            for row in cursor.fetchall():
                task_names.add(row[0])
            
            # Fetch from PMTest table just in case
            cursor.execute("SELECT DISTINCT \"Task Name\" FROM PMTest WHERE \"Task Name\" IS NOT NULL AND \"Task Name\" != ''")
            for row in cursor.fetchall():
                task_names.add(row[0])
                
    except Exception as e:
        logger.error(f"Error reading DB: {e}")
        return False

    task_list = list(task_names)
    if not task_list:
        logger.warning("No Task Names found in PM2025.db")
        return False

    # Get Embeddings
    model = _get_model()
    embeddings = model.encode(task_list, convert_to_numpy=True)
    embeddings = embeddings.astype('float32') # FAISS requires float32
    # Normalize for cosine similarity instead of raw L2 distance
    faiss.normalize_L2(embeddings)

    dimension = embeddings.shape[1]
    # Use Inner Product (IP) index for cosine similarity with normalized vectors
    index = faiss.IndexFlatIP(dimension)
    index.add(embeddings)

    # Save to disk
    faiss.write_index(index, FAISS_INDEX_PATH)
    with open(TASK_NAMES_PATH, "w", encoding="utf-8") as f:
        json.dump(task_list, f, ensure_ascii=False, indent=2)

    logger.info(f"Successfully built vector index with {len(task_list)} task names.")
    
    # Update global cache
    global _index, _task_names
    _index = index
    _task_names = task_list
    
    return True


def _load_index_if_needed():
    global _index, _task_names
    if _index is None or not _task_names:
        if os.path.exists(FAISS_INDEX_PATH) and os.path.exists(TASK_NAMES_PATH):
            _index = faiss.read_index(FAISS_INDEX_PATH)
            with open(TASK_NAMES_PATH, "r", encoding="utf-8") as f:
                _task_names = json.load(f)
        else:
            logger.info("Index not found. Building it now...")
            build_index()

def match_pm_task_name(keyword: str, top_k: int = 2, threshold: float = 0.40) -> List[str]:
    """
    Search for similar Task Names using FAISS Cosine Similarity (Inner Product).
    Returns exact matching Task Name strings found in PM2025.db
    """
    if not keyword or not keyword.strip():
        return []

    _load_index_if_needed()
    if _index is None or not _task_names:
        return []

    model = _get_model()
    query_vector = model.encode([keyword], convert_to_numpy=True).astype('float32')
    faiss.normalize_L2(query_vector)

    distances, indices = _index.search(query_vector, top_k)
    
    results = []
    # indices and distances are 2D arrays: [[idx1, idx2, ...]]
    for point_dist, point_idx in zip(distances[0], indices[0]):
        if point_idx != -1 and point_dist >= threshold:
            results.append(_task_names[point_idx])

    return results

if __name__ == "__main__":
    # Test script locally
    logging.basicConfig(level=logging.INFO)
    build_index()
    print("Testing 'ISPU':", match_pm_task_name("ISPU"))
    print("Testing 'LED A':", match_pm_task_name("LED A"))
    print("Testing 'MOPF1':", match_pm_task_name("MOPF1"))
