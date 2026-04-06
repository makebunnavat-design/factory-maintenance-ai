import sqlite3
import faiss
import numpy as np
import os
import logging
import json
from typing import List, Dict, Tuple, Optional
from pathlib import Path
from sentence_transformers import SentenceTransformer
from core.config import WORK_DB_PATH, BASE_DIR

logger = logging.getLogger("ENTITY_MATCHING")

# Models - Use /app/models in container
MODELS_DIR = Path(BASE_DIR) / "models"
LOCAL_BGE_M3 = MODELS_DIR / "bge-m3"
DEFAULT_MODEL = str(LOCAL_BGE_M3) if LOCAL_BGE_M3.exists() else "BAAI/bge-m3"

class EntityMatchingEngine:
    """
    Engine for matching Line and Process names using embeddings.
    """
    def __init__(self, model_name: str = DEFAULT_MODEL):
        self.model_name = model_name
        self.model = None
        self.index = None
        self.metadata = []  # List of dicts {id, type, value, display}
        
        self._load_model()
        self.reload_index()
        
    def _load_model(self):
        if self.model is None:
            logger.info(f"Loading embedding model: {self.model_name}...")
            # Suppress stdout/stderr
            import sys
            from io import StringIO
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = StringIO()
            sys.stderr = StringIO()
            
            try:
                self.model = SentenceTransformer(self.model_name)
            finally:
                sys.stdout = old_stdout
                sys.stderr = old_stderr
            logger.info("Model loaded successfully")
            
    def _setup_db(self):
        """Creates the entity_embeddings table if it doesn't exist."""
        try:
            conn = sqlite3.connect(WORK_DB_PATH)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS entity_embeddings (
                    entity_type TEXT,
                    entity_value TEXT,
                    embedding BLOB,
                    PRIMARY KEY (entity_type, entity_value)
                )
            ''')
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error setting up entity_embeddings table: {e}")

    def reload_index(self):
        """
        Builds/Updates the in-memory FAISS index.
        Uses SQLite to cache embeddings and avoid redundant AI calls.
        """
        if self.model is None:
            logger.error("Cannot reload index: Model not loaded.")
            return

        logger.info("Syncing Entity Matching index with SQLite...")
        self._setup_db()
        
        if not os.path.exists(WORK_DB_PATH):
            return
            
        try:
            conn = sqlite3.connect(WORK_DB_PATH)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # 1. Get current unique entities from source
            cursor.execute('SELECT DISTINCT Line FROM repairs_enriched WHERE Line IS NOT NULL AND Line != ""')
            source_lines = {row['Line'] for row in cursor.fetchall()}
            
            cursor.execute('SELECT DISTINCT Process FROM repairs_enriched WHERE Process IS NOT NULL AND Process != ""')
            source_procs = {row['Process'] for row in cursor.fetchall()}
            
            # 2. Get existing embeddings from cache
            cursor.execute('SELECT entity_type, entity_value, embedding FROM entity_embeddings')
            cache = {}
            for row in cursor.fetchall():
                cache[(row['entity_type'], row['entity_value'])] = np.frombuffer(row['embedding'], dtype=np.float32)
            
            # 3. Identify missing entities
            missing_metadata = []
            missing_texts = []
            
            current_metadata = []
            all_embeddings = []
            
            for line in source_lines:
                key = ("Line", line)
                if key in cache:
                    current_metadata.append({"type": "Line", "value": line})
                    all_embeddings.append(cache[key])
                else:
                    missing_metadata.append({"type": "Line", "value": line})
                    missing_texts.append(f"Line: {line}")
            
            for proc in source_procs:
                key = ("Process", proc)
                if key in cache:
                    current_metadata.append({"type": "Process", "value": proc})
                    all_embeddings.append(cache[key])
                else:
                    missing_metadata.append({"type": "Process", "value": proc})
                    missing_texts.append(f"Process: {proc}")
            
            # 4. Embed missing entities and save to cache
            if missing_texts:
                logger.info(f"Embedding {len(missing_texts)} new entities...")
                new_embeddings = self.model.encode(missing_texts, show_progress_bar=False).astype(np.float32)
                
                for meta, emb in zip(missing_metadata, new_embeddings):
                    cursor.execute(
                        'INSERT OR REPLACE INTO entity_embeddings (entity_type, entity_value, embedding) VALUES (?, ?, ?)',
                        (meta['type'], meta['value'], emb.tobytes())
                    )
                    current_metadata.append(meta)
                    all_embeddings.append(emb)
                conn.commit()
            
            conn.close()
            
            if not all_embeddings:
                logger.warning("No entities found to index.")
                return
                
            self.metadata = current_metadata
            embeddings_np = np.stack(all_embeddings)
            
            dim = embeddings_np.shape[1]
            self.index = faiss.IndexFlatL2(dim)
            self.index.add(embeddings_np)
            
            logger.info(f"Entity index updated: {len(self.metadata)} items total.")
            
        except Exception as e:
            logger.error(f"Error rebuilding entity index: {e}")

    def sync_if_needed(self, force: bool = False):
        """
        Reloads index if there are new unique entities.
        """
        # Incremental reload is cheap, so we just call it.
        # More advanced: check if unique count in DB > unique count in metadata
        self.reload_index()

    def search(self, query: str, top_k: int = 5, threshold: float = 0.45) -> List[Dict]:
        """
        Searches for the best matching entities.
        Threshold lowered slightly to 0.45 to catch similar names.
        """
        if self.index is None or not self.metadata or self.model is None:
            return []
            
        results = []
        
        # 1. First, check for exact matches (case-insensitive) to ensure 1.0 similarity
        query_lower = query.lower()
        exact_indices = []
        for i, meta in enumerate(self.metadata):
            if meta.get("value", "").lower() == query_lower:
                res = meta.copy()
                res["similarity"] = 1.0
                results.append(res)
                exact_indices.append(i)
                if len(results) >= top_k:
                    return results
                
        # 2. Perform vector search for semantic similarity
        query_vector = self.model.encode([query], show_progress_bar=False).astype(np.float32)
        distances, indices = self.index.search(query_vector, min(top_k, len(self.metadata)))
        
        for idx, distance in zip(indices[0], distances[0]):
            if 0 <= idx < len(self.metadata) and idx not in exact_indices:
                similarity = 1.0 / (1.0 + float(distance))
                if similarity >= threshold:
                    res = self.metadata[idx].copy()
                    res["similarity"] = similarity
                    results.append(res)
                    if len(results) >= top_k:
                        break
                    
        return results

# Singleton instance
_entity_engine_instance: Optional[EntityMatchingEngine] = None

def get_entity_engine() -> EntityMatchingEngine:
    global _entity_engine_instance
    if _entity_engine_instance is None:
        _entity_engine_instance = EntityMatchingEngine()
    return _entity_engine_instance

def match_entities(query: str, top_k: int = 3) -> List[Dict]:
    """
    Helper function to match entities from a query string.
    """
    try:
        engine = get_entity_engine()
        return engine.search(query, top_k=top_k)
    except Exception as e:
        logger.error(f"Entity matching error: {e}")
        return []
