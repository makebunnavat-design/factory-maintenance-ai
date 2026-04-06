"""
Embeddings Module for Vector Search
====================================
This module handles text embedding generation for vector search.

Uses BGE-M3 model for multilingual embeddings (supports Thai).
Generates embeddings from repair logs and stores them in SQLite.
"""

from typing import List, Optional, Tuple
import numpy as np
import sqlite3
import json
import os
import logging
import warnings
from pathlib import Path

from core.config import WORK_DB_PATH, BASE_DIR

# Suppress ALL progress bars and warnings
os.environ['TRANSFORMERS_VERBOSITY'] = 'error'
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
os.environ['HF_HUB_DISABLE_PROGRESS_BARS'] = '1'
logging.getLogger('transformers').setLevel(logging.ERROR)
logging.getLogger('sentence_transformers').setLevel(logging.ERROR)
logging.getLogger('torch').setLevel(logging.ERROR)
warnings.filterwarnings('ignore')

from sentence_transformers import SentenceTransformer


# Database path
DB_PATH = WORK_DB_PATH

# Model paths (local first, fallback to HuggingFace)
MODELS_DIR = Path(BASE_DIR) / "models"
LOCAL_BGE_M3 = MODELS_DIR / "bge-m3"
# Prefer specific local directory, then fall back to ID (which HF will find in cache)
DEFAULT_MODEL = str(LOCAL_BGE_M3) if LOCAL_BGE_M3.exists() else "BAAI/bge-m3"
BATCH_SIZE = 64
PROGRESS_INTERVAL = 500


class EmbeddingModel:
    """
    Wrapper for BGE-M3 embedding model
    
    Supports Thai language and generates high-quality embeddings
    """
    
    def __init__(self, model_name: str = DEFAULT_MODEL):
        """
        Initialize embedding model
        
        Args:
            model_name: HuggingFace model name for embeddings
        """
        self.model_name = model_name
        self.model = None
        print(f"[EMBEDDINGS] Initializing model: {model_name}")
    
    def load_model(self):
        """Load the embedding model (local first, fallback to HuggingFace)"""
        if self.model is None:
            print(f"[EMBEDDINGS] Loading model: {self.model_name}...")
            
            # Check if using local model
            if os.path.exists(self.model_name):
                print(f"[EMBEDDINGS] Using local model from: {self.model_name}")
            else:
                print(f"[EMBEDDINGS] Downloading from HuggingFace: {self.model_name}")
                print(f"[EMBEDDINGS] ⚠️  For offline use, run: python download_models.py")
            
            # Suppress progress bars during model loading
            import sys
            from io import StringIO
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = StringIO()
            sys.stderr = StringIO()
            
            # Force CPU usage - ให้ GPU สำหรับ Ollama เท่านั้น
            self.model = SentenceTransformer(self.model_name, device='cpu')
            
            # Restore stdout/stderr
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            
            print(f"[EMBEDDINGS] Model loaded successfully on CPU")
    
    def encode(self, texts: List[str], batch_size: int = BATCH_SIZE) -> np.ndarray:
        """
        Convert texts to embeddings
        
        Args:
            texts: List of text strings to encode
            batch_size: Batch size for encoding
            
        Returns:
            numpy array of shape (len(texts), embedding_dim)
        """
        if self.model is None:
            self.load_model()
        
        embeddings = self.model.encode(texts, batch_size=batch_size, show_progress_bar=False)
        return embeddings
    
    def encode_single(self, text: str) -> np.ndarray:
        """
        Encode a single text string
        
        Args:
            text: Text string to encode
            
        Returns:
            numpy array of shape (embedding_dim,)
        """
        return self.encode([text])[0]


# Singleton instance (will be initialized on first use)
_embedding_model_instance: Optional[EmbeddingModel] = None


def get_embedding_model() -> EmbeddingModel:
    """
    Get or create singleton embedding model instance (lazy loading)
    
    Returns:
        EmbeddingModel instance
    """
    global _embedding_model_instance
    if _embedding_model_instance is None:
        _embedding_model_instance = EmbeddingModel()
        # ไม่ load model ทันที - รอจนกว่าจะใช้จริง
    return _embedding_model_instance


def combine_repair_text(problem: str, cause: str, solution: str, note: str) -> str:
    """
    Combine repair log fields into single text
    
    Args:
        problem: ปัญหา
        cause: สาเหตุ
        solution: การแก้ไข
        note: บันทึกเพิ่มเติม
        
    Returns:
        Combined text string
    """
    # Handle None values
    problem = str(problem).strip() if problem else ""
    cause = str(cause).strip() if cause else ""
    solution = str(solution).strip() if solution else ""
    note = str(note).strip() if note else ""
    
    # Combine with space separator
    combined = f"{problem} {cause} {solution} {note}".strip()
    
    return combined if combined else "ไม่มีข้อมูล"


def create_embedding_table(conn: sqlite3.Connection):
    """
    Create repair_notes_embeddings table if not exists
    
    Args:
        conn: SQLite connection
    """
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS repair_notes_embeddings (
            id INTEGER PRIMARY KEY,
            note TEXT NOT NULL,
            embedding TEXT NOT NULL
        )
    """)
    
    conn.commit()
    print("[EMBEDDINGS] Table 'repair_notes_embeddings' created/verified")


def check_existing_embeddings(conn: sqlite3.Connection) -> set:
    """
    Get set of IDs that already have embeddings
    
    Args:
        conn: SQLite connection
        
    Returns:
        Set of existing IDs
    """
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT id FROM repair_notes_embeddings")
        existing_ids = {row[0] for row in cursor.fetchall()}
        print(f"[EMBEDDINGS] Found {len(existing_ids)} existing embeddings")
        return existing_ids
    except sqlite3.OperationalError:
        # Table doesn't exist yet
        return set()


def load_repair_logs(conn: sqlite3.Connection) -> List[Tuple[int, str, str, str, str]]:
    """
    Load repair logs from database
    
    Args:
        conn: SQLite connection
        
    Returns:
        List of tuples (rowid, problem, cause, solution, note)
    """
    cursor = conn.cursor()
    
    query = """
        SELECT rowid, "ปัญหา", "สาเหตุ", "การแก้ไข", "บันทึกเพิ่มเติม"
        FROM repairs_enriched
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    print(f"[EMBEDDINGS] Loaded {len(rows)} repair logs")
    return rows


def insert_embeddings_batch(conn: sqlite3.Connection, batch_data: List[Tuple[int, str, str]]):
    """
    Insert batch of embeddings into database
    
    Args:
        conn: SQLite connection
        batch_data: List of tuples (id, note, embedding_json)
    """
    cursor = conn.cursor()
    
    cursor.executemany("""
        INSERT OR REPLACE INTO repair_notes_embeddings (id, note, embedding)
        VALUES (?, ?, ?)
    """, batch_data)
    
    conn.commit()


def build_repair_embeddings(db_path: str = DB_PATH, force_rebuild: bool = False):
    """
    Build embeddings for all repair logs and store in database
    
    Args:
        db_path: Path to SQLite database
        force_rebuild: If True, rebuild all embeddings (ignore existing)
    """
    print("=" * 60)
    print("Building Repair Log Embeddings")
    print("=" * 60)
    
    # Check if database exists
    if not os.path.exists(db_path):
        print(f"[ERROR] Database not found: {db_path}")
        return
    
    # Connect to database
    print(f"[EMBEDDINGS] Connecting to database: {db_path}")
    conn = sqlite3.connect(db_path)
    
    try:
        # Create embedding table
        create_embedding_table(conn)
        
        # Check existing embeddings
        existing_ids = set() if force_rebuild else check_existing_embeddings(conn)
        
        # Load repair logs
        repair_logs = load_repair_logs(conn)
        
        if not repair_logs:
            print("[EMBEDDINGS] No repair logs found")
            return
        
        # Filter out existing embeddings
        if not force_rebuild:
            repair_logs = [(rowid, p, c, s, n) for rowid, p, c, s, n in repair_logs 
                          if rowid not in existing_ids]
            print(f"[EMBEDDINGS] {len(repair_logs)} new logs to process")
        
        if not repair_logs:
            print("[EMBEDDINGS] All embeddings are up to date")
            return
        
        # Get embedding model
        model = get_embedding_model()
        
        # Process in batches
        total_processed = 0
        batch_texts = []
        batch_ids = []
        batch_notes = []
        
        for rowid, problem, cause, solution, note in repair_logs:
            # Combine text
            combined_text = combine_repair_text(problem, cause, solution, note)
            
            batch_ids.append(rowid)
            batch_notes.append(combined_text)
            batch_texts.append(combined_text)
            
            # Process batch when full
            if len(batch_texts) >= BATCH_SIZE:
                # Generate embeddings
                embeddings = model.encode(batch_texts, batch_size=BATCH_SIZE)
                
                # Prepare data for insertion
                insert_data = [
                    (batch_ids[i], batch_notes[i], json.dumps(embeddings[i].tolist()))
                    for i in range(len(batch_ids))
                ]
                
                # Insert into database
                insert_embeddings_batch(conn, insert_data)
                
                total_processed += len(batch_texts)
                
                # Progress logging
                if total_processed % PROGRESS_INTERVAL == 0:
                    print(f"[EMBEDDINGS] Processed {total_processed} rows")
                
                # Clear batch
                batch_texts = []
                batch_ids = []
                batch_notes = []
        
        # Process remaining batch
        if batch_texts:
            embeddings = model.encode(batch_texts, batch_size=len(batch_texts))
            
            insert_data = [
                (batch_ids[i], batch_notes[i], json.dumps(embeddings[i].tolist()))
                for i in range(len(batch_ids))
            ]
            
            insert_embeddings_batch(conn, insert_data)
            total_processed += len(batch_texts)
        
        print(f"[EMBEDDINGS] ✓ Completed! Processed {total_processed} rows")
        
        # Show statistics
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM repair_notes_embeddings")
        total_embeddings = cursor.fetchone()[0]
        print(f"[EMBEDDINGS] Total embeddings in database: {total_embeddings}")
        
    finally:
        conn.close()
        print("[EMBEDDINGS] Database connection closed")


def get_embedding_for_query(query: str) -> np.ndarray:
    """
    Get embedding for a query string
    
    Args:
        query: Query text
        
    Returns:
        Embedding vector
    """
    model = get_embedding_model()
    return model.encode_single(query)


def auto_generate_embeddings_if_needed(db_path: str = DB_PATH, threshold: int = 500) -> bool:
    """
    Automatically generate embeddings if there are enough new rows.
    
    This function checks if there are new repair logs without embeddings.
    If the count exceeds the threshold, it generates embeddings for them.
    
    Args:
        db_path: Path to SQLite database
        threshold: Minimum number of new rows to trigger embedding generation (default: 500)
        
    Returns:
        True if embeddings were generated, False otherwise
    """
    from utils.log_throttle import throttled_embed_log
    
    if not os.path.exists(db_path):
        throttled_embed_log(f"[AUTO_EMBED] Database not found: {db_path}")
        return False
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Create embedding table if not exists
        create_embedding_table(conn)
        
        # Get existing embeddings
        existing_ids = check_existing_embeddings(conn)
        
        # Get total repair logs
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM repairs_enriched")
        total_logs = cursor.fetchone()[0]
        
        # Calculate new rows
        new_rows_count = total_logs - len(existing_ids)
        
        throttled_embed_log(f"[AUTO_EMBED] Total logs: {total_logs}, Existing embeddings: {len(existing_ids)}, New rows: {new_rows_count}")
        
        # Check if threshold is met
        if new_rows_count >= threshold:
            throttled_embed_log(f"[AUTO_EMBED] ✓ Threshold met ({new_rows_count} >= {threshold}), generating embeddings...", force=True)
            conn.close()
            
            # Generate embeddings for new rows only
            build_repair_embeddings(db_path=db_path, force_rebuild=False)
            
            # Rebuild FAISS index
            throttled_embed_log(f"[AUTO_EMBED] Rebuilding FAISS index...", force=True)
            try:
                from services.vector_index_service import build_index
                build_index(db_path=db_path, verify=False)
                throttled_embed_log(f"[AUTO_EMBED] ✓ FAISS index rebuilt successfully", force=True)
            except Exception as e:
                throttled_embed_log(f"[AUTO_EMBED] ⚠️  Failed to rebuild FAISS index: {e}", force=True)
                throttled_embed_log(f"[AUTO_EMBED] Please run: python services/vector_index_service.py", force=True)
            
            return True
        else:
            throttled_embed_log(f"[AUTO_EMBED] Threshold not met ({new_rows_count} < {threshold}), skipping")
            return False
            
    except Exception as e:
        throttled_embed_log(f"[AUTO_EMBED] Error: {e}", force=True)
        return False
    finally:
        if conn:
            conn.close()


def check_embedding_status(db_path: str = DB_PATH) -> dict:
    """
    Check embedding status and return statistics.
    
    Args:
        db_path: Path to SQLite database
        
    Returns:
        Dictionary with status information
    """
    if not os.path.exists(db_path):
        return {
            "status": "error",
            "message": "Database not found",
            "total_logs": 0,
            "total_embeddings": 0,
            "new_rows": 0
        }
    
    conn = sqlite3.connect(db_path)
    
    try:
        cursor = conn.cursor()
        
        # Get total repair logs
        cursor.execute("SELECT COUNT(*) FROM repairs_enriched")
        total_logs = cursor.fetchone()[0]
        
        # Get total embeddings
        try:
            cursor.execute("SELECT COUNT(*) FROM repair_notes_embeddings")
            total_embeddings = cursor.fetchone()[0]
        except sqlite3.OperationalError:
            total_embeddings = 0
        
        new_rows = total_logs - total_embeddings
        
        return {
            "status": "ok",
            "total_logs": total_logs,
            "total_embeddings": total_embeddings,
            "new_rows": new_rows,
            "needs_update": new_rows > 0
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "total_logs": 0,
            "total_embeddings": 0,
            "new_rows": 0
        }
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    
    # Check command line arguments
    force_rebuild = "--force" in sys.argv or "-f" in sys.argv
    
    if force_rebuild:
        print("[EMBEDDINGS] Force rebuild mode enabled")
    
    # Build embeddings
    build_repair_embeddings(force_rebuild=force_rebuild)
    
    print("\n" + "=" * 60)
    print("Embedding generation complete!")
    print("=" * 60)
