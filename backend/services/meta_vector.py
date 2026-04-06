import sqlite3
import faiss
import numpy as np
import os
import logging
import warnings
from typing import List, Dict, Optional
from pathlib import Path
from sentence_transformers import SentenceTransformer
from services.meta_database import META_DB_PATH

# Suppress ALL progress bars and warnings
os.environ['TRANSFORMERS_VERBOSITY'] = 'error'
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
os.environ['HF_HUB_DISABLE_PROGRESS_BARS'] = '1'
logging.getLogger('transformers').setLevel(logging.ERROR)
logging.getLogger('sentence_transformers').setLevel(logging.ERROR)
logging.getLogger('torch').setLevel(logging.ERROR)
warnings.filterwarnings('ignore')

# Models - Use /app/models in container
BASE_DIR = Path(__file__).parent.parent  # Go up to /app
MODELS_DIR = BASE_DIR / "models"
LOCAL_BGE_M3 = MODELS_DIR / "bge-m3"
DEFAULT_MODEL = str(LOCAL_BGE_M3) if LOCAL_BGE_M3.exists() else "BAAI/bge-m3"
DEFAULT_TOP_K = 3

logger = logging.getLogger("META_VECTOR")

class MetaVectorEngine:
    """
    Vector engine for Meta Database
    - Reads from Meta Database directly
    - Builds an in-memory FAISS index
    - Performs semantic search
    """
    def __init__(self, model_name: str = DEFAULT_MODEL):
        self.model_name = model_name
        self.model = None
        self.index = None
        self.metadata = []  # List of dicts {id, text, answer}
        
        self._load_model()
        self.reload_index()
        
    def _load_model(self):
        if self.model is None:
            logger.info(f"Loading embedding model: {self.model_name}...")
            
            # ใช้ shared BGE model แทนการโหลดใหม่
            try:
                from services.shared_models import get_shared_bge_model
                self.model = get_shared_bge_model()
                logger.info("Meta Vector Search using shared BGE model (CPU-only)")
            except ImportError:
                # Fallback: โหลดแบบเดิม
                import sys
                from io import StringIO
                old_stdout = sys.stdout
                old_stderr = sys.stderr
                sys.stdout = StringIO()
                sys.stderr = StringIO()
                
                try:
                    # Force CPU-only to avoid GPU conflict with Ollama
                    self.model = SentenceTransformer(self.model_name, device='cpu')
                    logger.info("Meta Vector Search using CPU-only mode (GPU reserved for Ollama)")
                finally:
                    sys.stdout = old_stdout
                    sys.stderr = old_stderr
            logger.info("Model loaded successfully")
            
    def reload_index(self):
        """
        Rebuilds the in-memory FAISS index from the database.
        Also saves/loads embeddings from database for persistence.
        """
        from services.meta_database import _DB_LOCK
        logger.info("Rebuilding Meta Database in-memory index...")
        if not os.path.exists(META_DB_PATH):
            logger.warning("Meta Database not found. Skipping index build.")
            self.index = None
            self.metadata = []
            return
            
        try:
            with _DB_LOCK:
                conn = sqlite3.connect(META_DB_PATH)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # ตรวจสอบว่ามีคอลัมน์และตารางอยู่หรือไม่ (เผื่อยังไม่ได้ init)
                try:
                    cursor.execute("SELECT id, topic, answer FROM meta_knowledge")
                    rows = cursor.fetchall()
                except sqlite3.OperationalError:
                    logger.warning("Table meta_knowledge not found yet.")
                    rows = []
                    conn.close()
                    return
            
            if not rows:
                logger.info("No knowledge found in Meta Database. Index is empty.")
                self.index = None
                self.metadata = []
                with _DB_LOCK:
                    conn.close()
                return
                
            texts = []
            self.metadata = []
            embeddings_list = []
            
            for row in rows:
                topic = row['topic']
                answer = row['answer']
                # ให้น้ำหนัก answer มากขึ้น เพื่อให้ค้นหาจาก answer ได้ดีขึ้น
                combined_text = f"Topic: {topic}\nAnswer: {answer} {answer}"
                texts.append(combined_text)
                self.metadata.append({
                    "id": row['id'],
                    "topic": topic,
                    "answer": answer,
                    "text": combined_text
                })
            
            # ลองโหลด embeddings จาก database ก่อน
            with _DB_LOCK:
                cursor.execute("SELECT id, embedding FROM meta_embeddings")
                stored_embeddings = {row[0]: row[1] for row in cursor.fetchall()}
                conn.close()
            
            # สร้าง embeddings (ใช้ที่มีอยู่ หรือสร้างใหม่)
            all_embeddings = []
            ids_to_save = []
            embeddings_to_save = []
            
            for i, meta in enumerate(self.metadata):
                meta_id = meta['id']
                if meta_id in stored_embeddings:
                    # โหลดจาก database
                    embedding = np.frombuffer(stored_embeddings[meta_id], dtype=np.float32)
                    all_embeddings.append(embedding)
                else:
                    # สร้างใหม่
                    embedding = self.model.encode([texts[i]], show_progress_bar=False)[0].astype(np.float32)
                    all_embeddings.append(embedding)
                    ids_to_save.append(meta_id)
                    embeddings_to_save.append(embedding)
            
            # บันทึก embeddings ใหม่ลง database
            if ids_to_save:
                with _DB_LOCK:
                    conn = sqlite3.connect(META_DB_PATH)
                    for meta_id, embedding in zip(ids_to_save, embeddings_to_save):
                        conn.execute(
                            "INSERT OR REPLACE INTO meta_embeddings (id, embedding) VALUES (?, ?)",
                            (meta_id, embedding.tobytes())
                        )
                    conn.commit()
                    conn.close()
                logger.info(f"Saved {len(ids_to_save)} new embeddings to database")
            
            embeddings = np.array(all_embeddings).astype(np.float32)
            
            dim = embeddings.shape[1]
            self.index = faiss.IndexFlatL2(dim)
            self.index.add(embeddings)
            
            logger.info(f"Index rebuilt with {len(self.metadata)} items.")
            
        except Exception as e:
            logger.error(f"Error rebuilding index: {e}", exc_info=True)
            self.index = None
            self.metadata = []
            
    def insert_and_reload(self, name: str, topic: str, answer: str) -> bool:
        """
        Helper to insert data and then reload the index
        """
        from services.meta_database import insert_meta_knowledge
        inserted_id = insert_meta_knowledge(name, topic, answer)
        if inserted_id != -1:
            self.reload_index()
            return True
        return False

    def encode_query(self, query: str) -> np.ndarray:
        if self.model is None:
            self._load_model()
        query_vector = self.model.encode([query], show_progress_bar=False)
        return query_vector.astype(np.float32)

    def search(self, query: str, top_k: int = DEFAULT_TOP_K) -> List[Dict]:
        if not self.index or len(self.metadata) == 0:
            return []
            
        query_vector = self.encode_query(query)
        distances, indices = self.index.search(query_vector, min(top_k, len(self.metadata)))
        
        results = []
        for rank, (idx, distance) in enumerate(zip(indices[0], distances[0]), start=1):
            if 0 <= idx < len(self.metadata):
                similarity = 1.0 / (1.0 + float(distance))
                
                # Log for debugging
                logger.debug(f"[SEARCH] Rank {rank}: topic='{self.metadata[idx]['topic']}' distance={distance:.4f} similarity={similarity:.4f}")
                
                results.append({
                    "rank": rank,
                    "id": self.metadata[idx]["id"],
                    "topic": self.metadata[idx]["topic"],
                    "answer": self.metadata[idx]["answer"],
                    "text": self.metadata[idx]["text"],
                    "distance": float(distance),
                    "similarity": similarity
                })
        return results

# Singleton mapping
_meta_engine_instance: Optional[MetaVectorEngine] = None

def get_meta_engine() -> MetaVectorEngine:
    global _meta_engine_instance
    if _meta_engine_instance is None:
        _meta_engine_instance = MetaVectorEngine()
    return _meta_engine_instance

def meta_vector_search(query: str, top_k: int = 3) -> Dict:
    """
    Helper function for Meta Mode vector search.
    
    Returns a dict with:
    - text: Formatted answer text (for backward compatibility)
    - data: List of matched results
    - row_count: Number of results
    - matches: Raw search results for LLM synthesis
    """
    try:
        engine = get_meta_engine()
        results = engine.search(query, top_k=top_k)
        
        # Log search results for debugging
        logger.info(f"[META_SEARCH] Query: '{query}' | Found: {len(results)} results")
        for r in results:
            logger.info(f"  → Rank {r['rank']}: '{r['topic']}' (similarity: {r['similarity']:.4f}, distance: {r['distance']:.4f})")
        
        # Filter by similarity threshold (ปรับได้ตามความเหมาะสม)
        # Similarity > 0.65 = ใกล้เคียงมาก (ปรับให้เข้มงวดขึ้น)
        # Similarity > 0.60 = ค่อนข้างใกล้เคียง
        MIN_SIMILARITY = 0.50
        filtered_results = [r for r in results if r['similarity'] >= MIN_SIMILARITY]
        
        # Fallback: ถ้าไม่เจอจาก vector search ให้ลอง fuzzy matching
        if not filtered_results and results:
            logger.info("[META_SEARCH] No results above threshold, trying fuzzy matching...")
            from thefuzz import fuzz
            
            fuzzy_results = []
            for r in results:
                # คำนวณ fuzzy score แยกแต่ละฟิลด์
                topic_score = fuzz.partial_ratio(query.lower(), r['topic'].lower())
                answer_score = fuzz.partial_ratio(query.lower(), r['answer'].lower())
                
                # ลอง token_sort_ratio ด้วย (ดีกับคำที่เรียงต่างกัน)
                topic_token_score = fuzz.token_sort_ratio(query.lower(), r['topic'].lower())
                answer_token_score = fuzz.token_sort_ratio(query.lower(), r['answer'].lower())
                
                # เอาคะแนนที่ดีที่สุด
                best_topic_score = max(topic_score, topic_token_score)
                best_answer_score = max(answer_score, answer_token_score)
                max_score = max(best_topic_score, best_answer_score)
                
                logger.info(f"  → Fuzzy: '{r['topic']}' topic_score={best_topic_score} answer_score={best_answer_score}")
                
                # ลด threshold เล็กน้อยสำหรับ answer matching
                threshold = 70 if best_answer_score > best_topic_score else 80
                if max_score >= threshold:
                    r['fuzzy_score'] = max_score
                    fuzzy_results.append(r)
            
            if fuzzy_results:
                logger.info(f"[META_SEARCH] Found {len(fuzzy_results)} results via fuzzy matching")
                filtered_results = sorted(fuzzy_results, key=lambda x: x.get('fuzzy_score', 0), reverse=True)
        
        if not filtered_results:
            # Final fallback: แสดงผลลัพธ์ที่ดีที่สุดแม้ไม่ผ่าน threshold
            if results:
                logger.info(f"[META_SEARCH] No results above threshold, showing best match anyway")
                best_result = results[0]  # ผลลัพธ์ที่ดีที่สุด
                best_result['is_fallback'] = True
                filtered_results = [best_result]
            else:
                logger.info(f"[META_SEARCH] No results found (vector + fuzzy)")
                return {
                    "text": "ไม่พบข้อมูลที่ตรงกับคำถาม",
                    "data": [],
                    "row_count": 0,
                    "matches": []
                }
        
        # Format text for display (backward compatibility)
        formatted_lines = []
        for r in filtered_results:
            formatted_lines.append(f"📌 {r['topic']}")
            formatted_lines.append(f"   {r['answer']}")
            formatted_lines.append("")
        
        # Filter out internal columns (text, distance, similarity) for frontend display
        display_data = []
        for r in filtered_results:
            display_data.append({
                "rank": r.get("rank"),
                "id": r.get("id"),
                "topic": r.get("topic"),
                "answer": r.get("answer")
            })
        
        return {
            "text": "\n".join(formatted_lines),
            "data": display_data,  # Only show: rank, id, topic, answer
            "row_count": len(filtered_results),
            "matches": filtered_results  # Full data for LLM synthesis
        }
        
    except Exception as e:
        logger.error(f"Meta vector search error: {e}")
        return {
            "text": f"เกิดข้อผิดพลาด: {e}",
            "data": [],
            "row_count": 0,
            "matches": []
        }
