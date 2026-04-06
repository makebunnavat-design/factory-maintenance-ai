"""
Vector Search Engine
====================
This module implements semantic search using FAISS and BGE-M3 embeddings.

Searches for similar repair logs based on semantic meaning rather than exact keyword matching.
"""

import faiss
import pickle
import numpy as np
import os
import logging
import warnings
from typing import List, Dict, Optional
from pathlib import Path

# Suppress ALL progress bars and warnings
os.environ['TRANSFORMERS_VERBOSITY'] = 'error'
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
os.environ['HF_HUB_DISABLE_PROGRESS_BARS'] = '1'
logging.getLogger('transformers').setLevel(logging.ERROR)
logging.getLogger('sentence_transformers').setLevel(logging.ERROR)
logging.getLogger('torch').setLevel(logging.ERROR)
warnings.filterwarnings('ignore')

from sentence_transformers import SentenceTransformer


# Paths - Use /app/vector_index in container
from core.config import BASE_DIR
VECTOR_INDEX_DIR = Path(BASE_DIR) / "vector_index"
INDEX_PATH = VECTOR_INDEX_DIR / "repair.index"
METADATA_PATH = VECTOR_INDEX_DIR / "metadata.pkl"

# Model paths - Use /app/models in container
BASE_DIR = Path(__file__).parent.parent  # Go up to /app
MODELS_DIR = BASE_DIR / "models"
LOCAL_BGE_M3 = MODELS_DIR / "bge-m3"
DEFAULT_MODEL = str(LOCAL_BGE_M3) if LOCAL_BGE_M3.exists() else "BAAI/bge-m3"
DEFAULT_TOP_K = 5


class VectorSearchEngine:
    """
    Vector search engine using FAISS and BGE-M3 embeddings
    
    Provides semantic search over repair logs using pre-built FAISS index.
    """
    
    def __init__(self, 
                 index_path: str = str(INDEX_PATH),
                 metadata_path: str = str(METADATA_PATH),
                 model_name: str = DEFAULT_MODEL):
        """
        Initialize vector search engine
        
        Args:
            index_path: Path to FAISS index file
            metadata_path: Path to metadata pickle file
            model_name: HuggingFace model name for embeddings
        """
        self.index_path = index_path
        self.metadata_path = metadata_path
        self.model_name = model_name
        
        self.model = None
        self.index = None
        self.metadata = None
        
        print(f"[VECTOR_SEARCH] Initializing search engine...")
        self._load_model()
        self._load_index()
        self._load_metadata()
        print(f"[VECTOR_SEARCH] Search engine ready!")
    
    def _load_model(self):
        """Load BGE-M3 embedding model (local first, fallback to HuggingFace)"""
        if self.model is None:
            print(f"[VECTOR_SEARCH] Loading embedding model: {self.model_name}...")
            
            # Check if using local model
            if os.path.exists(self.model_name):
                print(f"[VECTOR_SEARCH] Using local model from: {self.model_name}")
            else:
                print(f"[VECTOR_SEARCH] Downloading from HuggingFace: {self.model_name}")
                print(f"[VECTOR_SEARCH] ⚠️  For offline use, run: python download_models.py")
            
            # Suppress progress bars during model loading
            import sys
            from io import StringIO
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = StringIO()
            sys.stderr = StringIO()
            
            self.model = SentenceTransformer(self.model_name)
            
            # Restore stdout/stderr
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            
            print(f"[VECTOR_SEARCH] Model loaded successfully")
    
    def _load_index(self):
        """Load FAISS index from disk"""
        if not os.path.exists(self.index_path):
            raise FileNotFoundError(
                f"FAISS index not found: {self.index_path}\n"
                f"Please run: python build_vector_index.py"
            )
        
        print(f"[VECTOR_SEARCH] Loading FAISS index from: {self.index_path}")
        self.index = faiss.read_index(self.index_path)
        print(f"[VECTOR_SEARCH] Index loaded: {self.index.ntotal} vectors, dimension {self.index.d}")
    
    def _load_metadata(self):
        """Load metadata from disk"""
        if not os.path.exists(self.metadata_path):
            raise FileNotFoundError(
                f"Metadata not found: {self.metadata_path}\n"
                f"Please run: python build_vector_index.py"
            )
        
        print(f"[VECTOR_SEARCH] Loading metadata from: {self.metadata_path}")
        with open(self.metadata_path, "rb") as f:
            self.metadata = pickle.load(f)
        print(f"[VECTOR_SEARCH] Metadata loaded: {len(self.metadata)} entries")
    
    def encode_query(self, query: str) -> np.ndarray:
        """
        Convert query text to embedding vector
        
        Args:
            query: Query text
            
        Returns:
            Embedding vector as numpy array (1, dimension)
        """
        if self.model is None:
            self._load_model()
        
        # Encode query to vector
        query_vector = self.model.encode([query], show_progress_bar=False)
        
        # Convert to float32 (FAISS requirement)
        query_vector = query_vector.astype(np.float32)
        
        return query_vector
    
    def search(self, query: str, top_k: int = DEFAULT_TOP_K) -> List[Dict]:
        """
        Search for similar repair logs using semantic similarity
        
        Args:
            query: Search query text
            top_k: Number of results to return
            
        Returns:
            List of dicts with keys: id, text, distance, rank
        """
        print(f"[VECTOR_SEARCH] Vector query received: '{query}' (top_k={top_k})")
        
        # Convert query to embedding
        query_vector = self.encode_query(query)
        
        # Perform similarity search
        # D = distances (L2), I = indices
        distances, indices = self.index.search(query_vector, top_k)
        
        # Retrieve matching repair logs
        results = []
        for rank, (idx, distance) in enumerate(zip(indices[0], distances[0]), start=1):
            if idx < len(self.metadata):
                result = {
                    "rank": rank,
                    "id": self.metadata[idx]["id"],
                    "text": self.metadata[idx]["text"],
                    "distance": float(distance),
                    "similarity": self._distance_to_similarity(float(distance))
                }
                results.append(result)
        
        print(f"[VECTOR_SEARCH] Top {len(results)} results returned")
        
        return results
    
    def search_vectors(self, query: str, k: int = DEFAULT_TOP_K) -> List[str]:
        """
        Simple search function that returns only text results
        
        Args:
            query: Search query text
            k: Number of results to return
            
        Returns:
            List of repair log texts
        """
        results = self.search(query, top_k=k)
        return [result["text"] for result in results]
    
    def _distance_to_similarity(self, distance: float) -> float:
        """
        Convert L2 distance to similarity score (0-1)
        
        Lower distance = higher similarity
        
        Args:
            distance: L2 distance from FAISS
            
        Returns:
            Similarity score (0-1, higher is more similar)
        """
        # Simple conversion: similarity = 1 / (1 + distance)
        # This maps distance [0, inf] to similarity [1, 0]
        return 1.0 / (1.0 + distance)
    
    def search_with_threshold(self, 
                             query: str, 
                             top_k: int = DEFAULT_TOP_K,
                             min_similarity: float = 0.5) -> List[Dict]:
        """
        Search with similarity threshold filtering
        
        Args:
            query: Search query text
            top_k: Number of results to return
            min_similarity: Minimum similarity score (0-1)
            
        Returns:
            List of results with similarity >= min_similarity
        """
        results = self.search(query, top_k=top_k)
        
        # Filter by similarity threshold
        filtered_results = [
            result for result in results 
            if result["similarity"] >= min_similarity
        ]
        
        print(f"[VECTOR_SEARCH] Filtered {len(filtered_results)}/{len(results)} results (min_similarity={min_similarity})")
        
        return filtered_results
    
    def batch_search(self, queries: List[str], top_k: int = DEFAULT_TOP_K) -> List[List[Dict]]:
        """
        Search multiple queries at once
        
        Args:
            queries: List of query texts
            top_k: Number of results per query
            
        Returns:
            List of result lists (one per query)
        """
        print(f"[VECTOR_SEARCH] Batch search: {len(queries)} queries")
        
        # Encode all queries
        query_vectors = self.model.encode(queries, show_progress_bar=False)
        query_vectors = query_vectors.astype(np.float32)
        
        # Perform batch search
        distances, indices = self.index.search(query_vectors, top_k)
        
        # Process results for each query
        all_results = []
        for query_idx in range(len(queries)):
            results = []
            for rank, (idx, distance) in enumerate(zip(indices[query_idx], distances[query_idx]), start=1):
                if idx < len(self.metadata):
                    result = {
                        "rank": rank,
                        "id": self.metadata[idx]["id"],
                        "text": self.metadata[idx]["text"],
                        "distance": float(distance),
                        "similarity": self._distance_to_similarity(float(distance))
                    }
                    results.append(result)
            all_results.append(results)
        
        print(f"[VECTOR_SEARCH] Batch search complete")
        
        return all_results


# Singleton instance (lazy initialization)
_search_engine_instance: Optional[VectorSearchEngine] = None


def get_search_engine() -> VectorSearchEngine:
    """
    Get or create singleton search engine instance
    
    Returns:
        VectorSearchEngine instance
    """
    global _search_engine_instance
    if _search_engine_instance is None:
        _search_engine_instance = VectorSearchEngine()
    return _search_engine_instance


def search_vectors(query: str, k: int = DEFAULT_TOP_K) -> List[str]:
    """
    Simple search function for backward compatibility
    
    Args:
        query: Search query text
        k: Number of results to return
        
    Returns:
        List of repair log texts
    """
    engine = get_search_engine()
    return engine.search_vectors(query, k=k)


if __name__ == "__main__":
    """
    Test vector search engine
    """
    print("=" * 60)
    print("Vector Search Engine Test")
    print("=" * 60)
    
    # Test queries
    test_queries = [
        "เครื่อง CNC เสียบ่อยเพราะอะไร",
        "motor overheating",
        "bearing แตก",
        "sensor calibration error",
        "ปัญหา conveyor belt"
    ]
    
    # Initialize search engine
    try:
        engine = VectorSearchEngine()
        
        # Test each query
        for query in test_queries:
            print(f"\n{'=' * 60}")
            print(f"Query: {query}")
            print("=" * 60)
            
            results = engine.search(query, top_k=5)
            
            for result in results:
                print(f"\n[Rank {result['rank']}] (Similarity: {result['similarity']:.3f}, Distance: {result['distance']:.3f})")
                print(f"ID: {result['id']}")
                text_preview = result['text'][:100] + "..." if len(result['text']) > 100 else result['text']
                print(f"Text: {text_preview}")
        
        # Test simple function
        print(f"\n{'=' * 60}")
        print("Testing simple search_vectors() function")
        print("=" * 60)
        
        results = search_vectors("เครื่อง CNC เสียบ่อยเพราะอะไร", k=3)
        for i, text in enumerate(results, 1):
            text_preview = text[:80] + "..." if len(text) > 80 else text
            print(f"{i}. {text_preview}")
        
        print(f"\n{'=' * 60}")
        print("✓ Vector search engine test complete!")
        print("=" * 60)
        
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        print("\nPlease run the following commands first:")
        print("1. docker exec -it repair-chatbot-backend python embeddings.py")
        print("2. docker exec -it repair-chatbot-backend python build_vector_index.py")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
