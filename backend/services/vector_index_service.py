"""
Build FAISS Vector Index
=========================
This module builds a FAISS vector index from repair log embeddings stored in SQLite.

The index enables fast similarity search for repair logs based on semantic meaning.
"""

import sqlite3
import json
import numpy as np
import faiss
import pickle
import os
import logging
import warnings
from typing import List, Dict, Tuple

# Suppress ALL progress bars and warnings
os.environ['TRANSFORMERS_VERBOSITY'] = 'error'
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
os.environ['HF_HUB_DISABLE_PROGRESS_BARS'] = '1'
logging.getLogger('transformers').setLevel(logging.ERROR)
logging.getLogger('sentence_transformers').setLevel(logging.ERROR)
logging.getLogger('torch').setLevel(logging.ERROR)
warnings.filterwarnings('ignore')


from core.config import WORK_DB_PATH, BASE_DIR

# Paths
DB_PATH = WORK_DB_PATH
INDEX_DIR = os.path.join(BASE_DIR, "vector_index")
INDEX_PATH = os.path.join(INDEX_DIR, "repair.index")
METADATA_PATH = os.path.join(INDEX_DIR, "metadata.pkl")


def load_embeddings_from_db(db_path: str = DB_PATH) -> Tuple[List[np.ndarray], List[Dict]]:
    """
    Load embeddings and metadata from SQLite database
    
    Args:
        db_path: Path to SQLite database
        
    Returns:
        Tuple of (vectors, metadata)
        - vectors: List of numpy arrays
        - metadata: List of dicts with id and text
    """
    from utils.log_throttle import throttled_index_log
    
    throttled_index_log(f"[INDEX] Loading embeddings from database: {db_path}")
    
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Query embeddings
    query = """
        SELECT id, note, embedding
        FROM repair_notes_embeddings
        ORDER BY id
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    if not rows:
        raise ValueError("No embeddings found in database. Run embeddings.py first.")
    
    throttled_index_log(f"[INDEX] Found {len(rows)} embeddings")
    
    # Parse embeddings and metadata
    vectors = []
    metadata = []
    
    for row_id, note, embedding_json in rows:
        # Convert JSON string to numpy array
        try:
            vector = np.array(json.loads(embedding_json), dtype=np.float32)
            vectors.append(vector)
            
            metadata.append({
                "id": row_id,
                "text": note
            })
        except (json.JSONDecodeError, ValueError) as e:
            throttled_index_log(f"[WARNING] Failed to parse embedding for id={row_id}: {e}")
            continue
    
    conn.close()
    
    throttled_index_log(f"[INDEX] Successfully loaded {len(vectors)} vectors")
    
    return vectors, metadata


def create_faiss_index(vectors: List[np.ndarray]) -> faiss.Index:
    """
    Create FAISS index from vectors
    
    Args:
        vectors: List of numpy arrays
        
    Returns:
        FAISS index
    """
    from utils.log_throttle import throttled_index_log
    
    # Convert list to numpy matrix
    vectors_matrix = np.array(vectors, dtype=np.float32)
    
    throttled_index_log(f"[INDEX] Vector matrix shape: {vectors_matrix.shape}")
    
    # Get dimension
    dimension = vectors_matrix.shape[1]
    throttled_index_log(f"[INDEX] Vector dimension: {dimension}")
    
    # Create FAISS index (L2 distance)
    index = faiss.IndexFlatL2(dimension)
    
    throttled_index_log(f"[INDEX] Created FAISS IndexFlatL2 with dimension {dimension}")
    
    # Add vectors to index
    index.add(vectors_matrix)
    
    throttled_index_log(f"[INDEX] Added {index.ntotal} vectors to index")
    
    return index


def save_index(index: faiss.Index, metadata: List[Dict], 
               index_path: str = INDEX_PATH, 
               metadata_path: str = METADATA_PATH):
    """
    Save FAISS index and metadata to disk
    
    Args:
        index: FAISS index
        metadata: List of metadata dicts
        index_path: Path to save FAISS index
        metadata_path: Path to save metadata
    """
    from utils.log_throttle import throttled_index_log
    
    # Create directory if not exists
    os.makedirs(os.path.dirname(index_path), exist_ok=True)
    
    # Save FAISS index
    throttled_index_log(f"[INDEX] Saving FAISS index to: {index_path}")
    faiss.write_index(index, index_path)
    
    # Save metadata
    throttled_index_log(f"[INDEX] Saving metadata to: {metadata_path}")
    with open(metadata_path, "wb") as f:
        pickle.dump(metadata, f)
    
    # Get file sizes
    index_size = os.path.getsize(index_path) / (1024 * 1024)  # MB
    metadata_size = os.path.getsize(metadata_path) / (1024 * 1024)  # MB
    
    throttled_index_log(f"[INDEX] Index file size: {index_size:.2f} MB")
    throttled_index_log(f"[INDEX] Metadata file size: {metadata_size:.2f} MB")


def load_index(index_path: str = INDEX_PATH, 
               metadata_path: str = METADATA_PATH) -> Tuple[faiss.Index, List[Dict]]:
    """
    Load FAISS index and metadata from disk
    
    Args:
        index_path: Path to FAISS index
        metadata_path: Path to metadata
        
    Returns:
        Tuple of (index, metadata)
    """
    if not os.path.exists(index_path):
        raise FileNotFoundError(f"Index not found: {index_path}")
    
    if not os.path.exists(metadata_path):
        raise FileNotFoundError(f"Metadata not found: {metadata_path}")
    
    print(f"[INDEX] Loading FAISS index from: {index_path}")
    index = faiss.read_index(index_path)
    
    print(f"[INDEX] Loading metadata from: {metadata_path}")
    with open(metadata_path, "rb") as f:
        metadata = pickle.load(f)
    
    print(f"[INDEX] Loaded index with {index.ntotal} vectors")
    print(f"[INDEX] Loaded {len(metadata)} metadata entries")
    
    return index, metadata


def verify_index(index: faiss.Index, metadata: List[Dict]):
    """
    Verify index integrity
    
    Args:
        index: FAISS index
        metadata: Metadata list
    """
    print("\n" + "=" * 60)
    print("Index Verification")
    print("=" * 60)
    
    # Check counts match
    if index.ntotal != len(metadata):
        print(f"[WARNING] Index count ({index.ntotal}) != metadata count ({len(metadata)})")
    else:
        print(f"✓ Index and metadata counts match: {index.ntotal}")
    
    # Check dimension
    print(f"✓ Vector dimension: {index.d}")
    
    # Test search
    print("\n[TEST] Performing test search...")
    
    # Get first vector from index
    test_vector = index.reconstruct(0).reshape(1, -1)
    
    # Search for similar vectors
    distances, indices = index.search(test_vector, k=5)
    
    print(f"✓ Test search successful")
    print(f"  Top 5 similar indices: {indices[0]}")
    print(f"  Distances: {distances[0]}")
    
    # Show sample metadata
    print("\n[SAMPLE] First 3 metadata entries:")
    for i, meta in enumerate(metadata[:3]):
        text_preview = meta['text'][:80] + "..." if len(meta['text']) > 80 else meta['text']
        print(f"  {i+1}. ID={meta['id']}: {text_preview}")


def build_index(db_path: str = DB_PATH, 
                index_path: str = INDEX_PATH,
                metadata_path: str = METADATA_PATH,
                verify: bool = True):
    """
    Main function to build FAISS index from database embeddings
    
    Args:
        db_path: Path to SQLite database
        index_path: Path to save FAISS index
        metadata_path: Path to save metadata
        verify: Whether to verify index after building
    """
    from utils.log_throttle import throttled_index_log
    
    throttled_index_log("=" * 60)
    throttled_index_log("Building FAISS Vector Index")
    throttled_index_log("=" * 60)
    
    try:
        # Step 1: Load embeddings from database
        vectors, metadata = load_embeddings_from_db(db_path)
        
        if not vectors:
            throttled_index_log("[ERROR] No vectors loaded. Exiting.", force=True)
            return
        
        # Step 2: Create FAISS index
        index = create_faiss_index(vectors)
        
        # Step 3: Save index and metadata
        save_index(index, metadata, index_path, metadata_path)
        
        throttled_index_log("\n" + "=" * 60)
        throttled_index_log("✓ Index built successfully!")
        throttled_index_log("=" * 60)
        
        # Step 4: Verify index (optional)
        if verify:
            verify_index(index, metadata)
        
        # Summary
        throttled_index_log("\n" + "=" * 60)
        throttled_index_log("Summary")
        throttled_index_log("=" * 60)
        throttled_index_log(f"Total embeddings loaded: {len(vectors)}")
        throttled_index_log(f"Vector dimension: {index.d}")
        throttled_index_log(f"Index size: {index.ntotal} vectors")
        throttled_index_log(f"Index saved to: {index_path}")
        throttled_index_log(f"Metadata saved to: {metadata_path}")
        throttled_index_log("=" * 60)
        
    except Exception as e:
        throttled_index_log(f"\n[ERROR] Failed to build index: {e}", force=True)
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import sys
    
    # Check command line arguments
    verify = "--no-verify" not in sys.argv
    
    # Build index
    build_index(verify=verify)
    
    print("\n✓ FAISS index building complete!")
