#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reranker Module
===============
Cross-encoder reranker to improve vector search result accuracy.

Uses BGE-reranker-large model to re-score and re-rank retrieved documents.
Cross-encoders are more accurate than bi-encoders (vector search) but slower,
so we use them as a second stage after initial retrieval.

Pipeline:
1. Vector search retrieves top-k candidates (e.g., k=20)
2. Reranker re-scores all candidates
3. Return top-n documents (e.g., n=5)
"""

import os
import logging
import warnings
from pathlib import Path
from typing import List, Dict, Tuple, Optional

# Suppress ALL progress bars and warnings
os.environ['TRANSFORMERS_VERBOSITY'] = 'error'
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
os.environ['HF_HUB_DISABLE_PROGRESS_BARS'] = '1'
logging.getLogger('transformers').setLevel(logging.ERROR)
logging.getLogger('sentence_transformers').setLevel(logging.ERROR)
logging.getLogger('torch').setLevel(logging.ERROR)
warnings.filterwarnings('ignore')

from sentence_transformers import CrossEncoder


# Logger
logger = logging.getLogger(__name__)


# Model paths - Use /app/models in container
from core.config import BASE_DIR
MODELS_DIR = Path(BASE_DIR) / "models"
LOCAL_RERANKER = MODELS_DIR / "bge-reranker-large"
# Prefer specific local directory, then fall back to ID (which HF will find in cache)
DEFAULT_RERANKER_MODEL = str(LOCAL_RERANKER) if LOCAL_RERANKER.exists() else "BAAI/bge-reranker-large"
DEFAULT_TOP_K = 5


class Reranker:
    """
    Cross-encoder reranker for improving search result accuracy
    
    Uses a cross-encoder model that jointly encodes query and document
    to produce a relevance score. More accurate than vector similarity
    but slower, so used as second stage after initial retrieval.
    """
    
    def __init__(self, model_name: str = DEFAULT_RERANKER_MODEL):
        """
        Initialize reranker
        
        Args:
            model_name: HuggingFace model name for cross-encoder
        """
        self.model_name = model_name
        self.model = None
        
        logger.info(f"[RERANKER] Initializing reranker: {model_name}")
    
    def load_model(self):
        """Load cross-encoder model (local first, fallback to HuggingFace)"""
        if self.model is None:
            logger.info(f"[RERANKER] Loading model: {self.model_name}...")
            
            # Check if using local model
            if os.path.exists(self.model_name):
                logger.info(f"[RERANKER] Using local model from: {self.model_name}")
            else:
                logger.info(f"[RERANKER] Downloading from HuggingFace: {self.model_name}")
                logger.warning(f"[RERANKER] ⚠️  For offline use, run: python download_models.py")
            
            try:
                # Suppress progress bars during model loading
                import sys
                from io import StringIO
                old_stdout = sys.stdout
                old_stderr = sys.stderr
                sys.stdout = StringIO()
                sys.stderr = StringIO()
                
                # Force CPU usage - ให้ GPU สำหรับ Ollama เท่านั้น
                self.model = CrossEncoder(self.model_name, device='cpu')
                
                # Restore stdout/stderr
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                
                logger.info(f"[RERANKER] ✓ Model loaded successfully on CPU")
            except Exception as e:
                # Restore stdout/stderr in case of error
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                logger.error(f"[RERANKER] ✗ Failed to load model: {e}")
                raise
    
    def rerank(self, 
               query: str, 
               documents: List[str], 
               top_k: int = DEFAULT_TOP_K) -> List[str]:
        """
        Rerank documents by relevance to query
        
        Args:
            query: User query text
            documents: List of document texts to rerank
            top_k: Number of top documents to return
            
        Returns:
            List of top-k reranked documents
        """
        if not documents:
            logger.warning("[RERANKER] No documents to rerank")
            return []
        
        # Load model if not loaded
        if self.model is None:
            self.load_model()
        
        logger.info(f"[RERANKER] Reranking {len(documents)} documents (top_k={top_k})")
        
        # Prepare query-document pairs
        pairs = [(query, doc) for doc in documents]
        
        # Compute relevance scores
        try:
            scores = self.model.predict(pairs)
            logger.info(f"[RERANKER] ✓ Computed scores for {len(documents)} documents")
        except Exception as e:
            logger.error(f"[RERANKER] ✗ Failed to compute scores: {e}")
            # Fallback: return original order
            return documents[:top_k]
        
        # Combine documents with scores
        scored_docs = list(zip(documents, scores))
        
        # Sort by score (descending)
        sorted_docs = sorted(scored_docs, key=lambda x: x[1], reverse=True)
        
        # Log top scores
        logger.info(f"[RERANKER] Top 3 scores: {[f'{score:.3f}' for _, score in sorted_docs[:3]]}")
        
        # Select top-k documents
        top_docs = [doc for doc, score in sorted_docs[:top_k]]
        
        logger.info(f"[RERANKER] ✓ Selected top {len(top_docs)} documents")
        
        return top_docs
    
    def rerank_with_scores(self,
                          query: str,
                          documents: List[str],
                          top_k: int = DEFAULT_TOP_K) -> List[Tuple[str, float]]:
        """
        Rerank documents and return with scores
        
        Args:
            query: User query text
            documents: List of document texts to rerank
            top_k: Number of top documents to return
            
        Returns:
            List of tuples (document, score) sorted by score
        """
        if not documents:
            logger.warning("[RERANKER] No documents to rerank")
            return []
        
        # Load model if not loaded
        if self.model is None:
            self.load_model()
        
        logger.info(f"[RERANKER] Reranking {len(documents)} documents with scores (top_k={top_k})")
        
        # Prepare query-document pairs
        pairs = [(query, doc) for doc in documents]
        
        # Compute relevance scores
        try:
            scores = self.model.predict(pairs)
            logger.info(f"[RERANKER] ✓ Computed scores for {len(documents)} documents")
        except Exception as e:
            logger.error(f"[RERANKER] ✗ Failed to compute scores: {e}")
            # Fallback: return original order with dummy scores
            return [(doc, 0.0) for doc in documents[:top_k]]
        
        # Combine documents with scores
        scored_docs = list(zip(documents, scores))
        
        # Sort by score (descending)
        sorted_docs = sorted(scored_docs, key=lambda x: x[1], reverse=True)
        
        # Log top scores
        logger.info(f"[RERANKER] Top 3 scores: {[f'{score:.3f}' for _, score in sorted_docs[:3]]}")
        
        # Select top-k
        top_docs = sorted_docs[:top_k]
        
        logger.info(f"[RERANKER] ✓ Selected top {len(top_docs)} documents with scores")
        
        return top_docs
    
    def rerank_with_metadata(self,
                            query: str,
                            documents: List[Dict],
                            top_k: int = DEFAULT_TOP_K,
                            text_key: str = "text") -> List[Dict]:
        """
        Rerank documents with metadata
        
        Args:
            query: User query text
            documents: List of document dicts with metadata
            top_k: Number of top documents to return
            text_key: Key in document dict containing text
            
        Returns:
            List of top-k reranked documents with metadata and rerank_score
        """
        if not documents:
            logger.warning("[RERANKER] No documents to rerank")
            return []
        
        # Load model if not loaded
        if self.model is None:
            self.load_model()
        
        logger.info(f"[RERANKER] Reranking {len(documents)} documents with metadata (top_k={top_k})")
        
        # Extract texts
        texts = [doc.get(text_key, "") for doc in documents]
        
        # Prepare query-document pairs
        pairs = [(query, text) for text in texts]
        
        # Compute relevance scores
        try:
            scores = self.model.predict(pairs)
            logger.info(f"[RERANKER] ✓ Computed scores for {len(documents)} documents")
        except Exception as e:
            logger.error(f"[RERANKER] ✗ Failed to compute scores: {e}")
            # Fallback: return original order
            return documents[:top_k]
        
        # Add rerank scores to documents
        scored_docs = []
        for doc, score in zip(documents, scores):
            doc_copy = doc.copy()
            doc_copy["rerank_score"] = float(score)
            scored_docs.append(doc_copy)
        
        # Sort by rerank score (descending)
        sorted_docs = sorted(scored_docs, key=lambda x: x["rerank_score"], reverse=True)
        
        # Log top scores
        top_scores = [f"{doc['rerank_score']:.3f}" for doc in sorted_docs[:3]]
        logger.info(f"[RERANKER] Top 3 rerank scores: {top_scores}")
        
        # Select top-k
        top_docs = sorted_docs[:top_k]
        
        logger.info(f"[RERANKER] ✓ Selected top {len(top_docs)} documents with metadata")
        
        return top_docs


# Singleton instance
_reranker_instance: Optional[Reranker] = None


def get_reranker() -> Reranker:
    """
    Get or create singleton reranker instance
    
    Returns:
        Reranker instance
    """
    global _reranker_instance
    if _reranker_instance is None:
        _reranker_instance = Reranker()
        _reranker_instance.load_model()
    return _reranker_instance


def rerank(query: str, documents: List[str], top_k: int = DEFAULT_TOP_K) -> List[str]:
    """
    Convenience function to rerank documents
    
    Args:
        query: User query text
        documents: List of document texts to rerank
        top_k: Number of top documents to return
        
    Returns:
        List of top-k reranked documents
    """
    reranker = get_reranker()
    return reranker.rerank(query, documents, top_k)


def rerank_with_scores(query: str, 
                       documents: List[str], 
                       top_k: int = DEFAULT_TOP_K) -> List[Tuple[str, float]]:
    """
    Convenience function to rerank documents with scores
    
    Args:
        query: User query text
        documents: List of document texts to rerank
        top_k: Number of top documents to return
        
    Returns:
        List of tuples (document, score) sorted by score
    """
    reranker = get_reranker()
    return reranker.rerank_with_scores(query, documents, top_k)


def rerank_with_metadata(query: str,
                        documents: List[Dict],
                        top_k: int = DEFAULT_TOP_K,
                        text_key: str = "text") -> List[Dict]:
    """
    Convenience function to rerank documents with metadata
    
    Args:
        query: User query text
        documents: List of document dicts with metadata
        top_k: Number of top documents to return
        text_key: Key in document dict containing text
        
    Returns:
        List of top-k reranked documents with metadata and rerank_score
    """
    reranker = get_reranker()
    return reranker.rerank_with_metadata(query, documents, top_k, text_key)


if __name__ == "__main__":
    """
    Test reranker
    """
    print("=" * 60)
    print("Reranker Test")
    print("=" * 60)
    
    # Test query
    query = "เครื่อง CNC เสียบ่อยเพราะอะไร"
    
    # Test documents (some relevant, some not)
    documents = [
        "bearing แตก เปลี่ยน bearing ใหม่",
        "motor overheating ทำความสะอาด cooling fan",
        "sensor calibration error reset controller",
        "conveyor belt ขาด เปลี่ยน belt ใหม่",
        "hydraulic pump leak ซ่อม seal",
        "CNC spindle bearing worn out replace bearing",
        "CNC motor overload check load and cooling",
        "PLC communication error check cable",
        "pneumatic cylinder leak replace seal",
        "robot arm encoder error recalibrate encoder"
    ]
    
    print(f"\nQuery: {query}")
    print(f"Documents: {len(documents)}")
    
    # Test 1: Basic reranking
    print("\n" + "=" * 60)
    print("Test 1: Basic Reranking (top 5)")
    print("=" * 60)
    
    try:
        top_docs = rerank(query, documents, top_k=5)
        
        print(f"\nTop 5 reranked documents:")
        for i, doc in enumerate(top_docs, 1):
            print(f"{i}. {doc}")
    
    except Exception as e:
        print(f"✗ Test 1 failed: {e}")
        import traceback
        traceback.print_exc()
    
    # Test 2: Reranking with scores
    print("\n" + "=" * 60)
    print("Test 2: Reranking with Scores (top 5)")
    print("=" * 60)
    
    try:
        scored_docs = rerank_with_scores(query, documents, top_k=5)
        
        print(f"\nTop 5 documents with scores:")
        for i, (doc, score) in enumerate(scored_docs, 1):
            print(f"{i}. [{score:.3f}] {doc}")
    
    except Exception as e:
        print(f"✗ Test 2 failed: {e}")
        import traceback
        traceback.print_exc()
    
    # Test 3: Reranking with metadata
    print("\n" + "=" * 60)
    print("Test 3: Reranking with Metadata (top 5)")
    print("=" * 60)
    
    try:
        # Create documents with metadata
        docs_with_metadata = [
            {
                "text": doc,
                "line": f"LINE_{i}",
                "process": f"PROCESS_{i}",
                "date": "2024-01-01"
            }
            for i, doc in enumerate(documents, 1)
        ]
        
        reranked_docs = rerank_with_metadata(query, docs_with_metadata, top_k=5)
        
        print(f"\nTop 5 documents with metadata:")
        for i, doc in enumerate(reranked_docs, 1):
            print(f"{i}. [{doc['rerank_score']:.3f}] {doc['text'][:50]}...")
            print(f"   Line: {doc['line']}, Process: {doc['process']}")
    
    except Exception as e:
        print(f"✗ Test 3 failed: {e}")
        import traceback
        traceback.print_exc()
    
    # Test 4: Compare with and without reranking
    print("\n" + "=" * 60)
    print("Test 4: Compare Original vs Reranked Order")
    print("=" * 60)
    
    try:
        print("\nOriginal order (first 5):")
        for i, doc in enumerate(documents[:5], 1):
            print(f"{i}. {doc}")
        
        print("\nReranked order (top 5):")
        scored_docs = rerank_with_scores(query, documents, top_k=5)
        for i, (doc, score) in enumerate(scored_docs, 1):
            print(f"{i}. [{score:.3f}] {doc}")
    
    except Exception as e:
        print(f"✗ Test 4 failed: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("✓ Reranker test complete!")
    print("=" * 60)
