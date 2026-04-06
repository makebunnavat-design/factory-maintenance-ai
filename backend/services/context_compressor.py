#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Context Compressor
==================
Compress context by extracting most relevant sentences from documents.

Reduces LLM input size while preserving key information.
Uses keyword-based sentence scoring to select most relevant content.
"""

import re
import logging
from typing import List, Dict, Tuple, Optional


# Logger
logger = logging.getLogger(__name__)


# Configuration
DEFAULT_MAX_SENTENCES = 2
MIN_SENTENCE_LENGTH = 10  # Minimum characters for a valid sentence


def compress_context(query: str, 
                     documents: List[str], 
                     max_sentences: int = DEFAULT_MAX_SENTENCES) -> List[str]:
    """
    Compress context by selecting most relevant sentences from documents
    
    Args:
        query: User query text
        documents: List of document texts
        max_sentences: Maximum sentences to keep per document
        
    Returns:
        List of compressed document texts
    """
    if not documents:
        logger.warning("[CONTEXT_COMPRESSOR] No documents to compress")
        return []
    
    logger.info(f"[CONTEXT_COMPRESSOR] Compressing {len(documents)} documents (max_sentences={max_sentences})")
    
    # Calculate original length
    original_length = sum(len(doc) for doc in documents)
    
    # Compress each document
    compressed_docs = []
    for i, doc in enumerate(documents, 1):
        compressed = _compress_document(query, doc, max_sentences)
        compressed_docs.append(compressed)
    
    # Calculate compressed length
    compressed_length = sum(len(doc) for doc in compressed_docs)
    compression_ratio = (1 - compressed_length / original_length) * 100 if original_length > 0 else 0
    
    logger.info(f"[CONTEXT_COMPRESSOR] Original length: {original_length} chars")
    logger.info(f"[CONTEXT_COMPRESSOR] Compressed length: {compressed_length} chars")
    logger.info(f"[CONTEXT_COMPRESSOR] Compression ratio: {compression_ratio:.1f}%")
    
    return compressed_docs


def _compress_document(query: str, document: str, max_sentences: int) -> str:
    """
    Compress a single document by selecting most relevant sentences
    
    Args:
        query: User query text
        document: Document text
        max_sentences: Maximum sentences to keep
        
    Returns:
        Compressed document text
    """
    if not document or len(document.strip()) < MIN_SENTENCE_LENGTH:
        return document
    
    # Split into sentences
    sentences = _split_sentences(document)
    
    if len(sentences) <= max_sentences:
        # Document is already short enough
        return document
    
    # Score sentences by relevance
    scored_sentences = _score_sentences(query, sentences)
    
    # Sort by score (descending)
    scored_sentences.sort(key=lambda x: x[1], reverse=True)
    
    # Select top sentences
    top_sentences = scored_sentences[:max_sentences]
    
    # Sort by original order to maintain coherence
    top_sentences.sort(key=lambda x: x[2])  # Sort by original index
    
    # Combine sentences
    compressed = " ".join([sent for sent, score, idx in top_sentences])
    
    return compressed


def _split_sentences(text: str) -> List[str]:
    """
    Split text into sentences
    
    Args:
        text: Input text
        
    Returns:
        List of sentences
    """
    # Split by common sentence delimiters
    # Handle Thai and English punctuation
    sentences = re.split(r'[.!?。！？]\s*', text)
    
    # Filter out empty sentences and very short ones
    sentences = [s.strip() for s in sentences if s.strip() and len(s.strip()) >= MIN_SENTENCE_LENGTH]
    
    return sentences


def _score_sentences(query: str, sentences: List[str]) -> List[Tuple[str, float, int]]:
    """
    Score sentences by relevance to query
    
    Args:
        query: User query text
        sentences: List of sentences
        
    Returns:
        List of tuples (sentence, score, original_index)
    """
    # Extract query keywords
    query_words = _extract_keywords(query)
    
    scored_sentences = []
    
    for idx, sentence in enumerate(sentences):
        # Extract sentence keywords
        sentence_words = _extract_keywords(sentence)
        
        # Calculate score based on keyword overlap
        score = _calculate_keyword_score(query_words, sentence_words)
        
        # Bonus for sentence position (earlier sentences often more important)
        position_bonus = 1.0 / (idx + 1) * 0.1
        
        # Bonus for sentence length (prefer informative sentences)
        length_bonus = min(len(sentence) / 100, 1.0) * 0.1
        
        total_score = score + position_bonus + length_bonus
        
        scored_sentences.append((sentence, total_score, idx))
    
    return scored_sentences


def _extract_keywords(text: str) -> set:
    """
    Extract keywords from text
    
    Args:
        text: Input text
        
    Returns:
        Set of keywords (lowercase)
    """
    # Convert to lowercase
    text_lower = text.lower()
    
    # Extract words (Thai and English)
    # Thai: \u0e00-\u0e7f
    # English: a-z
    # Numbers: 0-9
    words = re.findall(r'[\u0e00-\u0e7fa-z0-9]+', text_lower)
    
    # Filter out very short words and common stop words
    stop_words = {
        # Thai
        'ที่', 'และ', 'ใน', 'ของ', 'เป็น', 'มี', 'ได้', 'จาก', 'ให้', 'ไป',
        'มา', 'ว่า', 'ไว้', 'นี้', 'นั้น', 'ก็', 'จะ', 'ถ้า', 'แล้ว', 'หรือ',
        # English
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        'of', 'with', 'by', 'from', 'is', 'are', 'was', 'were', 'be', 'been',
    }
    
    keywords = {w for w in words if len(w) >= 2 and w not in stop_words}
    
    return keywords


def _calculate_keyword_score(query_words: set, sentence_words: set) -> float:
    """
    Calculate keyword overlap score
    
    Args:
        query_words: Set of query keywords
        sentence_words: Set of sentence keywords
        
    Returns:
        Score (0.0 to 1.0+)
    """
    if not query_words:
        return 0.0
    
    # Count matching keywords
    matches = query_words & sentence_words
    
    # Calculate score
    # Base score: proportion of query words found
    base_score = len(matches) / len(query_words)
    
    # Bonus for multiple matches
    match_bonus = len(matches) * 0.1
    
    total_score = base_score + match_bonus
    
    return total_score


def compress_context_with_metadata(query: str,
                                   documents: List[Dict],
                                   max_sentences: int = DEFAULT_MAX_SENTENCES,
                                   text_key: str = "text") -> List[Dict]:
    """
    Compress context while preserving metadata
    
    Args:
        query: User query text
        documents: List of document dicts with metadata
        max_sentences: Maximum sentences to keep per document
        text_key: Key in document dict containing text
        
    Returns:
        List of documents with compressed text
    """
    if not documents:
        logger.warning("[CONTEXT_COMPRESSOR] No documents to compress")
        return []
    
    logger.info(f"[CONTEXT_COMPRESSOR] Compressing {len(documents)} documents with metadata")
    
    # Extract texts
    texts = [doc.get(text_key, "") for doc in documents]
    
    # Compress texts
    compressed_texts = compress_context(query, texts, max_sentences)
    
    # Create new documents with compressed text
    compressed_docs = []
    for doc, compressed_text in zip(documents, compressed_texts):
        doc_copy = doc.copy()
        doc_copy[text_key] = compressed_text
        doc_copy["compressed"] = True
        compressed_docs.append(doc_copy)
    
    return compressed_docs


def get_compression_stats(original_docs: List[str], compressed_docs: List[str]) -> Dict:
    """
    Get compression statistics
    
    Args:
        original_docs: Original documents
        compressed_docs: Compressed documents
        
    Returns:
        Dict with compression statistics
    """
    original_length = sum(len(doc) for doc in original_docs)
    compressed_length = sum(len(doc) for doc in compressed_docs)
    
    original_sentences = sum(len(_split_sentences(doc)) for doc in original_docs)
    compressed_sentences = sum(len(_split_sentences(doc)) for doc in compressed_docs)
    
    compression_ratio = (1 - compressed_length / original_length) * 100 if original_length > 0 else 0
    
    return {
        "original_length": original_length,
        "compressed_length": compressed_length,
        "original_sentences": original_sentences,
        "compressed_sentences": compressed_sentences,
        "compression_ratio": compression_ratio,
        "bytes_saved": original_length - compressed_length
    }


if __name__ == "__main__":
    """
    Test context compressor
    """
    print("=" * 60)
    print("Context Compressor Test")
    print("=" * 60)
    
    # Test query
    query = "เครื่อง CNC เสียบ่อยเพราะอะไร"
    
    # Test documents (long texts with multiple sentences)
    documents = [
        "bearing แตก ทำให้ motor หยุดทำงาน. lubrication ไม่เพียงพอ. ต้องเปลี่ยน bearing ใหม่. ตรวจสอบ oil level ทุกวัน. ใช้ bearing คุณภาพดี.",
        "motor overheating เกิดจาก cooling fan สกปรก. ทำความสะอาด fan ทุกสัปดาห์. ตรวจสอบ temperature sensor. เปลี่ยน thermal paste. ติดตั้ง fan เพิ่มเติม.",
        "CNC spindle bearing worn out. Replace bearing immediately. Check alignment. Use proper lubrication. Monitor vibration levels. Schedule regular maintenance.",
        "sensor calibration error reset controller. Check wiring connections. Replace faulty sensor. Update firmware. Test after repair.",
        "conveyor belt ขาด เปลี่ยน belt ใหม่. ตรวจสอบ tension. ทำความสะอาด pulley. ใช้ belt ที่เหมาะสม. ตรวจสอบ alignment."
    ]
    
    print(f"\nQuery: {query}")
    print(f"Documents: {len(documents)}")
    
    # Test 1: Basic compression
    print("\n" + "=" * 60)
    print("Test 1: Basic Compression (max_sentences=2)")
    print("=" * 60)
    
    compressed = compress_context(query, documents, max_sentences=2)
    
    print(f"\nOriginal documents:")
    for i, doc in enumerate(documents, 1):
        print(f"{i}. {doc}")
    
    print(f"\nCompressed documents:")
    for i, doc in enumerate(compressed, 1):
        print(f"{i}. {doc}")
    
    # Test 2: Compression with different max_sentences
    print("\n" + "=" * 60)
    print("Test 2: Different max_sentences Values")
    print("=" * 60)
    
    for max_sent in [1, 2, 3]:
        compressed = compress_context(query, documents, max_sentences=max_sent)
        stats = get_compression_stats(documents, compressed)
        print(f"\nmax_sentences={max_sent}:")
        print(f"  Compression ratio: {stats['compression_ratio']:.1f}%")
        print(f"  Bytes saved: {stats['bytes_saved']}")
        print(f"  Sentences: {stats['original_sentences']} → {stats['compressed_sentences']}")
    
    # Test 3: Compression with metadata
    print("\n" + "=" * 60)
    print("Test 3: Compression with Metadata")
    print("=" * 60)
    
    docs_with_metadata = [
        {
            "text": doc,
            "line": f"LINE_{i}",
            "process": f"PROCESS_{i}",
            "similarity": 0.8 - (i * 0.1)
        }
        for i, doc in enumerate(documents, 1)
    ]
    
    compressed_with_meta = compress_context_with_metadata(
        query, 
        docs_with_metadata, 
        max_sentences=2
    )
    
    print(f"\nCompressed documents with metadata:")
    for i, doc in enumerate(compressed_with_meta, 1):
        print(f"{i}. Line: {doc['line']}, Similarity: {doc['similarity']:.2f}")
        print(f"   Text: {doc['text'][:80]}...")
        print(f"   Compressed: {doc['compressed']}")
    
    # Test 4: Compression statistics
    print("\n" + "=" * 60)
    print("Test 4: Compression Statistics")
    print("=" * 60)
    
    compressed = compress_context(query, documents, max_sentences=2)
    stats = get_compression_stats(documents, compressed)
    
    print(f"\nStatistics:")
    print(f"  Original length: {stats['original_length']} chars")
    print(f"  Compressed length: {stats['compressed_length']} chars")
    print(f"  Original sentences: {stats['original_sentences']}")
    print(f"  Compressed sentences: {stats['compressed_sentences']}")
    print(f"  Compression ratio: {stats['compression_ratio']:.1f}%")
    print(f"  Bytes saved: {stats['bytes_saved']}")
    
    # Test 5: Edge cases
    print("\n" + "=" * 60)
    print("Test 5: Edge Cases")
    print("=" * 60)
    
    # Empty documents
    print("\nEmpty documents:")
    result = compress_context(query, [], max_sentences=2)
    print(f"  Result: {result}")
    
    # Single sentence documents
    print("\nSingle sentence documents:")
    short_docs = ["bearing แตก", "motor overheating", "sensor error"]
    result = compress_context(query, short_docs, max_sentences=2)
    print(f"  Original: {short_docs}")
    print(f"  Compressed: {result}")
    
    # Very long document
    print("\nVery long document:")
    long_doc = ". ".join([f"sentence {i}" for i in range(20)])
    result = compress_context(query, [long_doc], max_sentences=3)
    print(f"  Original sentences: {len(_split_sentences(long_doc))}")
    print(f"  Compressed sentences: {len(_split_sentences(result[0]))}")
    
    print("\n" + "=" * 60)
    print("✓ Context compressor test complete!")
    print("=" * 60)
