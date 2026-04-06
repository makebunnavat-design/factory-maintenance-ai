"""
Vector Pipeline
===============
This module implements the vector search pipeline that retrieves similar repair logs
and generates answers using the chat model.

Pipeline flow:
1. User query → Vector search
2. Retrieve top-k similar repair logs
3. Build context from retrieved logs
4. Generate answer using chat model with context
"""

import logging
import requests
from typing import List, Dict, Optional
from requests.exceptions import Timeout, ConnectionError

from services.vector_search import search_vectors, get_search_engine
from services.reranker import rerank_with_metadata
from services.context_compressor import compress_context_with_metadata
from utils.response_formatter import format_response
from core.config import (
    OLLAMA_GENERATE_URL,
    CHAT_MODEL,
    OLLAMA_REQUEST_TIMEOUT,
    CHAT_FALLBACK_RESPONSE
)


# Logger
logger = logging.getLogger(__name__)


def vector_pipeline(query: str, top_k: int = 5, min_similarity: float = 0.3, use_reranker: bool = True, use_compression: bool = True, max_sentences: int = 2) -> str:
    """
    Vector search pipeline: retrieve similar repair logs and generate answer
    
    Args:
        query: User query text
        top_k: Number of similar logs to retrieve (default: 5)
        min_similarity: Minimum similarity threshold (default: 0.3)
        use_reranker: Whether to use reranker for improved accuracy (default: True)
        use_compression: Whether to compress context to reduce LLM input (default: True)
        max_sentences: Maximum sentences per document when compressing (default: 2)
        
    Returns:
        Generated answer from chat model
    """
    logger.info(f"[VECTOR_PIPELINE] Vector pipeline activated for query: '{query}'")
    
    try:
        # Step 1: Retrieve similar repair logs using vector search
        # Retrieve more candidates if using reranker (e.g., 20 candidates -> rerank to top 5)
        initial_k = top_k * 4 if use_reranker else top_k
        logger.info(f"[VECTOR_PIPELINE] Retrieving top {initial_k} similar repair logs...")
        
        engine = get_search_engine()
        results = engine.search_with_threshold(
            query=query,
            top_k=initial_k,
            min_similarity=min_similarity
        )
        
        if not results:
            logger.warning(f"[VECTOR_PIPELINE] No similar repair logs found (min_similarity={min_similarity})")
            return _generate_no_results_response(query)
        
        logger.info(f"[VECTOR_PIPELINE] Retrieved {len(results)} documents")
        
        # Step 2: Rerank documents if enabled
        if use_reranker and len(results) > top_k:
            logger.info(f"[VECTOR_PIPELINE] Reranking {len(results)} documents to top {top_k}...")
            try:
                results = rerank_with_metadata(query, results, top_k=top_k, text_key="text")
                logger.info(f"[VECTOR_PIPELINE] ✓ Reranked to top {len(results)} documents")
            except Exception as e:
                logger.warning(f"[VECTOR_PIPELINE] Reranking failed: {e}, using original order")
                results = results[:top_k]
        else:
            results = results[:top_k]
        
        # Step 3: Compress context if enabled
        if use_compression:
            logger.info(f"[VECTOR_PIPELINE] Compressing context (max_sentences={max_sentences})...")
            try:
                results = compress_context_with_metadata(query, results, max_sentences=max_sentences, text_key="text")
                logger.info(f"[VECTOR_PIPELINE] ✓ Context compressed")
            except Exception as e:
                logger.warning(f"[VECTOR_PIPELINE] Compression failed: {e}, using original context")
        
        # Step 4: Extract repair log texts
        repair_logs = [result["text"] for result in results]
        
        # Step 5: Build context from retrieved logs
        context = _build_context(repair_logs, results)
        
        # Step 6: Build prompt for chat model
        prompt = _build_prompt(query, context, len(results))
        
        # Step 7: Call chat model to generate answer
        logger.info(f"[VECTOR_PIPELINE] Calling chat model to generate answer...")
        response = _call_chat_model(prompt)
        
        # Step 8: Format response for better readability
        response = format_response(response, "VECTOR")
        
        logger.info(f"[VECTOR_PIPELINE] Vector pipeline completed successfully")
        
        return response
        
    except FileNotFoundError as e:
        logger.error(f"[VECTOR_PIPELINE] Missing index or model: {e}")
        return (
            "หนูยังไม่สามารถเข้าถึงฐานความรู้เชิงลึกได้ในขณะนี้ค่ะ "
            "พี่ช่วยบอกให้ Admin รันคำสั่ง `python build_vector_index.py` ใน backend ก่อนนะคะ "
            "เพื่อให้หนูสามารถช่วยวิเคราะห์สาเหตุและวิธีแก้ไขได้อย่างแม่นยำค่ะ"
        )
    except Exception as e:
        logger.error(f"[VECTOR_PIPELINE] Error in vector pipeline: {e}")
        import traceback
        traceback.print_exc()
        return CHAT_FALLBACK_RESPONSE


def _build_context(repair_logs: List[str], results: List[Dict]) -> str:
    """
    Build context string from repair logs with similarity scores
    
    Args:
        repair_logs: List of repair log texts
        results: List of search results with metadata
        
    Returns:
        Formatted context string
    """
    context_parts = []
    
    for i, (log, result) in enumerate(zip(repair_logs, results), 1):
        # Use rerank_score if available, otherwise use similarity
        if "rerank_score" in result:
            score = result["rerank_score"]
            score_pct = (score + 1) * 50  # Convert [-1, 1] to [0, 100]
            score_label = "คะแนนความเกี่ยวข้อง"
        else:
            score = result.get("similarity", 0)
            score_pct = score * 100
            score_label = "ความเกี่ยวข้อง"
        
        # Format: [1] (ความเกี่ยวข้อง: 85%) repair log text
        context_parts.append(
            f"[{i}] ({score_label}: {score_pct:.0f}%) {log}"
        )
    
    return "\n\n".join(context_parts)


def _build_prompt(query: str, context: str, num_docs: int) -> str:
    """
    Build prompt for chat model with context
    
    Args:
        query: User query
        context: Context from retrieved repair logs
        num_docs: Number of documents retrieved
        
    Returns:
        Formatted prompt string
    """
    prompt = f"""คุณคือ "Elin" AI ผู้ช่วยซ่อมบำรุงในโรงงาน

หนูได้ค้นหาข้อมูลการซ่อมที่เกี่ยวข้องกับคำถามของพี่แล้วค่ะ พบ {num_docs} รายการที่คล้ายกัน:

{context}

คำถามของพี่: {query}

คำแนะนำในการตอบ:
- อธิบายสาเหตุที่เป็นไปได้จากข้อมูลการซ่อมที่พบ
- ใช้ข้อมูลจากบันทึกการซ่อมข้างต้นเป็นหลัก
- จัดรูปแบบคำตอบให้อ่านง่าย:
  * เริ่มต้นด้วยประโยคสรุปสั้นๆ (1 บรรทัด)
  * เว้นบรรทัด 1 บรรทัด
  * แจกแจงสาเหตุเป็นข้อ ๆ (1. 2. 3. ...) โดยแต่ละข้อเว้นบรรทัด
  * แต่ละข้อให้มีรูปแบบ: ปัญหา, สาเหตุ, วิธีแก้ไข (ความเกี่ยวข้อง: XX%)
  * เว้นบรรทัด 1 บรรทัด
  * ปิดท้ายด้วยประโยคแนะนำ (1 บรรทัด)
- ใช้น้ำเสียงเป็นกันเอง แทนตัวเองว่า "หนู" และเรียกผู้ใช้ว่า "พี่"

ตอบคำถามของพี่:"""
    
    return prompt


def _call_chat_model(prompt: str, temperature: float = 0.6, max_tokens: int = 512) -> str:
    """
    Call chat model to generate response
    
    Args:
        prompt: Prompt text
        temperature: Sampling temperature (default: 0.6)
        max_tokens: Maximum tokens to generate (default: 512)
        
    Returns:
        Generated response text
    """
    try:
        # Set timeout for chat model (100 seconds for VECTOR_PIPELINE)
        chat_timeout = OLLAMA_REQUEST_TIMEOUT
        
        # Call Ollama API
        res = requests.post(
            OLLAMA_GENERATE_URL,
            json={
                "model": CHAT_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens,
                    "top_p": 0.85
                },
            },
            timeout=chat_timeout,
        )
        
        # Check HTTP status
        res.raise_for_status()
        
        # Parse response
        data = res.json()
        text = (data.get("response") or "").strip()
        
        if not text:
            logger.warning(f"[VECTOR_PIPELINE] Empty response from chat model")
            return CHAT_FALLBACK_RESPONSE
        
        # Clean up response
        text = _clean_response(text)
        
        # Limit length
        if len(text) > 1500:
            text = text[:1500]
        
        return text
        
    except Timeout:
        logger.warning(f"[VECTOR_PIPELINE] Chat model timeout after {chat_timeout}s")
        return CHAT_FALLBACK_RESPONSE
    except ConnectionError:
        logger.error(f"[VECTOR_PIPELINE] Connection error - Is Ollama running?")
        return CHAT_FALLBACK_RESPONSE
    except Exception as e:
        logger.error(f"[VECTOR_PIPELINE] Error calling chat model: {e}")
        return CHAT_FALLBACK_RESPONSE


def _clean_response(text: str) -> str:
    """
    Clean up chat model response
    
    Args:
        text: Raw response text
        
    Returns:
        Cleaned response text
    """
    # Remove "Elin:" prefix if present
    text = text.replace("Elin:", "").strip()
    
    # Remove surrounding quotes
    if (text.startswith('"') and text.endswith('"')) or \
       (text.startswith("'") and text.endswith("'")):
        text = text[1:-1].strip()
    
    return text


def _generate_no_results_response(query: str) -> str:
    """
    Generate response when no similar repair logs are found
    
    Args:
        query: User query
        
    Returns:
        Fallback response
    """
    return (
        f"หนูค้นหาข้อมูลการซ่อมที่เกี่ยวข้องกับ '{query}' แล้วค่ะ "
        f"แต่ยังไม่พบข้อมูลที่ตรงกันมากพอในระบบ\n\n"
        f"พี่ลองถามแบบอื่นได้นะคะ เช่น:\n"
        f"- 'motor เสีย'\n"
        f"- 'bearing แตก'\n"
        f"- 'sensor error'\n"
        f"- 'conveyor belt problem'\n\n"
        f"หรือถามเกี่ยวกับข้อมูลการซ่อมและ PM ได้เลยค่ะ"
    )


def vector_pipeline_with_details(query: str, top_k: int = 5) -> Dict:
    """
    Vector pipeline with detailed results (for debugging/API)
    
    Args:
        query: User query text
        top_k: Number of similar logs to retrieve
        
    Returns:
        Dict with keys: answer, retrieved_docs, num_docs, query
    """
    logger.info(f"[VECTOR_PIPELINE] Vector pipeline with details for query: '{query}'")
    
    try:
        # Retrieve similar repair logs
        engine = get_search_engine()
        results = engine.search(query, top_k=top_k)
        
        # Build context
        repair_logs = [result["text"] for result in results]
        context = _build_context(repair_logs, results)
        
        # Build prompt
        prompt = _build_prompt(query, context, len(results))
        
        # Generate answer
        answer = _call_chat_model(prompt)
        
        return {
            "answer": answer,
            "retrieved_docs": results,
            "num_docs": len(results),
            "query": query
        }
        
    except Exception as e:
        logger.error(f"[VECTOR_PIPELINE] Error in detailed pipeline: {e}")
        return {
            "answer": CHAT_FALLBACK_RESPONSE,
            "retrieved_docs": [],
            "num_docs": 0,
            "query": query,
            "error": str(e)
        }


if __name__ == "__main__":
    """
    Test vector pipeline
    """
    print("=" * 60)
    print("Vector Pipeline Test")
    print("=" * 60)
    
    # Test queries
    test_queries = [
        "เครื่อง CNC เสียบ่อยเพราะอะไร",
        "motor overheating ทำยังไง",
        "bearing แตกบ่อย สาเหตุอะไร",
        "sensor error แก้ไขอย่างไร",
        "ปัญหา conveyor belt"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{'=' * 60}")
        print(f"Test {i}: {query}")
        print("=" * 60)
        
        try:
            # Test basic pipeline
            answer = vector_pipeline(query, top_k=3)
            print(f"\nAnswer:\n{answer}")
            
            # Test detailed pipeline
            print(f"\n{'-' * 60}")
            print("Detailed Results:")
            print("-" * 60)
            
            details = vector_pipeline_with_details(query, top_k=3)
            print(f"\nQuery: {details['query']}")
            print(f"Retrieved docs: {details['num_docs']}")
            
            if details['retrieved_docs']:
                print("\nTop 3 similar repair logs:")
                for j, doc in enumerate(details['retrieved_docs'], 1):
                    text_preview = doc['text'][:80] + "..." if len(doc['text']) > 80 else doc['text']
                    print(f"  [{j}] (sim: {doc['similarity']:.3f}) {text_preview}")
            
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{'=' * 60}")
    print("✓ Vector pipeline test complete!")
    print("=" * 60)
