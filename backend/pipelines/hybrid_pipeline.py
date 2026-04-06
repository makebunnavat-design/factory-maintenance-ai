#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hybrid Reasoning Pipeline
==========================
Combines SQL analytics with vector search for complex questions.

Example queries:
- "Line ไหนเสียมากที่สุด และเพราะอะไร"
- "เดือนนี้ Line ไหนมีปัญหาบ่อย และแก้ไขอย่างไร"
- "Top 3 Process ที่เสีย และสาเหตุคืออะไร"

Pipeline:
1. SQL Analytics → Identify top items (lines, processes, etc.)
2. Vector Search → Find causes/solutions from repair logs
3. LLM Generation → Combine insights into natural answer
"""

import logging
import sqlite3
import pandas as pd
import requests
from typing import List, Dict, Optional, Tuple
from requests.exceptions import Timeout, ConnectionError

from services.vector_search import get_search_engine
from services.reranker import rerank_with_metadata
from services.context_compressor import compress_context_with_metadata
from utils.response_formatter import format_response
from core.config import (
    WORK_DB_PATH,
    OLLAMA_GENERATE_URL,
    CHAT_MODEL,
    OLLAMA_REQUEST_TIMEOUT,
    CHAT_FALLBACK_RESPONSE
)


# Logger
logger = logging.getLogger(__name__)


def hybrid_pipeline(query: str, top_k: int = 5) -> str:
    """
    Hybrid reasoning pipeline combining SQL analytics and vector search
    
    Args:
        query: User query requiring both analytics and knowledge
        top_k: Number of vector search results to retrieve
        
    Returns:
        Generated answer combining SQL results and vector knowledge
    """
    logger.info(f"[HYBRID_PIPELINE] Hybrid pipeline activated for query: '{query}'")
    
    try:
        # Step 1: SQL Analytics - Identify top problematic items
        logger.info(f"[HYBRID_PIPELINE] Step 1: Running SQL analytics...")
        sql_results = _run_sql_analytics(query)
        
        if not sql_results:
            logger.warning(f"[HYBRID_PIPELINE] No SQL results found")
            return CHAT_FALLBACK_RESPONSE
        
        logger.info(f"[HYBRID_PIPELINE] SQL analytics completed: {sql_results}")
        
        # Step 2: Build vector query based on SQL results
        logger.info(f"[HYBRID_PIPELINE] Step 2: Building vector query...")
        vector_query = _build_vector_query(query, sql_results)
        logger.info(f"[HYBRID_PIPELINE] Vector query: '{vector_query}'")
        
        # Step 3: Vector search for causes/solutions
        logger.info(f"[HYBRID_PIPELINE] Step 3: Running vector search...")
        engine = get_search_engine()
        results = engine.search(vector_query, top_k=top_k * 4)  # Retrieve more for reranking
        
        if not results:
            logger.warning(f"[HYBRID_PIPELINE] No vector results found")
            # Return SQL results only
            return _generate_sql_only_response(query, sql_results)
        
        logger.info(f"[HYBRID_PIPELINE] Retrieved {len(results)} documents")
        
        # Step 4: Rerank results
        logger.info(f"[HYBRID_PIPELINE] Step 4: Reranking documents...")
        try:
            results = rerank_with_metadata(vector_query, results, top_k=top_k, text_key="text")
            logger.info(f"[HYBRID_PIPELINE] ✓ Reranked to top {len(results)} documents")
        except Exception as e:
            logger.warning(f"[HYBRID_PIPELINE] Reranking failed: {e}, using original order")
            results = results[:top_k]
        
        # Step 5: Compress context
        logger.info(f"[HYBRID_PIPELINE] Step 5: Compressing context...")
        try:
            results = compress_context_with_metadata(vector_query, results, max_sentences=2, text_key="text")
            logger.info(f"[HYBRID_PIPELINE] ✓ Context compressed")
        except Exception as e:
            logger.warning(f"[HYBRID_PIPELINE] Compression failed: {e}, using original context")
        
        # Step 6: Build context
        repair_logs = [result["text"] for result in results]
        context = _build_context(repair_logs, results)
        
        # Step 7: Build prompt combining SQL and vector results
        logger.info(f"[HYBRID_PIPELINE] Step 6: Building prompt...")
        prompt = _build_hybrid_prompt(query, sql_results, context)
        
        # Step 8: Call chat model
        logger.info(f"[HYBRID_PIPELINE] Step 7: Calling chat model...")
        response = _call_chat_model(prompt)
        
        # Step 9: Format response for better readability
        response = format_response(response, "HYBRID")
        
        logger.info(f"[HYBRID_PIPELINE] Hybrid pipeline completed successfully")
        
        return response
        
    except Exception as e:
        logger.error(f"[HYBRID_PIPELINE] Error in hybrid pipeline: {e}")
        import traceback
        traceback.print_exc()
        return CHAT_FALLBACK_RESPONSE


def _run_sql_analytics(query: str) -> Dict:
    """
    Run SQL analytics to identify top problematic items
    
    Args:
        query: User query
        
    Returns:
        Dict with SQL results (e.g., {"top_line": "LINE_A", "failure_count": 50})
    """
    # Detect query type and build appropriate SQL
    query_lower = query.lower()
    
    # Default: Find line with most failures
    if "line" in query_lower and ("มากที่สุด" in query_lower or "บ่อย" in query_lower or "most" in query_lower):
        sql = """
            SELECT Line, COUNT(*) as failure_count
            FROM repairs_enriched
            WHERE Date >= date('now', '-30 days')
            GROUP BY Line
            ORDER BY failure_count DESC
            LIMIT 1
        """
        result_type = "line"
    
    elif "process" in query_lower and ("มากที่สุด" in query_lower or "บ่อย" in query_lower or "most" in query_lower):
        sql = """
            SELECT Process, COUNT(*) as failure_count
            FROM repairs_enriched
            WHERE Date >= date('now', '-30 days')
            GROUP BY Process
            ORDER BY failure_count DESC
            LIMIT 1
        """
        result_type = "process"
    
    elif "tech" in query_lower or "ช่าง" in query_lower:
        sql = """
            SELECT Tech, COUNT(*) as repair_count
            FROM repairs_enriched
            WHERE Date >= date('now', '-30 days')
            GROUP BY Tech
            ORDER BY repair_count DESC
            LIMIT 1
        """
        result_type = "tech"
    
    else:
        # Default to line analysis
        sql = """
            SELECT Line, COUNT(*) as failure_count
            FROM repairs_enriched
            WHERE Date >= date('now', '-30 days')
            GROUP BY Line
            ORDER BY failure_count DESC
            LIMIT 1
        """
        result_type = "line"
    
    try:
        with sqlite3.connect(WORK_DB_PATH) as conn:
            df = pd.read_sql_query(sql, conn)
        
        if df.empty:
            return {}
        
        # Extract results
        row = df.iloc[0]
        
        if result_type == "line":
            return {
                "type": "line",
                "top_line": row["Line"],
                "failure_count": int(row["failure_count"])
            }
        elif result_type == "process":
            return {
                "type": "process",
                "top_process": row["Process"],
                "failure_count": int(row["failure_count"])
            }
        elif result_type == "tech":
            return {
                "type": "tech",
                "top_tech": row["Tech"],
                "repair_count": int(row["repair_count"])
            }
        
        return {}
    
    except Exception as e:
        logger.error(f"[HYBRID_PIPELINE] SQL analytics error: {e}")
        return {}


def _build_vector_query(original_query: str, sql_results: Dict) -> str:
    """
    Build vector search query based on SQL results
    
    Args:
        original_query: Original user query
        sql_results: Results from SQL analytics
        
    Returns:
        Vector search query string
    """
    result_type = sql_results.get("type", "line")
    
    if result_type == "line":
        top_line = sql_results.get("top_line", "")
        return f"สาเหตุที่ {top_line} เสีย"
    
    elif result_type == "process":
        top_process = sql_results.get("top_process", "")
        return f"สาเหตุที่ {top_process} เสีย"
    
    elif result_type == "tech":
        top_tech = sql_results.get("top_tech", "")
        return f"งานซ่อมของ {top_tech}"
    
    # Fallback: extract keywords from original query
    return original_query


def _build_context(repair_logs: List[str], results: List[Dict]) -> str:
    """
    Build context string from repair logs
    
    Args:
        repair_logs: List of repair log texts
        results: List of search results with metadata
        
    Returns:
        Formatted context string
    """
    context_parts = []
    
    for i, (log, result) in enumerate(zip(repair_logs, results), 1):
        # Use rerank_score if available
        if "rerank_score" in result:
            score = result["rerank_score"]
            score_pct = (score + 1) * 50
            score_label = "คะแนนความเกี่ยวข้อง"
        else:
            score = result.get("similarity", 0)
            score_pct = score * 100
            score_label = "ความเกี่ยวข้อง"
        
        context_parts.append(
            f"[{i}] ({score_label}: {score_pct:.0f}%) {log}"
        )
    
    return "\n\n".join(context_parts)


def _build_hybrid_prompt(query: str, sql_results: Dict, context: str) -> str:
    """
    Build prompt combining SQL analytics and vector search results
    
    Args:
        query: Original user query
        sql_results: Results from SQL analytics
        context: Context from vector search
        
    Returns:
        Formatted prompt string
    """
    result_type = sql_results.get("type", "line")
    
    # Build SQL summary
    if result_type == "line":
        sql_summary = f"Line ที่เสียบ่อยที่สุด: {sql_results.get('top_line')} ({sql_results.get('failure_count')} ครั้งในเดือนนี้)"
    elif result_type == "process":
        sql_summary = f"Process ที่เสียบ่อยที่สุด: {sql_results.get('top_process')} ({sql_results.get('failure_count')} ครั้งในเดือนนี้)"
    elif result_type == "tech":
        sql_summary = f"ช่างที่ซ่อมมากที่สุด: {sql_results.get('top_tech')} ({sql_results.get('repair_count')} ครั้งในเดือนนี้)"
    else:
        sql_summary = "ข้อมูลจากการวิเคราะห์"
    
    prompt = f"""คุณคือ "Elin" AI ผู้ช่วยซ่อมบำรุงในโรงงาน

หนูได้วิเคราะห์ข้อมูลและค้นหาสาเหตุจากบันทึกการซ่อมแล้วค่ะ

ข้อมูลจากการวิเคราะห์:
{sql_summary}

สาเหตุที่เป็นไปได้จากบันทึกการซ่อม:
{context}

คำถามของพี่: {query}

คำแนะนำในการตอบ:
- จัดรูปแบบคำตอบให้อ่านง่าย:
  * เริ่มต้นด้วยประโยคสรุปสั้นๆ พร้อมข้อมูลการวิเคราะห์ (Line/Process ไหนเสียบ่อยที่สุด)
  * เว้นบรรทัด 1 บรรทัด
  * แจกแจงสาเหตุเป็นข้อ ๆ (1. 2. 3. ...) โดยแต่ละข้อเว้นบรรทัด
  * แต่ละข้อให้มีรูปแบบ: ปัญหา, สาเหตุ, วิธีแก้ไข (ความเกี่ยวข้อง: XX%)
  * เว้นบรรทัด 1 บรรทัด
  * ปิดท้ายด้วยประโยคแนะนำ (1 บรรทัด)
- อธิบายสาเหตุที่เป็นไปได้จากบันทึกการซ่อม
- ใช้น้ำเสียงเป็นกันเอง แทนตัวเองว่า "หนู" และเรียกผู้ใช้ว่า "พี่"

ตอบคำถามของพี่:"""
    
    return prompt


def _generate_sql_only_response(query: str, sql_results: Dict) -> str:
    """
    Generate response using only SQL results (when vector search fails)
    
    Args:
        query: Original user query
        sql_results: Results from SQL analytics
        
    Returns:
        Response text
    """
    result_type = sql_results.get("type", "line")
    
    if result_type == "line":
        top_line = sql_results.get("top_line", "")
        count = sql_results.get("failure_count", 0)
        return f"จากข้อมูลการวิเคราะห์ {top_line} เป็น Line ที่เสียบ่อยที่สุดค่ะ มีการเสีย {count} ครั้งในเดือนนี้ แต่หนูยังไม่พบข้อมูลสาเหตุที่ชัดเจนในระบบค่ะ"
    
    elif result_type == "process":
        top_process = sql_results.get("top_process", "")
        count = sql_results.get("failure_count", 0)
        return f"จากข้อมูลการวิเคราะห์ {top_process} เป็น Process ที่เสียบ่อยที่สุดค่ะ มีการเสีย {count} ครั้งในเดือนนี้ แต่หนูยังไม่พบข้อมูลสาเหตุที่ชัดเจนในระบบค่ะ"
    
    elif result_type == "tech":
        top_tech = sql_results.get("top_tech", "")
        count = sql_results.get("repair_count", 0)
        return f"จากข้อมูลการวิเคราะห์ {top_tech} เป็นช่างที่ซ่อมมากที่สุดค่ะ มีการซ่อม {count} ครั้งในเดือนนี้"
    
    return CHAT_FALLBACK_RESPONSE


def _call_chat_model(prompt: str, temperature: float = 0.6, max_tokens: int = 512) -> str:
    """
    Call chat model to generate response
    
    Args:
        prompt: Prompt text
        temperature: Sampling temperature
        max_tokens: Maximum tokens to generate
        
    Returns:
        Generated response text
    """
    try:
        chat_timeout = OLLAMA_REQUEST_TIMEOUT
        
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
        
        res.raise_for_status()
        
        data = res.json()
        text = (data.get("response") or "").strip()
        
        if not text:
            logger.warning(f"[HYBRID_PIPELINE] Empty response from chat model")
            return CHAT_FALLBACK_RESPONSE
        
        # Clean up response
        text = text.replace("Elin:", "").strip()
        
        if (text.startswith('"') and text.endswith('"')) or \
           (text.startswith("'") and text.endswith("'")):
            text = text[1:-1].strip()
        
        if len(text) > 1500:
            text = text[:1500]
        
        return text
        
    except Timeout:
        logger.warning(f"[HYBRID_PIPELINE] Chat model timeout")
        return CHAT_FALLBACK_RESPONSE
    except ConnectionError:
        logger.error(f"[HYBRID_PIPELINE] Connection error")
        return CHAT_FALLBACK_RESPONSE
    except Exception as e:
        logger.error(f"[HYBRID_PIPELINE] Error calling chat model: {e}")
        return CHAT_FALLBACK_RESPONSE


if __name__ == "__main__":
    """
    Test hybrid pipeline
    """
    print("=" * 60)
    print("Hybrid Pipeline Test")
    print("=" * 60)
    
    # Test queries
    test_queries = [
        "Line ไหนเสียมากที่สุด และเพราะอะไร",
        "เดือนนี้ Line ไหนมีปัญหาบ่อย และแก้ไขอย่างไร",
        "Process ไหนเสียบ่อยที่สุด และสาเหตุคืออะไร",
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{'=' * 60}")
        print(f"Test {i}: {query}")
        print("=" * 60)
        
        try:
            answer = hybrid_pipeline(query, top_k=5)
            print(f"\nAnswer:\n{answer}")
        
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{'=' * 60}")
    print("✓ Hybrid pipeline test complete!")
    print("=" * 60)
