#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Intent Classifier Router
=========================
Classify user query into one of:
- SQL: Queries for structured data (lists, counts, statistics, reports)
- VECTOR: Queries about causes, troubleshooting, how to fix, maintenance instructions
- HYBRID: Queries asking for BOTH data/statistics AND explanations/causes
- CHAT: Greetings, small talk, general conversation

LLM-first strategy:
- Use router LLM (SCB10X/Ollama configurable via router_llm_client)
- Fallback to deterministic keywords when LLM is unavailable
"""

from __future__ import annotations

import logging
from typing import Dict, List, Tuple

from requests.exceptions import ConnectionError, Timeout

from pipelines.router_llm_client import call_router_llm


logger = logging.getLogger(__name__)

# Allowed routes (4-way classification)
ALLOWED_ROUTES = {"SQL", "VECTOR", "HYBRID", "CHAT"}

# SQL keywords (structured data queries)
SQL_KEYWORDS = [
    # Counting/aggregation
    "จำนวน", "รวม", "เฉลี่ย", "sum", "count", "avg", "total",
    # Time-based
    "วันนี้", "เมื่อวาน", "สัปดาห์นี้", "เดือนนี้", "today", "yesterday", "this week", "this month",
    # Listing
    "มีอะไรบ้าง", "รายการ", "แสดง", "ดู", "list", "show", "display", "check",
    # General data
    "ประวัติ", "pm", "แผน", "schedule", "plan", "history", "records"
]

# VECTOR keywords (causes, troubleshooting, how-to)
VECTOR_KEYWORDS = [
    # Why/causes
    "ทำไม", "เพราะอะไร", "สาเหตุ", "why", "cause", "reason", "root cause",
    # How to fix
    "ยังไง", "อย่างไร", "วิธี", "วิธีแก้", "แก้ยังไง", "แก้ไข", "ซ่อมที่ไหน",
    "how", "how to", "fix", "solution", "resolve", "troubleshoot", "how to fix",
    # Symptoms and diagnosis
    "อาการ", "symptom", "sign", "diagnosis",
    # Recommendations
    "แนะนำ", "ควร", "recommend", "suggest", "advice", "guide"
]

# HYBRID keywords (analytics + causes)
HYBRID_KEYWORDS = [
    # Ranking with causes
    "มากที่สุด", "น้อยที่สุด", "บ่อยที่สุด", "บ่อยสุด",
    "most", "least", "top",
    # Comparison
    "เปรียบเทียบ", "compare", "comparison", "vs",
    # Analytics
    "สถิติ", "วิเคราะห์", "analyze", "analysis", "trend"
]

# Common maintenance keywords (used for all data-related routes)
MAINTENANCE_KEYWORDS = [
    "เสีย", "ซ่อม", "พัง", "breakdown", "repair", "problem", "issue",
    "error", "alarm", "fault", "broke", "broken",
    "line", "process", "tech", "ช่าง", "เครื่อง", "ไลน์"
]


def route_query(query: str) -> str:
    """Route query to SQL, VECTOR, HYBRID, or CHAT."""
    route, _confidence, _source, _raw = _route_query_internal(query)
    return route


def route_query_with_confidence(query: str) -> Tuple[str, float]:
    """Route query with confidence score."""
    route, confidence, _source, _raw = _route_query_internal(query)
    return route, confidence


def route_vector_query(query: str) -> bool:
    """Backward compatibility helper - returns True if query needs vector search."""
    route = route_query(query)
    return route in {"VECTOR", "HYBRID"}


def get_matched_keywords(query: str) -> Dict[str, List[str]]:
    """Return matched keywords by category for debugging."""
    if not query:
        return {"sql": [], "vector": [], "hybrid": []}
    
    q = query.lower().strip()
    return {
        "sql": [kw for kw in SQL_KEYWORDS if kw in q],
        "vector": [kw for kw in VECTOR_KEYWORDS if kw in q],
        "hybrid": [kw for kw in HYBRID_KEYWORDS if kw in q]
    }


def should_use_vector_search(query: str, threshold: float = 0.6) -> bool:
    """Decide whether vector pipeline should be used."""
    route, confidence = route_query_with_confidence(query)
    return route in {"VECTOR", "HYBRID"} and confidence >= threshold


def _route_query_internal(query: str) -> Tuple[str, float, str, str]:
    if not query or not query.strip():
        return "CHAT", 0.50, "empty_query", ""

    prompt = _build_router_prompt(query)
    try:
        raw = call_router_llm(
            prompt,
            temperature=0.0,
            max_tokens=12,
            top_p=0.9,
            timeout=100,
        ).strip()
        parsed = _parse_route_token(raw)
        if parsed:
            confidence = 0.92 if parsed != "CHAT" else 0.78
            logger.info(
                "[VECTOR_ROUTER] LLM route=%s confidence=%.2f raw=%s",
                parsed,
                confidence,
                raw[:80],
            )
            return parsed, confidence, "llm_router", raw
        logger.warning("[VECTOR_ROUTER] Unexpected LLM output: %s", raw)
    except (Timeout, ConnectionError) as exc:
        logger.warning("[VECTOR_ROUTER] LLM timeout/connection error: %s", exc)
    except Exception as exc:
        logger.warning("[VECTOR_ROUTER] LLM route failed: %s", exc)

    fallback = _fallback_route(query)
    conf = _fallback_confidence(query, fallback)
    logger.info(
        "[VECTOR_ROUTER] Fallback route=%s confidence=%.2f query=%s",
        fallback,
        conf,
        query[:100],
    )
    return fallback, conf, "keyword_fallback", ""


def _build_router_prompt(query: str) -> str:
    return f"""You are an intent classifier for a factory maintenance AI assistant.
Classify the user query into exactly ONE of the following intents:

SQL
VECTOR
HYBRID
CHAT

Definitions:

SQL
The user is asking for structured data from the database such as:
- lists
- counts
- statistics
- reports
- tables

VECTOR
The user is asking about:
- causes of machine failure
- troubleshooting
- how to fix something
- maintenance instructions
- explanations

HYBRID
The user asks BOTH:
- data/statistics
- AND explanation or cause

CHAT
Greeting or casual conversation.

Return ONLY the intent word.

Examples:
วันนี้มีเครื่องเสียอะไรบ้าง → SQL
เครื่องไหนเสียบ่อยที่สุด → SQL
COATING เสียบ่อยเพราะอะไร → HYBRID
torque พังซ่อมที่ไหน ยังไง → VECTOR
สวัสดี → CHAT

Query: "{query}"

Intent:"""


def _parse_route_token(raw: str) -> str:
    """Parse LLM response to extract intent."""
    if not raw:
        return ""
    upper = raw.upper().strip()

    tokens = [
        tok
        for tok in "".join(ch if ch.isalpha() else " " for ch in upper).split()
        if tok
    ]
    
    # Check for valid routes
    for tok in tokens:
        if tok in ALLOWED_ROUTES:
            return tok

    # Backup parse
    if "HYBRID" in upper:
        return "HYBRID"
    if "VECTOR" in upper:
        return "VECTOR"
    if "SQL" in upper:
        return "SQL"
    if "CHAT" in upper:
        return "CHAT"
    
    return ""


def _fallback_route(query: str) -> str:
    """Fallback routing using keyword matching (SQL/VECTOR/HYBRID/CHAT)."""
    q = query.lower().strip()
    
    has_sql = any(kw in q for kw in SQL_KEYWORDS)
    has_vector = any(kw in q for kw in VECTOR_KEYWORDS)
    has_hybrid = any(kw in q for kw in HYBRID_KEYWORDS)
    has_maintenance = any(kw in q for kw in MAINTENANCE_KEYWORDS)
    
    # Priority 1: HYBRID (has both data and cause keywords)
    if has_hybrid and has_vector:
        logger.info("[VECTOR_ROUTER] Fallback: HYBRID detected (analytics + causes)")
        return "HYBRID"
    
    # Priority 2: VECTOR (troubleshooting, causes, how-to)
    if has_vector:
        logger.info("[VECTOR_ROUTER] Fallback: VECTOR detected (troubleshooting)")
        return "VECTOR"
    
    # Priority 3: SQL (data queries)
    if has_sql or has_maintenance:
        logger.info("[VECTOR_ROUTER] Fallback: SQL detected (data query)")
        return "SQL"
    
    # Priority 4: Check for entity + maintenance intent
    tokens = [t for t in query.replace(",", " ").split() if t]
    strong_entity_token = any(
        len(t) >= 3
        and any(ch.isalpha() for ch in t)
        and all(ch.isalnum() or ch in {"_", "-"} for ch in t)
        and (
            t.upper() == t
            or any(ch.isdigit() for ch in t)
        )
        for t in tokens
    )
    
    if strong_entity_token and has_maintenance:
        # Check if asking why/how
        if any(kw in q for kw in ["ทำไม", "เพราะอะไร", "ยังไง", "วิธี", "why", "how"]):
            logger.info("[VECTOR_ROUTER] Fallback: VECTOR detected (entity + why/how)")
            return "VECTOR"
        logger.info("[VECTOR_ROUTER] Fallback: SQL detected (entity + maintenance)")
        return "SQL"
    
    # Default: CHAT
    logger.info("[VECTOR_ROUTER] Fallback: CHAT detected")
    return "CHAT"


def _fallback_confidence(query: str, route: str) -> float:
    """Calculate confidence for fallback routing (SQL/VECTOR/HYBRID/CHAT)."""
    q = query.lower().strip()
    score = 0.50

    if route == "HYBRID":
        # Count keyword matches
        hybrid_hits = sum(1 for kw in HYBRID_KEYWORDS if kw in q)
        vector_hits = sum(1 for kw in VECTOR_KEYWORDS if kw in q)
        score += 0.10 * min(3, hybrid_hits + vector_hits)
    elif route == "VECTOR":
        # Count vector keyword matches
        keyword_hits = sum(1 for kw in VECTOR_KEYWORDS if kw in q)
        score += 0.10 * min(4, keyword_hits)
    elif route == "SQL":
        # Count SQL keyword matches
        keyword_hits = sum(1 for kw in SQL_KEYWORDS if kw in q)
        maintenance_hits = sum(1 for kw in MAINTENANCE_KEYWORDS if kw in q)
        score += 0.08 * min(4, keyword_hits + maintenance_hits)
    elif route == "CHAT":
        score = 0.55

    return max(0.50, min(0.85, score))
