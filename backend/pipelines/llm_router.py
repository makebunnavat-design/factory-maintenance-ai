#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Unified LLM router for intent, pipeline, and target database."""

from __future__ import annotations

import logging
import re
import pandas as pd
from dataclasses import asdict, dataclass
from typing import Any, Dict, Tuple

from requests.exceptions import ConnectionError, Timeout

from core.config import ROUTER_TIMEOUT
from pipelines.query_preprocessing import (
    determine_target_database,
    normalize_user_query,
)
from pipelines.router_llm_client import call_router_llm
from pipelines.vector_router import _fallback_route, _fallback_confidence


logger = logging.getLogger("[UNIFIED_ROUTER]")

ALLOWED_INTENTS = {"CHAT", "SQL", "VECTOR", "HYBRID"}
ALLOWED_PIPELINES = {"CHAT", "SQL", "VECTOR", "HYBRID"}
ALLOWED_TARGET_DBS = {"PM", "REPAIR"}
UNIFIED_ROUTER_TIMEOUT = 100  # As requested by user

_UNIFIED_ROUTER_PROMPT = """You are a factory maintenance AI router for "Elin" with ENHANCED DATABASE KNOWLEDGE.
Your task is to classify the intent, select the pipeline, choose the database, and rewrite the query in ONE step.

DATABASE ENTITY KNOWLEDGE:
LINES (Production Lines): {lines_context}
PROCESSES (Manufacturing Steps): {processes_context}  
TECHNICIANS (Repair Staff): {techs_context}

CRITICAL MAINTENANCE DETECTION RULES:
🔧 ALWAYS use SQL pipeline when query contains:
   - Entity names (PCB, MOPF, LED, technician names, line names) + action words
   - Counting questions: "กี่ครั้ง", "กี่นาที", "how many", "count"
   - Data requests: "เสียบ้าง", "ซ่อมอะไร", "มีปัญหา", "แสดงข้อมูล", "รายการ"
   - History queries: "ประวัติ", "history", "เมื่อวาน", "วันนี้", "สัปดาห์"
   - Status queries: "broken", "repair", "fix", "problem", "issue"

🔧 ENTITY PATTERNS (ALWAYS SQL for data queries):
   - PCB (any variant): Production line data query → SQL + REPAIR
   - MOPF1/MOPF2: Production line data query → SQL + REPAIR  
   - LED Manual/Auto: Production line data query → SQL + REPAIR
   - Technician names: Staff performance query → SQL + REPAIR
   - Line names: Equipment status query → SQL + REPAIR

🔧 CHAT pipeline ONLY for:
   - Pure greetings without entities: "สวัสดี", "hello"
   - General definitions WITHOUT data request: "PCB คืออะไร" (but NOT "PCB เสียกี่ครั้ง")
   - Casual conversation without maintenance context

OUTPUT FORMAT (JSON-like, exactly 3 lines):
PIPELINE: <CHAT|SQL|VECTOR|HYBRID>
DB: <PM|REPAIR>
CONTENT: <Rewritten query or Chat response>

DEFINITIONS:
1. PIPELINE:
   - CHAT: Pure greetings, definitions WITHOUT data requests, casual talk.
   - SQL: ALL data queries, counting, listing, history, status checks with entities.
   - VECTOR: "HOW to fix", "WHY it broke", "Root cause", "Recommendations" (knowledge-based).
   - HYBRID: Combination of data + knowledge (rare).

2. DB:
   - PM: Preventive Maintenance tasks, plans, schedules, due dates.
   - REPAIR: Machine breakdowns, repair history, technicians, production lines.

3. CONTENT:
   - For CHAT: A friendly Thai response as Elin (แทนตัวเองว่าหนู, เรียกผู้ใช้ว่าพี่).
   - For SQL/VECTOR/HYBRID: A concise natural language summary in English.
     *ENTITY ENHANCEMENT*: Include detected entities like "PCB lines (all variants)", "technician [name]".

EXAMPLES:
❌ WRONG: "PCB คืออะไร" → CHAT (definition only)
✅ CORRECT: "PCB เสียกี่ครั้ง" → SQL (data query with entity)
✅ CORRECT: "ช่างสมชายซ่อมอะไร" → SQL (technician + action)
✅ CORRECT: "MOPF1 มีปัญหาไหม" → SQL (line + status query)
✅ CORRECT: "วันนี้เสียบ้าง" → SQL (general data query)

User Query: "{query}"

Response:"""

@dataclass
class RouteDecision:
    intent: str
    pipeline: str
    target_db: str
    normalized_for_routing: str
    normalized_query: str
    rewritten_query: str
    confidence: float
    source: str
    metadata: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def route_message_with_llm(clean_message: str, message_with_date_context: str) -> RouteDecision:
    """
    Enhanced route with pre-processing entity detection and maintenance context.
    
    1. Pre-process: Detect entities and add maintenance context
    2. Normalize query
    3. Call Unified Router LLM with enhanced context
    4. Post-process: Apply deterministic rules if needed
    5. Final normalize based on DB
    """
    # Step 1: Pre-process entity detection and maintenance context
    enhanced_message, detected_entities = _preprocess_maintenance_context(message_with_date_context)
    
    # Step 2: Normalize query for routing
    normalized_for_routing = normalize_user_query(enhanced_message, target_db=None)
    
    # Step 3: Unified LLM Call with database context
    pipeline, target_db, rewritten_query, source, confidence = _call_unified_router_with_context(normalized_for_routing)
    
    # Step 4: Post-process with deterministic rules if LLM made wrong decision
    pipeline, target_db, confidence, source = _apply_maintenance_override_rules(
        clean_message, detected_entities, pipeline, target_db, confidence, source
    )
    
    # Step 5: Normalize query with the selected target database
    normalized_query = normalize_user_query(message_with_date_context, target_db=target_db)
    
    # Step 6: Map Intent (usually matches pipeline for data queries)
    intent = pipeline if pipeline in {"SQL", "VECTOR", "HYBRID"} else "SQL"
    if pipeline == "CHAT":
        intent = "CHAT"

    decision = RouteDecision(
        intent=intent,
        pipeline=pipeline,
        target_db=target_db,
        normalized_for_routing=normalized_for_routing,
        normalized_query=normalized_query,
        rewritten_query=rewritten_query if pipeline != "CHAT" else "",
        confidence=confidence,
        source=source,
        metadata={
            "raw_content": rewritten_query if pipeline == "CHAT" else "unified_rewritten",
            "timeout_setting": UNIFIED_ROUTER_TIMEOUT,
            "database_context_used": True,
            "detected_entities": detected_entities,
            "maintenance_override_applied": "override" in source
        },
    )
    
    _log_router_decision(clean_message, decision)
    
    logger.info(
        "[UNIFIED_ROUTER] intent=%s pipeline=%s target_db=%s confidence=%.3f source=%s entities=%s",
        decision.intent,
        decision.pipeline,
        decision.target_db,
        decision.confidence,
        decision.source,
        detected_entities
    )
    
    return decision


def _call_unified_router_with_context(query: str) -> Tuple[str, str, str, str, float]:
    """Enhanced unified router that includes database entity knowledge."""
    # Get database context
    db_context = _get_database_context()
    
    # Format context strings
    lines_context = ", ".join(db_context.get("lines", [])[:10]) + ("..." if len(db_context.get("lines", [])) > 10 else "")
    processes_context = ", ".join(db_context.get("processes", [])[:8]) + ("..." if len(db_context.get("processes", [])) > 8 else "")
    techs_context = ", ".join(db_context.get("techs", [])[:8]) + ("..." if len(db_context.get("techs", [])) > 8 else "")
    
    # Build enhanced prompt with database knowledge
    prompt = _UNIFIED_ROUTER_PROMPT.format(
        query=query,
        lines_context=lines_context,
        processes_context=processes_context,
        techs_context=techs_context
    )
    
    try:
        raw = call_router_llm(
            prompt,
            temperature=0.1,
            max_tokens=256,
            top_p=0.9,
            timeout=UNIFIED_ROUTER_TIMEOUT,
        ).strip()
        logger.info("[UNIFIED_ROUTER] Raw response with DB context: %s", raw)
        
        pipeline, db, content = _parse_unified_response(raw)
        
        # Validation and mapping
        if pipeline not in ALLOWED_PIPELINES:
            pipeline = "SQL"
        if db not in ALLOWED_TARGET_DBS:
            db = "REPAIR"
            
        return pipeline, db, content, "unified_llm_with_db", 0.95
        
    except Exception as e:
        logger.warning("[UNIFIED_ROUTER] Enhanced unified call failed: %s, falling back to original", e, exc_info=True)
        # Fallback to original unified router
        return _call_unified_router(query)


def _preprocess_maintenance_context(message: str) -> Tuple[str, Dict[str, Any]]:
    """
    Pre-process message to detect entities and add maintenance context.
    Returns enhanced message and detected entities info.
    """
    detected_entities = {
        "lines": [],
        "processes": [],
        "techs": [],
        "maintenance_keywords": [],
        "is_maintenance_query": False
    }
    
    enhanced_message = message
    db_context = _get_database_context()
    
    # Detect maintenance keywords
    maintenance_keywords = [
        "เสีย", "ซ่อม", "แก้ไข", "ปัญหา", "สาเหตุ", "เครื่อง", "ไลน์", "ช่าง",
        "repair", "fix", "broken", "problem", "issue", "machine", "line", "tech",
        "กี่ครั้ง", "กี่นาที", "ประวัติ", "รายการ", "แสดง", "ดู", "เช็ค",
        "count", "minutes", "history", "list", "show", "check",
        # เพิ่ม production line keywords
        "PCB", "MOPF", "LED", "TOYOTA", "AIRBAG", "ECU", "SDA"
    ]
    
    for keyword in maintenance_keywords:
        if keyword in message.lower():
            detected_entities["maintenance_keywords"].append(keyword)
            detected_entities["is_maintenance_query"] = True
    
    # Detect specific entities
    message_upper = message.upper()
    
    # Line detection with enhanced patterns
    lines_found = []
    
    # PCB pattern detection
    if "PCB" in message_upper:
        lines_found.append("PCB_PATTERN")
        detected_entities["is_maintenance_query"] = True
        # Add specific PCB context
        enhanced_message += "\n[MAINTENANCE_CONTEXT: PCB production lines query - use LIKE '%PCB%' pattern for comprehensive coverage]"
    
    # MOPF pattern detection  
    if "MOPF" in message_upper:
        if "MOPF1" in message_upper or "MOPF 1" in message_upper:
            lines_found.append("MOPF1")
        elif "MOPF2" in message_upper or "MOPF 2" in message_upper:
            lines_found.append("MOPF2")
        else:
            lines_found.append("MOPF_PATTERN")
        detected_entities["is_maintenance_query"] = True
        enhanced_message += "\n[MAINTENANCE_CONTEXT: MOPF production lines query]"
    
    # LED pattern detection
    if "LED" in message_upper:
        if "MANUAL" in message_upper or "MANUL" in message_upper:
            lines_found.append("LED_MANUAL")
        elif "AUTO" in message_upper:
            lines_found.append("LED_AUTO")
        else:
            lines_found.append("LED_PATTERN")
        detected_entities["is_maintenance_query"] = True
        enhanced_message += "\n[MAINTENANCE_CONTEXT: LED production lines query]"
    
    # Specific line name detection
    for line_name in db_context.get("lines", []):
        if line_name.upper() in message_upper:
            lines_found.append(line_name)
            detected_entities["is_maintenance_query"] = True
    
    detected_entities["lines"] = lines_found
    
    # Process detection
    processes_found = []
    for process_name in db_context.get("processes", []):
        if process_name.upper() in message_upper:
            processes_found.append(process_name)
            detected_entities["is_maintenance_query"] = True
    detected_entities["processes"] = processes_found
    
    # Technician detection
    techs_found = []
    for tech_name in db_context.get("techs", []):
        # Check if any part of tech name is in message
        tech_parts = tech_name.split()
        for part in tech_parts:
            if len(part) >= 3 and part in message:
                techs_found.append(tech_name)
                detected_entities["is_maintenance_query"] = True
                enhanced_message += f"\n[MAINTENANCE_CONTEXT: Technician '{tech_name}' query]"
                break
    detected_entities["techs"] = techs_found
    
    # Add general maintenance context if entities detected
    if detected_entities["is_maintenance_query"]:
        context_parts = []
        if lines_found:
            context_parts.append(f"Lines: {', '.join(lines_found[:3])}")
        if processes_found:
            context_parts.append(f"Processes: {', '.join(processes_found[:3])}")
        if techs_found:
            context_parts.append(f"Technicians: {', '.join(techs_found[:2])}")
        
        if context_parts:
            enhanced_message += f"\n[DETECTED_ENTITIES: {' | '.join(context_parts)}]"
    
    logger.info(f"[ROUTER_PREPROCESS] Entities detected: {detected_entities}")
    
    return enhanced_message, detected_entities


def _apply_maintenance_override_rules(
    original_message: str, 
    detected_entities: Dict[str, Any], 
    pipeline: str, 
    target_db: str, 
    confidence: float, 
    source: str
) -> Tuple[str, str, float, str]:
    """
    Apply deterministic override rules for maintenance queries that LLM might misclassify.
    """
    # Rule 1: If entities detected but pipeline is CHAT, override to SQL
    if detected_entities["is_maintenance_query"] and pipeline == "CHAT":
        logger.info("[ROUTER_OVERRIDE] Maintenance entities detected, overriding CHAT -> SQL")
        return "SQL", "REPAIR", 0.95, f"{source}_maintenance_override"
    
    # Rule 2: If maintenance keywords + entities, ensure SQL pipeline
    if (detected_entities["maintenance_keywords"] and 
        (detected_entities["lines"] or detected_entities["processes"] or detected_entities["techs"])):
        if pipeline == "CHAT":
            logger.info("[ROUTER_OVERRIDE] Maintenance keywords + entities, overriding CHAT -> SQL")
            return "SQL", "REPAIR", 0.95, f"{source}_keyword_override"
    
    # Rule 3: Specific patterns that should always be SQL
    message_lower = original_message.lower()
    sql_indicators = [
        "กี่ครั้ง", "กี่นาที", "เสียบ้าง", "ซ่อมอะไร", "มีปัญหา", "แสดงข้อมูล",
        "รายการ", "ประวัติ", "เช็ค", "ดู", "count", "minutes", "broken", "repair"
    ]
    
    for indicator in sql_indicators:
        if indicator in message_lower and (detected_entities["lines"] or detected_entities["techs"]):
            if pipeline == "CHAT":
                logger.info(f"[ROUTER_OVERRIDE] SQL indicator '{indicator}' + entities, overriding CHAT -> SQL")
                return "SQL", "REPAIR", 0.90, f"{source}_pattern_override"
    
    # Rule 4: Production line keywords should always be SQL
    production_keywords = ["PCB", "MOPF", "LED", "TOYOTA", "AIRBAG", "ECU", "SDA"]
    for keyword in production_keywords:
        if keyword.upper() in original_message.upper():
            if pipeline == "CHAT":
                logger.info(f"[ROUTER_OVERRIDE] Production line keyword '{keyword}', overriding CHAT -> SQL")
                return "SQL", "REPAIR", 0.95, f"{source}_production_override"
    
    # Rule 5: PM-related queries
    pm_keywords = ["pm", "preventive", "maintenance", "แผน", "กำหนดการ", "due"]
    for keyword in pm_keywords:
        if keyword in message_lower:
            if pipeline == "CHAT":
                logger.info(f"[ROUTER_OVERRIDE] PM keyword '{keyword}', overriding CHAT -> SQL")
                return "SQL", "PM", 0.90, f"{source}_pm_override"
            elif target_db == "REPAIR":
                logger.info(f"[ROUTER_OVERRIDE] PM keyword '{keyword}', changing DB REPAIR -> PM")
                return pipeline, "PM", confidence, f"{source}_pm_db_override"
    
    # No override needed
    return pipeline, target_db, confidence, source
    """Get database context from global db_context or load minimal context."""
    try:
        # Try to import db_context from main module
        import sys
        if 'main' in sys.modules:
            main_module = sys.modules['main']
            if hasattr(main_module, 'db_context'):
                return main_module.db_context
        
        # Fallback: load minimal context directly
        from core.database import get_work_db_readonly
        import pandas as pd
        
        context = {"lines": [], "processes": [], "techs": []}
        
        try:
            with get_work_db_readonly() as conn:
                # Get lines
                try:
                    lines_df = pd.read_sql_query('SELECT DISTINCT Line FROM repairs_enriched WHERE Line IS NOT NULL', conn)
                    context["lines"] = sorted([str(x) for x in lines_df['Line'].dropna().unique() if len(str(x)) > 1])
                except:
                    pass
                
                # Get processes  
                try:
                    proc_df = pd.read_sql_query('SELECT DISTINCT Process FROM repairs_enriched WHERE Process IS NOT NULL', conn)
                    context["processes"] = sorted([str(x) for x in proc_df['Process'].dropna().unique() if len(str(x)) > 1])
                except:
                    pass
                
                # Get techs
                try:
                    tech_df = pd.read_sql_query('SELECT DISTINCT Tech FROM repairs_enriched WHERE Tech IS NOT NULL AND Tech != "" AND Tech != "Unknown"', conn)
                    techs_raw = [str(x).strip() for x in tech_df['Tech'].dropna().unique()]
                    # Filter out English-only names and common exclusions
                    TECH_EXCLUDE = {"KUSOL", "POOLAWAT Suphakson", "Support", "SupportA", "อนุวัฒน์ ลบออก", "อนุวัฒน์"}
                    context["techs"] = sorted([t for t in techs_raw if t and t not in TECH_EXCLUDE and not t.replace(" ", "").isascii()])
                except:
                    pass
                    
        except Exception as e:
            logger.warning(f"[UNIFIED_ROUTER] Failed to load database context: {e}")
            
        return context
        
    except Exception as e:
        logger.warning(f"[UNIFIED_ROUTER] Database context loading failed: {e}")
        return {"lines": [], "processes": [], "techs": []}


def _call_unified_router(query: str) -> Tuple[str, str, str, str, float]:
    """Consolidated implementation that makes 1 LLM call instead of 3."""
    # Use basic prompt without database context for fallback
    basic_prompt = """You are a factory maintenance AI router for "Elin".
Your task is to classify the intent, select the pipeline, choose the database, and rewrite the query in ONE step.

OUTPUT FORMAT (JSON-like, exactly 3 lines):
PIPELINE: <CHAT|SQL|VECTOR|HYBRID>
DB: <PM|REPAIR>
CONTENT: <Rewritten query or Chat response>

DEFINITIONS:
1. PIPELINE:
   - CHAT: Greetings, soft talk, or non-data questions.
   - SQL: Use this for ALL queries that ask to LIST items, COUNT records, show HISTORY, or get SPECIFIC data fields (e.g., "What is broken?", "Who fixed X?").
   - VECTOR: Use this ONLY for asking "HOW to fix", "WHY it broke", "Root cause", "Recommendations", or "Maintenance instructions".
   - HYBRID: Use this if the user asks for BOTH specific stats AND a reason/how-to.

2. DB:
   - PM: Preventive Maintenance tasks, plans, due dates.
   - REPAIR: Machine breakdowns, repair history, technicians.

3. CONTENT:
   - For CHAT: A friendly Thai response as Elin (แทนตัวเองว่าหนู, เรียกผู้ใช้ว่าพี่).
   - For SQL/VECTOR/HYBRID: A concise natural language summary of the user's intent in English.
     *CRITICAL*: DO NOT write SQL, Code, or "SELECT * FROM ...". Just describe what the user wants to know.

RULES:
- If the user asks for a definition or explanation of a term (e.g., "What is PM?", "การทำ PM คืออะไร"), ALWAYS use CHAT or VECTOR.
- If use asks "What is broken?" (อะไรเสียบ้าง) or "Show records", ALWAYS use SQL.
- If unsure of DB, default to REPAIR.
- If unsure of PIPELINE, default to SQL.
- If it's a greeting, PIPELINE is CHAT.
- If high-level summary is needed, ALWAYS write it in Thai (ภาษาไทย).
- NEVER generate SQL or database table names in the CONTENT field.

User Query: "{query}"

Response:"""
    
    prompt = basic_prompt.format(query=query)
    
    try:
        raw = call_router_llm(
            prompt,
            temperature=0.1,
            max_tokens=256,
            top_p=0.9,
            timeout=UNIFIED_ROUTER_TIMEOUT,
        ).strip()
        logger.info("[UNIFIED_ROUTER] Raw response: %s", raw)
        
        pipeline, db, content = _parse_unified_response(raw)
        
        # Validation and mapping
        if pipeline not in ALLOWED_PIPELINES:
            pipeline = "SQL"
        if db not in ALLOWED_TARGET_DBS:
            db = "REPAIR"
            
        return pipeline, db, content, "unified_llm", 0.90
        
    except Exception as e:
        logger.warning("[UNIFIED_ROUTER] Unified call failed: %s, using keyword fallback", e, exc_info=True)
        # Fallback to deterministic logic
        try:
            from pipelines.vector_router import _fallback_route, _fallback_confidence
            from pipelines.query_preprocessing import determine_target_database
            
            pipeline = _fallback_route(query)
            confidence = _fallback_confidence(query, pipeline)
            target_db = determine_target_database(query)
            
            return pipeline, target_db, query, "keyword_fallback", confidence
        except Exception as fe:
            logger.error("[UNIFIED_ROUTER] Ultimate fallback failed: %s", fe)
            return "SQL", "REPAIR", query, "failed_total", 0.50


def _parse_unified_response(raw: str) -> Tuple[str, str, str]:
    """Parse the specific multi-line format from the unified prompt."""
    pipeline = "SQL"
    db = "REPAIR"
    content = ""
    
    lines = raw.split('\n')
    for line in lines:
        upper_line = line.upper().strip()
        if upper_line.startswith("PIPELINE:"):
            pipeline = upper_line.replace("PIPELINE:", "").strip()
        elif upper_line.startswith("DB:"):
            db = upper_line.replace("DB:", "").strip()
        elif upper_line.startswith("CONTENT:"):
            # Content might have its own casing, don't use upper_line
            content = line[8:].strip()
            
    # Cleaning
    pipeline = re.sub(r'[^A-Z]', '', pipeline)
    db = re.sub(r'[^A-Z]', '', db)
    
    return pipeline, db, content


def _log_router_decision(query: str, decision: RouteDecision) -> None:
    """Best-effort observability hook."""
    try:
        from observability import log_router_decision
        log_router_decision(
            query=query,
            selected_pipeline=decision.pipeline,
            confidence=decision.confidence,
            matched_keywords={
                "intent": decision.intent,
                "target_db": decision.target_db,
                "source": decision.source,
            },
        )
    except Exception as exc:
        logger.debug("[UNIFIED_ROUTER] Router decision logging skipped: %s", exc)




def _get_database_context() -> Dict[str, Any]:
    """Get database context from global db_context or load minimal context."""
    try:
        # Try to import db_context from main module
        import sys
        if 'main' in sys.modules:
            main_module = sys.modules['main']
            if hasattr(main_module, 'db_context'):
                return main_module.db_context
        
        # Fallback: load minimal context directly
        from core.database import get_work_db_readonly
        import pandas as pd
        
        context = {"lines": [], "processes": [], "techs": []}
        
        try:
            with get_work_db_readonly() as conn:
                # Get lines
                try:
                    lines_df = pd.read_sql_query('SELECT DISTINCT Line FROM repairs_enriched WHERE Line IS NOT NULL', conn)
                    context["lines"] = sorted([str(x) for x in lines_df['Line'].dropna().unique() if len(str(x)) > 1])
                except:
                    pass
                
                # Get processes  
                try:
                    proc_df = pd.read_sql_query('SELECT DISTINCT Process FROM repairs_enriched WHERE Process IS NOT NULL', conn)
                    context["processes"] = sorted([str(x) for x in proc_df['Process'].dropna().unique() if len(str(x)) > 1])
                except:
                    pass
                
                # Get techs
                try:
                    tech_df = pd.read_sql_query('SELECT DISTINCT Tech FROM repairs_enriched WHERE Tech IS NOT NULL AND Tech != "" AND Tech != "Unknown"', conn)
                    techs_raw = [str(x).strip() for x in tech_df['Tech'].dropna().unique()]
                    # Filter out English-only names and common exclusions
                    TECH_EXCLUDE = {"KUSOL", "POOLAWAT Suphakson", "Support", "SupportA", "อนุวัฒน์ ลบออก", "อนุวัฒน์"}
                    context["techs"] = sorted([t for t in techs_raw if t and t not in TECH_EXCLUDE and not t.replace(" ", "").isascii()])
                except:
                    pass
                    
        except Exception as e:
            logger.warning(f"[UNIFIED_ROUTER] Failed to load database context: {e}")
            
        return context
        
    except Exception as e:
        logger.warning(f"[UNIFIED_ROUTER] Database context loading failed: {e}")
        return {"lines": [], "processes": [], "techs": []}