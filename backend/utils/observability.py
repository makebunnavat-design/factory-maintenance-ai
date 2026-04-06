"""
Observability and Evaluation Logging for Elin AI

This module provides logging functionality to track:
- Query execution across all pipelines (SQL, Vector, Hybrid)
- Performance metrics (latency, retrieved documents)
- Errors and failures
- Pipeline selection decisions

Logs are stored in JSONL format for easy analysis.
"""

import json
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from contextlib import contextmanager


# Log file path - Use /app/data in container
from core.config import BASE_DIR
LOG_FILE = Path(BASE_DIR) / "data" / "ai_observability.jsonl"


def log_event(event: dict) -> None:
    """
    Log an event to the observability log file.
    
    Args:
        event: Dictionary containing event data
        
    Event structure:
        {
            "timestamp": ISO format timestamp,
            "query": User query string,
            "pipeline": Pipeline name (SQL/VECTOR/HYBRID),
            "retrieved_docs": Number of documents retrieved,
            "latency_ms": Execution time in milliseconds,
            "error": Error message (optional),
            "metadata": Additional metadata (optional)
        }
    """
    try:
        # Ensure log directory exists
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Add timestamp if not present
        if "timestamp" not in event:
            event["timestamp"] = datetime.utcnow().isoformat()
        
        # Write to JSONL file (one JSON per line)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
            
    except Exception as e:
        # Don't let logging errors crash the application
        print(f"[OBSERVABILITY] Failed to log event: {e}")


def log_query_start(query: str, pipeline: str, metadata: Optional[Dict] = None) -> Dict:
    """
    Log the start of a query execution.
    
    Args:
        query: User query string
        pipeline: Pipeline name (SQL/VECTOR/HYBRID)
        metadata: Additional metadata to log
        
    Returns:
        Context dictionary with start_time for later use
    """
    context = {
        "query": query,
        "pipeline": pipeline,
        "start_time": time.time(),
        "metadata": metadata or {}
    }
    
    event = {
        "timestamp": datetime.utcnow().isoformat(),
        "event_type": "query_start",
        "query": query,
        "pipeline": pipeline,
        "metadata": metadata or {}
    }
    
    log_event(event)
    return context


def log_query_end(
    context: Dict,
    retrieved_docs: Optional[int] = None,
    result_count: Optional[int] = None,
    success: bool = True,
    error: Optional[str] = None,
    metadata: Optional[Dict] = None
) -> None:
    """
    Log the end of a query execution.
    
    Args:
        context: Context dictionary from log_query_start
        retrieved_docs: Number of documents retrieved
        result_count: Number of results returned
        success: Whether execution was successful
        error: Error message if failed
        metadata: Additional metadata to log
    """
    latency_ms = int((time.time() - context["start_time"]) * 1000)
    
    event = {
        "timestamp": datetime.utcnow().isoformat(),
        "event_type": "query_end",
        "query": context["query"],
        "pipeline": context["pipeline"],
        "latency_ms": latency_ms,
        "success": success,
        "retrieved_docs": retrieved_docs,
        "result_count": result_count,
        "metadata": {**context.get("metadata", {}), **(metadata or {})}
    }
    
    if error:
        event["error"] = error
    
    log_event(event)


def log_pipeline_execution(
    query: str,
    pipeline: str,
    latency_ms: int,
    retrieved_docs: Optional[int] = None,
    result_count: Optional[int] = None,
    success: bool = True,
    error: Optional[str] = None,
    metadata: Optional[Dict] = None
) -> None:
    """
    Log a complete pipeline execution.
    
    Args:
        query: User query string
        pipeline: Pipeline name (SQL/VECTOR/HYBRID)
        latency_ms: Execution time in milliseconds
        retrieved_docs: Number of documents retrieved
        result_count: Number of results returned
        success: Whether execution was successful
        error: Error message if failed
        metadata: Additional metadata to log
    """
    event = {
        "timestamp": datetime.utcnow().isoformat(),
        "event_type": "pipeline_execution",
        "query": query,
        "pipeline": pipeline,
        "latency_ms": latency_ms,
        "success": success,
        "retrieved_docs": retrieved_docs,
        "result_count": result_count,
        "metadata": metadata or {}
    }
    
    if error:
        event["error"] = error
    
    log_event(event)


def log_error(
    query: str,
    pipeline: str,
    error: Exception,
    metadata: Optional[Dict] = None
) -> None:
    """
    Log an error that occurred during pipeline execution.
    
    Args:
        query: User query string
        pipeline: Pipeline name (SQL/VECTOR/HYBRID)
        error: Exception that occurred
        metadata: Additional metadata to log
    """
    event = {
        "timestamp": datetime.utcnow().isoformat(),
        "event_type": "error",
        "query": query,
        "pipeline": pipeline,
        "error": str(error),
        "error_type": type(error).__name__,
        "traceback": traceback.format_exc(),
        "metadata": metadata or {}
    }
    
    log_event(event)


def log_router_decision(
    query: str,
    selected_pipeline: str,
    confidence: Optional[float] = None,
    matched_keywords: Optional[Dict] = None
) -> None:
    """
    Log the router's pipeline selection decision.
    
    Args:
        query: User query string
        selected_pipeline: Pipeline selected by router
        confidence: Confidence score (0-1)
        matched_keywords: Keywords that matched
    """
    event = {
        "timestamp": datetime.utcnow().isoformat(),
        "event_type": "router_decision",
        "query": query,
        "selected_pipeline": selected_pipeline,
        "confidence": confidence,
        "matched_keywords": matched_keywords
    }
    
    log_event(event)


@contextmanager
def track_execution(query: str, pipeline: str, metadata: Optional[Dict] = None):
    """
    Context manager to track pipeline execution time and log results.
    
    Usage:
        with track_execution(query, "VECTOR") as tracker:
            results = vector_pipeline(query)
            tracker["retrieved_docs"] = len(results)
    
    Args:
        query: User query string
        pipeline: Pipeline name
        metadata: Additional metadata to log
    """
    start_time = time.time()
    tracker = {
        "query": query,
        "pipeline": pipeline,
        "metadata": metadata or {},
        "retrieved_docs": None,
        "result_count": None
    }
    
    try:
        yield tracker
        
        # Success - log completion
        latency_ms = int((time.time() - start_time) * 1000)
        log_pipeline_execution(
            query=query,
            pipeline=pipeline,
            latency_ms=latency_ms,
            retrieved_docs=tracker.get("retrieved_docs"),
            result_count=tracker.get("result_count"),
            success=True,
            metadata=tracker.get("metadata")
        )
        
    except Exception as e:
        # Error - log failure
        latency_ms = int((time.time() - start_time) * 1000)
        log_pipeline_execution(
            query=query,
            pipeline=pipeline,
            latency_ms=latency_ms,
            retrieved_docs=tracker.get("retrieved_docs"),
            result_count=tracker.get("result_count"),
            success=False,
            error=str(e),
            metadata=tracker.get("metadata")
        )
        raise


def get_log_stats() -> Dict[str, Any]:
    """
    Get statistics from the observability log.
    
    Returns:
        Dictionary with statistics:
        - total_queries: Total number of queries
        - by_pipeline: Count by pipeline type
        - avg_latency: Average latency by pipeline
        - error_rate: Error rate by pipeline
    """
    if not LOG_FILE.exists():
        return {
            "total_queries": 0,
            "by_pipeline": {},
            "avg_latency": {},
            "error_rate": {}
        }
    
    stats = {
        "total_queries": 0,
        "by_pipeline": {},
        "latencies": {},
        "errors": {}
    }
    
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    event = json.loads(line.strip())
                    
                    if event.get("event_type") == "pipeline_execution":
                        pipeline = event.get("pipeline", "unknown")
                        
                        # Count queries
                        stats["total_queries"] += 1
                        stats["by_pipeline"][pipeline] = stats["by_pipeline"].get(pipeline, 0) + 1
                        
                        # Track latencies
                        if pipeline not in stats["latencies"]:
                            stats["latencies"][pipeline] = []
                        if "latency_ms" in event:
                            stats["latencies"][pipeline].append(event["latency_ms"])
                        
                        # Track errors
                        if not event.get("success", True):
                            stats["errors"][pipeline] = stats["errors"].get(pipeline, 0) + 1
                            
                except json.JSONDecodeError:
                    continue
        
        # Calculate averages
        avg_latency = {}
        for pipeline, latencies in stats["latencies"].items():
            if latencies:
                avg_latency[pipeline] = sum(latencies) / len(latencies)
        
        # Calculate error rates
        error_rate = {}
        for pipeline in stats["by_pipeline"]:
            total = stats["by_pipeline"][pipeline]
            errors = stats["errors"].get(pipeline, 0)
            error_rate[pipeline] = (errors / total * 100) if total > 0 else 0
        
        return {
            "total_queries": stats["total_queries"],
            "by_pipeline": stats["by_pipeline"],
            "avg_latency": avg_latency,
            "error_rate": error_rate
        }
        
    except Exception as e:
        print(f"[OBSERVABILITY] Failed to get stats: {e}")
        return {
            "total_queries": 0,
            "by_pipeline": {},
            "avg_latency": {},
            "error_rate": {}
        }


def clear_logs() -> None:
    """Clear the observability log file."""
    try:
        if LOG_FILE.exists():
            LOG_FILE.unlink()
            print(f"[OBSERVABILITY] Cleared log file: {LOG_FILE}")
    except Exception as e:
        print(f"[OBSERVABILITY] Failed to clear logs: {e}")


# Test block
if __name__ == "__main__":
    print("=" * 60)
    print("Testing Observability Logging")
    print("=" * 60)
    
    # Test 1: Simple event logging
    print("\n1. Testing simple event logging...")
    log_event({
        "query": "test query",
        "pipeline": "VECTOR",
        "latency_ms": 100
    })
    print("✓ Simple event logged")
    
    # Test 2: Pipeline execution logging
    print("\n2. Testing pipeline execution logging...")
    log_pipeline_execution(
        query="Line ไหนเสียมากที่สุด",
        pipeline="SQL",
        latency_ms=250,
        retrieved_docs=10,
        result_count=5,
        success=True
    )
    print("✓ Pipeline execution logged")
    
    # Test 3: Error logging
    print("\n3. Testing error logging...")
    try:
        raise ValueError("Test error")
    except Exception as e:
        log_error(
            query="เครื่อง CNC เสียบ่อยเพราะอะไร",
            pipeline="VECTOR",
            error=e
        )
    print("✓ Error logged")
    
    # Test 4: Router decision logging
    print("\n4. Testing router decision logging...")
    log_router_decision(
        query="Line ไหนเสียมากที่สุด และเพราะอะไร",
        selected_pipeline="HYBRID",
        confidence=0.95,
        matched_keywords={"analytics": ["มากที่สุด"], "cause": ["เพราะอะไร"]}
    )
    print("✓ Router decision logged")
    
    # Test 5: Context manager
    print("\n5. Testing context manager...")
    with track_execution("test query", "HYBRID") as tracker:
        time.sleep(0.1)  # Simulate work
        tracker["retrieved_docs"] = 20
        tracker["result_count"] = 5
    print("✓ Context manager logged")
    
    # Test 6: Get statistics
    print("\n6. Testing statistics...")
    stats = get_log_stats()
    print(f"Total queries: {stats['total_queries']}")
    print(f"By pipeline: {stats['by_pipeline']}")
    print(f"Avg latency: {stats['avg_latency']}")
    print(f"Error rate: {stats['error_rate']}")
    print("✓ Statistics retrieved")
    
    print("\n" + "=" * 60)
    print(f"Log file location: {LOG_FILE}")
    print("=" * 60)
    
    # Show sample log entries
    print("\nSample log entries:")
    if LOG_FILE.exists():
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
            for line in lines[-3:]:  # Show last 3 entries
                event = json.loads(line.strip())
                print(f"  {event.get('event_type', 'unknown')}: {event.get('pipeline', 'N/A')} - {event.get('query', 'N/A')[:50]}...")
    
    print("\n✓ All tests passed!")
