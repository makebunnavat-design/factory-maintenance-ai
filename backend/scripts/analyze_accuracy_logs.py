#!/usr/bin/env python3
"""
Analyze QA and observability logs for accuracy/flexibility regressions.

Run:
    python backend/scripts/analyze_accuracy_logs.py
"""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


BASE_DIR = Path(__file__).resolve().parent.parent
QA_LOG_PATH = BASE_DIR / "data" / "qa_log.jsonl"
OBS_LOG_PATH = BASE_DIR / "data" / "ai_observability.jsonl"

DEFINITION_HINTS = ("คืออะไร", "what is", "what's", "meaning", "ทำไม", "why")
RANKING_HINTS = ("มากที่สุด", "น้อยที่สุด", "เร็วที่สุด", "ช้าที่สุด", "อันดับ", "top", "best", "worst")
METRIC_HINTS = (
    "นาที",
    "minute",
    "minutes",
    "ครั้ง",
    "count",
    "จำนวน",
    "repairminutes",
    "responseminutes",
    "เวลา",
    "time",
)


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []

    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def normalize_text(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


def display_text(text: str, limit: int = 160) -> str:
    compact = " ".join(str(text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def as_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def percentile(values: Iterable[int], p: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return float(ordered[0])
    idx = max(0.0, min(1.0, p)) * (len(ordered) - 1)
    low = int(idx)
    high = min(low + 1, len(ordered) - 1)
    frac = idx - low
    return ordered[low] + ((ordered[high] - ordered[low]) * frac)


def print_counter(title: str, counter: Counter[str], limit: int = 5) -> None:
    print(title)
    if not counter:
        print("  - none")
        return
    for key, count in counter.most_common(limit):
        print(f"  - {count:>4}  {key}")


def analyze_qa(records: List[Dict[str, Any]]) -> None:
    print("== QA Log ==")
    print(f"records: {len(records)}")

    exception_counter: Counter[str] = Counter()
    debug_type_counter: Counter[str] = Counter()
    repeated: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    broad_definition_sql: List[Tuple[str, int, str]] = []
    ambiguous_ranking_without_clarify: List[Tuple[str, str, int]] = []

    for record in records:
        question = str(record.get("question") or "").strip()
        if question:
            repeated[normalize_text(question)].append(record)

        debug = record.get("debug") or {}
        debug_type = str(debug.get("type") or ("sql" if record.get("sql") else "unknown"))
        debug_type_counter[debug_type] += 1

        if debug_type == "chat_fallback_from_exception":
            original_error = str(debug.get("original_error") or "unknown error")
            exception_counter[original_error] += 1

        row_count = as_int(debug.get("row_count")) or 0
        sql = str(record.get("sql") or "").strip()
        lowered = question.lower()

        if sql and row_count >= 20 and any(hint in lowered for hint in DEFINITION_HINTS):
            broad_definition_sql.append((question, row_count, sql[:160]))

        if any(hint in lowered for hint in RANKING_HINTS) and not any(metric in lowered for metric in METRIC_HINTS):
            if debug_type != "clarification":
                ambiguous_ranking_without_clarify.append((question, debug_type, row_count))

    print_counter("debug types:", debug_type_counter, limit=10)
    print_counter("top exception fallbacks:", exception_counter, limit=10)

    divergent_questions: List[Tuple[str, int, int, int, List[int], List[str]]] = []
    for _, items in repeated.items():
        if len(items) < 2:
            continue
        sample_question = str(items[0].get("question") or "").strip()
        sql_variants = {str(item.get("sql") or "").strip() for item in items}
        row_count_variants = {
            as_int((item.get("debug") or {}).get("row_count")) or 0
            for item in items
        }
        debug_types = {
            str((item.get("debug") or {}).get("type") or ("sql" if item.get("sql") else "unknown"))
            for item in items
        }
        if len(sql_variants) > 1 or len(row_count_variants) > 1 or len(debug_types) > 1:
            divergent_questions.append(
                (
                    sample_question,
                    len(items),
                    len(sql_variants),
                    len(row_count_variants),
                    sorted(row_count_variants),
                    sorted(debug_types),
                )
            )

    divergent_questions.sort(key=lambda item: (-item[1], -item[2], item[0]))
    print("repeated questions with divergent behavior:")
    if not divergent_questions:
        print("  - none")
    else:
        for question, seen, sql_variants, row_variants, row_counts, debug_types in divergent_questions[:10]:
            print(
                f"  - seen={seen}, sql_variants={sql_variants}, row_variants={row_variants} :: "
                f"{display_text(question)} :: rows={row_counts[:6]} :: types={debug_types[:4]}"
            )

    print("definition-like questions that still hit broad SQL:")
    if not broad_definition_sql:
        print("  - none")
    else:
        for question, row_count, sql_snippet in broad_definition_sql[:10]:
            print(f"  - rows={row_count:>4} :: {display_text(question)} :: {display_text(sql_snippet)}")

    print("ranking questions without a metric-specific clarification:")
    if not ambiguous_ranking_without_clarify:
        print("  - none")
    else:
        seen_questions = set()
        for question, debug_type, row_count in ambiguous_ranking_without_clarify:
            normalized = normalize_text(question)
            if normalized in seen_questions:
                continue
            seen_questions.add(normalized)
            print(f"  - type={debug_type}, rows={row_count:>4} :: {display_text(question)}")


def analyze_observability(records: List[Dict[str, Any]]) -> None:
    print("\n== Observability Log ==")
    print(f"records: {len(records)}")

    pipeline_latencies: Dict[str, List[int]] = defaultdict(list)
    repeated_queries: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for record in records:
        pipeline = str(record.get("pipeline") or "UNKNOWN")
        latency_ms = as_int(record.get("latency_ms"))
        if latency_ms is not None:
            pipeline_latencies[pipeline].append(latency_ms)

        query = str(record.get("query") or "").strip()
        if query:
            repeated_queries[normalize_text(query)].append(record)

    print("pipeline latency summary:")
    if not pipeline_latencies:
        print("  - none")
    else:
        for pipeline in sorted(pipeline_latencies):
            values = pipeline_latencies[pipeline]
            print(
                f"  - {pipeline}: count={len(values)}, avg={round(sum(values) / len(values), 1)} ms, "
                f"p95={round(percentile(values, 0.95), 1)} ms, max={max(values)} ms"
            )

    inconsistent_queries: List[Tuple[str, int, List[int], List[str]]] = []
    for _, items in repeated_queries.items():
        if len(items) < 2:
            continue
        question = str(items[0].get("query") or "").strip()
        result_counts = sorted({as_int(item.get("result_count")) or 0 for item in items})
        pipelines = sorted({str(item.get("pipeline") or "UNKNOWN") for item in items})
        if len(result_counts) > 1 or len(pipelines) > 1:
            inconsistent_queries.append((question, len(items), result_counts, pipelines))

    inconsistent_queries.sort(key=lambda item: (-item[1], item[0]))
    print("repeated observability queries with drifting results/routes:")
    if not inconsistent_queries:
        print("  - none")
    else:
        for question, seen, result_counts, pipelines in inconsistent_queries[:10]:
            print(
                f"  - seen={seen}, results={result_counts[:6]}, pipelines={pipelines[:4]} :: {display_text(question)}"
            )


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    qa_records = load_jsonl(QA_LOG_PATH)
    obs_records = load_jsonl(OBS_LOG_PATH)

    if not qa_records and not obs_records:
        raise SystemExit("No QA or observability logs found to analyze.")

    analyze_qa(qa_records)
    analyze_observability(obs_records)


if __name__ == "__main__":
    main()
