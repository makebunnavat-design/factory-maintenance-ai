#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared LLM client for router/rewrite tasks."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import requests
from requests.exceptions import ConnectionError, Timeout

from core.config import (
    OLLAMA_GENERATE_URL,
    ROUTER_API_KEY,
    ROUTER_API_TIMEOUT,
    ROUTER_API_URL,
    ROUTER_LLM_PROVIDER,
    ROUTER_MODEL,
    ROUTER_TIMEOUT,
)


logger = logging.getLogger("[ROUTER_LLM]")


def call_router_llm(
    prompt: str,
    *,
    temperature: float = 0.1,
    max_tokens: int = 64,
    top_p: float = 0.9,
    timeout: Optional[int] = None,
) -> str:
    """
    Call router LLM using configured provider.

    Supported providers:
    - ollama: use /api/generate (default)
    - openai_compat / scb10x: use Chat Completions compatible endpoint
    """
    if not prompt or not str(prompt).strip():
        return ""

    provider = str(ROUTER_LLM_PROVIDER or "ollama").strip().lower()
    timeout_s = int(timeout or ROUTER_API_TIMEOUT or ROUTER_TIMEOUT or 100)
    timeout_s = max(5, timeout_s)

    if provider in {"openai", "openai_compat", "scb10x"}:
        return _call_openai_compat(
            prompt=prompt,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            timeout_s=timeout_s,
        )

    if provider not in {"ollama", "local_ollama"}:
        logger.warning(
            "[ROUTER_LLM] Unknown provider '%s' -> fallback to ollama",
            provider,
        )
    return _call_ollama(
        prompt=prompt,
        temperature=temperature,
        max_tokens=max_tokens,
        top_p=top_p,
        timeout_s=timeout_s,
    )


def get_router_llm_settings() -> Dict[str, Any]:
    """Expose effective router LLM settings for debug/observability."""
    return {
        "provider": str(ROUTER_LLM_PROVIDER or "ollama").strip().lower(),
        "model": ROUTER_MODEL,
        "api_url": ROUTER_API_URL,
        "timeout_sec": int(ROUTER_API_TIMEOUT or ROUTER_TIMEOUT or 100),
    }


def _call_ollama(
    *,
    prompt: str,
    temperature: float,
    max_tokens: int,
    top_p: float,
    timeout_s: int,
) -> str:
    import requests
    from core.config import OLLAMA_GENERATE_URL, ROUTER_MODEL
    
    try:
        # เรียก Ollama API ตรงๆ
        response = requests.post(
            OLLAMA_GENERATE_URL,
            json={
                "model": ROUTER_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens,
                    "top_p": top_p
                }
            },
            timeout=timeout_s
        )
        
        if response.status_code == 200:
            return response.json().get("response", "").strip()
        else:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text}")
        
    except Exception as exc:
        raise RuntimeError(f"ollama router call failed: {exc}") from exc


def _call_openai_compat(
    *,
    prompt: str,
    temperature: float,
    max_tokens: int,
    top_p: float,
    timeout_s: int,
) -> str:
    url = str(ROUTER_API_URL or "").strip()
    if not url:
        raise RuntimeError("ROUTER_API_URL is empty for openai_compat provider")
    if "chat/completions" not in url:
        url = url.rstrip("/") + "/chat/completions"

    headers = {"Content-Type": "application/json"}
    if ROUTER_API_KEY:
        headers["Authorization"] = f"Bearer {ROUTER_API_KEY}"

    payload = {
        "model": ROUTER_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": float(temperature),
        "top_p": float(top_p),
        "max_tokens": int(max_tokens),
    }

    try:
        res = requests.post(url, json=payload, headers=headers, timeout=timeout_s)
        res.raise_for_status()
        data = res.json()

        choices = data.get("choices") or []
        if choices:
            first = choices[0] or {}
            message = first.get("message") or {}
            content = message.get("content")
            if isinstance(content, str):
                return content.strip()
            if isinstance(content, list):
                parts = []
                for item in content:
                    if isinstance(item, dict):
                        txt = item.get("text")
                        if txt:
                            parts.append(str(txt))
                    elif isinstance(item, str):
                        parts.append(item)
                return " ".join(parts).strip()
            text = first.get("text")
            if text:
                return str(text).strip()

        output_text = data.get("output_text")
        if output_text:
            return str(output_text).strip()

        return ""
    except (Timeout, ConnectionError):
        raise
    except Exception as exc:
        raise RuntimeError(f"openai_compat router call failed: {exc}") from exc
