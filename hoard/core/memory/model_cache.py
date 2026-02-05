from __future__ import annotations

from typing import Any

_st_cache: dict[str, Any] = {}


def get_sentence_transformer(model_name: str):
    """Return a cached SentenceTransformer instance for the given model name."""
    if model_name not in _st_cache:
        from sentence_transformers import SentenceTransformer
        _st_cache[model_name] = SentenceTransformer(model_name)
    return _st_cache[model_name]
