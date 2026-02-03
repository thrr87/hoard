from __future__ import annotations

from dataclasses import dataclass
from typing import List

from hoard.core.ingest.chunking import ChunkSpan, chunk_text

__all__ = ["ChunkSpan", "chunk_plain_text"]


def chunk_plain_text(content: str, max_tokens: int = 400, overlap_tokens: int = 50) -> List[ChunkSpan]:
    return chunk_text(content, max_tokens=max_tokens, overlap_tokens=overlap_tokens)
