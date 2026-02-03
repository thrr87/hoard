from __future__ import annotations

from dataclasses import dataclass
import re
from typing import List


@dataclass
class ChunkSpan:
    text: str
    start: int
    end: int


def chunk_text(content: str, max_tokens: int = 400, overlap_tokens: int = 50) -> List[ChunkSpan]:
    if not content:
        return []

    tokens = [(m.group(0), m.start(), m.end()) for m in re.finditer(r"\S+", content)]
    if not tokens:
        return []

    chunks: List[ChunkSpan] = []
    step = max(max_tokens - overlap_tokens, 1)

    for i in range(0, len(tokens), step):
        window = tokens[i : i + max_tokens]
        if not window:
            continue
        start = window[0][1]
        end = window[-1][2]
        text = content[start:end]
        chunks.append(ChunkSpan(text=text, start=start, end=end))

        if i + max_tokens >= len(tokens):
            break

    return chunks
