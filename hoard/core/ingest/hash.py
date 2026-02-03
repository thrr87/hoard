from __future__ import annotations

import hashlib


def compute_content_hash(content: str) -> str:
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    return digest[:32]
