from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class EntityInput:
    source: str
    source_id: str
    entity_type: str
    title: str
    uri: Optional[str] = None
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None
    sensitivity: str = "normal"
    content_hash: Optional[str] = None
    mime_type: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    connector_name: Optional[str] = None
    connector_version: Optional[str] = None


@dataclass
class ChunkInput:
    content: str
    chunk_type: str = "semantic"
    char_offset_start: Optional[int] = None
    char_offset_end: Optional[int] = None


@dataclass
class DiscoverResult:
    success: bool
    message: str = ""
    entity_count_estimate: Optional[int] = None
    source_info: Optional[Dict[str, Any]] = None
