from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class SyncStats:
    entities_seen: int = 0
    chunks_written: int = 0
    entities_tombstoned: int = 0
    errors: int = 0
    started_at: datetime = field(default_factory=datetime.utcnow)
