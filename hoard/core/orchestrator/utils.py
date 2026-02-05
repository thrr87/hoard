from __future__ import annotations

import json
from datetime import datetime
from typing import Any


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="milliseconds")


def dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value)
