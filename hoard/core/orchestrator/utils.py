from __future__ import annotations

import json
from typing import Any

from hoard.core.time import utc_now_naive_iso


def now_iso() -> str:
    return utc_now_naive_iso(timespec="milliseconds")


def dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value)
