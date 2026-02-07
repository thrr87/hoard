from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from hoard.core.time import utc_now_naive


def write_inbox_entry(
    config: dict,
    *,
    content: str,
    title: Optional[str] = None,
    tags: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    filename: Optional[str] = None,
    extension: str = ".md",
) -> Path:
    inbox_config = config.get("connectors", {}).get("inbox", {})
    inbox_path_raw = inbox_config.get("path")
    if not inbox_path_raw:
        raise ValueError("Inbox path not configured")

    inbox_path = Path(inbox_path_raw).expanduser()
    inbox_path.mkdir(parents=True, exist_ok=True)

    allowed_exts = [ext.lower() for ext in (inbox_config.get("include_extensions") or [])]
    allowed_set = set(allowed_exts)

    resolved_extension = extension if extension.startswith(".") else f".{extension}"
    resolved_extension = resolved_extension.lower()
    if allowed_set and resolved_extension not in allowed_set:
        if ".md" in allowed_set:
            resolved_extension = ".md"
        else:
            resolved_extension = sorted(allowed_set)[0]

    if filename:
        safe_name = _sanitize_filename(filename)
        if not safe_name.endswith(resolved_extension):
            safe_name += resolved_extension
    else:
        slug = _slugify(title or "inbox")
        timestamp = utc_now_naive().strftime("%Y%m%d_%H%M%S")
        safe_name = f"{timestamp}_{slug}{resolved_extension}"

    path = inbox_path / safe_name
    if path.exists():
        stem = path.stem
        suffix = path.suffix
        for idx in range(1, 1000):
            candidate = inbox_path / f"{stem}_{idx}{suffix}"
            if not candidate.exists():
                path = candidate
                break

    body = content
    frontmatter = {}
    if tags:
        frontmatter["tags"] = tags
    if metadata:
        frontmatter["metadata"] = metadata

    if frontmatter:
        yaml_text = yaml.safe_dump(frontmatter, sort_keys=False, allow_unicode=False).strip()
        body = f"---\n{yaml_text}\n---\n\n{content}".rstrip() + "\n"

    path.write_text(body, encoding="utf-8")
    return path


def _slugify(value: str) -> str:
    cleaned = value.lower().strip()
    cleaned = re.sub(r"[^a-z0-9]+", "-", cleaned)
    cleaned = cleaned.strip("-")
    return cleaned[:60] or "inbox"


def _sanitize_filename(value: str) -> str:
    name = Path(value).name
    name = name.strip().replace(" ", "_")
    name = re.sub(r"[^a-zA-Z0-9._-]", "_", name)
    return name
