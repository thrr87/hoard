from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterator, List, Tuple

from hoard.sdk.base import ConnectorV1
from hoard.sdk.hash import compute_content_hash
from hoard.sdk.types import ChunkInput, DiscoverResult, EntityInput


class ChromeBookmarksConnector(ConnectorV1):
    @property
    def name(self) -> str:
        return "bookmarks_chrome"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def source_name(self) -> str:
        return "bookmarks_chrome"

    def discover(self, config: dict) -> DiscoverResult:
        path = _resolve_bookmarks_path(config)
        if not path or not path.exists():
            return DiscoverResult(success=False, message="Chrome bookmarks file not found")

        nodes = list(_iter_bookmark_nodes(_load_bookmarks(path)))
        return DiscoverResult(
            success=True,
            message=f"Found {len(nodes)} bookmarks",
            entity_count_estimate=len(nodes),
        )

    def scan(self, config: dict) -> Iterator[Tuple[EntityInput, List[ChunkInput]]]:
        path = _resolve_bookmarks_path(config)
        if not path or not path.exists():
            return

        data = _load_bookmarks(path)
        for node, folder_path in _iter_bookmark_nodes(data):
            url = node.get("url")
            if not url:
                continue

            title = node.get("name") or url
            source_id = str(node.get("id") or url)
            content = f"{title}\n{url}"

            created_at = _chrome_time(node.get("date_added"))
            updated_at = _chrome_time(node.get("date_modified"))

            entity = EntityInput(
                source=self.source_name,
                source_id=source_id,
                entity_type="bookmark",
                title=title,
                uri=url,
                tags=["bookmark", "chrome"],
                metadata={"folder": folder_path} if folder_path else None,
                content_hash=compute_content_hash(content),
                created_at=created_at,
                updated_at=updated_at,
                connector_name=self.name,
                connector_version=self.version,
            )

            chunk = ChunkInput(content=content)
            yield entity, [chunk]


def _resolve_bookmarks_path(config: dict) -> Path | None:
    if config.get("bookmark_path"):
        return Path(config["bookmark_path"]).expanduser()

    home = Path.home()
    candidates = [
        home / "Library/Application Support/Google/Chrome/Default/Bookmarks",
        home / "Library/Application Support/Google/Chrome/Profile 1/Bookmarks",
        home / "AppData/Local/Google/Chrome/User Data/Default/Bookmarks",
        home / ".config/google-chrome/Default/Bookmarks",
        home / ".config/chromium/Default/Bookmarks",
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _load_bookmarks(path: Path) -> Dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _iter_bookmark_nodes(data: Dict) -> Iterator[Tuple[Dict, str]]:
    roots = data.get("roots", {})
    for root in roots.values():
        yield from _walk_node(root, parent_path="")


def _walk_node(node: Dict, parent_path: str) -> Iterator[Tuple[Dict, str]]:
    name = node.get("name") or ""
    path = f"{parent_path}/{name}" if name else parent_path

    if node.get("type") == "url":
        yield node, parent_path.strip("/")
        return

    for child in node.get("children", []):
        yield from _walk_node(child, path)


def _chrome_time(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        # Chrome stores microseconds since 1601-01-01
        microseconds = int(raw)
        epoch_start = datetime(1601, 1, 1)
        return epoch_start + timedelta(microseconds=microseconds)
    except Exception:
        return None
