from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterator, List, Tuple

from hoard.sdk.base import ConnectorV1
from hoard.sdk.hash import compute_content_hash
from hoard.sdk.types import ChunkInput, DiscoverResult, EntityInput


class FirefoxBookmarksConnector(ConnectorV1):
    @property
    def name(self) -> str:
        return "bookmarks_firefox"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def source_name(self) -> str:
        return "bookmarks_firefox"

    def discover(self, config: dict) -> DiscoverResult:
        path = _resolve_places_path(config)
        if not path or not path.exists():
            return DiscoverResult(success=False, message="Firefox places.sqlite not found")

        count = _count_bookmarks(path)
        return DiscoverResult(
            success=True,
            message=f"Found {count} bookmarks",
            entity_count_estimate=count,
        )

    def scan(self, config: dict) -> Iterator[Tuple[EntityInput, List[ChunkInput]]]:
        path = _resolve_places_path(config)
        if not path or not path.exists():
            return

        temp_path = _copy_places(path)
        try:
            with sqlite3.connect(temp_path) as conn:
                conn.row_factory = sqlite3.Row
                for row in _iter_bookmarks(conn):
                    url = row["url"]
                    title = row["title"] or url
                    source_id = row["guid"] or str(row["id"])
                    folder = row["folder"]

                    content = f"{title}\n{url}"

                    created_at = _mozilla_time(row["date_added"])
                    updated_at = _mozilla_time(row["last_modified"])

                    entity = EntityInput(
                        source=self.source_name,
                        source_id=source_id,
                        entity_type="bookmark",
                        title=title,
                        uri=url,
                        tags=["bookmark", "firefox"],
                        metadata={"folder": folder} if folder else None,
                        content_hash=compute_content_hash(content),
                        created_at=created_at,
                        updated_at=updated_at,
                        connector_name=self.name,
                        connector_version=self.version,
                    )

                    yield entity, [ChunkInput(content=content)]
        finally:
            if temp_path.exists():
                temp_path.unlink()


def _resolve_places_path(config: dict) -> Path | None:
    if config.get("places_path"):
        return Path(config["places_path"]).expanduser()

    home = Path.home()
    candidates = []

    # macOS
    candidates.extend((home / "Library/Application Support/Firefox/Profiles").glob("*"))
    # Windows
    candidates.extend((home / "AppData/Roaming/Mozilla/Firefox/Profiles").glob("*"))
    # Linux
    candidates.extend((home / ".mozilla/firefox").glob("*"))

    places = [path / "places.sqlite" for path in candidates if (path / "places.sqlite").exists()]
    if not places:
        return None

    return max(places, key=lambda p: p.stat().st_mtime)


def _copy_places(path: Path) -> Path:
    temp_path = Path("/tmp") / f"hoard-firefox-{path.stat().st_mtime_ns}.sqlite"
    shutil.copy2(path, temp_path)
    return temp_path


def _iter_bookmarks(conn: sqlite3.Connection):
    cursor = conn.execute(
        """
        SELECT b.id, b.guid, b.title, b.dateAdded AS date_added,
               b.lastModified AS last_modified,
               p.url,
               (SELECT title FROM moz_bookmarks AS parent WHERE parent.id = b.parent) AS folder
        FROM moz_bookmarks AS b
        JOIN moz_places AS p ON p.id = b.fk
        WHERE b.type = 1
        """
    )
    yield from cursor.fetchall()


def _count_bookmarks(path: Path) -> int:
    temp_path = _copy_places(path)
    try:
        with sqlite3.connect(temp_path) as conn:
            row = conn.execute("SELECT COUNT(*) FROM moz_bookmarks WHERE type = 1").fetchone()
            return int(row[0]) if row else 0
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _mozilla_time(raw: int | None) -> datetime | None:
    if raw is None:
        return None
    try:
        # microseconds since Unix epoch
        return datetime.utcfromtimestamp(raw / 1_000_000)
    except Exception:
        return None
