from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from hoard.connectors.bookmarks_chrome.connector import ChromeBookmarksConnector
from hoard.connectors.bookmarks_firefox.connector import FirefoxBookmarksConnector
from hoard.connectors.inbox.connector import InboxConnector
from hoard.connectors.notion_export.connector import NotionExportConnector
from hoard.connectors.obsidian.connector import ObsidianConnector


def test_obsidian_connector_scans_notes(tmp_path: Path) -> None:
    vault = tmp_path / "Vault"
    (vault / ".obsidian").mkdir(parents=True)
    (vault / "Note.md").write_text("Hello Obsidian")
    (vault / ".obsidian" / "config").write_text("ignored")

    connector = ObsidianConnector()
    results = list(connector.scan({"vault_path": str(vault)}))
    assert len(results) == 1

    entity, chunks = results[0]
    assert entity.source == "obsidian"
    assert entity.source_id == f"{vault.name}:Note.md"
    assert chunks


def test_chrome_bookmarks_connector_scan(tmp_path: Path) -> None:
    bookmarks_path = tmp_path / "Bookmarks"
    data = {
        "roots": {
            "bookmark_bar": {
                "name": "Bookmarks Bar",
                "children": [
                    {
                        "type": "folder",
                        "name": "Work",
                        "children": [
                            {
                                "type": "url",
                                "name": "Spec",
                                "url": "https://example.com/spec",
                                "id": "1",
                                "date_added": "13217451500000000",
                            }
                        ],
                    }
                ],
            }
        }
    }
    bookmarks_path.write_text(json.dumps(data))

    connector = ChromeBookmarksConnector()
    results = list(connector.scan({"bookmark_path": str(bookmarks_path)}))
    assert len(results) == 1

    entity, chunks = results[0]
    assert entity.source == "bookmarks_chrome"
    assert entity.title == "Spec"
    assert entity.metadata and entity.metadata["folder"] == "Bookmarks Bar/Work"
    assert chunks and "https://example.com/spec" in chunks[0].content


def test_firefox_bookmarks_connector_scan(tmp_path: Path) -> None:
    places_path = tmp_path / "places.sqlite"
    with sqlite3.connect(places_path) as conn:
        conn.executescript(
            """
            CREATE TABLE moz_places (
                id INTEGER PRIMARY KEY,
                url TEXT
            );
            CREATE TABLE moz_bookmarks (
                id INTEGER PRIMARY KEY,
                fk INTEGER,
                type INTEGER,
                parent INTEGER,
                title TEXT,
                dateAdded INTEGER,
                lastModified INTEGER,
                guid TEXT
            );
            """
        )
        conn.execute("INSERT INTO moz_places (id, url) VALUES (?, ?)", (1, "https://example.com"))
        conn.execute(
            "INSERT INTO moz_bookmarks (id, type, parent, title, guid) VALUES (?, ?, ?, ?, ?)",
            (10, 2, 0, "Folder", "folder-guid"),
        )
        conn.execute(
            """
            INSERT INTO moz_bookmarks (id, fk, type, parent, title, dateAdded, lastModified, guid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (11, 1, 1, 10, "Example", 1_700_000_000_000_000, 1_700_000_000_000_000, "guid-1"),
        )
        conn.commit()

    connector = FirefoxBookmarksConnector()
    results = list(connector.scan({"places_path": str(places_path)}))
    assert len(results) == 1

    entity, chunks = results[0]
    assert entity.source == "bookmarks_firefox"
    assert entity.metadata and entity.metadata["folder"] == "Folder"
    assert chunks and "https://example.com" in chunks[0].content


def test_notion_export_html_page(tmp_path: Path) -> None:
    export_dir = tmp_path / "notion"
    export_dir.mkdir()
    html = "<html><head><title>My Page</title></head><body><p>Hello</p></body></html>"
    (export_dir / "My Page.html").write_text(html)

    connector = NotionExportConnector()
    results = list(
        connector.scan(
            {
                "export_path": str(export_dir),
                "include_databases": True,
                "include_csv_databases": True,
            }
        )
    )
    assert results

    entity, chunks = results[0]
    assert entity.entity_type == "page"
    assert "notion" in (entity.tags or [])
    assert chunks and "Hello" in chunks[0].content


def test_notion_export_csv_database_tags(tmp_path: Path) -> None:
    export_dir = tmp_path / "notion_csv"
    export_dir.mkdir()
    (export_dir / "Tasks.csv").write_text("Name,Status\nBuild,Done\n")

    connector = NotionExportConnector()
    results = list(
        connector.scan(
            {
                "export_path": str(export_dir),
                "include_databases": True,
                "include_csv_databases": True,
            }
        )
    )
    assert results
    entity, _ = results[0]
    tags = set(entity.tags or [])
    assert "database" in tags
    assert "csv" in tags
    assert "field:name" in tags
    assert "field:status" in tags


def test_inbox_connector_scan(tmp_path: Path) -> None:
    inbox_dir = tmp_path / "inbox"
    inbox_dir.mkdir()
    (inbox_dir / "note.md").write_text("Inbox note")
    (inbox_dir / "skip.txt").write_text("Should skip")

    connector = InboxConnector()
    results = list(
        connector.scan(
            {"path": str(inbox_dir), "include_extensions": [".md"]}
        )
    )
    assert len(results) == 1
    entity, chunks = results[0]
    assert entity.source == "inbox"
    assert "inbox" in (entity.tags or [])
    assert chunks and "Inbox note" in chunks[0].content
