from __future__ import annotations

from pathlib import Path

from hoard.core.ingest.inbox import write_inbox_entry


def test_write_inbox_entry_creates_file(tmp_path: Path) -> None:
    inbox_dir = tmp_path / "inbox"
    config = {
        "connectors": {
            "inbox": {
                "enabled": True,
                "path": str(inbox_dir),
                "include_extensions": [".md"],
            }
        }
    }

    path = write_inbox_entry(
        config,
        content="Hello inbox",
        title="Test Note",
        extension=".txt",
        tags=["tag1"],
    )

    assert path.exists()
    assert path.suffix == ".md"
    assert "Hello inbox" in path.read_text(encoding="utf-8")
