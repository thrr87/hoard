from __future__ import annotations

from typing import Dict, Iterator, Tuple

import importlib

from hoard.connectors.bookmarks_chrome.connector import ChromeBookmarksConnector
from hoard.connectors.bookmarks_firefox.connector import FirefoxBookmarksConnector
from hoard.connectors.inbox.connector import InboxConnector
from hoard.connectors.local_files.connector import LocalFilesConnector
from hoard.connectors.notion_export.connector import NotionExportConnector
from hoard.connectors.obsidian.connector import ObsidianConnector
from hoard.sdk.base import ConnectorV1


BUILTIN_CONNECTORS: Dict[str, type[ConnectorV1]] = {
    "local_files": LocalFilesConnector,
    "inbox": InboxConnector,
    "obsidian": ObsidianConnector,
    "bookmarks_chrome": ChromeBookmarksConnector,
    "bookmarks_firefox": FirefoxBookmarksConnector,
    "notion_export": NotionExportConnector,
}


def load_connector(name: str, settings: dict | None = None) -> ConnectorV1 | None:
    connector_cls = BUILTIN_CONNECTORS.get(name)
    if connector_cls:
        return connector_cls()

    if settings:
        entry_point = settings.get("entry_point")
        if entry_point:
            return _load_from_entry_point(entry_point)

    return None


def iter_enabled_connectors(config: dict) -> Iterator[Tuple[str, ConnectorV1, dict]]:
    connectors_config = config.get("connectors", {})
    for name, settings in connectors_config.items():
        if not isinstance(settings, dict):
            continue
        if not settings.get("enabled", False):
            continue
        connector = load_connector(name, settings)
        if connector is None:
            continue
        yield name, connector, settings


def _load_from_entry_point(entry_point: str) -> ConnectorV1 | None:
    if ":" not in entry_point:
        return None
    module_name, class_name = entry_point.split(":", 1)
    try:
        module = importlib.import_module(module_name)
        connector_cls = getattr(module, class_name, None)
        if connector_cls is None:
            return None
        return connector_cls()
    except Exception:
        return None
