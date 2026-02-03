from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable, List, Tuple

COMMON_PATHS = [
    ("~/Documents/Notes", "Notes folder"),
    ("~/Documents/Obsidian", "Obsidian vault"),
    ("~/Obsidian", "Obsidian vault"),
    ("~/notes", "Notes folder"),
    ("~/Desktop", "Desktop"),
]

NOTION_ID_PATTERN = re.compile(r"[0-9a-fA-F]{32}")


def detect_document_folders(extensions: Iterable[str]) -> List[Tuple[Path, int]]:
    results: List[Tuple[Path, int]] = []
    for raw_path, _label in COMMON_PATHS:
        path = Path(raw_path).expanduser()
        if not path.exists() or not path.is_dir():
            continue
        count = _count_files(path, extensions)
        if count > 0:
            results.append((path, count))

    results.sort(key=lambda item: item[1], reverse=True)
    return results


def detect_obsidian_vaults() -> List[Path]:
    candidates = [
        Path.home() / "Obsidian",
        Path.home() / "Documents" / "Obsidian",
        Path.home() / "Documents" / "Notes",
    ]
    vaults = []
    for path in candidates:
        if (path / ".obsidian").exists():
            vaults.append(path)

    # Obsidian config (macOS)
    obsidian_config = Path.home() / "Library/Application Support/obsidian/obsidian.json"
    if obsidian_config.exists():
        try:
            import json

            data = json.loads(obsidian_config.read_text())
            for value in data.get("vaults", {}).values():
                vault_path = Path(value.get("path", ""))
                if vault_path.exists() and (vault_path / ".obsidian").exists():
                    vaults.append(vault_path)
        except Exception:
            pass

    unique = []
    seen = set()
    for vault in vaults:
        if vault in seen:
            continue
        seen.add(vault)
        unique.append(vault)
    return unique


def detect_chrome_bookmarks_paths() -> List[Path]:
    home = Path.home()
    candidates = [
        home / "Library/Application Support/Google/Chrome/Default/Bookmarks",
        home / "Library/Application Support/Google/Chrome/Profile 1/Bookmarks",
        home / "AppData/Local/Google/Chrome/User Data/Default/Bookmarks",
        home / ".config/google-chrome/Default/Bookmarks",
        home / ".config/chromium/Default/Bookmarks",
    ]
    return [path for path in candidates if path.exists()]


def detect_notion_exports(path: Path) -> List[Path]:
    exports: List[Path] = []
    if path.is_file():
        if _looks_like_notion_export(path):
            exports.append(path)
        return exports

    if not path.exists():
        return exports

    for child in path.iterdir():
        if _looks_like_notion_export(child):
            exports.append(child)
    return exports


def _looks_like_notion_export(path: Path) -> bool:
    lower = path.name.lower()
    if path.is_file() and path.suffix.lower() == ".zip" and "notion" in lower:
        return True
    if path.is_file() and path.suffix.lower() == ".csv" and NOTION_ID_PATTERN.search(path.name):
        return True
    if path.is_dir():
        # heuristic: exported folder contains HTML/MD files
        for child in path.iterdir():
            if child.suffix.lower() in {".html", ".md", ".csv"}:
                return True
    return False


def _count_files(path: Path, extensions: Iterable[str]) -> int:
    exts = {ext.lower() for ext in extensions}
    count = 0
    for file_path in path.rglob("*"):
        if not file_path.is_file():
            continue
        if exts and file_path.suffix.lower() not in exts:
            continue
        count += 1
    return count
