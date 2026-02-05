from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

INSTRUCTION_START_MARKER = "<!-- HOARD:START -->"
INSTRUCTION_END_MARKER = "<!-- HOARD:END -->"
PROJECT_MANIFESTS = (
    "pyproject.toml",
    "package.json",
    "Cargo.toml",
    "go.mod",
    "pom.xml",
    "build.gradle",
    "requirements.txt",
    "Gemfile",
)


@dataclass(frozen=True)
class TargetPlan:
    target: str
    path: Path


@dataclass(frozen=True)
class UpsertResult:
    content: str
    changed: bool
    block_count: int


@dataclass(frozen=True)
class FileChange:
    target: str
    path: Path
    action: str
    changed: bool
    before: str
    after: str
    block_count: int


@dataclass(frozen=True)
class ApplyResult:
    applied: List[Path]
    skipped: List[Path]


def resolve_project_root(cwd: Path, explicit_root: Path | None = None) -> Path | None:
    if explicit_root is not None:
        candidate = explicit_root.expanduser().resolve()
        return candidate if validate_project_root(candidate) else None

    current = cwd.expanduser().resolve()
    for candidate in [current, *current.parents]:
        if (candidate / ".git").exists():
            return candidate

    if _has_manifest(current):
        return current
    return None


def validate_project_root(path: Path) -> bool:
    candidate = path.expanduser().resolve()
    if not candidate.exists() or not candidate.is_dir():
        return False
    if (candidate / ".git").exists():
        return True
    return _has_manifest(candidate)


def compute_targets(root: Path | None, requested_targets: Iterable[str]) -> list[TargetPlan]:
    selected = {target.strip().lower() for target in requested_targets if target.strip()}
    plans: list[TargetPlan] = []

    if "claude" in selected:
        if root is None:
            raise ValueError("Project root is required for claude target")
        plans.append(TargetPlan(target="claude", path=_preferred_case_path(root, "claude.md", "CLAUDE.md")))

    if "codex" in selected:
        if root is None:
            raise ValueError("Project root is required for codex target")
        plans.append(TargetPlan(target="codex", path=_preferred_case_path(root, "agents.md", "AGENTS.md")))

    if "openclaw" in selected:
        plans.append(TargetPlan(target="openclaw", path=Path.home() / ".openclaw" / "skills" / "hoard" / "SKILL.md"))

    return plans


def render_instruction_block(link_url: str) -> str:
    lines = [
        "## Hoard memory and sync",
        "",
        "- When you learn a stable project fact or user preference, call `memory_write`.",
        "- When you create a durable note or document, call `inbox_put` (`sync_immediately=true` when needed).",
        "- If file-based changes are not visible in search yet, call `sync`.",
        f"- Advanced memory lifecycle tools: {link_url}",
    ]
    return "\n".join(lines).strip()


def upsert_marked_block(
    existing_text: str,
    block_text: str,
    start_marker: str = INSTRUCTION_START_MARKER,
    end_marker: str = INSTRUCTION_END_MARKER,
) -> UpsertResult:
    source = existing_text or ""
    wrapped = f"{start_marker}\n{block_text.strip()}\n{end_marker}"

    pattern = re.compile(rf"{re.escape(start_marker)}[\s\S]*?{re.escape(end_marker)}")
    matches = list(pattern.finditer(source))

    if not matches:
        updated = _append_block(source, wrapped)
    elif len(matches) == 1:
        match = matches[0]
        updated = source[: match.start()] + wrapped + source[match.end() :]
    else:
        first = matches[0]
        pieces = [source[: first.start()], wrapped]
        cursor = first.end()
        for match in matches[1:]:
            pieces.append(source[cursor : match.start()])
            cursor = match.end()
        pieces.append(source[cursor:])
        updated = "".join(pieces)

    if updated and not updated.endswith("\n"):
        updated += "\n"

    return UpsertResult(content=updated, changed=updated != source, block_count=len(matches))


def build_change_plan(
    targets: Sequence[TargetPlan],
    block_text: str,
    start_marker: str = INSTRUCTION_START_MARKER,
    end_marker: str = INSTRUCTION_END_MARKER,
) -> list[FileChange]:
    changes: list[FileChange] = []
    for target in targets:
        before = target.path.read_text() if target.path.exists() else ""
        upsert = upsert_marked_block(before, block_text, start_marker=start_marker, end_marker=end_marker)
        if not target.path.exists():
            action = "create"
        elif upsert.changed:
            action = "modify"
        else:
            action = "noop"
        changes.append(
            FileChange(
                target=target.target,
                path=target.path,
                action=action,
                changed=upsert.changed,
                before=before,
                after=upsert.content,
                block_count=upsert.block_count,
            )
        )
    return changes


def apply_change_plan(changes: Sequence[FileChange]) -> ApplyResult:
    applied: list[Path] = []
    skipped: list[Path] = []
    for change in changes:
        if not change.changed:
            skipped.append(change.path)
            continue
        change.path.parent.mkdir(parents=True, exist_ok=True)
        change.path.write_text(change.after)
        applied.append(change.path)
    return ApplyResult(applied=applied, skipped=skipped)


def _has_manifest(path: Path) -> bool:
    return any((path / manifest).exists() for manifest in PROJECT_MANIFESTS)


def _preferred_case_path(root: Path, lower_name: str, upper_name: str) -> Path:
    lower = root / lower_name
    upper = root / upper_name
    if lower.exists():
        return lower
    if upper.exists():
        return upper
    return upper


def _append_block(existing_text: str, wrapped_block: str) -> str:
    stripped = existing_text.rstrip("\n")
    if not stripped:
        return f"{wrapped_block}\n"
    return f"{stripped}\n\n{wrapped_block}\n"
