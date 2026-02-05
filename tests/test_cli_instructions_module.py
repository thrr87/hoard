from __future__ import annotations

from pathlib import Path

from hoard.cli.instructions import (
    INSTRUCTION_END_MARKER,
    INSTRUCTION_START_MARKER,
    build_change_plan,
    compute_targets,
    render_instruction_block,
    resolve_project_root,
    upsert_marked_block,
)


def test_resolve_project_root_prefers_nearest_git(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    nested = repo_root / "a" / "b"
    nested.mkdir(parents=True)
    (repo_root / ".git").mkdir()

    resolved = resolve_project_root(nested)
    assert resolved == repo_root


def test_resolve_project_root_manifest_fallback(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "pyproject.toml").write_text("[project]\nname='demo'\n")

    resolved = resolve_project_root(project_dir)
    assert resolved == project_dir


def test_resolve_project_root_invalid_returns_none(tmp_path: Path) -> None:
    plain_dir = tmp_path / "plain"
    plain_dir.mkdir()
    assert resolve_project_root(plain_dir) is None


def test_compute_targets_respects_case_fallback(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    (root / "claude.md").write_text("existing")

    targets = compute_targets(root, ["claude", "codex"])
    by_name = {item.target: item.path for item in targets}

    assert by_name["claude"] == root / "claude.md"
    assert by_name["codex"] == root / "AGENTS.md"


def test_upsert_marked_block_replaces_existing_block() -> None:
    existing = (
        "before\n"
        "<!-- HOARD:START -->\n"
        "old\n"
        "<!-- HOARD:END -->\n"
        "after\n"
    )
    replacement = "new"

    result = upsert_marked_block(existing, replacement)

    assert result.changed is True
    assert result.block_count == 1
    assert "old" not in result.content
    assert "new" in result.content


def test_upsert_marked_block_collapses_multiple_blocks() -> None:
    existing = (
        "one\n"
        "<!-- HOARD:START -->\n"
        "first\n"
        "<!-- HOARD:END -->\n"
        "two\n"
        "<!-- HOARD:START -->\n"
        "second\n"
        "<!-- HOARD:END -->\n"
        "three\n"
    )

    result = upsert_marked_block(existing, "replacement")

    assert result.changed is True
    assert result.block_count == 2
    assert result.content.count(INSTRUCTION_START_MARKER) == 1
    assert result.content.count(INSTRUCTION_END_MARKER) == 1


def test_build_change_plan_marks_modify(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    target = root / "AGENTS.md"
    target.write_text("Existing text\n")

    plans = compute_targets(root, ["codex"])
    block = render_instruction_block("https://example.test")
    changes = build_change_plan(plans, block)

    assert len(changes) == 1
    assert changes[0].action == "modify"
    assert changes[0].changed is True
