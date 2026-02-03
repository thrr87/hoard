from __future__ import annotations

import secrets
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import List

import click
from rich.console import Console
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeRemainingColumn
from rich.table import Table

from hoard.core.config import ensure_config_file, load_config, resolve_paths, save_config
from hoard.core.db.connection import connect, initialize_db
from hoard.core.embeddings.model import EmbeddingError, EmbeddingModel
from hoard.core.embeddings.store import build_embeddings
from hoard.core.ingest.registry import iter_enabled_connectors
from hoard.core.ingest.sync import sync_connector
from hoard.core.mcp.server import run_server
from hoard.core.mcp.stdio import run_stdio
from hoard.core.memory.store import memory_get, memory_put, memory_search
from hoard.core.search.service import search_entities
from hoard.core.onboarding import (
    detect_chrome_bookmarks_paths,
    detect_document_folders,
    detect_notion_exports,
    detect_obsidian_vaults,
)

console = Console()


@click.group()
def cli() -> None:
    """Hoard CLI."""


@cli.command("init")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
@click.option("--quick", is_flag=True, default=False, help="Accept defaults")
@click.option("--vectors", is_flag=True, default=False, help="Enable semantic search")
@click.option("--connector", "connectors", multiple=True, help="Enable specific connector")
def init_command(
    config_path: Path | None,
    quick: bool,
    vectors: bool,
    connectors: tuple[str, ...],
) -> None:
    """Interactive setup wizard."""
    path = ensure_config_file(config_path)
    config = load_config(path)

    run_setup = False
    if quick:
        config = _apply_quick_defaults(config)
    else:
        config, run_setup = _run_init_wizard(config, list(connectors))

    if vectors:
        config.setdefault("vectors", {})["enabled"] = True

    save_config(config, path)

    paths = resolve_paths(config, path)
    conn = connect(paths.db_path)
    initialize_db(conn)
    conn.close()

    console.print(f"\n✓ Config saved to {paths.config_path}")
    console.print(f"✓ Database ready at {paths.db_path}")

    console.print("\nRunning initial sync...")
    sync_command(config_path=path)

    if run_setup:
        setup_command(
            claude=False,
            codex=False,
            openclaw=False,
            setup_all=True,
            project_scope=False,
            verify=False,
            uninstall=None,
        )


@cli.command("add")
@click.argument("path", required=False)
@click.option("--obsidian", "obsidian_path", type=click.Path(path_type=Path))
@click.option("--notion", "notion_path", type=click.Path(path_type=Path))
def add_command(
    path: Path | None,
    obsidian_path: Path | None,
    notion_path: Path | None,
) -> None:
    """Quickly add sources to index."""
    config_path = ensure_config_file(None)
    config = load_config(config_path)

    if obsidian_path:
        _configure_obsidian(config, obsidian_path)
    elif notion_path:
        _configure_notion_export(config, notion_path)
    elif path:
        exports = detect_notion_exports(path)
        if exports:
            use_notion = click.confirm("Found Notion export. Index as Notion database?", default=True)
            if use_notion:
                _configure_notion_export(config, exports[0])
            else:
                _add_local_path(config, path)
        else:
            _add_local_path(config, path)
    else:
        console.print("Provide a path or --obsidian/--notion.")
        return

    save_config(config, config_path)
    console.print(f"Updated config at {config_path}")
    sync_command(config_path=config_path)


@cli.command("sync")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def sync_command(config_path: Path | None) -> None:
    """Run sync for enabled connectors."""
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)
    conn = connect(paths.db_path)
    initialize_db(conn)

    any_connector = False
    progress = Progress(
        TextColumn("{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        transient=True,
    )

    with progress:
        for name, connector, settings in iter_enabled_connectors(config):
            any_connector = True
            console.print(f"Syncing {name}...")
            discover = connector.discover(settings)
            if not discover.success:
                console.print(f"  [red]Discover failed:[/red] {discover.message}")
                continue

            total = discover.entity_count_estimate or 0
            task_id = progress.add_task(f"  Indexing {name}", total=total)

            def _advance() -> None:
                progress.advance(task_id)

            stats = sync_connector(conn, connector, settings, on_entity=_advance)
            progress.update(task_id, completed=stats.entities_seen)

            console.print(
                f"  Entities: {stats.entities_seen}, Chunks: {stats.chunks_written}, "
                f"Tombstoned: {stats.entities_tombstoned}, Errors: {stats.errors}"
            )

    if not any_connector:
        console.print("No enabled connectors found.")

    conn.close()


@cli.command("search")
@click.argument("query", type=str)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
@click.option("--limit", default=20, show_default=True)
@click.option("--source", default=None, help="Filter by source name")
@click.option("--offset", default=0, show_default=True, help="Result offset (entity-based)")
@click.option("--json", "as_json", is_flag=True, default=False)
def search_command(
    query: str,
    config_path: Path | None,
    limit: int,
    source: str | None,
    offset: int,
    as_json: bool,
) -> None:
    """Search indexed content."""
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)
    conn = connect(paths.db_path)

    results, next_cursor = search_entities(
        conn,
        query=query,
        config=config,
        limit=limit,
        offset=offset,
        source=source,
        allow_sensitive=True,
    )
    if as_json:
        console.print_json(json.dumps({"results": results, "next_cursor": next_cursor}))
        conn.close()
        return

    if not results:
        total_entities = conn.execute(
            "SELECT COUNT(*) FROM entities WHERE tombstoned_at IS NULL"
        ).fetchone()[0]
        console.print(f'No results for "{query}".')
        console.print("\nSuggestions:")
        console.print("  • Check spelling")
        console.print(f"  • {total_entities} entities indexed — run 'hoard sync' to update")
        local_exts = (
            config.get("connectors", {})
            .get("local_files", {})
            .get("include_extensions", [])
        )
        if ".csv" not in local_exts:
            console.print("  • CSV files not indexed — add .csv to include_extensions")
        console.print("\nRun 'hoard doctor' to diagnose indexing issues.")
        conn.close()
        return

    for entity in results:
        table = Table(title=entity.get("entity_title") or entity["entity_id"], show_header=True)
        table.add_column("Chunk ID", style="dim")
        table.add_column("Score", style="cyan", justify="right")
        table.add_column("Content", style="white")

        for chunk in entity["chunks"]:
            snippet = chunk["content"].replace("\n", " ").strip()
            if len(snippet) > 200:
                snippet = snippet[:200] + "..."
            table.add_row(chunk["chunk_id"], f"{chunk['score']:.4f}", snippet)

        console.print(table)

    conn.close()


@cli.group("memory")
def memory_group() -> None:
    """Manage memory entries."""


@memory_group.command("put")
@click.argument("key", type=str)
@click.argument("content", type=str)
@click.option("--tags", default="", help="Comma-separated tags")
@click.option("--metadata", default=None, help="JSON metadata")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def memory_put_command(
    key: str,
    content: str,
    tags: str,
    metadata: str | None,
    config_path: Path | None,
) -> None:
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)
    conn = connect(paths.db_path)
    initialize_db(conn)

    tag_list = [tag.strip() for tag in tags.split(",") if tag.strip()] if tags else []
    metadata_obj = json.loads(metadata) if metadata else None
    entry = memory_put(conn, key=key, content=content, tags=tag_list, metadata=metadata_obj)
    console.print_json(json.dumps(entry))
    conn.close()


@memory_group.command("get")
@click.argument("key", type=str)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def memory_get_command(key: str, config_path: Path | None) -> None:
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)
    conn = connect(paths.db_path)
    initialize_db(conn)

    entry = memory_get(conn, key)
    conn.close()
    if not entry:
        console.print("No entry found.")
        return
    console.print_json(json.dumps(entry))


@memory_group.command("search")
@click.argument("query", type=str)
@click.option("--limit", default=20, show_default=True)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def memory_search_command(query: str, limit: int, config_path: Path | None) -> None:
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)
    conn = connect(paths.db_path)
    initialize_db(conn)

    results = memory_search(conn, query, limit=limit)
    conn.close()
    console.print_json(json.dumps({"results": results}))


@cli.group("embeddings")
def embeddings_group() -> None:
    """Manage embeddings."""


@embeddings_group.command("build")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
@click.option("--source", default=None, help="Filter by source name")
def embeddings_build_command(config_path: Path | None, source: str | None) -> None:
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)
    conn = connect(paths.db_path)
    initialize_db(conn)

    vectors_config = config.get("vectors", {})
    model_name = vectors_config.get("model_name", "sentence-transformers/all-MiniLM-L6-v2")
    batch_size = int(vectors_config.get("batch_size", 32))

    try:
        model = EmbeddingModel(model_name)
    except EmbeddingError as exc:
        console.print(f"[red]{exc}[/red]")
        conn.close()
        return

    total = build_embeddings(conn, model, batch_size=batch_size, source=source)
    console.print(f"Built {total} embeddings.")
    conn.close()


@cli.group("tokens")
def tokens_group() -> None:
    """Manage API tokens."""


@tokens_group.command("add")
@click.argument("name", type=str)
@click.option("--scopes", default="search,get,memory", help="Comma-separated scopes")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def tokens_add_command(name: str, scopes: str, config_path: Path | None) -> None:
    config = load_config(config_path)
    scope_list = [scope.strip() for scope in scopes.split(",") if scope.strip()]
    token_value = f"hoard_sk_{secrets.token_hex(16)}"

    security = config.setdefault("security", {})
    tokens = security.setdefault("tokens", [])
    tokens.append({"name": name, "token": token_value, "scopes": scope_list})

    save_config(config, config_path)
    console.print(f"Token created for {name}: {token_value}")


@tokens_group.command("list")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def tokens_list_command(config_path: Path | None) -> None:
    config = load_config(config_path)
    tokens = config.get("security", {}).get("tokens", [])

    table = Table(title="Tokens", show_header=True)
    table.add_column("Name")
    table.add_column("Scopes")
    table.add_column("Token")

    for token in tokens:
        token_value = token.get("token", "")
        masked = f"{token_value[:6]}...{token_value[-4:]}" if token_value else ""
        table.add_row(token.get("name", ""), ", ".join(token.get("scopes", [])), masked)

    console.print(table)


@tokens_group.command("remove")
@click.argument("name", type=str)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def tokens_remove_command(name: str, config_path: Path | None) -> None:
    config = load_config(config_path)
    tokens = config.get("security", {}).get("tokens", [])
    updated = [token for token in tokens if token.get("name") != name]
    config.setdefault("security", {})["tokens"] = updated
    save_config(config, config_path)
    console.print(f"Removed token {name}.")


@cli.group("connectors")
def connectors_group() -> None:
    """Connector management."""


@connectors_group.command("status")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def connectors_status_command(config_path: Path | None) -> None:
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)
    conn = connect(paths.db_path)
    initialize_db(conn)

    status_rows = conn.execute(
        """
        SELECT source, COUNT(*) AS count, MAX(synced_at) AS last_sync
        FROM entities
        WHERE tombstoned_at IS NULL
        GROUP BY source
        """
    ).fetchall()
    status_map = {row["source"]: row for row in status_rows}

    table = Table(title="Connectors", show_header=True)
    table.add_column("Connector")
    table.add_column("Enabled")
    table.add_column("Entities", justify="right")
    table.add_column("Last Sync")

    for name, settings in config.get("connectors", {}).items():
        enabled = settings.get("enabled", False) if isinstance(settings, dict) else False
        row = status_map.get(name)
        count = str(row["count"]) if row else "0"
        last_sync = row["last_sync"] if row else "-"
        table.add_row(name, "yes" if enabled else "no", count, last_sync)

    console.print(table)
    conn.close()


@connectors_group.command("check")
@click.argument("name", type=str)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def connectors_check_command(name: str, config_path: Path | None) -> None:
    config = load_config(config_path)
    settings = config.get("connectors", {}).get(name, {})
    if not isinstance(settings, dict):
        console.print("Connector config not found.")
        return

    from hoard.core.ingest.registry import load_connector

    connector = load_connector(name, settings)
    if connector is None:
        console.print("Connector not found.")
        return

    result = connector.discover(settings)
    status = "ok" if result.success else "failed"
    console.print(f"{name}: {status} - {result.message}")


@connectors_group.command("inspect")
@click.option("--path", "connector_path", type=click.Path(path_type=Path), required=True)
def connectors_inspect_command(connector_path: Path) -> None:
    from hoard.core.connectors.manifest import load_manifest

    manifest = load_manifest(connector_path)
    console.print_json(json.dumps(manifest.to_dict()))


@cli.command("doctor")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def doctor_command(config_path: Path | None) -> None:
    config_path = ensure_config_file(config_path)
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)

    console.print("Checking Hoard installation...\n")

    conn = connect(paths.db_path)
    initialize_db(conn)

    python_ok = sys.version_info >= (3, 11)
    python_version = ".".join(str(x) for x in sys.version_info[:3])
    _check(f"Python {python_version}", python_ok)

    if shutil.which("hoard") is None:
        console.print("[yellow][!][/yellow] 'hoard' not on PATH (restart shell or check install)")

    _check(f"Config exists: {paths.config_path}", True)
    _check(f"Database exists: {paths.db_path}", paths.db_path.exists())

    entity_count = conn.execute(
        "SELECT COUNT(*) FROM entities WHERE tombstoned_at IS NULL"
    ).fetchone()[0]
    chunk_count = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    _check(f"{entity_count} entities indexed", True)
    _check(f"{chunk_count} chunks created", True)

    fts_ok = _fts_present(conn)
    _check("FTS tables present", fts_ok)

    vectors_enabled = config.get("vectors", {}).get("enabled", False)
    if vectors_enabled:
        try:
            EmbeddingModel(config.get("vectors", {}).get("model_name", ""))
            _check("Embedding model loaded", True)
        except EmbeddingError:
            _check("Embedding model loaded", False)
    else:
        console.print("[yellow][!][/yellow] Vectors disabled (run 'hoard init --vectors' to enable)")

    tokens = config.get("security", {}).get("tokens", [])
    if not tokens:
        console.print("[yellow][!][/yellow] No MCP token configured for Claude Code")

    conn.close()


@cli.command("sync-status")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def sync_status_command(config_path: Path | None) -> None:
    connectors_status_command(config_path)


@cli.command("serve")
@click.option("--host", default=None, help="Server host")
@click.option("--port", default=None, type=int, help="Server port")
@click.option("--daemon", is_flag=True, default=False, help="Run as background daemon")
@click.option("--status", is_flag=True, default=False, help="Check daemon status")
@click.option("--stop", is_flag=True, default=False, help="Stop daemon")
@click.option(
    "--install-autostart",
    is_flag=True,
    default=False,
    help="Install autostart on login",
)
def serve_command(
    host: str | None,
    port: int | None,
    daemon: bool,
    status: bool,
    stop: bool,
    install_autostart: bool,
) -> None:
    config = load_config(None)
    server_config = config.get("server", {})
    host = host or server_config.get("host", "127.0.0.1")
    port = port or int(server_config.get("port", 19850))

    if status:
        _serve_status()
        return
    if stop:
        _serve_stop()
        return
    if install_autostart:
        _install_autostart(host, port)
        return

    if daemon:
        _serve_daemon(host, port)
        return

    console.print(f"Starting Hoard server on http://{host}:{port}/mcp")
    run_server(host=host, port=port, config_path=None)


@cli.group("mcp")
def mcp_group() -> None:
    """Run MCP HTTP server."""


@mcp_group.command("serve")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=19850, show_default=True)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def mcp_serve_command(host: str, port: int, config_path: Path | None) -> None:
    console.print(f"Starting MCP server on {host}:{port}")
    run_server(host=host, port=port, config_path=config_path)


@mcp_group.command("stdio")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def mcp_stdio_command(config_path: Path | None) -> None:
    run_stdio(config_path=config_path)


@cli.command("setup")
@click.option("--claude", is_flag=True, default=False, help="Configure Claude Code")
@click.option("--codex", is_flag=True, default=False, help="Configure Codex")
@click.option("--openclaw", is_flag=True, default=False, help="Configure OpenClaw")
@click.option("--all", "setup_all", is_flag=True, default=False, help="Configure all detected clients")
@click.option("--project-scope", is_flag=True, default=False, help="Use project-scope config for Claude")
@click.option("--verify", is_flag=True, default=False, help="Verify integrations")
@click.option("--uninstall", type=str, default=None, help="Uninstall client integration")
def setup_command(
    claude: bool,
    codex: bool,
    openclaw: bool,
    setup_all: bool,
    project_scope: bool,
    verify: bool,
    uninstall: str | None,
) -> None:
    if uninstall:
        _uninstall_integration(uninstall.lower())
        return

    if verify:
        _verify_setup()
        return

    config_path = ensure_config_file(None)
    config = load_config(config_path)

    host = config.get("server", {}).get("host", "127.0.0.1")
    port = int(config.get("server", {}).get("port", 19850))
    url = f"http://{host}:{port}/mcp"

    token_value = _ensure_token(config, name="default")
    save_config(config, config_path)

    targets = _resolve_setup_targets(claude, codex, openclaw, setup_all)
    if not targets:
        console.print("No clients selected.")
        return

    _ensure_server_running(host, port)

    if "claude" in targets:
        if project_scope:
            _configure_claude_project_scope(url)
        else:
            _configure_claude_user_scope(url, token_value)
    if "codex" in targets:
        _configure_codex(url)
    if "openclaw" in targets:
        _configure_openclaw(url, token_value)

    console.print("Setup complete.")

def _check(label: str, ok: bool) -> None:
    status = "[green]✓[/green]" if ok else "[red]✗[/red]"
    console.print(f"{status} {label}")


def _fts_present(conn) -> bool:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('chunks_fts', 'entities_fts')"
    ).fetchall()
    return len(rows) == 2


def _apply_quick_defaults(config: dict) -> dict:
    local_files = config.setdefault("connectors", {}).setdefault("local_files", {})
    local_files["enabled"] = True
    local_files["include_extensions"] = [".md", ".txt", ".csv", ".json", ".yaml", ".rst"]

    if not local_files.get("paths"):
        candidates = detect_document_folders(local_files["include_extensions"])
        if candidates:
            local_files["paths"] = [str(candidates[0][0])]
        else:
            local_files["paths"] = [str(Path.home() / "Documents")]

    return config


def _run_init_wizard(config: dict, connectors: List[str]) -> tuple[dict, bool]:
    console.print("Welcome to Hoard! Let's set up your personal data layer.\n")

    connectors_cfg = config.setdefault("connectors", {})
    for name in connectors_cfg:
        if isinstance(connectors_cfg.get(name), dict):
            connectors_cfg[name]["enabled"] = False

    connector_options = [
        ("local_files", "Local files (markdown, text, code)"),
        ("obsidian", "Obsidian vault"),
        ("bookmarks", "Browser bookmarks"),
        ("notion_export", "Notion export (CSV/HTML)"),
    ]

    if not connectors:
        selected = _prompt_multi_select("Step 1/4: What do you want to index?", connector_options, ["local_files"])
    else:
        selected = connectors

    if "bookmarks" in selected:
        selected.remove("bookmarks")
        selected.extend(["bookmarks_chrome", "bookmarks_firefox"])

    if "local_files" in selected:
        _configure_local_files(config)

    if "obsidian" in selected:
        _configure_obsidian(config, None)

    if "notion_export" in selected:
        _configure_notion_export(config, None)

    if "bookmarks_chrome" in selected or "bookmarks_firefox" in selected:
        _configure_bookmarks(config)

    enable_vectors = click.confirm(
        "Step 4/4: Enable semantic search? (downloads model)",
        default=False,
    )
    config.setdefault("vectors", {})["enabled"] = enable_vectors

    setup_tools = click.confirm("Configure AI tools now?", default=True)
    return config, setup_tools


def _prompt_multi_select(title: str, options: List[tuple], defaults: List[str]) -> List[str]:
    console.print(title)
    for idx, (_, label) in enumerate(options, start=1):
        console.print(f"  {idx}. {label}")

    default_labels = [str(idx + 1) for idx, (key, _) in enumerate(options) if key in defaults]
    default_prompt = ",".join(default_labels)
    choice = click.prompt("Select options (comma-separated)", default=default_prompt, show_default=True)
    selected = []
    for item in choice.split(","):
        item = item.strip()
        if not item:
            continue
        if item.isdigit() and 1 <= int(item) <= len(options):
            selected.append(options[int(item) - 1][0])
    return selected


def _configure_local_files(config: dict) -> None:
    local_files = config.setdefault("connectors", {}).setdefault("local_files", {})
    local_files["enabled"] = True

    include_exts = local_files.get("include_extensions") or [
        ".md",
        ".txt",
        ".csv",
        ".json",
        ".yaml",
        ".rst",
    ]

    candidates = detect_document_folders(include_exts)
    console.print("\nStep 2/4: Local files - pick folders to index")
    for idx, (path, count) in enumerate(candidates, start=1):
        warning = " ⚠️ Large" if count > 5000 else ""
        console.print(f"  {idx}. {path} ({count} files){warning}")

    choice = click.prompt("Enter numbers or a custom path", default="1" if candidates else "", show_default=bool(candidates))
    paths: List[str] = []
    if choice:
        parts = [part.strip() for part in choice.split(",") if part.strip()]
        for part in parts:
            if part.isdigit() and candidates:
                index = int(part) - 1
                if 0 <= index < len(candidates):
                    paths.append(str(candidates[index][0]))
            else:
                paths.append(str(Path(part).expanduser()))

    if not paths:
        paths = [str(Path.home() / "Documents")]

    local_files["paths"] = paths

    console.print("\nStep 3/4: File types to include")
    ext_options = [
        (".md", "Markdown"),
        (".txt", "Text"),
        (".csv", "CSV"),
        (".json", "JSON"),
        (".yaml", "YAML"),
        (".rst", "reStructuredText"),
    ]
    selected_exts = _prompt_extension_select(ext_options, include_exts)
    local_files["include_extensions"] = selected_exts


def _prompt_extension_select(options: List[tuple], defaults: List[str]) -> List[str]:
    for idx, (_, label) in enumerate(options, start=1):
        console.print(f"  {idx}. {label}")
    default_labels = [str(idx + 1) for idx, (ext, _) in enumerate(options) if ext in defaults]
    choice = click.prompt("Select file types (comma-separated)", default=",".join(default_labels))
    selected = []
    for part in choice.split(","):
        part = part.strip()
        if part.isdigit() and 1 <= int(part) <= len(options):
            selected.append(options[int(part) - 1][0])
    return selected


def _configure_obsidian(config: dict, vault_path: Path | None) -> None:
    obsidian = config.setdefault("connectors", {}).setdefault("obsidian", {})
    obsidian["enabled"] = True

    if vault_path is None:
        vaults = detect_obsidian_vaults()
        if vaults:
            choice = click.prompt("Obsidian vault path", default=str(vaults[0]))
            vault_path = Path(choice).expanduser()
        else:
            choice = click.prompt("Obsidian vault path", default=str(Path.home() / "Obsidian"))
            vault_path = Path(choice).expanduser()

    obsidian["vault_path"] = str(vault_path)


def _configure_bookmarks(config: dict) -> None:
    chrome = config.setdefault("connectors", {}).setdefault("bookmarks_chrome", {})
    chrome["enabled"] = True

    firefox = config.setdefault("connectors", {}).setdefault("bookmarks_firefox", {})
    firefox["enabled"] = True

    chrome_paths = detect_chrome_bookmarks_paths()
    if chrome_paths:
        chrome["bookmark_path"] = str(chrome_paths[0])


def _configure_notion_export(config: dict, export_path: Path | None) -> None:
    notion = config.setdefault("connectors", {}).setdefault("notion_export", {})
    notion["enabled"] = True

    if export_path is None:
        choice = click.prompt("Notion export path", default=str(Path.home() / "Downloads"))
        export_path = Path(choice).expanduser()

    notion["export_path"] = str(export_path)


def _add_local_path(config: dict, path: Path) -> None:
    local_files = config.setdefault("connectors", {}).setdefault("local_files", {})
    local_files["enabled"] = True
    paths = local_files.setdefault("paths", [])
    resolved = str(path.expanduser())
    if resolved not in paths:
        paths.append(resolved)


def _resolve_setup_targets(claude: bool, codex: bool, openclaw: bool, setup_all: bool) -> List[str]:
    targets: List[str] = []
    if setup_all:
        targets = _detect_clients()
    else:
        if claude:
            targets.append("claude")
        if codex:
            targets.append("codex")
        if openclaw:
            targets.append("openclaw")
        if not targets:
            targets = _detect_clients()
    return targets


def _detect_clients() -> List[str]:
    targets = []
    if (Path.home() / ".claude.json").exists() or (Path.home() / ".config/claude").exists():
        targets.append("claude")
    if (Path.home() / ".codex").exists():
        targets.append("codex")
    if (Path.home() / ".openclaw").exists():
        targets.append("openclaw")
    return targets


def _ensure_token(config: dict, name: str) -> str:
    tokens = config.setdefault("security", {}).setdefault("tokens", [])
    for token in tokens:
        if token.get("name") == name:
            return token.get("token")
    token_value = f"hoard_sk_{secrets.token_hex(16)}"
    tokens.append({"name": name, "token": token_value, "scopes": ["search", "get", "memory", "sync"]})
    return token_value


def _ensure_server_running(host: str, port: int) -> None:
    if _is_server_healthy(host, port):
        return
    _serve_daemon(host, port)
    for _ in range(10):
        if _is_server_healthy(host, port):
            return
        time.sleep(0.5)


def _is_server_healthy(host: str, port: int) -> bool:
    import urllib.request
    import urllib.error

    try:
        req = urllib.request.Request(
            f"http://{host}:{port}/mcp",
            data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).encode(),
            headers={"Content-Type": "application/json", "Authorization": "Bearer invalid"},
        )
        urllib.request.urlopen(req, timeout=2)
    except urllib.error.HTTPError as exc:
        return exc.code in {401, 403}
    except Exception:
        return False
    return True


def _configure_claude_user_scope(url: str, token: str) -> None:
    config_path = Path.home() / ".claude.json"
    data = {}
    if config_path.exists():
        try:
            data = json.loads(config_path.read_text())
        except Exception:
            data = {}

    servers = data.setdefault("mcpServers", {})
    servers["hoard"] = {
        "url": url,
        "headers": {"Authorization": f"Bearer {token}"},
    }
    config_path.write_text(json.dumps(data, indent=2))
    console.print("Configuring Claude Code... done")


def _configure_claude_project_scope(url: str) -> None:
    config_path = Path.cwd() / ".mcp.json"
    data = {"mcpServers": {"hoard": {"url": url, "headers": {"Authorization": "Bearer ${HOARD_TOKEN}"}}}}
    config_path.write_text(json.dumps(data, indent=2))

    gitignore = Path.cwd() / ".gitignore"
    if gitignore.exists():
        content = gitignore.read_text()
        if ".mcp.json" not in content:
            gitignore.write_text(content + "\n.mcp.json\n")
    else:
        gitignore.write_text(".mcp.json\n")

    console.print("Configured Claude Code (project-scope)")


def _configure_codex(url: str) -> None:
    config_path = Path.home() / ".codex" / "config.toml"
    config_path.parent.mkdir(parents=True, exist_ok=True)

    block = f"""
[mcp_servers.hoard]
url = "{url}"
bearer_token_env_var = "HOARD_TOKEN"
"""
    existing = config_path.read_text() if config_path.exists() else ""
    new_text = _replace_toml_block(existing, "mcp_servers.hoard", block)
    config_path.write_text(new_text)
    console.print("Configuring Codex... done")


def _replace_toml_block(text: str, block_name: str, block_content: str) -> str:
    import re

    pattern = re.compile(rf"\[{re.escape(block_name)}\][\s\S]*?(?=\n\[|\Z)")
    if pattern.search(text):
        return pattern.sub(block_content.strip() + "\n", text)
    return text + "\n" + block_content.strip() + "\n"


def _configure_openclaw(url: str, token: str) -> None:
    base_dir = Path.home() / ".openclaw" / "skills" / "hoard"
    scripts_dir = base_dir / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    skill_md = _openclaw_skill_md()
    (base_dir / "SKILL.md").write_text(skill_md)
    (base_dir / "README.md").write_text("Hoard OpenClaw skill")

    client_script = scripts_dir / "hoard_client.py"
    client_script.write_text(_openclaw_client_script())
    client_script.chmod(0o755)

    config_path = Path.home() / ".openclaw" / "openclaw.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    data = {}
    if config_path.exists():
        try:
            data = json.loads(config_path.read_text())
        except Exception:
            data = {}

    skills = data.setdefault("skills", {}).setdefault("entries", {})
    skills["hoard"] = {
        "enabled": True,
        "apiKey": token,
        "env": {"HOARD_URL": url},
    }
    config_path.write_text(json.dumps(data, indent=2))
    console.print("Configuring OpenClaw... done")


def _openclaw_skill_md() -> str:
    return """---
name: hoard
description: Search your Hoard knowledge base (local HTTP MCP)
metadata: {"openclaw":{"requires":{"bins":["python3"],"env":["HOARD_URL","HOARD_TOKEN"]},"primaryEnv":"HOARD_TOKEN"}}
---

# Hoard

Use Hoard to search and retrieve documents from your local index.

## Config

This skill expects:
- `HOARD_TOKEN` (Bearer token)
- `HOARD_URL` (default: http://127.0.0.1:19850)

## Commands

Search:
```
{baseDir}/scripts/hoard_client.py search "meeting notes" --limit 5
```

Get doc by id:
```
{baseDir}/scripts/hoard_client.py get "abc123"
```

Memory get:
```
{baseDir}/scripts/hoard_client.py memory_get "some_key"
```
"""


def _openclaw_client_script() -> str:
    return """#!/usr/bin/env python3
import os
import json
import urllib.request
import urllib.error

HOARD_URL = os.environ.get("HOARD_URL", "http://127.0.0.1:19850")
HOARD_TOKEN = os.environ.get("HOARD_TOKEN", "")


def _call_mcp(method: str, params: dict) -> dict:
    req = urllib.request.Request(
        f"{HOARD_URL}/mcp",
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {HOARD_TOKEN}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.URLError as exc:
        return {"error": str(exc)}


def search(query: str, limit: int = 10) -> dict:
    return _call_mcp("tools/call", {"name": "search", "arguments": {"query": query, "limit": limit}})


def get(entity_id: str) -> dict:
    return _call_mcp("tools/call", {"name": "get", "arguments": {"entity_id": entity_id}})


def memory_get(key: str) -> dict:
    return _call_mcp("tools/call", {"name": "memory_get", "arguments": {"key": key}})


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Hoard API client")
    parser.add_argument("command", choices=["search", "get", "memory_get"])
    parser.add_argument("value", help="Query string, entity ID, or memory key")
    parser.add_argument("--limit", type=int, default=10, help="Result limit (search only)")
    args = parser.parse_args()

    if args.command == "search":
        result = search(args.value, args.limit)
    elif args.command == "get":
        result = get(args.value)
    else:
        result = memory_get(args.value)

    print(json.dumps(result, indent=2))
"""


def _verify_setup() -> None:
    config = load_config(None)
    host = config.get("server", {}).get("host", "127.0.0.1")
    port = int(config.get("server", {}).get("port", 19850))
    token = None
    tokens = config.get("security", {}).get("tokens", [])
    if tokens:
        token = tokens[0].get("token")

    console.print("\nTier 1: Hoard Server Health")
    if _is_server_healthy(host, port):
        console.print(f"  [green]✓[/green] Server responding: http://{host}:{port}")
    else:
        console.print(f"  [red]✗[/red] Server not responding: http://{host}:{port}")
        return

    if token:
        if _check_tools_list(host, port, token):
            console.print("  [green]✓[/green] Tools available")
        else:
            console.print("  [red]✗[/red] Tools list failed")

    console.print("\nTier 2: Client Configs")
    _check_file(Path.home() / ".claude.json", "Claude Code")
    _check_file(Path.home() / ".codex" / "config.toml", "Codex")

    console.print("\nTier 3: OpenClaw Skill")
    _check_file(Path.home() / ".openclaw" / "skills" / "hoard" / "SKILL.md", "OpenClaw SKILL.md")
    _check_file(Path.home() / ".openclaw" / "openclaw.json", "OpenClaw config")


def _check_file(path: Path, label: str) -> None:
    if path.exists():
        console.print(f"  [green]✓[/green] {label}: {path}")
    else:
        console.print(f"  [yellow]![/yellow] {label} missing")


def _check_tools_list(host: str, port: int, token: str) -> bool:
    import urllib.request
    import urllib.error

    try:
        req = urllib.request.Request(
            f"http://{host}:{port}/mcp",
            data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).encode(),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except urllib.error.HTTPError:
        return False
    except Exception:
        return False


def _uninstall_integration(client: str) -> None:
    if client == "openclaw":
        skill_dir = Path.home() / ".openclaw" / "skills" / "hoard"
        if skill_dir.exists():
            shutil.rmtree(skill_dir)
        config_path = Path.home() / ".openclaw" / "openclaw.json"
        if config_path.exists():
            try:
                data = json.loads(config_path.read_text())
                entries = data.get("skills", {}).get("entries", {})
                if "hoard" in entries:
                    entries.pop("hoard")
                    config_path.write_text(json.dumps(data, indent=2))
            except Exception:
                pass
        console.print("OpenClaw integration removed.")
    else:
        console.print(f"No uninstall handler for {client}.")


def _daemon_paths() -> tuple[Path, Path]:
    base = Path.home() / ".hoard"
    base.mkdir(parents=True, exist_ok=True)
    return base / "hoard.pid", base / "hoard.log"


def _serve_daemon(host: str, port: int) -> None:
    pid_path, log_path = _daemon_paths()
    if pid_path.exists():
        console.print("Hoard server already running.")
        return

    log_file = log_path.open("ab")
    process = subprocess.Popen(
        [sys.executable, "-m", "hoard.cli.main", "serve", "--host", host, "--port", str(port)],
        stdout=log_file,
        stderr=log_file,
        start_new_session=True,
    )
    pid_path.write_text(str(process.pid))
    console.print(f"Started daemon with PID {process.pid}")


def _serve_status() -> None:
    pid_path, _ = _daemon_paths()
    if not pid_path.exists():
        console.print("Hoard server is not running.")
        return
    pid = int(pid_path.read_text())
    if _pid_alive(pid):
        console.print(f"Hoard server running (PID {pid})")
    else:
        console.print("Hoard server not running (stale PID)")


def _serve_stop() -> None:
    pid_path, _ = _daemon_paths()
    if not pid_path.exists():
        console.print("No daemon PID found.")
        return
    pid = int(pid_path.read_text())
    try:
        os.kill(pid, signal.SIGTERM)
    except Exception:
        pass
    pid_path.unlink(missing_ok=True)
    console.print("Hoard server stopped.")


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _install_autostart(host: str, port: int) -> None:
    if sys.platform == "darwin":
        plist_path = Path.home() / "Library/LaunchAgents/com.hoard.server.plist"
        plist_path.parent.mkdir(parents=True, exist_ok=True)
        plist_content = f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
  <dict>
    <key>Label</key><string>com.hoard.server</string>
    <key>ProgramArguments</key>
    <array>
      <string>{sys.executable}</string>
      <string>-m</string>
      <string>hoard.cli.main</string>
      <string>serve</string>
      <string>--host</string><string>{host}</string>
      <string>--port</string><string>{port}</string>
    </array>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key><true/>
  </dict>
</plist>
"""
        plist_path.write_text(plist_content)
        console.print(f"Autostart installed: {plist_path}")
    elif sys.platform.startswith("linux"):
        systemd_path = Path.home() / ".config/systemd/user/hoard.service"
        systemd_path.parent.mkdir(parents=True, exist_ok=True)
        systemd_content = f"""[Unit]
Description=Hoard server

[Service]
ExecStart={sys.executable} -m hoard.cli.main serve --host {host} --port {port}
Restart=always

[Install]
WantedBy=default.target
"""
        systemd_path.write_text(systemd_content)
        console.print(f"Autostart installed: {systemd_path}")
    else:
        console.print("Autostart not supported on this platform.")
