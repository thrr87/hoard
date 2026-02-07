from __future__ import annotations

import json
import ipaddress
import os
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import List

import click
from rich.console import Console
from rich.table import Table

from hoard.cli.instructions import (
    INSTRUCTION_END_MARKER,
    INSTRUCTION_START_MARKER,
    apply_change_plan,
    build_change_plan,
    compute_targets,
    render_instruction_block,
    resolve_project_root,
)
from hoard.core.config import default_data_path, ensure_config_file, load_config, resolve_paths, save_config
from hoard.core.db.connection import connect, initialize_db, write_locked
from hoard.core.embeddings.model import EmbeddingError, EmbeddingModel
from hoard.core.mcp.server import run_server
from hoard.core.mcp.stdio import run_stdio
from hoard.core.mcp.tools import is_write_tool
from hoard.core.memory.store import memory_get, memory_prune, memory_put, memory_search
from hoard.core.onboarding import (
    detect_chrome_bookmarks_paths,
    detect_document_folders,
    detect_notion_exports,
    detect_obsidian_vaults,
)
from hoard.core.search.service import search_entities
from hoard.core.security.server_secret import (
    ensure_server_secret,
    require_server_secret,
    resolve_server_secret,
    server_secret_env_key,
    server_secret_file_path,
)
from hoard.core.sync.background import BackgroundSync
from hoard.core.sync.service import run_sync_with_lock
from hoard.core.sync.watcher import WATCHDOG_AVAILABLE
from hoard.sdk.retry import run_with_retry, should_retry_http_exception

console = Console()
DEFAULT_AGENT_SCOPES = ["search", "get", "memory", "sync", "ingest"]


def _server_base_url(config: dict) -> str:
    host = config.get("server", {}).get("host", "127.0.0.1")
    port = int(config.get("server", {}).get("port", 19850))
    return f"http://{host}:{port}"


def _normalize_base_url(url: str) -> str:
    value = (url or "").strip()
    if not value:
        raise click.ClickException("Missing URL.")

    parsed = urllib.parse.urlparse(value)
    if not parsed.scheme:
        parsed = urllib.parse.urlparse(f"http://{value}")
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise click.ClickException("URL must be a valid http(s) address.")

    path = parsed.path.rstrip("/")
    if path.endswith("/mcp"):
        path = path[:-4]
    normalized = parsed._replace(path=path.rstrip("/"), params="", query="", fragment="")
    return urllib.parse.urlunparse(normalized).rstrip("/")


def _normalize_mcp_url(url: str) -> str:
    base = _normalize_base_url(url)
    return f"{base}/mcp"


def _mcp_url(config: dict) -> str:
    return _normalize_mcp_url(_server_base_url(config))


def _persistence_warning_suffix(method: str, params: dict) -> str:
    if method != "tools/call":
        return ""
    tool_name = params.get("name") if isinstance(params, dict) else None
    if isinstance(tool_name, str) and is_write_tool(tool_name):
        return " Any attempted write may not have been persisted."
    return ""


def _call_mcp(config: dict, method: str, params: dict, *, token_override: str | None = None) -> dict:
    token = token_override or os.environ.get("HOARD_TOKEN")
    if not token:
        raise click.ClickException("HOARD_TOKEN is required for this command.")
    req = urllib.request.Request(
        _mcp_url(config),
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    try:
        def _send():
            return urllib.request.urlopen(req, timeout=30)

        with run_with_retry(_send, should_retry=should_retry_http_exception) as resp:
            payload = json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise click.ClickException(f"Server error: {exc}") from exc
    except urllib.error.URLError as exc:
        suffix = _persistence_warning_suffix(method, params)
        raise click.ClickException(f"Server unreachable: {exc}.{suffix}") from exc

    if isinstance(payload, dict) and payload.get("error"):
        message = payload["error"].get("message", payload["error"])
        raise click.ClickException(str(message))
    result = payload.get("result", payload)
    if method == "tools/call" and isinstance(result, dict) and "content" in result:
        try:
            return json.loads(result["content"][0]["text"])
        except (KeyError, IndexError, json.JSONDecodeError):
            pass
    return result


def _call_admin(config: dict, method: str, params: dict) -> dict:
    try:
        admin_token = require_server_secret(config)
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc
    return _call_mcp(config, method, params, token_override=admin_token)


def _registration_token(config: dict) -> str | None:
    env_key = config.get("orchestrator", {}).get("registration_token_env", "HOARD_REGISTRATION_TOKEN")
    value = os.environ.get(env_key) if env_key else None
    if value:
        return value
    return config.get("orchestrator", {}).get("registration_token")


def _call_registration(config: dict, method: str, params: dict) -> dict:
    token = _registration_token(config)
    if not token:
        raise click.ClickException("Registration token not configured. Run 'hoard orchestrate init'.")
    return _call_mcp(config, method, params, token_override=token)


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
    try:
        _run_sync(config_path=path)
    except click.ClickException as exc:
        console.print(f"[yellow]![/yellow] Sync skipped: {exc}")

    if run_setup:
        _run_setup_local(
            claude=False,
            codex=False,
            openclaw=False,
            setup_all=True,
            project_scope=False,
            verify=False,
            uninstall=None,
            no_instructions=False,
        )


@cli.command("add")
@click.argument("path", required=False)
@click.option("--obsidian", "obsidian_path", type=click.Path(path_type=Path))
@click.option("--notion", "notion_path", type=click.Path(path_type=Path))
@click.option("--inbox", "inbox_path", type=click.Path(path_type=Path))
def add_command(
    path: Path | None,
    obsidian_path: Path | None,
    notion_path: Path | None,
    inbox_path: Path | None,
) -> None:
    """Quickly add sources to index."""
    config_path = ensure_config_file(None)
    config = load_config(config_path)

    if inbox_path:
        _configure_inbox(config, inbox_path)
    elif obsidian_path:
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
    _run_sync(config_path=config_path)


@cli.command("sync")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def sync_command(config_path: Path | None) -> None:
    """Run sync for enabled connectors."""
    _run_sync(config_path)


def _run_sync(config_path: Path | None) -> None:
    config = load_config(config_path)
    result = run_sync_with_lock(config=config, config_path=config_path)
    if result.get("skipped"):
        console.print("Sync already running.")
        return

    connectors = result.get("connectors", [])
    if not connectors:
        console.print("No enabled connectors found.")
        return

    for entry in connectors:
        name = entry.get("source")
        success = entry.get("success", False)
        message = entry.get("message", "")
        if not success:
            console.print(f"Syncing {name}...")
            console.print(f"  [red]Discover failed:[/red] {message}")
            continue
        stats = entry.get("stats") or {}
        console.print(f"Syncing {name}...")
        console.print(
            f"  Entities: {stats.get('entities_seen', 0)}, "
            f"Chunks: {stats.get('chunks_written', 0)}, "
            f"Tombstoned: {stats.get('entities_tombstoned', 0)}, "
            f"Errors: {stats.get('errors', 0)}"
        )

    pruned = result.get("memory_pruned", 0)
    if pruned:
        console.print(f"Pruned {pruned} expired memory entries.")


@cli.command("search")
@click.argument("query", type=str)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
@click.option("--limit", default=20, show_default=True)
@click.option("--source", default=None, help="Filter by source name")
@click.option("--offset", default=0, show_default=True, help="Result offset (entity-based)")
@click.option("--json", "as_json", is_flag=True, default=False)
@click.option("--types", default=None, help="Comma-separated result types (entity,memory)")
@click.option("--no-memory", is_flag=True, default=False, help="Exclude memory results")
def search_command(
    query: str,
    config_path: Path | None,
    limit: int,
    source: str | None,
    offset: int,
    as_json: bool,
    types: str | None,
    no_memory: bool,
) -> None:
    """Search indexed content."""
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)
    conn = connect(paths.db_path)

    parsed_types = None
    if types:
        parsed_types = [value.strip() for value in types.split(",") if value.strip()]
    if no_memory:
        parsed_types = ["entity"]

    results, next_cursor = search_entities(
        conn,
        query=query,
        config=config,
        limit=limit,
        offset=offset,
        source=source,
        allow_sensitive=True,
        types=parsed_types,
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
        table_title = entity.get("entity_title") or entity.get("entity_id")
        table = Table(title=table_title, show_header=True)
        table.add_column("Type", style="magenta")
        table.add_column("Source", style="green")
        table.add_column("Chunk ID", style="dim")
        table.add_column("Score", style="cyan", justify="right")
        table.add_column("Content", style="white")

        for chunk in entity["chunks"]:
            snippet = chunk["content"].replace("\n", " ").strip()
            if len(snippet) > 200:
                snippet = snippet[:200] + "..."
            table.add_row(
                entity.get("result_type", "entity"),
                entity.get("source", "-"),
                chunk["chunk_id"],
                f"{chunk['score']:.4f}",
                snippet,
            )

        console.print(table)

    conn.close()


@cli.group("db")
def db_group() -> None:
    """Database utilities."""


@db_group.command("status")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def db_status_command(config_path: Path | None) -> None:
    from hoard.migrations import (
        MigrationError,
        get_current_version,
        get_migrations,
        get_pending_versions,
    )

    config = load_config(config_path)
    paths = resolve_paths(config, config_path)
    conn = connect(paths.db_path)

    migrations = get_migrations()
    latest = max(migrations.keys()) if migrations else 0
    current = get_current_version(conn)

    console.print(f"Database: {paths.db_path}")
    console.print(f"Schema version: {current}")
    console.print(f"Latest available: {latest}")

    if current > latest:
        console.print(
            "[yellow]⚠️  Database version is newer than code. "
            "Did you downgrade Hoard?[/yellow]"
        )

    try:
        pending = get_pending_versions(conn, target_version=latest) if current <= latest else []
    except MigrationError as exc:
        conn.close()
        raise click.ClickException(str(exc)) from exc
    console.print(f"Pending migrations: {len(pending)}")
    for version in pending:
        name = migrations[version].__name__.split(".")[-1]
        console.print(f"  - {name}")

    conn.close()


@db_group.command("migrate")
@click.option("--to", "target_version", type=int, default=None, help="Target version")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def db_migrate_command(target_version: int | None, config_path: Path | None) -> None:
    from hoard import __version__
    from hoard.migrations import MigrationError, get_current_version, get_migrations, migrate

    config = load_config(config_path)
    paths = resolve_paths(config, config_path)
    with write_locked(paths.db_path) as conn:
        current = get_current_version(conn)
        migrations = get_migrations()
        latest = max(migrations.keys()) if migrations else 0
        target = target_version if target_version is not None else latest
        console.print(f"📦 Applying schema migrations (v{current} → v{target})...")

        try:
            applied = migrate(conn, target_version=target_version, app_version=__version__)
        except MigrationError as exc:
            raise click.ClickException(str(exc)) from exc

        if not applied:
            console.print("No pending migrations.")
            return

        placeholders = ",".join("?" for _ in applied)
        rows = conn.execute(
            f"""
            SELECT version, name, duration_ms
            FROM schema_migrations
            WHERE version IN ({placeholders})
            ORDER BY version
            """,
            applied,
        ).fetchall()
        for row in rows:
            _, name, duration_ms = row[0], row[1], row[2]
            console.print(f"  {name} ... done ({duration_ms}ms)")

        console.print(f"Migrated from version {current} to {applied[-1]}")


@db_group.command("history")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def db_history_command(config_path: Path | None) -> None:
    import sqlite3

    config = load_config(config_path)
    paths = resolve_paths(config, config_path)
    conn = connect(paths.db_path)

    try:
        rows = conn.execute(
            "SELECT version, name, applied_at, duration_ms FROM schema_migrations ORDER BY version"
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []

    table = Table(title="Migration History", show_header=True)
    table.add_column("Version", justify="right")
    table.add_column("Name")
    table.add_column("Applied")
    table.add_column("Duration", justify="right")

    for row in rows:
        version, name, applied_at, duration_ms = row[0], row[1], row[2], row[3]
        table.add_row(str(version), name, applied_at, f"{duration_ms}ms" if duration_ms else "-")

    if not rows:
        console.print("No migration history found.")
    else:
        console.print(table)

    conn.close()


@db_group.command("verify")
@click.option("--deep", is_flag=True, default=False, help="Run integrity_check (slower)")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def db_verify_command(deep: bool, config_path: Path | None) -> None:
    from hoard.migrations import check_migration_integrity, get_migrations

    config = load_config(config_path)
    paths = resolve_paths(config, config_path)
    conn = connect(paths.db_path)

    fk_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_errors:
        console.print(f"[red]✗[/red] Foreign key check: {len(fk_errors)} violations")
    else:
        console.print("[green]✓[/green] Foreign key check: passed")

    mismatches = check_migration_integrity(conn, get_migrations(), warn=False)
    if mismatches:
        console.print(f"[red]✗[/red] Migration checksums: {len(mismatches)} mismatches")
        for version, name, stored, current in mismatches:
            console.print(f"  {version} {name}: stored={stored} current={current}")
    else:
        console.print("[green]✓[/green] Migration checksums: all match")

    if deep:
        result = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if result == "ok":
            console.print("[green]✓[/green] SQLite integrity_check: ok")
        else:
            console.print(f"[red]✗[/red] SQLite integrity_check: {result}")

    conn.close()


@cli.group("memory")
def memory_group() -> None:
    """Manage memory entries."""


@memory_group.command("put")
@click.argument("key", type=str)
@click.argument("content", type=str)
@click.option("--tags", default="", help="Comma-separated tags")
@click.option("--metadata", default=None, help="JSON metadata")
@click.option("--ttl-days", type=int, default=None, help="Optional TTL in days")
@click.option("--expires-at", default=None, help="Optional ISO expiry timestamp")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def memory_put_command(
    key: str,
    content: str,
    tags: str,
    metadata: str | None,
    ttl_days: int | None,
    expires_at: str | None,
    config_path: Path | None,
) -> None:
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)
    with write_locked(paths.db_path) as conn:
        initialize_db(conn)

        tag_list = [tag.strip() for tag in tags.split(",") if tag.strip()] if tags else []
        metadata_obj = json.loads(metadata) if metadata else None
        entry = memory_put(
            conn,
            key=key,
            content=content,
            tags=tag_list,
            metadata=metadata_obj,
            ttl_days=ttl_days,
            expires_at=expires_at,
            default_ttl_days=config.get("memory", {}).get("default_ttl_days"),
        )
        console.print_json(json.dumps(entry))


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

    results = memory_search(conn, query, limit=limit, config=config)
    conn.close()
    console.print_json(json.dumps({"results": results}))


@memory_group.command("prune")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def memory_prune_command(config_path: Path | None) -> None:
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)
    with write_locked(paths.db_path) as conn:
        initialize_db(conn)
        removed = memory_prune(conn)
    console.print(f"Pruned {removed} expired memory entries.")


@cli.group("embeddings")
def embeddings_group() -> None:
    """Manage embeddings."""


@embeddings_group.command("build")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
@click.option("--source", default=None, help="Filter by source name")
def embeddings_build_command(config_path: Path | None, source: str | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {"name": "embeddings_build", "arguments": {"source": source}},
    )
    console.print_json(json.dumps(result))


@cli.group("tokens")
def tokens_group() -> None:
    """Manage API tokens."""


@tokens_group.command("add")
@click.argument("name", type=str)
@click.option("--scopes", default="search,get,memory,sync,ingest", help="Comma-separated scopes")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def tokens_add_command(name: str, scopes: str, config_path: Path | None) -> None:
    config = load_config(config_path)
    scope_list = [scope.strip() for scope in scopes.split(",") if scope.strip()]
    result = _call_admin(
        config,
        "tools/call",
        {"name": "agent_register", "arguments": {"agent_id": name, "scopes": scope_list}},
    )
    console.print(f"Token created for {name}: {result.get('token')}")


@tokens_group.command("list")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def tokens_list_command(config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_admin(config, "tools/call", {"name": "agent_list", "arguments": {}})
    agents = result.get("agents", [])

    table = Table(title="Agents", show_header=True)
    table.add_column("Agent")
    table.add_column("Scopes")
    table.add_column("Sensitive")
    table.add_column("Restricted")

    for agent in agents:
        table.add_row(
            agent.get("agent_id", ""),
            ", ".join(agent.get("scopes", [])),
            "yes" if agent.get("can_access_sensitive") else "no",
            "yes" if agent.get("can_access_restricted") else "no",
        )

    console.print(table)


@tokens_group.command("remove")
@click.argument("name", type=str)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def tokens_remove_command(name: str, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_admin(
        config,
        "tools/call",
        {"name": "agent_remove", "arguments": {"agent_id": name}},
    )
    if result.get("success"):
        console.print(f"Removed agent {name}.")
    else:
        console.print(f"Agent {name} not found.")


@cli.group("orchestrate")
def orchestrate_group() -> None:
    """Orchestration setup."""


@orchestrate_group.command("init")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def orchestrate_init_command(config_path: Path | None) -> None:
    config_path = ensure_config_file(config_path)
    config = load_config(config_path)

    orchestrator = config.setdefault("orchestrator", {})
    token = orchestrator.get("registration_token")
    if not token:
        token = f"hoard_reg_{secrets.token_hex(16)}"
        orchestrator["registration_token"] = token

    artifacts = config.setdefault("artifacts", {})
    default_path = artifacts.get("blob_path") or str(default_data_path("artifacts"))
    blob_path = click.prompt("Artifact store path", default=default_path)
    artifacts["blob_path"] = blob_path

    default_retention = int(artifacts.get("retention_days") or 30)
    retention_days = click.prompt("Artifact retention days", default=default_retention)
    artifacts["retention_days"] = int(retention_days)

    save_config(config, config_path)

    env_key = config.get("orchestrator", {}).get("registration_token_env", "HOARD_REGISTRATION_TOKEN")
    console.print("\n✓ Orchestration initialized.")
    console.print(f"Registration token stored in config. To use via env:\n  export {env_key}={token}")
    console.print("\nNext steps:")
    console.print("  1. Start server: hoard serve")
    console.print("  2. Register agents: hoard agent register <name> --type worker")


@cli.group("agent")
def agent_group() -> None:
    """Manage orchestration agents."""


@agent_group.command("register")
@click.argument("name", type=str)
@click.option("--type", "agent_type", default="worker")
@click.option("--scopes", default="", help="Comma-separated scopes")
@click.option("--capabilities", default="", help="Comma-separated capabilities")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def agent_register_command(
    name: str,
    agent_type: str,
    scopes: str,
    capabilities: str,
    config_path: Path | None,
) -> None:
    config = load_config(config_path)
    scope_list = [s.strip() for s in scopes.split(",") if s.strip()] if scopes else None
    cap_list = [s.strip() for s in capabilities.split(",") if s.strip()] if capabilities else None
    result = _call_registration(
        config,
        "tools/call",
        {
            "name": "agent.register",
            "arguments": {
                "name": name,
                "agent_type": agent_type,
                "scopes": scope_list,
                "capabilities": cap_list,
            },
        },
    )
    console.print_json(json.dumps(result))


@agent_group.command("list")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def agent_list_command(config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(config, "tools/call", {"name": "agent.list", "arguments": {}})
    console.print_json(json.dumps(result))


@agent_group.command("deregister")
@click.argument("agent_id", type=str)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def agent_deregister_command(agent_id: str, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {"name": "agent.deregister", "arguments": {"agent_id": agent_id}},
    )
    console.print_json(json.dumps(result))


@cli.group("task")
def task_group() -> None:
    """Manage orchestration tasks."""


@task_group.command("create")
@click.argument("name", type=str)
@click.option("--description", default=None)
@click.option("--capability", "requires_capability", default=None)
@click.option("--priority", default=5, type=int)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def task_create_command(
    name: str,
    description: str | None,
    requires_capability: str | None,
    priority: int,
    config_path: Path | None,
) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {
            "name": "task.create",
            "arguments": {
                "name": name,
                "description": description,
                "requires_capability": requires_capability,
                "priority": priority,
            },
        },
    )
    console.print_json(json.dumps(result))


@task_group.command("list")
@click.option("--status", default=None)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def task_list_command(status: str | None, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {"name": "task.list", "arguments": {"status": status}},
    )
    console.print_json(json.dumps(result))


@task_group.command("poll")
@click.option("--limit", default=5, type=int)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def task_poll_command(limit: int, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {"name": "task.poll", "arguments": {"limit": limit}},
    )
    console.print_json(json.dumps(result))


@task_group.command("claim")
@click.argument("task_id", type=str)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def task_claim_command(task_id: str, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {"name": "task.claim", "arguments": {"task_id": task_id}},
    )
    console.print_json(json.dumps(result))


@task_group.command("complete")
@click.argument("task_id", type=str)
@click.option("--summary", default=None)
@click.option("--artifact", "output_artifact_id", default=None)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def task_complete_command(task_id: str, summary: str | None, output_artifact_id: str | None, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {
            "name": "task.complete",
            "arguments": {"task_id": task_id, "output_summary": summary, "output_artifact_id": output_artifact_id},
        },
    )
    console.print_json(json.dumps(result))


@cli.group("artifact")
def artifact_group() -> None:
    """Manage task artifacts."""


@artifact_group.command("put")
@click.argument("task_id", type=str)
@click.argument("name", type=str)
@click.option("--type", "artifact_type", default="text")
@click.option("--content", default=None)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def artifact_put_command(task_id: str, name: str, artifact_type: str, content: str | None, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {
            "name": "artifact.put",
            "arguments": {"task_id": task_id, "name": name, "artifact_type": artifact_type, "content": content},
        },
    )
    console.print_json(json.dumps(result))


@artifact_group.command("get")
@click.argument("artifact_id", type=str)
@click.option("--include-content", is_flag=True, default=False)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def artifact_get_command(artifact_id: str, include_content: bool, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {"name": "artifact.get", "arguments": {"artifact_id": artifact_id, "include_content": include_content}},
    )
    console.print_json(json.dumps(result))


@cli.group("event")
def event_group() -> None:
    """Manage events."""


@event_group.command("poll")
@click.option("--since", default=None)
@click.option("--limit", default=50, type=int)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def event_poll_command(since: str | None, limit: int, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {"name": "event.poll", "arguments": {"since": since, "limit": limit}},
    )
    console.print_json(json.dumps(result))


@cli.group("cost")
def cost_group() -> None:
    """Cost reporting."""


@cost_group.command("summary")
@click.option("--period", default="today")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def cost_summary_command(period: str, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {"name": "cost.summary", "arguments": {"period": period}},
    )
    console.print_json(json.dumps(result))


@cli.group("workflow")
def workflow_group() -> None:
    """Manage workflows."""


@workflow_group.command("list")
@click.option("--status", default=None)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def workflow_list_command(status: str | None, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {"name": "workflow.list", "arguments": {"status": status}},
    )
    console.print_json(json.dumps(result))


@workflow_group.command("create")
@click.argument("name", type=str)
@click.option("--definition", "definition_path", type=click.Path(path_type=Path), required=True)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def workflow_create_command(name: str, definition_path: Path, config_path: Path | None) -> None:
    config = load_config(config_path)
    definition = json.loads(definition_path.read_text())
    result = _call_mcp(
        config,
        "tools/call",
        {"name": "workflow.create", "arguments": {"name": name, "definition": definition}},
    )
    console.print_json(json.dumps(result))


@workflow_group.command("start")
@click.argument("workflow_id", type=str)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def workflow_start_command(workflow_id: str, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {"name": "workflow.start", "arguments": {"workflow_id": workflow_id}},
    )
    console.print_json(json.dumps(result))


@workflow_group.command("status")
@click.argument("workflow_id", type=str)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def workflow_status_command(workflow_id: str, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {"name": "workflow.status", "arguments": {"workflow_id": workflow_id}},
    )
    console.print_json(json.dumps(result))


@event_group.command("publish")
@click.argument("event_type", type=str)
@click.option("--payload", default="{}", help="JSON payload")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def event_publish_command(event_type: str, payload: str, config_path: Path | None) -> None:
    config = load_config(config_path)
    payload_obj = json.loads(payload) if payload else {}
    result = _call_mcp(
        config,
        "tools/call",
        {"name": "event.publish", "arguments": {"event_type": event_type, "payload": payload_obj}},
    )
    console.print_json(json.dumps(result))


@cost_group.command("budget")
@click.option("--agent", "agent_id", default=None)
@click.option("--workflow", "workflow_id", default=None)
@click.option("--period", default="today")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def cost_budget_command(agent_id: str | None, workflow_id: str | None, period: str, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {
            "name": "cost.budget",
            "arguments": {"agent_id": agent_id, "workflow_id": workflow_id, "period": period},
        },
    )
    console.print_json(json.dumps(result))


@artifact_group.command("list")
@click.option("--task", "task_id", default=None)
@click.option("--workflow", "workflow_id", default=None)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def artifact_list_command(task_id: str | None, workflow_id: str | None, config_path: Path | None) -> None:
    config = load_config(config_path)
    result = _call_mcp(
        config,
        "tools/call",
        {"name": "artifact.list", "arguments": {"task_id": task_id, "workflow_id": workflow_id}},
    )
    console.print_json(json.dumps(result))


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
    db_token_count = conn.execute("SELECT COUNT(*) FROM agent_tokens").fetchone()[0]
    if not tokens and db_token_count == 0:
        console.print("[yellow][!][/yellow] No MCP token configured for Claude Code")

    inbox_cfg = config.get("connectors", {}).get("inbox", {})
    inbox_enabled = isinstance(inbox_cfg, dict) and inbox_cfg.get("enabled", False)
    inbox_path = inbox_cfg.get("path") if isinstance(inbox_cfg, dict) else ""
    if inbox_enabled:
        exists = Path(inbox_path).expanduser().exists() if inbox_path else False
        status = "ok" if exists else "missing"
        console.print(f"Inbox: {status} ({inbox_path})")

    watcher_enabled = config.get("sync", {}).get("watcher_enabled", False)
    if watcher_enabled and not WATCHDOG_AVAILABLE:
        console.print("[yellow][!][/yellow] Watcher enabled but watchdog not installed")

    conn.close()


@cli.command("sync-status")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def sync_status_command(config_path: Path | None) -> None:
    connectors_status_command(config_path)


def _is_loopback_host(host: str) -> bool:
    normalized = (host or "").strip().lower()
    if normalized in {"127.0.0.1", "localhost", "::1", "[::1]"}:
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def _require_remote_bind_opt_in(host: str, allow_remote: bool) -> None:
    if _is_loopback_host(host):
        return
    if allow_remote:
        return
    raise click.ClickException(
        "Refusing non-loopback bind without explicit remote opt-in.\n"
        "Use --allow-remote or set server.allow_remote: true in config."
    )


@cli.command("serve")
@click.option("--host", default=None, help="Server host")
@click.option("--port", default=None, type=int, help="Server port")
@click.option("--allow-remote", is_flag=True, default=False, help="Allow non-loopback bind addresses")
@click.option("--daemon", is_flag=True, default=False, help="Run as background daemon")
@click.option("--status", is_flag=True, default=False, help="Check daemon status")
@click.option("--stop", is_flag=True, default=False, help="Stop daemon")
@click.option("--no-migrate", is_flag=True, default=False, help="Skip automatic schema migrations")
@click.option(
    "--install-autostart",
    is_flag=True,
    default=False,
    help="Install autostart on login",
)
def serve_command(
    host: str | None,
    port: int | None,
    allow_remote: bool,
    daemon: bool,
    status: bool,
    stop: bool,
    no_migrate: bool,
    install_autostart: bool,
) -> None:
    config = load_config(None)
    server_config = config.get("server", {})
    host = host or server_config.get("host", "127.0.0.1")
    port = port or int(server_config.get("port", 19850))
    allow_remote = allow_remote or bool(server_config.get("allow_remote", False))

    if status:
        _serve_status()
        return
    if stop:
        _serve_stop()
        return
    if install_autostart:
        _install_autostart(host, port, allow_remote=allow_remote)
        return

    _require_remote_bind_opt_in(host, allow_remote)

    if config.get("write", {}).get("enabled", True):
        had_secret = bool(resolve_server_secret(config))
        try:
            _, source = ensure_server_secret(config, generate=True)
        except RuntimeError as exc:
            raise click.ClickException(str(exc)) from exc
        if not had_secret and source == "file":
            console.print(f"Generated server secret in {server_secret_file_path(config)}")

    if daemon:
        _serve_daemon(host, port, no_migrate=no_migrate, allow_remote=allow_remote)
        return

    background = BackgroundSync(config=config, config_path=None, log=console.print)
    background.start()

    console.print(f"Starting Hoard server on http://{host}:{port}/mcp")
    run_server(host=host, port=port, config_path=None, no_migrate=no_migrate)


@cli.group("mcp")
def mcp_group() -> None:
    """Run MCP HTTP server."""


@mcp_group.command("serve")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=19850, show_default=True)
@click.option("--allow-remote", is_flag=True, default=False, help="Allow non-loopback bind addresses")
@click.option("--no-migrate", is_flag=True, default=False, help="Skip automatic schema migrations")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def mcp_serve_command(
    host: str,
    port: int,
    allow_remote: bool,
    no_migrate: bool,
    config_path: Path | None,
) -> None:
    config = load_config(config_path)
    allow_remote = allow_remote or bool(config.get("server", {}).get("allow_remote", False))
    _require_remote_bind_opt_in(host, allow_remote)
    if config.get("write", {}).get("enabled", True):
        had_secret = bool(resolve_server_secret(config))
        try:
            _, source = ensure_server_secret(config, generate=True)
        except RuntimeError as exc:
            raise click.ClickException(str(exc)) from exc
        if not had_secret and source == "file":
            console.print(f"Generated server secret in {server_secret_file_path(config)}")
    background = BackgroundSync(config=config, config_path=config_path, log=console.print)
    background.start()
    console.print(f"Starting MCP server on {host}:{port}")
    run_server(host=host, port=port, config_path=config_path, no_migrate=no_migrate)


@mcp_group.command("stdio")
@click.option("--config", "config_path", type=click.Path(path_type=Path))
def mcp_stdio_command(config_path: Path | None) -> None:
    run_stdio(config_path=config_path)


@cli.command("instructions")
@click.option("--claude", is_flag=True, default=False, help="Update Claude project instructions")
@click.option("--codex", is_flag=True, default=False, help="Update Codex project instructions")
@click.option("--openclaw", is_flag=True, default=False, help="Update OpenClaw skill instructions")
@click.option("--all", "instructions_all", is_flag=True, default=False, help="Update all targets")
@click.option("--root", "root_path", type=click.Path(path_type=Path), default=None)
@click.option("--dry-run", is_flag=True, default=False, help="Preview planned changes without writing")
@click.option("--yes", "assume_yes", is_flag=True, default=False, help="Apply without confirmation")
def instructions_command(
    claude: bool,
    codex: bool,
    openclaw: bool,
    instructions_all: bool,
    root_path: Path | None,
    dry_run: bool,
    assume_yes: bool,
) -> None:
    targets = _resolve_instruction_targets(
        claude=claude,
        codex=codex,
        openclaw=openclaw,
        instructions_all=instructions_all,
    )

    project_required = any(target in {"claude", "codex"} for target in targets)
    root = resolve_project_root(Path.cwd(), explicit_root=root_path) if project_required else None
    if project_required and root is None:
        flags = _instruction_target_flags(targets)
        command = f"hoard instructions {flags} --root {Path.cwd()}"
        raise click.ClickException(f"Could not determine project root.\nRun: {command}")

    plans = compute_targets(root, targets)
    block = render_instruction_block(_instruction_docs_url())
    changes = build_change_plan(
        plans,
        block,
        start_marker=INSTRUCTION_START_MARKER,
        end_marker=INSTRUCTION_END_MARKER,
    )
    _print_instruction_plan(changes)

    if dry_run:
        console.print("Dry run complete. No files were changed.")
        return

    if not any(change.changed for change in changes):
        console.print("Instruction files already up to date.")
        return

    if not assume_yes:
        prompt = "Apply Hoard instruction updates to these files?"
        if not click.confirm(prompt, default=True):
            console.print("Instruction update canceled.")
            return

    result = apply_change_plan(changes)
    console.print(f"Instruction update complete. Changed {len(result.applied)} file(s).")


@cli.group("setup", invoke_without_command=True)
@click.pass_context
@click.option("--claude", is_flag=True, default=False, help="Configure Claude Code")
@click.option("--codex", is_flag=True, default=False, help="Configure Codex")
@click.option("--openclaw", is_flag=True, default=False, help="Configure OpenClaw")
@click.option("--all", "setup_all", is_flag=True, default=False, help="Configure all detected clients")
@click.option("--project-scope", is_flag=True, default=False, help="Use project-scope config for Claude")
@click.option("--verify", is_flag=True, default=False, help="Verify integrations")
@click.option("--uninstall", type=str, default=None, help="Uninstall client integration")
@click.option("--no-instructions", is_flag=True, default=False, help="Skip instruction updates")
def setup_group(
    ctx: click.Context,
    claude: bool,
    codex: bool,
    openclaw: bool,
    setup_all: bool,
    project_scope: bool,
    verify: bool,
    uninstall: str | None,
    no_instructions: bool,
) -> None:
    if ctx.invoked_subcommand:
        return
    _run_setup_local(
        claude=claude,
        codex=codex,
        openclaw=openclaw,
        setup_all=setup_all,
        project_scope=project_scope,
        verify=verify,
        uninstall=uninstall,
        no_instructions=no_instructions,
    )


@setup_group.command("remote")
@click.option("--url", required=True, help="Remote Hoard URL or MCP endpoint.")
@click.option("--token", default=None, help="Existing agent token (recommended).")
@click.option(
    "--admin-token",
    default=None,
    help="Admin token for automation-only auto-provisioning (advanced).",
)
@click.option("--claude", is_flag=True, default=False, help="Configure Claude Code")
@click.option("--codex", is_flag=True, default=False, help="Configure Codex")
@click.option("--openclaw", is_flag=True, default=False, help="Configure OpenClaw")
@click.option("--all", "setup_all", is_flag=True, default=False, help="Configure all detected clients")
@click.option("--project-scope", is_flag=True, default=False, help="Use project-scope config for Claude")
@click.option("--dry-run", is_flag=True, default=False, help="Print actions without writing files")
def setup_remote_command(
    url: str,
    token: str | None,
    admin_token: str | None,
    claude: bool,
    codex: bool,
    openclaw: bool,
    setup_all: bool,
    project_scope: bool,
    dry_run: bool,
) -> None:
    _run_setup_remote(
        url=url,
        token=token,
        admin_token=admin_token,
        claude=claude,
        codex=codex,
        openclaw=openclaw,
        setup_all=setup_all,
        project_scope=project_scope,
        dry_run=dry_run,
    )


def _run_setup_local(
    *,
    claude: bool,
    codex: bool,
    openclaw: bool,
    setup_all: bool,
    project_scope: bool,
    verify: bool,
    uninstall: str | None,
    no_instructions: bool,
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
    url = _normalize_mcp_url(f"http://{host}:{port}")

    targets = _resolve_setup_targets(claude, codex, openclaw, setup_all)
    if not targets:
        console.print("No clients selected.")
        return

    had_secret = bool(resolve_server_secret(config))
    try:
        _, secret_source = ensure_server_secret(config, generate=True)
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc
    if not had_secret and secret_source == "file":
        console.print(f"Generated server secret in {server_secret_file_path(config)}")

    _ensure_server_running(host, port)
    token_value = _ensure_token(config, name="default")

    client_status: dict[str, str] = {}
    if "claude" in targets:
        if project_scope:
            _configure_claude_project_scope(url)
            client_status["Claude Code"] = "configured (project-scope)"
        else:
            _configure_claude_user_scope(url, token_value)
            client_status["Claude Code"] = "configured (user-scope)"
    if "codex" in targets:
        _configure_codex(url)
        client_status["Codex"] = "configured"
    if "openclaw" in targets:
        _configure_openclaw(url, token_value)
        client_status["OpenClaw"] = "configured"

    if no_instructions:
        console.print("Skipping instruction injection (--no-instructions).")
        instructions_result = {
            "status": "skipped",
            "detail": "Disabled by --no-instructions.",
            "command": f"hoard instructions {_instruction_target_flags(targets)} --root {Path.cwd()}",
        }
    else:
        instructions_result = _maybe_apply_instructions_for_setup(targets)

    _print_setup_summary(
        config=config,
        host=host,
        port=port,
        targets=targets,
        secret_source=secret_source,
        token_value=token_value,
        client_status=client_status,
        instructions_result=instructions_result,
    )


def _run_setup_remote(
    *,
    url: str,
    token: str | None,
    admin_token: str | None,
    claude: bool,
    codex: bool,
    openclaw: bool,
    setup_all: bool,
    project_scope: bool,
    dry_run: bool,
) -> None:
    mcp_url = _normalize_mcp_url(url)
    base_url = _normalize_base_url(url)
    targets = _resolve_setup_targets(claude, codex, openclaw, setup_all)
    if not targets:
        console.print("No clients selected.")
        return

    console.print(
        "Credential flow:\n"
        "  Recommended: --token with a pre-provisioned agent token.\n"
        "  Advanced automation: --admin-token for remote auto-provisioning."
    )

    if not token and not admin_token:
        raise click.ClickException("Provide --token (recommended) or --admin-token (advanced).")

    health = _remote_health(base_url)
    if health is None:
        console.print(f"[yellow][!][/yellow] Health check unavailable at {base_url}/health")
    else:
        status_text = "ok" if health.get("status") == "ok" else "degraded"
        console.print(f"Remote health: {status_text}")

    token_map: dict[str, str] = {}
    if token:
        _validate_remote_token(mcp_url, token, label="token")
        for target in targets:
            token_map[target] = token
    else:
        _validate_remote_token(mcp_url, admin_token, label="admin-token")
        for target in targets:
            provisioned = _provision_remote_token(
                mcp_url=mcp_url,
                admin_token=admin_token or "",
                target=target,
            )
            token_map[target] = provisioned
            console.print(f"Provisioned token for {target}.")

    if dry_run:
        console.print("Dry run: no files were changed.")
        _print_remote_setup_next_steps(
            mcp_url=mcp_url,
            targets=targets,
            token_map=token_map,
            project_scope=project_scope,
        )
        return

    if "claude" in targets:
        if project_scope:
            _configure_claude_project_scope(mcp_url)
        else:
            _configure_claude_user_scope(mcp_url, token_map["claude"])
    if "codex" in targets:
        _configure_codex(mcp_url)
    if "openclaw" in targets:
        _configure_openclaw(mcp_url, token_map["openclaw"])

    _print_remote_setup_next_steps(
        mcp_url=mcp_url,
        targets=targets,
        token_map=token_map,
        project_scope=project_scope,
    )


def _print_remote_setup_next_steps(
    *,
    mcp_url: str,
    targets: list[str],
    token_map: dict[str, str],
    project_scope: bool,
) -> None:
    console.print("\nRemote setup complete.")
    console.print(f"Remote MCP endpoint: {mcp_url}")
    if "claude" in targets and project_scope:
        console.print("\nClaude project-scope token setup:")
        console.print(f'  export HOARD_TOKEN="{token_map["claude"]}"')
    if "codex" in targets:
        console.print("\nCodex token setup:")
        console.print(f'  export HOARD_TOKEN="{token_map["codex"]}"')
    if "claude" in targets and not project_scope:
        console.print("\nClaude Code uses the configured token directly.")
    if "openclaw" in targets:
        console.print("\nOpenClaw stores token in ~/.openclaw/openclaw.json.")
    console.print("\nNext steps:")
    console.print("  1. Restart your AI tools")
    console.print("  2. Run hoard setup --verify for local config checks")


def _check(label: str, ok: bool) -> None:
    status = "[green]✓[/green]" if ok else "[red]✗[/red]"
    console.print(f"{status} {label}")


def _print_setup_summary(
    *,
    config: dict,
    host: str,
    port: int,
    targets: list[str],
    secret_source: str,
    token_value: str,
    client_status: dict[str, str],
    instructions_result: dict[str, str],
) -> None:
    table = Table(title="Setup Summary", show_header=True)
    table.add_column("Item")
    table.add_column("Status")

    table.add_row("Server health", "ok")
    if secret_source == "env":
        table.add_row("Server secret", f"env ({server_secret_env_key(config)})")
    else:
        table.add_row("Server secret", f"file ({server_secret_file_path(config)})")
    table.add_row("Token provisioning", "registered default agent token")

    for name in ["Claude Code", "Codex", "OpenClaw"]:
        status = client_status.get(name)
        if status:
            table.add_row(name, status)
        elif name.lower().split()[0] in targets:
            table.add_row(name, "configured")
    table.add_row("Instructions", f"{instructions_result.get('status', 'unknown')}: {instructions_result.get('detail', '')}")

    console.print(table)

    command = instructions_result.get("command")
    if command:
        console.print(f"Instruction fix: {command}")

    if "codex" in targets:
        console.print("\nCodex token setup:")
        console.print(f'  export HOARD_TOKEN="{token_value}"')

    console.print("\nNext steps:")
    console.print("  1. Verify setup: hoard setup --verify")
    console.print(f"  2. Confirm server: http://{host}:{port}/mcp")
    console.print("  3. Restart your AI tools to load updated MCP config")


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
        ("inbox", "Agent inbox (drop folder)"),
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

    if "inbox" in selected:
        _configure_inbox(config, None)

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

    enable_schedule = click.confirm("Enable background sync schedule?", default=True)
    if enable_schedule:
        interval = click.prompt(
            "Sync interval (minutes)",
            default=str(config.get("sync", {}).get("interval_minutes", 15)),
        )
        try:
            config.setdefault("sync", {})["interval_minutes"] = int(interval)
        except ValueError:
            config.setdefault("sync", {})["interval_minutes"] = 15
    else:
        config.setdefault("sync", {})["interval_minutes"] = 0

    enable_watcher = click.confirm("Enable file watcher for live updates?", default=False)
    config.setdefault("sync", {})["watcher_enabled"] = enable_watcher

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


def _configure_inbox(config: dict, inbox_path: Path | None) -> None:
    inbox = config.setdefault("connectors", {}).setdefault("inbox", {})
    inbox["enabled"] = True
    inbox.setdefault(
        "include_extensions",
        [".md", ".txt", ".csv", ".json", ".yaml", ".rst"],
    )

    if inbox_path is None:
        default_path = inbox.get("path") or str(default_data_path("inbox"))
        choice = click.prompt("Agent inbox folder", default=default_path)
        inbox_path = Path(choice).expanduser()

    inbox["path"] = str(inbox_path)
    inbox_path.mkdir(parents=True, exist_ok=True)


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


def _resolve_instruction_targets(
    *,
    claude: bool,
    codex: bool,
    openclaw: bool,
    instructions_all: bool,
) -> List[str]:
    if instructions_all:
        return ["claude", "codex", "openclaw"]

    targets: List[str] = []
    if claude:
        targets.append("claude")
    if codex:
        targets.append("codex")
    if openclaw:
        targets.append("openclaw")
    if targets:
        return targets
    return ["claude", "codex", "openclaw"]


def _instruction_target_flags(targets: List[str]) -> str:
    flags = []
    if "claude" in targets:
        flags.append("--claude")
    if "codex" in targets:
        flags.append("--codex")
    if "openclaw" in targets:
        flags.append("--openclaw")
    return " ".join(flags) if flags else "--all"


def _instruction_docs_url() -> str:
    return "https://github.com/thrr87/hoard/blob/main/README.md#additional-mcp-tools"


def _print_instruction_plan(changes) -> None:
    table = Table(title="Instruction updates")
    table.add_column("Target")
    table.add_column("Action")
    table.add_column("Path")
    for change in changes:
        action = change.action if change.changed else "noop"
        table.add_row(change.target, action, str(change.path))
    console.print(table)


def _is_interactive_session() -> bool:
    return bool(sys.stdin.isatty())


def _maybe_apply_instructions_for_setup(setup_targets: List[str]) -> dict[str, str]:
    instruction_targets = [target for target in setup_targets if target in {"claude", "codex", "openclaw"}]
    if not instruction_targets:
        return {"status": "skipped", "detail": "No instruction-capable clients selected.", "command": ""}

    if not _is_interactive_session():
        flags = _instruction_target_flags(instruction_targets)
        console.print("Skipping instruction injection in non-interactive mode.")
        command = f"hoard instructions {flags} --root {Path.cwd()}"
        console.print(f"Run: {command}")
        return {"status": "skipped", "detail": "Non-interactive session.", "command": command}

    root = resolve_project_root(Path.cwd()) if any(t in {"claude", "codex"} for t in instruction_targets) else None
    if any(t in {"claude", "codex"} for t in instruction_targets) and root is None:
        flags = _instruction_target_flags(instruction_targets)
        console.print("Could not determine a project root for instruction injection.")
        command = f"hoard instructions {flags} --root {Path.cwd()}"
        console.print(f"Run: {command}")
        return {"status": "skipped", "detail": "Project root not detected.", "command": command}

    plans = compute_targets(root, instruction_targets)
    block = render_instruction_block(_instruction_docs_url())
    changes = build_change_plan(
        plans,
        block,
        start_marker=INSTRUCTION_START_MARKER,
        end_marker=INSTRUCTION_END_MARKER,
    )

    if not any(change.changed for change in changes):
        console.print("Instruction files already up to date.")
        return {"status": "up-to-date", "detail": "Instruction files already up to date.", "command": ""}

    _print_instruction_plan(changes)
    if not click.confirm("Apply Hoard instruction updates to these files?", default=True):
        console.print("Instruction update canceled.")
        flags = _instruction_target_flags(instruction_targets)
        command = f"hoard instructions {flags} --root {Path.cwd()}"
        return {"status": "skipped", "detail": "User canceled instruction update.", "command": command}

    result = apply_change_plan(changes)
    console.print(f"Instruction update complete. Changed {len(result.applied)} file(s).")
    return {"status": "applied", "detail": f"Changed {len(result.applied)} file(s).", "command": ""}


def _ensure_token(config: dict, name: str) -> str:
    result = _call_admin(
        config,
        "tools/call",
        {
            "name": "agent_register",
            "arguments": {
                "agent_id": name,
                "scopes": DEFAULT_AGENT_SCOPES,
                "overwrite": True,
            },
        },
    )
    token_value = result.get("token")
    if not token_value:
        raise click.ClickException("Failed to provision token.")
    return token_value


def _validate_remote_token(mcp_url: str, token: str | None, label: str) -> None:
    if not token:
        raise click.ClickException(f"{label} is required.")
    response = _mcp_jsonrpc_url(mcp_url, token, "tools/list", {})
    if response.get("error"):
        message = response["error"].get("message", "unknown error")
        raise click.ClickException(f"{label} validation failed: {message}")


def _remote_agent_id(target: str) -> str:
    host = socket.gethostname().lower()
    safe_host = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in host).strip("-_")
    safe_host = safe_host or "host"
    return f"{safe_host}-{target}"


def _provision_remote_token(*, mcp_url: str, admin_token: str, target: str) -> str:
    result = _mcp_jsonrpc_url(
        mcp_url,
        admin_token,
        "tools/call",
        {
            "name": "agent_register",
            "arguments": {
                "agent_id": _remote_agent_id(target),
                "scopes": DEFAULT_AGENT_SCOPES,
                "overwrite": True,
            },
        },
    )
    if result.get("error"):
        message = result["error"].get("message", "unknown error")
        raise click.ClickException(f"Failed to provision token for {target}: {message}")
    try:
        tool_result = result["result"]["content"][0]["text"]
        payload = json.loads(tool_result)
        token_value = payload["token"]
    except Exception as exc:
        raise click.ClickException(f"Failed to parse token provisioning response for {target}.") from exc
    return token_value


def _ensure_server_running(host: str, port: int) -> None:
    if _is_server_healthy(host, port):
        return
    _serve_daemon(host, port)
    for _ in range(10):
        if _is_server_healthy(host, port):
            return
        time.sleep(0.5)
    _, log_path = _daemon_paths()
    raise click.ClickException(
        "Hoard server failed to start.\n"
        f"Check logs: {log_path}\n"
        "Ensure the server secret is configured and retry `hoard setup --verify`."
    )


def _is_server_healthy(host: str, port: int) -> bool:
    base_url = f"http://{host}:{port}"
    status, health = _remote_health_probe(base_url, retry=False)
    if isinstance(health, dict):
        if not health.get("db_ready"):
            return False
        return not bool(health.get("migrations_pending"))
    if status != "missing":
        return False

    # Backward compatibility for older servers that do not expose /health yet.
    response = _mcp_jsonrpc_url(base_url, "invalid", "tools/list", {}, retry=False)
    if not isinstance(response, dict):
        return False
    error = response.get("error")
    if isinstance(error, dict):
        message = str(error.get("message", "")).lower()
        return "invalid token" in message or "missing bearer token" in message
    return False


def _remote_health(base_url: str, *, retry: bool = True) -> dict | None:
    status, payload = _remote_health_probe(base_url, retry=retry)
    if status in {"ok", "degraded"}:
        return payload
    return None


def _remote_health_probe(base_url: str, *, retry: bool = True) -> tuple[str, dict | None]:
    target = f"{_normalize_base_url(base_url)}/health"
    req = urllib.request.Request(target, method="GET")
    try:
        def _send():
            return urllib.request.urlopen(req, timeout=5)

        if retry:
            response = run_with_retry(_send, should_retry=should_retry_http_exception)
        else:
            response = _send()

        with response as resp:
            payload = json.loads(resp.read())
        return "ok", payload
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return "missing", None
        try:
            payload = json.loads(exc.read())
            return "degraded", payload
        except json.JSONDecodeError:
            return "error", None
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        return "unreachable", None


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
    base_url = _normalize_base_url(url)
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
        "env": {"HOARD_URL": base_url},
    }
    config_path.write_text(json.dumps(data, indent=2))
    console.print("Configuring OpenClaw... done")


def _openclaw_skill_md() -> str:
    block = render_instruction_block(_instruction_docs_url())
    marked_block = f"{INSTRUCTION_START_MARKER}\n{block}\n{INSTRUCTION_END_MARKER}"
    return f"""---
name: hoard
description: Search your Hoard knowledge base (local HTTP MCP)
metadata: {{"openclaw":{{"requires":{{"bins":["python3"],"env":["HOARD_URL","HOARD_TOKEN"]}},"primaryEnv":"HOARD_TOKEN"}}}}
---

# Hoard

Use Hoard to search and retrieve documents from your local index.

## Config

This skill expects:
- `HOARD_TOKEN` (Bearer token)
- `HOARD_URL` (default: http://127.0.0.1:19850)

{marked_block}

## Commands

Search:
```
{{baseDir}}/scripts/hoard_client.py search "meeting notes" --limit 5
```

Get doc by id:
```
{{baseDir}}/scripts/hoard_client.py get "abc123"
```

Memory get:
```
{{baseDir}}/scripts/hoard_client.py memory_get "some_key"
```

Memory put:
```
{{baseDir}}/scripts/hoard_client.py memory_put "project.key" "value to remember"
```

Sync:
```
{{baseDir}}/scripts/hoard_client.py sync --source inbox
```

Inbox put:
```
{{baseDir}}/scripts/hoard_client.py inbox_put "Persist this note" --title "Note" --sync-immediately
```
"""


def _openclaw_client_script() -> str:
    return """#!/usr/bin/env python3
import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

from hoard.sdk.retry import run_with_retry, should_retry_http_exception

HOARD_URL = os.environ.get("HOARD_URL", "http://127.0.0.1:19850")
HOARD_TOKEN = os.environ.get("HOARD_TOKEN", "")


def _normalize_base_url(url: str) -> str:
    raw = (url or "").strip()
    parsed = urllib.parse.urlparse(raw)
    if not parsed.scheme:
        parsed = urllib.parse.urlparse(f"http://{raw}")
    path = parsed.path.rstrip("/")
    if path.endswith("/mcp"):
        path = path[:-4]
    normalized = parsed._replace(path=path.rstrip("/"), params="", query="", fragment="")
    return urllib.parse.urlunparse(normalized).rstrip("/")


def _mcp_url() -> str:
    return f"{_normalize_base_url(HOARD_URL)}/mcp"


def _call_mcp(method: str, params: dict) -> dict:
    req = urllib.request.Request(
        _mcp_url(),
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {HOARD_TOKEN}",
        },
    )
    try:
        def _send():
            return urllib.request.urlopen(req, timeout=30)

        with run_with_retry(_send, should_retry=should_retry_http_exception) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        try:
            return json.loads(exc.read())
        except Exception:
            return {"error": {"message": str(exc)}}
    except urllib.error.URLError as exc:
        return {"error": {"message": f"{exc}. Any attempted write may not have been persisted."}}


def _call_tool(name: str, arguments: dict) -> dict:
    resp = _call_mcp("tools/call", {"name": name, "arguments": arguments})
    result = resp.get("result", resp)
    if isinstance(result, dict) and "content" in result:
        try:
            return json.loads(result["content"][0]["text"])
        except (KeyError, IndexError, json.JSONDecodeError):
            pass
    return result


def search(query: str, limit: int = 10) -> dict:
    return _call_tool("search", {"query": query, "limit": limit})


def get(entity_id: str) -> dict:
    return _call_tool("get", {"entity_id": entity_id})


def memory_get(key: str) -> dict:
    return _call_tool("memory_get", {"key": key})


def memory_put(key: str, content: str, tags: list[str] | None = None) -> dict:
    arguments = {"key": key, "content": content}
    if tags:
        arguments["tags"] = tags
    return _call_tool("memory_put", arguments)


def sync(source: str | None = None) -> dict:
    arguments = {}
    if source:
        arguments["source"] = source
    return _call_tool("sync", arguments)


def inbox_put(
    content: str,
    title: str | None = None,
    tags: list[str] | None = None,
    sync_immediately: bool = False,
) -> dict:
    arguments = {"content": content, "sync_immediately": sync_immediately}
    if title:
        arguments["title"] = title
    if tags:
        arguments["tags"] = tags
    return _call_tool("inbox_put", arguments)


class HoardArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_usage(sys.stderr)
        self.exit(2, f"{self.prog}: error: {message}\\nTry '{self.prog} --help' for command examples.\\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = HoardArgumentParser(description="Hoard API client")
    subparsers = parser.add_subparsers(dest="command", required=True)

    search_parser = subparsers.add_parser("search", help="Search indexed content")
    search_parser.add_argument("query", help="Query string")
    search_parser.add_argument("--limit", type=int, default=10, help="Result limit")

    get_parser = subparsers.add_parser("get", help="Get full entity")
    get_parser.add_argument("entity_id", help="Entity ID")

    memory_get_parser = subparsers.add_parser("memory_get", help="Get memory by key")
    memory_get_parser.add_argument("key", help="Memory key")

    memory_put_parser = subparsers.add_parser("memory_put", help="Store memory")
    memory_put_parser.add_argument("key", help="Memory key")
    memory_put_parser.add_argument("content", help="Memory content")
    memory_put_parser.add_argument("--tags", nargs="*", default=None, help="Optional memory tags")

    sync_parser = subparsers.add_parser("sync", help="Run sync")
    sync_parser.add_argument("--source", default=None, help="Optional source name")

    inbox_put_parser = subparsers.add_parser("inbox_put", help="Write to inbox")
    inbox_put_parser.add_argument("content", help="Inbox content")
    inbox_put_parser.add_argument("--title", default=None, help="Optional title")
    inbox_put_parser.add_argument("--tags", nargs="*", default=None, help="Optional tags")
    inbox_put_parser.add_argument("--sync-immediately", is_flag=True, default=False)

    return parser


if __name__ == "__main__":
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "search":
        result = search(args.query, args.limit)
    elif args.command == "get":
        result = get(args.entity_id)
    elif args.command == "memory_get":
        result = memory_get(args.key)
    elif args.command == "memory_put":
        result = memory_put(args.key, args.content, tags=args.tags)
    elif args.command == "sync":
        result = sync(source=args.source)
    else:
        result = inbox_put(
            args.content,
            title=args.title,
            tags=args.tags,
            sync_immediately=args.sync_immediately,
        )

    print(json.dumps(result, indent=2))
"""


def _verify_setup() -> None:
    config = load_config(None)
    host = config.get("server", {}).get("host", "127.0.0.1")
    port = int(config.get("server", {}).get("port", 19850))
    token = os.environ.get("HOARD_TOKEN")
    secret = resolve_server_secret(config)
    secret_path = server_secret_file_path(config)
    env_key = server_secret_env_key(config)

    console.print("\nTier 1: Hoard Server Health")
    if secret:
        source = f"env ({env_key})" if os.environ.get(env_key) else f"file ({secret_path})"
        console.print(f"  [green]✓[/green] Server secret available: {source}")
    else:
        console.print(
            "  [red]✗[/red] Server secret missing.\n"
            f"      Set {env_key} or create {secret_path}."
        )

    if _is_server_healthy(host, port):
        console.print(f"  [green]✓[/green] Server responding: http://{host}:{port}")
    else:
        console.print(f"  [red]✗[/red] Server not responding: http://{host}:{port}")
        return

    if secret:
        if _check_tools_list(host, port, secret):
            console.print("  [green]✓[/green] Tools available (admin token)")
        else:
            console.print("  [red]✗[/red] Tools list failed (admin token)")

        write_ok, detail = _check_write_smoke(host, port, secret)
        if write_ok:
            console.print("  [green]✓[/green] Write tools operational")
        else:
            console.print(f"  [red]✗[/red] Write smoke test failed: {detail}")
    else:
        console.print("  [yellow]![/yellow] Skipping write smoke check (no server secret)")

    if token:
        if _check_tools_list(host, port, token):
            console.print("  [green]✓[/green] HOARD_TOKEN tools check passed")
        else:
            console.print("  [yellow]![/yellow] HOARD_TOKEN tools check failed")
    else:
        console.print("  [yellow]![/yellow] HOARD_TOKEN not set; skipping tools check")

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
    response = _mcp_jsonrpc(host, port, token, "tools/list", {})
    return isinstance(response, dict) and "result" in response and "error" not in response


def _check_write_smoke(host: str, port: int, token: str) -> tuple[bool, str]:
    write_resp = _mcp_jsonrpc(
        host,
        port,
        token,
        "tools/call",
        {
            "name": "memory_write",
            "arguments": {
                "content": "setup verify smoke",
                "memory_type": "event",
                "scope_type": "user",
                "source_context": "setup.verify",
                "sensitivity": "normal",
                "tags": ["setup", "verify"],
            },
        },
    )
    if not isinstance(write_resp, dict):
        return False, "invalid response"
    if write_resp.get("error"):
        return False, write_resp["error"].get("message", "unknown error")

    try:
        memory_payload = json.loads(write_resp["result"]["content"][0]["text"])
        memory_id = memory_payload["memory"]["id"]
    except Exception:
        return False, "unable to parse memory_write response"

    retract_resp = _mcp_jsonrpc(
        host,
        port,
        token,
        "tools/call",
        {
            "name": "memory_retract",
            "arguments": {
                "id": memory_id,
                "reason": "setup verify smoke cleanup",
            },
        },
    )
    if not isinstance(retract_resp, dict):
        return False, "invalid retract response"
    if retract_resp.get("error"):
        return False, retract_resp["error"].get("message", "unknown retract error")
    return True, ""


def _mcp_jsonrpc(host: str, port: int, token: str, method: str, params: dict) -> dict:
    return _mcp_jsonrpc_url(f"http://{host}:{port}", token, method, params)


def _mcp_jsonrpc_url(
    base_url: str,
    token: str,
    method: str,
    params: dict,
    *,
    retry: bool = True,
) -> dict:
    target = _normalize_mcp_url(base_url)
    try:
        req = urllib.request.Request(
            target,
            data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        )

        def _send():
            return urllib.request.urlopen(req, timeout=5)

        if retry:
            response = run_with_retry(_send, should_retry=should_retry_http_exception)
        else:
            response = _send()

        with response as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        try:
            payload = json.loads(exc.read())
        except Exception:
            payload = {"error": {"message": str(exc)}}
        return payload
    except Exception as exc:
        suffix = _persistence_warning_suffix(method, params)
        return {
            "error": {
                "message": f"request failed: {exc}.{suffix}",
            }
        }


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
    base = default_data_path()
    base.mkdir(parents=True, exist_ok=True)
    return base / "hoard.pid", base / "hoard.log"


def _serve_daemon(
    host: str,
    port: int,
    no_migrate: bool = False,
    allow_remote: bool = False,
) -> None:
    pid_path, log_path = _daemon_paths()
    if pid_path.exists():
        console.print("Hoard server already running.")
        return

    log_file = log_path.open("ab")
    command = [sys.executable, "-m", "hoard.cli.main", "serve", "--host", host, "--port", str(port)]
    if allow_remote:
        command.append("--allow-remote")
    if no_migrate:
        command.append("--no-migrate")
    process = subprocess.Popen(
        command,
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


def _install_autostart(host: str, port: int, allow_remote: bool = False) -> None:
    remote_args = (
        "      <string>--allow-remote</string>\n"
        if allow_remote
        else ""
    )
    remote_flag = " --allow-remote" if allow_remote else ""
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
{remote_args}    </array>
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
ExecStart={sys.executable} -m hoard.cli.main serve --host {host} --port {port}{remote_flag}
Restart=always

[Install]
WantedBy=default.target
"""
        systemd_path.write_text(systemd_content)
        console.print(f"Autostart installed: {systemd_path}")
    else:
        console.print("Autostart not supported on this platform.")
