```
    ╔═══════════════════════════════════════════════════════╗
    ║  ██╗  ██╗ ██████╗  █████╗ ██████╗ ██████╗             ║
    ║  ██║  ██║██╔═══██╗██╔══██╗██╔══██╗██╔══██╗            ║
    ║  ███████║██║   ██║███████║██████╔╝██║  ██║            ║
    ║  ██╔══██║██║   ██║██╔══██║██╔══██╗██║  ██║            ║
    ║  ██║  ██║╚██████╔╝██║  ██║██║  ██║██████╔╝            ║
    ║  ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝             ║
    ║                                                       ║
    ║         Your Personal Data Layer                      ║
    ║                                                       ║
    ║  One local index • Hybrid search • MCP interface      ║
    ║  Chunk-level citations • Works with any AI agent      ║
    ╚═══════════════════════════════════════════════════════╝
```

**Hoard** is a local MCP data layer that indexes your personal knowledge into SQLite and exposes it through fast search, memory tools, and MCP-compatible servers.

**Author:** Piotr Ciechowicz

**Documentation:** [openhoard.vercel.app](https://openhoard.vercel.app)

**Versioning note:** The package version (currently 0.1.x, pre‑alpha) is independent from feature milestone labels in this README (e.g., “v0.7” write layer, “v1” security/connectors). Milestones describe maturity of specific subsystems, not a released package version.

---

## Why Hoard

- **Your data, local-first.** Index and search without sending content to third parties.
- **One layer for every agent.** Claude Code, Codex, OpenClaw, or any MCP client can connect.
- **Chunk-level retrieval.** Stable citations and predictable results.
- **Hybrid search.** BM25 + optional vector embeddings.

---

## What Hoard Does (and Doesn’t)

**Allowed / Supported**
- Read-only indexing of local files, Obsidian vaults, and browser bookmarks.
- Notion export ingestion (ZIP/HTML/CSV).
- Agent inbox (drop folder) + MCP ingestion.
- Local search, get, get_chunk, sync, and memory tools (read/write).
- Agent orchestration (registry, tasks, artifacts, events, costs).
- Trusted connectors (v1): install only what you trust.

**Not in v1**
- Writing back to sources
- OAuth live connectors (Notion/Google)
- Sandboxed connector execution
- Web UI

---

## Quick Start

```bash
pip install -e .
hoard init
hoard search "my notes"
```

### Recommended setup flow
```bash
hoard init           # wizard: choose sources, file types, vectors
hoard setup --all    # configure Claude Code / Codex / OpenClaw
hoard setup --verify # verify server + write tools + client config
```

By default, Hoard auto-generates a local server secret at `~/.hoard/server.key` (0600).
`HOARD_SERVER_SECRET` still overrides the file when set.

---

## Installation

**Option A: pipx (recommended)**
```bash
pipx install hoard
```

**Option B: Homebrew**
```bash
brew install hoard
```

**Option C: pip (requires Python 3.11+)**
```bash
python3.11 -m pip install hoard
```

---

## Core Commands

### Onboarding
```bash
hoard init                 # full wizard
hoard init --quick         # accept defaults
hoard init --vectors       # enable semantic search
hoard add <path>           # add folder quickly
hoard add --inbox <path>   # agent inbox (drop folder)
hoard add --obsidian <path>
hoard add --notion <path>
```

### Orchestration
```bash
hoard orchestrate init     # generate registration token + artifact config
hoard agent register <name> --type worker
hoard task create "Research X"
hoard task list
hoard artifact put <task-id> report.md --type text --content "..."
hoard event poll
```

### Sync & Search
```bash
hoard sync
hoard search "query" --limit 5
hoard search "query" --types entity,memory
hoard search "query" --no-memory
```

### Memory
```bash
hoard memory put key "content" --ttl-days 30
hoard memory prune
```

### Embeddings
```bash
pip install -e ".[vectors]"
hoard embeddings build
```

### Server
```bash
hoard serve                # HTTP MCP server (http://127.0.0.1:19850/mcp)
                         # SSE events: http://127.0.0.1:19850/events
                         # Health: http://127.0.0.1:19850/health
hoard serve --daemon
hoard serve --status
hoard serve --stop
hoard serve --install-autostart
```

Hoard defaults to loopback-only bind. To expose a non-loopback host (for remote clients), you must opt in:
```bash
hoard serve --host 0.0.0.0 --allow-remote
```

Local default: Hoard reads server secret from `${HOARD_DATA_DIR:-~/.hoard}/server.key` and auto-generates it if missing.
Optional override:
```bash
export HOARD_SERVER_SECRET=your-secret
```

### Data Directory
By default Hoard stores state under `~/.hoard`. Set `HOARD_DATA_DIR` to move all default paths (config/db/secret/artifacts/daemon files):
```bash
export HOARD_DATA_DIR=/var/lib/hoard
```

To run in read-only mode without a server secret, set in `${HOARD_DATA_DIR:-~/.hoard}/config.yaml`:
```yaml
write:
  enabled: false
```

### Optional Dependencies
```bash
pip install -e ".[watcher]"  # enable file watcher
```

### Write Layer (v0.7)
Cross-agent memory writes are supported via the HTTP server. Stdio mode is read-only.

Requirements:
- server secret available via either:
  - `HOARD_SERVER_SECRET` environment variable, or
  - `${HOARD_DATA_DIR:-~/.hoard}/server.key` (auto-generated local default)
- `hoard serve` running

```bash
hoard serve
```

Admin tasks (token management) use the same secret:
```bash
hoard tokens add claude-code --scopes search,get,memory,sync
```

Write coordination defaults (in `${HOARD_DATA_DIR:-~/.hoard}/config.yaml`):
```yaml
write:
  database:
    busy_timeout_ms: 5000
    lock_timeout_ms: 30000
    retry_budget_ms: 30000
    retry_backoff_ms: 50
```

### MCP Clients
```bash
hoard setup --all
hoard setup --claude
hoard setup --claude --project-scope
hoard setup --codex
hoard setup --openclaw
hoard setup remote --url https://hoard.example.com --token hoard_sk_xxx
hoard setup remote --url https://hoard.example.com --admin-token hoard_admin_xxx  # automation
hoard setup --verify
hoard setup --uninstall openclaw
```

`hoard setup remote` credential hierarchy:
- Recommended: `--token` with a pre-provisioned agent token.
- Advanced automation: `--admin-token` (auto-provisions one token per client type).

For production remote hosting (Docker + Caddy TLS), see `docs/REMOTE_DEPLOYMENT.md`.

### Diagnostics
```bash
hoard doctor
hoard db backup /path/to/backup.db
hoard db restore /path/to/backup.db --force
```

---

## Benchmarks

```bash
pip install -e ".[dev]"
python -m pytest -q tests/test_search_benchmark.py
python -m pytest -q tests/test_search_benchmark.py --benchmark-save baseline
```

Saved baselines live under `.benchmarks/` and are machine-specific.

---

## MCP Transport Options

### HTTP (default)
Runs at `http://127.0.0.1:19850/mcp` and speaks MCP JSON-RPC.

```bash
hoard serve
```

### Stdio (for MCP clients that require it)
```bash
hoard mcp stdio
```

By default, stdio mode blocks write tools. Enable explicitly in config:

```yaml
mcp:
  stdio:
    allow_writes: true
```

HTTP exposes a lightweight metrics endpoint at `/metrics` when enabled:

```yaml
observability:
  metrics_enabled: true
```

---

## Additional MCP Tools

Beyond `search/get/get_chunk/sync`, Hoard exposes tools for:
- Structured memory writes and review (`memory_write`, `memory_propose`, `memory_review`, `memory_supersede`, `memory_retract`)
- Memory conflict/duplicate resolution (`conflicts_list`, `conflict_resolve`, `duplicates_list`, `duplicate_resolve`)
- Agent inbox ingestion (`inbox_put`)
- Orchestration and artifacts (agents, tasks, workflows, events, cost reporting)

---

## Connectors

Enable in `${HOARD_DATA_DIR:-~/.hoard}/config.yaml`:

```yaml
connectors:
  local_files:
    enabled: true
    paths:
      - ~/Documents/Notes
    include_extensions:
      - .md
      - .txt
      - .csv
      - .json
      - .yaml
      - .rst

  obsidian:
    enabled: true
    vault_path: ~/Obsidian

  bookmarks_chrome:
    enabled: true

  bookmarks_firefox:
    enabled: true

  notion_export:
    enabled: true
    export_path: ~/Downloads/notion-export.zip

  inbox:
    enabled: true
    path: ~/.hoard/inbox

memory:
  default_ttl_days: 30
  prune_on_sync: true

sync:
  interval_minutes: 15
  watcher_enabled: false
  watcher_debounce_seconds: 2
```

---

## Contributing

Contributions are welcome. By default, **only Piotr Ciechowicz can approve changes**.

### How to contribute
1. Fork the repo
2. Create a feature branch
3. Open a pull request
4. Wait for review

### Guidelines
- Keep changes small and focused
- Include tests for new behavior
- Avoid breaking CLI or MCP tool schemas

See `CONTRIBUTING.md` for details.

---

## Security Model (v1)

- Connectors are **trusted code** (no sandbox yet).
- Tokens gate MCP access; scopes restrict tool usage.
- No bulk export tools are exposed.

---

## License

MIT

---

## Support

If you run into issues, open a GitHub issue or ping me directly.
