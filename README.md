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
- Local search, get, get_chunk, and memory tools (read/write).
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
hoard serve          # start HTTP MCP server (default port 19850)
hoard setup --all    # configure Claude Code / Codex / OpenClaw
```

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
hoard add --obsidian <path>
hoard add --notion <path>
```

### Sync & Search
```bash
hoard sync
hoard search "query" --limit 5
```

### Embeddings
```bash
pip install -e ".[vectors]"
hoard embeddings build
```

### Server
```bash
hoard serve                # HTTP MCP server (http://127.0.0.1:19850/mcp)
hoard serve --daemon
hoard serve --status
hoard serve --stop
hoard serve --install-autostart
```

### Write Layer (v0.7)
Cross-agent memory writes are supported via the HTTP server. Stdio mode is read-only.

Requirements:
- `HOARD_SERVER_SECRET` set in the environment (HMAC key for token lookup)
- `hoard serve` running

```bash
export HOARD_SERVER_SECRET=$(python -c "import secrets; print(secrets.token_hex(32))")
hoard serve
```

Admin tasks (token management) use the same secret:
```bash
export HOARD_SERVER_SECRET=...
hoard tokens add claude-code --scopes search,get,memory,sync
```

### MCP Clients
```bash
hoard setup --all
hoard setup --claude
hoard setup --claude --project-scope
hoard setup --codex
hoard setup --openclaw
hoard setup --verify
hoard setup --uninstall openclaw
```

### Diagnostics
```bash
hoard doctor
```

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

---

## Connectors

Enable in `~/.hoard/config.yaml`:

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
