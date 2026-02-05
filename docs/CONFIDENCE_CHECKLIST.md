# Hoard Confidence Checklist

This checklist is the minimum bar to say Hoard is “fully working” for the One Memory for All Agents use case.

**Scope:** Local documents + agent inbox + MCP read/write + unified search + sync + TTL.

---

## Prerequisites

1. Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,watcher]"
```

2. Ensure a clean config and DB for manual steps:

```bash
rm -rf ~/.hoard
```

---

## Manual End‑to‑End Tests

### 1) Cold install → init → sync

```bash
hoard init
hoard sync
hoard search "test" --limit 5
```

Pass:
- `hoard sync` completes without errors.
- `hoard search` returns results when indexing a known folder.

### 2) Inbox → sync → search

```bash
hoard add --inbox ~/.hoard/inbox
printf "Inbox sample content" > ~/.hoard/inbox/inbox_sample.md
hoard sync
hoard search "Inbox sample" --limit 5
```

Pass:
- Search returns the inbox document.

### 3) MCP HTTP server → tools/list → search/get

```bash
export HOARD_SERVER_SECRET=$(python -c "import secrets; print(secrets.token_hex(32))")
hoard serve
```

In another terminal:

```bash
TOKEN=$(python - <<'PY'
from hoard.core.config import load_config
from pathlib import Path
config = load_config(Path.home() / ".hoard" / "config.yaml")
print(config["security"]["tokens"][0]["token"])
PY
)

curl -s -X POST http://127.0.0.1:19850/mcp \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

Pass:
- `tools/list` returns a list of tools.

### 4) MCP memory write → unified search

```bash
curl -s -X POST http://127.0.0.1:19850/mcp \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"memory_put","arguments":{"key":"prd","content":"PRD test memory"}}}'

curl -s -X POST http://127.0.0.1:19850/mcp \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search","arguments":{"query":"PRD test","limit":3,"types":["entity","memory"]}}}'
```

Pass:
- Search results include a `result_type: "memory"` entry.

### 5) Agent switch (Claude ↔ Codex)

```bash
hoard setup --claude
hoard setup --codex
```

Pass:
- Both clients see the same memory + document context when searching.

### 6) Watcher live updates (if enabled)

```bash
hoard serve
printf "Watcher update" >> <a watched file>
```

Pass:
- Update appears in search within the debounce window.

### 7) Token scope enforcement

Create a token without `memory` scope and attempt `memory_put`.

Pass:
- MCP returns an error with missing scope.

### 8) TTL expiry

```bash
hoard memory put ttl_immediate "expire now" --ttl-days 0
hoard memory prune
hoard memory get ttl_immediate
```

Pass:
- Entry is pruned and no longer returned.

---

## Automated Tests to Run

```bash
python -m pytest -q
```

Expected:
- All tests pass.
- `tests/test_search_benchmark.py` runs and reports timing.

---

## Automated Tests Added for High Confidence

1. HTTP concurrency (multiple parallel search calls)
2. CLI + HTTP parity (write via CLI, read via MCP)
3. Watcher integration (if watchdog installed)
4. Large ingestion sync (200 files)
5. JSON‑RPC missing tool error handling
6. Security regression for restricted memories

If all manual steps and automated tests pass, Hoard is ready to claim “fully working” for the One Memory for All Agents use case.
