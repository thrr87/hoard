# Analysis: Database Locking in Multi-Agent Setups

## Question

> Sharing a DB across agents keeps state consistent without API calls. Does it lock during writes to avoid conflicts in multi-agent setups?

## Short Answer

Yes. Hoard uses a **five-layer defense** against write conflicts on a shared SQLite database:

1. **WAL mode** (database-level) -- readers never block writers
2. **WriteCoordinator** (process-level) -- serializes all writes through a single thread
3. **Busy timeout** (connection-level) -- retries on `SQLITE_BUSY` for up to 5 seconds
4. **Optimistic concurrency** (row-level) -- `rowcount` checks detect lost updates
5. **Async conflict detection** (application-level) -- background jobs find semantic conflicts

These layers work together so that multiple agents sharing one Hoard server will not corrupt data, lose writes, or deadlock.

---

## Layer 1: WAL Journal Mode

**File:** `hoard/core/db/connection.py:21`

```python
conn.execute("PRAGMA journal_mode = WAL;")
conn.execute("PRAGMA synchronous = NORMAL;")
```

SQLite's Write-Ahead Logging mode allows **concurrent readers and a single writer** without blocking each other. In the default rollback journal mode, a writer would block all readers (and vice versa). WAL eliminates this contention: agents performing searches or reads will never be blocked by an ongoing write, and writes will never wait for readers to finish.

**Implication for multi-agent:** Agents calling read-only tools (`search`, `get`, `context`) operate without any lock contention, even while another agent is writing memories.

---

## Layer 2: WriteCoordinator (Single-Writer Serialization)

**File:** `hoard/core/db/writer.py`

This is the **primary locking mechanism**. All database writes are funneled through a single background thread via a `queue.Queue`:

```python
class WriteCoordinator:
    def __init__(self, *, db_path, busy_timeout_ms):
        self._queue: queue.Queue[_WriteTask | None] = queue.Queue()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def submit(self, fn, *args, **kwargs):
        # If already on the writer thread, execute directly
        if threading.get_ident() == self._thread_id:
            return fn(self._conn, *args, **kwargs)
        # Otherwise, enqueue and block until completion
        task = _WriteTask(fn=fn, args=args, kwargs=kwargs, event=threading.Event())
        self._queue.put(task)
        task.event.wait()  # caller blocks here
        if task.error:
            raise task.error
        return task.result
```

The worker loop processes one task at a time, auto-committing on success and rolling back on error:

```python
def _run(self):
    self._conn = connect(self._db_path, busy_timeout_ms=self._busy_timeout_ms)
    while True:
        task = self._queue.get()
        try:
            task.result = task.fn(self._conn, *task.args, **task.kwargs)
            if self._conn.in_transaction:
                self._conn.commit()
        except Exception as exc:
            if self._conn.in_transaction:
                self._conn.rollback()
            task.error = exc
        finally:
            task.event.set()
```

**How the MCP server uses it** (`hoard/core/mcp/server.py:287-292`):

```python
def _dispatch_tool(self, tool, payload, conn, token):
    if is_write_tool(tool):
        return self.server.writer.submit(
            lambda writer_conn: dispatch_tool(tool, payload, writer_conn, self.server.config, token)
        )
    return dispatch_tool(tool, payload, conn, self.server.config, token)
```

Write tools go through `writer.submit()` (serialized). Read tools execute directly on a per-request connection (no contention).

**Implication for multi-agent:** Even though `ThreadingHTTPServer` spawns a new thread per request, every write from every agent is serialized into a single queue. Two agents writing memories simultaneously will never interleave their transactions. The second write blocks at `task.event.wait()` until the first completes.

---

## Layer 3: Busy Timeout

**File:** `hoard/core/config.py:92`

```python
"database": {
    "busy_timeout_ms": 5000,
}
```

Applied as a pragma in `connection.py:23-24`:

```python
if busy_timeout_ms:
    conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)};")
```

If a connection encounters `SQLITE_BUSY` (e.g., during migrations or if an external process holds the write lock), SQLite will internally retry for up to 5000ms before raising an error. This provides a safety net beyond the WriteCoordinator for any edge cases where multiple connections might attempt writes (e.g., migrations, the sync lock flow).

---

## Layer 4: Optimistic Concurrency (Row-Level Conflict Detection)

Multiple operations use `UPDATE ... WHERE status = ? ... ` + `cursor.rowcount` as an **optimistic lock**:

### Task Claiming (`hoard/core/orchestrator/tasks.py:121-132`)

```python
cursor = conn.execute(
    """
    UPDATE tasks
    SET status = 'claimed', assigned_agent_id = ?, ...
    WHERE id = ? AND status = 'queued'
      AND (assigned_agent_id IS NULL OR assigned_agent_id = ?)
    """, ...
)
if cursor.rowcount == 0:
    return None  # Another agent claimed it first
```

Two agents trying to claim the same task cannot both succeed -- the `WHERE status = 'queued'` guard ensures only one update takes effect. The loser gets `rowcount == 0` and a `None` return.

### Worker Lease (`hoard/core/worker.py:71-108`)

```python
INSERT INTO worker_lease (id, ...) VALUES (1, ...)
ON CONFLICT (id) DO UPDATE SET ...
WHERE worker_lease.expires_at < ?
```

Only one worker process can hold the background job lease. The `WHERE expires_at < now` condition ensures a stale lease can be reclaimed, but an active one cannot be stolen.

### Memory Updates (`hoard/core/memory/v2/store.py`)

Similar `rowcount > 0` checks guard memory updates and deletions.

---

## Layer 5: Application-Level Conflict Detection

Hoard recognizes that even with serialized writes, **semantic conflicts** can occur (two agents writing different values to the same memory slot). These are handled asynchronously:

### Background Jobs (`hoard/core/worker.py:350-439`)

After every memory write, three background jobs are enqueued:
1. `embed_memory` -- generate embeddings
2. `detect_duplicates` -- find semantically similar memories (threshold: 0.85)
3. `detect_conflicts` -- find memories in the same slot/scope from different agents

### Conflict & Duplicate Tables (migration 002)

```
memory_conflicts  -- tracks slot collisions between agents
conflict_members  -- maps conflicting memories with NLI labels
memory_duplicates -- tracks semantically similar memories
duplicate_members -- maps duplicate pairs
```

These are surfaced via MCP tools (`conflicts_list`, `conflict_resolve`, `duplicates_list`, `duplicate_resolve`) for human or agent resolution.

---

## Layer 6 (Bonus): File-Based Sync Lock

**File:** `hoard/core/sync/service.py:122-136`

```python
def _acquire_lock(path):
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)  # atomic
    # Write PID + timestamp for staleness detection
```

The data connector sync process uses a file-based lock (`~/.hoard/sync.lock`) with `O_CREAT | O_EXCL` for atomic creation. This prevents two sync operations from running concurrently. Staleness detection (PID alive check) handles crash recovery.

---

## Summary: What Happens When Two Agents Write Simultaneously

```
Agent A writes memory ─┐
                       ├─▶ ThreadingHTTPServer (thread per request)
Agent B writes memory ─┘
                            │
                            ▼
                    is_write_tool(tool) → True
                            │
                            ▼
                    WriteCoordinator.submit()
                            │
              ┌─────────────┴──────────────┐
              │    Queue (FIFO ordering)    │
              │  Agent A's write → slot 1   │
              │  Agent B's write → slot 2   │
              └─────────────┬──────────────┘
                            │
                            ▼
                  Single Writer Thread
                  (one connection, one txn at a time)
                            │
                  ┌─────────┼─────────┐
                  │ Execute │ Commit  │
                  │ A's fn  │ or      │
                  │         │ Rollback│
                  └─────────┼─────────┘
                            │
                  ┌─────────┼─────────┐
                  │ Execute │ Commit  │
                  │ B's fn  │ or      │
                  │         │ Rollback│
                  └─────────┼─────────┘
                            │
                            ▼
              Background jobs detect conflicts/duplicates
```

1. Both requests arrive on separate HTTP threads
2. Both call `writer.submit()`, enqueueing their operations
3. The writer thread processes them **sequentially** (FIFO)
4. Each gets its own transaction with auto-commit/rollback
5. Neither agent sees a `SQLITE_BUSY` error or partial data
6. Post-write background jobs check for semantic conflicts

---

## Potential Gaps

| Gap | Severity | Notes |
|-----|----------|-------|
| **Single-process only** | Medium | The WriteCoordinator serializes writes within one server process. If two separate `hoard serve` processes point at the same DB file, they would rely solely on WAL + busy_timeout (no queue serialization). This is not a supported deployment. |
| **No concurrent write tests** | Low | `test_http_mcp_concurrency.py` only tests concurrent reads (5 threads x 3 search calls). There are no tests exercising concurrent writes from multiple agents. |
| **Task claim is not atomic SELECT+UPDATE** | Low | `_claim_job` in `worker.py:140-161` does a SELECT then UPDATE in two statements. This is safe because it runs inside the WriteCoordinator (single thread), but would be a race condition if used with direct connections. |
| **Conflict resolution is manual** | Low | Detected conflicts require explicit resolution via MCP tools. There is no auto-merge strategy. |
