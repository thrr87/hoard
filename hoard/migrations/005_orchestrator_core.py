from __future__ import annotations

VERSION = 5


def up(conn) -> None:
    conn.executescript(
        """
        -- ============================================
        -- AGENTS
        -- ============================================
        CREATE TABLE IF NOT EXISTS agents (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            agent_type TEXT NOT NULL,
            registered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_heartbeat_at DATETIME,
            status TEXT NOT NULL DEFAULT 'active',
            default_model TEXT,
            model_provider TEXT,
            max_concurrent_tasks INTEGER DEFAULT 1,
            current_task_count INTEGER DEFAULT 0,
            scopes TEXT,
            metadata JSON,
            deregistered_at DATETIME
        );

        CREATE INDEX IF NOT EXISTS idx_agents_status ON agents(status);
        CREATE INDEX IF NOT EXISTS idx_agents_heartbeat ON agents(last_heartbeat_at);

        -- ============================================
        -- AGENT CAPABILITIES
        -- ============================================
        CREATE TABLE IF NOT EXISTS agent_capabilities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
            capability TEXT NOT NULL,
            proficiency TEXT DEFAULT 'standard',
            languages TEXT,
            domains TEXT,
            max_context_tokens INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(agent_id, capability)
        );

        CREATE INDEX IF NOT EXISTS idx_agent_caps_capability ON agent_capabilities(capability);
        CREATE INDEX IF NOT EXISTS idx_agent_caps_agent ON agent_capabilities(agent_id);

        -- ============================================
        -- TASKS
        -- ============================================
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            workflow_id TEXT,
            workflow_step_id TEXT,
            created_by TEXT,
            name TEXT NOT NULL,
            description TEXT,
            requires_capability TEXT,
            requires_proficiency TEXT DEFAULT 'standard',
            priority INTEGER DEFAULT 5,
            input_data JSON,
            input_artifact_ids TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            assigned_agent_id TEXT REFERENCES agents(id),
            claimed_at DATETIME,
            started_at DATETIME,
            completed_at DATETIME,
            timeout_seconds INTEGER DEFAULT 3600,
            deadline DATETIME,
            output_summary TEXT,
            output_artifact_id TEXT,
            error_message TEXT,
            attempt_number INTEGER DEFAULT 0,
            max_attempts INTEGER DEFAULT 3,
            retry_delay_seconds INTEGER DEFAULT 60,
            tokens_input INTEGER DEFAULT 0,
            tokens_output INTEGER DEFAULT 0,
            estimated_cost_usd REAL DEFAULT 0.0,
            model_used TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            CHECK(status IN ('pending','queued','claimed','running','completed','failed','cancelled','timed_out')),
            CHECK(priority BETWEEN 1 AND 10)
        );

        CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
        CREATE INDEX IF NOT EXISTS idx_tasks_workflow ON tasks(workflow_id);
        CREATE INDEX IF NOT EXISTS idx_tasks_agent ON tasks(assigned_agent_id);
        CREATE INDEX IF NOT EXISTS idx_tasks_capability ON tasks(requires_capability, status);
        CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority, created_at);
        CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at);

        -- ============================================
        -- TASK DEPENDENCIES
        -- ============================================
        CREATE TABLE IF NOT EXISTS task_dependencies (
            task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
            depends_on_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
            dependency_type TEXT DEFAULT 'completion',
            PRIMARY KEY (task_id, depends_on_task_id)
        );

        CREATE INDEX IF NOT EXISTS idx_task_deps_depends ON task_dependencies(depends_on_task_id);

        -- ============================================
        -- TASK ARTIFACTS
        -- ============================================
        CREATE TABLE IF NOT EXISTS task_artifacts (
            id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
            artifact_type TEXT NOT NULL,
            name TEXT NOT NULL,
            content_text TEXT,
            content_blob_path TEXT,
            content_url TEXT,
            mime_type TEXT,
            size_bytes INTEGER,
            content_hash TEXT,
            metadata JSON,
            role TEXT DEFAULT 'output',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_artifacts_task ON task_artifacts(task_id);
        CREATE INDEX IF NOT EXISTS idx_artifacts_role ON task_artifacts(role);

        -- ============================================
        -- EVENTS
        -- ============================================
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            agent_id TEXT,
            task_id TEXT,
            workflow_id TEXT,
            payload JSON NOT NULL,
            published_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            expires_at DATETIME
        );

        CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
        CREATE INDEX IF NOT EXISTS idx_events_published ON events(published_at);
        CREATE INDEX IF NOT EXISTS idx_events_agent ON events(agent_id);
        CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id);
        CREATE INDEX IF NOT EXISTS idx_events_workflow ON events(workflow_id);
        CREATE INDEX IF NOT EXISTS idx_events_expires ON events(expires_at);

        -- ============================================
        -- COST LEDGER
        -- ============================================
        CREATE TABLE IF NOT EXISTS cost_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id TEXT NOT NULL,
            task_id TEXT,
            workflow_id TEXT,
            model TEXT NOT NULL,
            provider TEXT NOT NULL,
            tokens_input INTEGER NOT NULL DEFAULT 0,
            tokens_output INTEGER NOT NULL DEFAULT 0,
            tokens_cache_read INTEGER DEFAULT 0,
            tokens_cache_write INTEGER DEFAULT 0,
            estimated_cost_usd REAL NOT NULL DEFAULT 0.0,
            input_price_per_mtok REAL,
            output_price_per_mtok REAL,
            recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_cost_agent ON cost_ledger(agent_id);
        CREATE INDEX IF NOT EXISTS idx_cost_task ON cost_ledger(task_id);
        CREATE INDEX IF NOT EXISTS idx_cost_workflow ON cost_ledger(workflow_id);
        CREATE INDEX IF NOT EXISTS idx_cost_recorded ON cost_ledger(recorded_at);
        CREATE INDEX IF NOT EXISTS idx_cost_model ON cost_ledger(model);

        -- ============================================
        -- COST BUDGETS
        -- ============================================
        CREATE TABLE IF NOT EXISTS cost_budgets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope_type TEXT NOT NULL,
            scope_id TEXT,
            period TEXT NOT NULL,
            amount_usd REAL NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(scope_type, scope_id, period)
        );
        """
    )


def down(conn) -> None:
    raise NotImplementedError("Rollback not supported for this migration")
