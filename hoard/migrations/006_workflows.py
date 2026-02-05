from __future__ import annotations

VERSION = 6


def up(conn) -> None:
    conn.executescript(
        """
        -- ============================================
        -- WORKFLOWS
        -- ============================================
        CREATE TABLE IF NOT EXISTS workflows (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            definition JSON NOT NULL,
            status TEXT NOT NULL DEFAULT 'draft',
            trigger_type TEXT DEFAULT 'manual',
            trigger_config JSON,
            started_at DATETIME,
            completed_at DATETIME,
            tags TEXT,
            created_by TEXT DEFAULT 'user',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            CHECK(status IN ('draft','running','paused','completed','failed','cancelled'))
        );

        CREATE INDEX IF NOT EXISTS idx_workflows_status ON workflows(status);
        CREATE INDEX IF NOT EXISTS idx_workflows_trigger ON workflows(trigger_type);

        -- ============================================
        -- WORKFLOW STEPS
        -- ============================================
        CREATE TABLE IF NOT EXISTS workflow_steps (
            id TEXT PRIMARY KEY,
            workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
            step_key TEXT NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            requires_capability TEXT,
            requires_proficiency TEXT DEFAULT 'standard',
            preferred_agent_id TEXT REFERENCES agents(id),
            input_mapping JSON,
            timeout_seconds INTEGER DEFAULT 3600,
            max_attempts INTEGER DEFAULT 3,
            on_failure TEXT DEFAULT 'retry',
            fallback_step_id TEXT,
            depends_on_steps TEXT,
            status TEXT DEFAULT 'pending',
            task_id TEXT REFERENCES tasks(id),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(workflow_id, step_key),
            CHECK(status IN ('pending','ready','running','completed','failed','skipped')),
            CHECK(on_failure IN ('retry','skip','fail_workflow','fallback'))
        );

        CREATE INDEX IF NOT EXISTS idx_wf_steps_workflow ON workflow_steps(workflow_id);
        CREATE INDEX IF NOT EXISTS idx_wf_steps_status ON workflow_steps(status);
        """
    )


def down(conn) -> None:
    raise NotImplementedError("Rollback not supported for this migration")
