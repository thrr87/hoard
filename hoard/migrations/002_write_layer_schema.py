from __future__ import annotations

VERSION = 2


def up(conn) -> None:
    conn.executescript(
        """
        -- ============================================
        -- MEMORIES
        -- ============================================
        CREATE TABLE IF NOT EXISTS memories (
            id TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            memory_type TEXT NOT NULL,
            slot TEXT,
            scope_type TEXT NOT NULL,
            scope_id TEXT,
            source_agent TEXT NOT NULL,
            source_agent_version TEXT,
            source_session_id TEXT,
            source_conversation_id TEXT,
            source_context TEXT,
            created_at TEXT NOT NULL,
            expires_at TEXT,
            superseded_by TEXT REFERENCES memories(id) ON DELETE SET NULL,
            superseded_at TEXT,
            retracted_at TEXT,
            retracted_by TEXT,
            retraction_reason TEXT,
            sensitivity TEXT NOT NULL DEFAULT 'normal',
            CHECK (memory_type IN ('fact','preference','decision','observation','event','context')),
            CHECK (scope_type IN ('user','project','entity','domain')),
            CHECK (sensitivity IN ('normal','sensitive','restricted')),
            CHECK ((scope_type = 'user' AND scope_id IS NULL) OR (scope_type != 'user' AND scope_id IS NOT NULL)),
            CHECK (
                slot IS NULL
                OR slot LIKE 'pref:%'
                OR slot LIKE 'fact:%'
                OR slot LIKE 'ctx:%'
                OR slot LIKE 'decision:%'
                OR slot LIKE 'event:%'
            )
        );

        -- ============================================
        -- MEMORY COUNTERS
        -- ============================================
        CREATE TABLE IF NOT EXISTS memory_counters (
            memory_id TEXT PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
            confidence REAL NOT NULL DEFAULT 0.8 CHECK (confidence >= 0 AND confidence <= 1),
            last_accessed_at TEXT,
            access_count_30d INTEGER DEFAULT 0,
            decay_count INTEGER DEFAULT 0
        );

        -- ============================================
        -- EMBEDDINGS
        -- ============================================
        CREATE TABLE IF NOT EXISTS memory_embeddings (
            memory_id TEXT PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
            embedding BLOB NOT NULL,
            model_name TEXT NOT NULL,
            model_version TEXT NOT NULL,
            dimensions INTEGER NOT NULL CHECK (dimensions > 0),
            embedded_at TEXT NOT NULL,
            CHECK (length(embedding) = dimensions * 4)
        );
        CREATE INDEX IF NOT EXISTS idx_embeddings_model
            ON memory_embeddings(model_name, model_version);

        -- ============================================
        -- TAGS (lowercase enforced)
        -- ============================================
        CREATE TABLE IF NOT EXISTS memory_tags (
            memory_id TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
            tag TEXT NOT NULL CHECK (tag = lower(tag)),
            PRIMARY KEY (memory_id, tag)
        );
        CREATE INDEX IF NOT EXISTS idx_tags_by_tag ON memory_tags(tag);

        -- ============================================
        -- RELATIONS
        -- ============================================
        CREATE TABLE IF NOT EXISTS memory_relations (
            memory_id TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
            related_uri TEXT NOT NULL,
            relation_type TEXT DEFAULT 'related',
            PRIMARY KEY (memory_id, related_uri)
        );

        -- ============================================
        -- EVENTS
        -- ============================================
        CREATE TABLE IF NOT EXISTS memory_events (
            id TEXT PRIMARY KEY,
            memory_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_at TEXT NOT NULL,
            actor TEXT NOT NULL,
            snapshot TEXT,
            event_data TEXT,
            CHECK (event_type IN (
                'created','superseded','retracted','hard_deleted',
                'conflict_detected','conflict_resolved','merged','decayed',
                'embedding_added','embedding_upgraded','accessed'
            ))
        );
        CREATE INDEX IF NOT EXISTS idx_events_by_memory ON memory_events(memory_id, event_at);

        -- ============================================
        -- DUPLICATES (normalized)
        -- ============================================
        CREATE TABLE IF NOT EXISTS memory_duplicates (
            id TEXT PRIMARY KEY,
            detected_at TEXT NOT NULL,
            similarity REAL NOT NULL CHECK (similarity >= 0 AND similarity <= 1),
            resolved_at TEXT,
            resolution TEXT
        );

        CREATE TABLE IF NOT EXISTS duplicate_members (
            duplicate_id TEXT NOT NULL REFERENCES memory_duplicates(id) ON DELETE CASCADE,
            memory_id TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
            is_canonical INTEGER DEFAULT 0,
            PRIMARY KEY (duplicate_id, memory_id)
        );
        CREATE INDEX IF NOT EXISTS idx_duplicate_members_by_memory ON duplicate_members(memory_id);

        -- ============================================
        -- CONFLICTS
        -- ============================================
        CREATE TABLE IF NOT EXISTS memory_conflicts (
            id TEXT PRIMARY KEY,
            slot TEXT NOT NULL,
            scope_type TEXT NOT NULL,
            scope_id TEXT,
            detected_at TEXT NOT NULL,
            resolved_at TEXT,
            resolution TEXT,
            resolved_by TEXT
        );

        CREATE TABLE IF NOT EXISTS conflict_members (
            conflict_id TEXT NOT NULL REFERENCES memory_conflicts(id) ON DELETE CASCADE,
            memory_id TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
            nli_label TEXT,
            added_at TEXT NOT NULL,
            PRIMARY KEY (conflict_id, memory_id)
        );
        CREATE INDEX IF NOT EXISTS idx_conflict_members_by_memory ON conflict_members(memory_id);

        -- ============================================
        -- PROPOSALS
        -- ============================================
        CREATE TABLE IF NOT EXISTS memory_proposals (
            id TEXT PRIMARY KEY,
            proposed_memory TEXT NOT NULL,
            proposed_by TEXT NOT NULL,
            proposed_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            reviewed_at TEXT,
            reviewed_by TEXT,
            rejection_reason TEXT,
            CHECK (status IN ('pending','approved','rejected','expired'))
        );
        CREATE INDEX IF NOT EXISTS idx_proposals_pending
            ON memory_proposals(status, proposed_at) WHERE status = 'pending';
        CREATE INDEX IF NOT EXISTS idx_proposals_expiring
            ON memory_proposals(expires_at) WHERE status = 'pending';

        -- ============================================
        -- AGENT TOKENS
        -- ============================================
        CREATE TABLE IF NOT EXISTS agent_tokens (
            agent_id TEXT PRIMARY KEY,
            token_lookup_hash TEXT NOT NULL UNIQUE,
            token_secure_hash TEXT NOT NULL,
            trust_level REAL NOT NULL DEFAULT 0.5,
            capabilities TEXT NOT NULL,
            allowed_scopes TEXT NOT NULL,
            rate_limit_per_hour INTEGER DEFAULT 100,
            requires_user_confirm INTEGER DEFAULT 0,
            proposal_ttl_days INTEGER,
            can_access_sensitive INTEGER DEFAULT 0,
            can_access_restricted INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            last_used_at TEXT
        );

        CREATE TABLE IF NOT EXISTS agent_rate_limits (
            agent_id TEXT NOT NULL REFERENCES agent_tokens(agent_id) ON DELETE CASCADE,
            window_start TEXT NOT NULL,
            write_count INTEGER DEFAULT 0,
            PRIMARY KEY (agent_id, window_start)
        );

        -- ============================================
        -- BACKGROUND JOBS
        -- ============================================
        CREATE TABLE IF NOT EXISTS background_jobs (
            id TEXT PRIMARY KEY,
            job_type TEXT NOT NULL,
            memory_id TEXT REFERENCES memories(id) ON DELETE CASCADE,
            status TEXT NOT NULL DEFAULT 'pending',
            priority INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            error TEXT,
            retry_count INTEGER DEFAULT 0,
            max_retries INTEGER DEFAULT 3,
            CHECK (status IN ('pending','running','completed','failed'))
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_pending
            ON background_jobs(status, priority DESC, created_at)
            WHERE status = 'pending';
        CREATE INDEX IF NOT EXISTS idx_jobs_running
            ON background_jobs(status, started_at)
            WHERE status = 'running';

        -- ============================================
        -- WORKER LEASE
        -- ============================================
        CREATE TABLE IF NOT EXISTS worker_lease (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            worker_id TEXT NOT NULL,
            hostname TEXT NOT NULL,
            pid INTEGER NOT NULL,
            acquired_at TEXT NOT NULL,
            heartbeat_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        );

        -- ============================================
        -- SYSTEM CONFIG
        -- ============================================
        CREATE TABLE IF NOT EXISTS system_config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        INSERT OR IGNORE INTO system_config (key, value, updated_at) VALUES
            ('embedding_model_name', 'sentence-transformers/all-MiniLM-L6-v2', datetime('now')),
            ('embedding_model_version', '2.0.0', datetime('now')),
            ('embedding_dimensions', '384', datetime('now')),
            ('schema_version', '0.7.0', datetime('now'));

        -- ============================================
        -- FTS (excludes restricted)
        -- ============================================
        CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
            content,
            content='memories',
            content_rowid='rowid'
        );

        CREATE TRIGGER IF NOT EXISTS memories_fts_ai AFTER INSERT ON memories
        WHEN NEW.sensitivity != 'restricted' BEGIN
            INSERT INTO memories_fts(rowid, content) VALUES (NEW.rowid, NEW.content);
        END;

        CREATE TRIGGER IF NOT EXISTS memories_fts_ad AFTER DELETE ON memories BEGIN
            INSERT INTO memories_fts(memories_fts, rowid, content)
            VALUES('delete', OLD.rowid, OLD.content);
        END;

        CREATE TRIGGER IF NOT EXISTS memories_fts_retract AFTER UPDATE OF retracted_at ON memories
        WHEN NEW.retracted_at IS NOT NULL AND OLD.retracted_at IS NULL BEGIN
            INSERT INTO memories_fts(memories_fts, rowid, content)
            VALUES('delete', OLD.rowid, OLD.content);
        END;

        CREATE TRIGGER IF NOT EXISTS memories_fts_supersede AFTER UPDATE OF superseded_at ON memories
        WHEN NEW.superseded_at IS NOT NULL AND OLD.superseded_at IS NULL BEGIN
            INSERT INTO memories_fts(memories_fts, rowid, content)
            VALUES('delete', OLD.rowid, OLD.content);
        END;

        -- ============================================
        -- INDEXES
        -- ============================================
        CREATE INDEX IF NOT EXISTS idx_memories_active ON memories(scope_type, scope_id)
            WHERE retracted_at IS NULL AND superseded_at IS NULL;
        CREATE INDEX IF NOT EXISTS idx_memories_slot ON memories(slot, scope_type, scope_id)
            WHERE slot IS NOT NULL AND retracted_at IS NULL AND superseded_at IS NULL;
        CREATE INDEX IF NOT EXISTS idx_memories_sensitivity ON memories(sensitivity)
            WHERE sensitivity != 'normal';
        CREATE INDEX IF NOT EXISTS idx_memories_agent ON memories(source_agent);
        CREATE INDEX IF NOT EXISTS idx_memories_created ON memories(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_memories_expires ON memories(expires_at)
            WHERE expires_at IS NOT NULL AND retracted_at IS NULL;
        """
    )


def down(conn) -> None:
    raise NotImplementedError("Rollback not supported for this migration")
