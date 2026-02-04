from __future__ import annotations

import sqlite3

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS entities (
        id TEXT PRIMARY KEY,
        source TEXT NOT NULL,
        source_id TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        title TEXT,
        uri TEXT,
        mime_type TEXT,
        tags TEXT,
        tags_text TEXT,
        metadata JSON,
        sensitivity TEXT DEFAULT 'normal',
        created_at DATETIME,
        updated_at DATETIME,
        synced_at DATETIME,
        last_seen_at DATETIME,
        tombstoned_at DATETIME,
        content_hash TEXT,
        connector_name TEXT,
        connector_version TEXT,
        UNIQUE(source, source_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS chunks (
        id TEXT PRIMARY KEY,
        entity_id TEXT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
        chunk_index INTEGER NOT NULL,
        content TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        char_offset_start INTEGER,
        char_offset_end INTEGER,
        chunk_type TEXT DEFAULT 'semantic',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(entity_id, chunk_index)
    );
    """,
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts USING fts5(
        title,
        tags_text,
        uri,
        content='entities',
        content_rowid='rowid'
    );
    """,
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
        content,
        entity_id UNINDEXED,
        chunk_id UNINDEXED,
        content='chunks',
        content_rowid='rowid'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS embeddings (
        chunk_id TEXT PRIMARY KEY REFERENCES chunks(id) ON DELETE CASCADE,
        model TEXT NOT NULL,
        vector BLOB NOT NULL,
        dims INTEGER NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TRIGGER IF NOT EXISTS entities_ai AFTER INSERT ON entities BEGIN
        INSERT INTO entities_fts(rowid, title, tags_text, uri)
        VALUES (new.rowid, new.title, new.tags_text, new.uri);
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS entities_ad AFTER DELETE ON entities BEGIN
        INSERT INTO entities_fts(entities_fts, rowid, title, tags_text, uri)
        VALUES('delete', old.rowid, old.title, old.tags_text, old.uri);
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS entities_au AFTER UPDATE ON entities BEGIN
        INSERT INTO entities_fts(entities_fts, rowid, title, tags_text, uri)
        VALUES('delete', old.rowid, old.title, old.tags_text, old.uri);
        INSERT INTO entities_fts(rowid, title, tags_text, uri)
        VALUES (new.rowid, new.title, new.tags_text, new.uri);
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS chunks_ai AFTER INSERT ON chunks BEGIN
        INSERT INTO chunks_fts(rowid, content, entity_id, chunk_id)
        VALUES (new.rowid, new.content, new.entity_id, new.id);
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS chunks_ad AFTER DELETE ON chunks BEGIN
        INSERT INTO chunks_fts(chunks_fts, rowid, content, entity_id, chunk_id)
        VALUES('delete', old.rowid, old.content, old.entity_id, old.id);
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS chunks_au AFTER UPDATE ON chunks BEGIN
        INSERT INTO chunks_fts(chunks_fts, rowid, content, entity_id, chunk_id)
        VALUES('delete', old.rowid, old.content, old.entity_id, old.id);
        INSERT INTO chunks_fts(rowid, content, entity_id, chunk_id)
        VALUES (new.rowid, new.content, new.entity_id, new.id);
    END;
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_name TEXT,
        tool TEXT NOT NULL,
        scope TEXT,
        request_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        success INTEGER NOT NULL,
        chunks_returned INTEGER DEFAULT 0,
        bytes_returned INTEGER DEFAULT 0,
        metadata JSON
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS audit_logs_time_idx
        ON audit_logs(request_at);
    """,
    """
    CREATE INDEX IF NOT EXISTS audit_logs_token_tool_time_idx
        ON audit_logs(token_name, tool, request_at);
    """,
    """
    CREATE TABLE IF NOT EXISTS memory_entries (
        id TEXT PRIMARY KEY,
        key TEXT NOT NULL UNIQUE,
        content TEXT NOT NULL,
        tags TEXT,
        tags_text TEXT,
        metadata JSON,
        created_at DATETIME,
        updated_at DATETIME
    );
    """,
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(
        content,
        tags_text,
        content='memory_entries',
        content_rowid='rowid'
    );
    """,
    """
    CREATE TRIGGER IF NOT EXISTS memory_ai AFTER INSERT ON memory_entries BEGIN
        INSERT INTO memory_fts(rowid, content, tags_text)
        VALUES (new.rowid, new.content, new.tags_text);
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS memory_ad AFTER DELETE ON memory_entries BEGIN
        INSERT INTO memory_fts(memory_fts, rowid, content, tags_text)
        VALUES('delete', old.rowid, old.content, old.tags_text);
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS memory_au AFTER UPDATE ON memory_entries BEGIN
        INSERT INTO memory_fts(memory_fts, rowid, content, tags_text)
        VALUES('delete', old.rowid, old.content, old.tags_text);
        INSERT INTO memory_fts(rowid, content, tags_text)
        VALUES (new.rowid, new.content, new.tags_text);
    END;
    """,
]

SCHEMA_SQL = "\n\n".join(statement.strip() for statement in SCHEMA_STATEMENTS if statement.strip())


def apply_schema(conn: sqlite3.Connection) -> None:
    for statement in SCHEMA_STATEMENTS:
        sql = statement.strip()
        if not sql:
            continue
        conn.execute(sql)
