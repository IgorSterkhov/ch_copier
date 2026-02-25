# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

ClickHouse Migration Tool — Python/Tkinter GUI app for migrating tables between two ClickHouse instances (source → destination). UI language is Russian.

## Setup & Run

```bash
# All packages go into venv, never install globally
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure connections
cp .env.example .env  # edit with real ClickHouse credentials

# Run
python ch_migrate.py
```

System packages required: `python3-tk`, `python3.12-venv`.

## Architecture

Single-file app (`ch_migrate.py`) with one class `CHMigrateApp`:

- **GUI**: Tkinter with ttk. PanedWindow layout — connection bar (top), schema tree (left ~30%), operations panel (right ~70%).
- **Treeview checkboxes**: Unicode ☐/☑ characters toggled on click, no native checkbox support in ttk.Treeview.
- **Threading**: All ClickHouse I/O runs in `threading.Thread(daemon=True)`. UI updates go through `root.after()` for thread safety.
- **Connection**: `clickhouse_connect.get_client()` with config from `.env` via `python-dotenv`. Supports ports 8123/8443/9000, SSL with CA cert.
- **Engine cleaning**: `_clean_replicated_engine()` uses balanced-parenthesis matching (not simple regex) to strip args from `Replicated*MergeTree(...)` — handles nested tuples like `(col1, col2)`.
- **DDL flow**: SHOW CREATE TABLE → clean Replicated engine → CREATE OR REPLACE TABLE → execute on destination → verify via `system.tables`.
- **Data migration**: `source_client.query(SELECT...)` → `dest_client.insert(table, data, columns)` per table, with per-table error handling.

## Key env vars (.env)

`SOURCE_HOST`, `SOURCE_PORT`, `SOURCE_USER`, `SOURCE_PASS`, `SOURCE_DB`, `SOURCE_SECURE`, `SOURCE_CA_CERT` — and identical `DESTINATION_*` set.
