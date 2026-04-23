# Migration Agent

Oracle SQL migration batch that reads `NEXT_SQL_INFO`, generates `TOBE SQL`, optionally generates `BIND SQL`, runs test SQL, and then performs a second-pass `TUNED_SQL` tuning review.

The current codebase is organized around:

- `flows`: runtime/job orchestration
- `features`: stage-specific implementation
- `repositories`: Oracle and local SQLite persistence
- `services`: shared LLM, prompt, validation, binding helpers
- `tools`: sync and inspection scripts

## Runtime Flow

Process startup:

```text
app/main.py
  -> app/runtime/main_flow.py
  -> app/flows/runtime_flow.py
  -> app/flows/job_flow.py
```

One batch cycle:

```text
init_cycle
  -> startup_rag_sync      (only when requested)
  -> load_pending_jobs
  -> process_jobs
  -> finish_cycle
```

One job:

```text
INIT
  -> LOAD_RULES
  -> LOAD_TOBE_FEEDBACK
  -> GENERATE_TOBE_SQL
  -> DETECT_BIND_PARAMS
      -> LOAD_BIND_FEEDBACK
      -> GENERATE_BIND_SQL
      -> EXECUTE_BIND_SQL
      -> BUILD_BIND_SET
  -> GENERATE_TEST_SQL
  -> EXECUTE_TEST_SQL
  -> EVALUATE_STATUS
  -> LOAD_TUNING_CONTEXT
  -> GENERATE_TUNED_SQL
  -> GENERATE_TUNED_TEST_SQL
  -> EXECUTE_TUNED_TEST_SQL
  -> EVALUATE_TUNING_STATUS
  -> PERSIST_*
```

## RAG Overview

The codebase currently uses stage-specific RAG instead of one generic RAG service.

### TOBE RAG

Files:

- `app/features/rag/tobe_rag_service.py`
- `app/features/rag/tobe_rag_indexer.py`
- `app/features/rag/tobe_rag_models.py`

Characteristics:

- case-based TOBE retrieval
- local SQLite metadata store
- local Faiss dense index
- BM25 sparse retrieval
- metadata filter before retrieval
- optional LLM rerank, disabled by default
- prompt injection uses compact evidence first

What TOBE RAG actually does:

1. Build a query case from `source_sql`
2. Preprocess MyBatis syntax
3. Normalize SQL with `sqlglot`
4. Extract feature tags, rule tags, target tables, functions
5. Build a canonical query representation and retrieval summary
6. Apply metadata filtering to narrow candidate cases
7. Run dense retrieval against Faiss using multi-view retrieval docs
8. Run sparse retrieval against tokenized SQLite docs
9. Merge scores into a deterministic pre-rerank score
10. Optionally rerank with LLM
11. Serialize compact prompt examples for TOBE SQL generation

Dense retrieval is vector search.
Sparse retrieval is BM25 token search.
Rule tags are symbolic features, not vectors.

### BIND RAG

Files:

- `app/features/rag/bind_rag_service.py`
- `app/features/bind/bind_feature.py`

Characteristics:

- dedicated `BIND_CORRECT_SQL` retrieval
- no legacy stage mixing in runtime flow
- compact embedding-based retrieval around bind params and pattern tags
- simpler than TOBE RAG on purpose

### TUNED_SQL Tuning

Files:

- `app/flows/tuning_flow.py`
- `app/features/sql_tuning/*`

This is not the same as TOBE RAG. It is a second-pass review pipeline that runs after TOBE/TEST stages.

Current tuning prompt strategy:

- rule-first retrieval from `data/rag/DATA/tobe_rule_catalog.json`
- top 3 rules are serialized into `top_rules_json`
- one compact support example is serialized into `support_case_json`
- `normalized_sql` stays internal for rule detection and is not injected directly into the prompt

## Local RAG Storage

Default local artifact directory:

- `data/rag/`

Default artifact layout:

- SQLite DB: `data/rag/rag.db`
- TOBE rule vector DB: `data/rag/rule_catalog.db`

SQLite stores:

- `bind_rag_index`
- `tobe_rule_vector_index`
- optional legacy vector tables

## RAG Experiment Flags

Current TOBE rule-vector settings:

- `TOBE_RULE_VECTOR_DB_PATH`
- `TOBE_RULE_VECTOR_TABLE`
- `TOBE_RULE_VECTOR_TOP_K`
- `TOBE_RULE_VECTOR_FALLBACK_DIM`

Current BIND settings:

- `BIND_RAG_DB_PATH`
- `BIND_RAG_TABLE`
- `BIND_RAG_TOP_K`
- `BIND_RAG_CORPUS_LIMIT`

Recommended starting point:

- keep full `correct_sql` injection off
- keep SQL block summaries and rule snippets on
- keep LLM rerank off until retrieval quality is proven

## Tools

Install dependencies:

```powershell
py -m pip install -r requirements.txt
```

Run batch:

```powershell
py app/main.py
```

Sync BIND RAG:

```powershell
py tools/sync_bind_rag.py
py tools/sync_bind_rag.py --limit 500
```

Deprecated compatibility wrapper:

```powershell
py tools/sync_feedback_rag.py
```

Inspect legacy/shared vector table:

```powershell
py tools/inspect_rag_index.py
```

Inspect local storage paths:

```powershell
py tools/inspect_rag_storage.py
```

Debug TOBE preprocessing:

```powershell
py tools/debug_tobe_rag.py --from-file sample.sql
```

List mapping rules:

```powershell
py tools/list_mapping_rules.py --format table
py tools/list_mapping_rules.py --format json --out rules.json
```

## Viewing Local RAG DBs

You do not need a separate server. These are local SQLite files.

Options:

- run the built-in inspection tools above
- use a VS Code SQLite extension
- use DB Browser for SQLite
- use Python directly:

```powershell
py -c "import sqlite3; conn=sqlite3.connect('data/rag/rag.db'); print(conn.execute(\"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name\").fetchall())"
py -c "import sqlite3; conn=sqlite3.connect('data/rag/rule_catalog.db'); print(conn.execute(\"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name\").fetchall())"
```

## Important Source Files

Main runtime:

- `app/main.py`
- `app/runtime/main_flow.py`
- `app/flows/runtime_flow.py`
- `app/flows/job_flow.py`

TOBE:

- `app/flows/tobe_flow.py`
- `app/features/tobe/tobe_feature.py`

BIND:

- `app/features/bind/bind_feature.py`
- `app/features/rag/bind_rag_service.py`

RAG:

- `app/features/rag/tobe_rag_service.py`
- `app/features/rag/tobe_rag_indexer.py`
- `app/features/rag/tobe_rag_models.py`

LLM / prompt:

- `app/services/llm_service.py`
- `app/services/prompt_service.py`
- `app/prompts/tobe_sql_prompt.txt`
- `app/prompts/bind_sql_prompt.txt`
- `app/prompts/tuning_sql_prompt.txt`

Persistence:

- `app/repositories/result_repository.py`
- `app/repositories/mapper_repository.py`

## Operational Checks

Before running:

- verify `.env` matches the current Oracle / LLM / embedding environment
- verify `data/rag/` exists or can be created
- verify `BIND_CORRECT_SQL` is actually populated
- verify startup sync logs show expected `source_rows`, `upserted`, `skipped_*`
- verify `NEXT_SQL_INFO.STATUS` and `TUNING_STATUS` move through expected states

## Tests

Useful local checks:

```powershell
py -m unittest tests.test_graph_runtime_routes
py -m compileall app tests tools
```

## Notes

- The current priority in this codebase is improving TOBE SQL success rate first.
- TOBE RAG and TUNED_SQL tuning are separate concerns.
- If TOBE retrieval is not helping, the right comparison is sparse-only vs hybrid retrieval, not TOBE vs TUNED_SQL tuning.

