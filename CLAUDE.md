# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run Streamlit UI
export ANTHROPIC_API_KEY="your-key"
python -m streamlit run app.py

# Run REST API
uvicorn core/api:app --host 0.0.0.0 --port 8000 --workers 4
# or
uvicorn api:app --host 0.0.0.0 --port 8000
```

## Architecture

**SellersFlow** converts product catalogs between marketplaces (Amazon, Shopee, Magalu, Mercado Livre, Temu, Vendor).

### Data flow

```
Reader → (normalize_source_df) → Mapper → [AIEngine] → Filler → output .xlsx
```

`pipeline.py:SellersFlowPipeline.run()` is the single orchestration entry point. `app.py` calls it directly; `core/api.py` wraps it in FastAPI background tasks.

### Module responsibilities

| Module | Role |
|--------|------|
| `core/reader.py` | Reads Amazon source sheets; detects PT-BR/EN-US; returns `AmazonReadResult` |
| `core/source_reader.py` | Reads non-Amazon source sheets; used when `source_marketplace != "Amazon"` |
| `core/mapper.py` | 4-strategy cascade mapping (learned → fixed+synonym → similarity → AI); persists decisions to `data/mappings_db/learned.json` |
| `core/normalizer.py` | Rule-based normalization (color, size, units, price) — no AI calls |
| `core/filler.py` | Writes mapped data into destination template preserving Excel formatting; validates required fields |
| `ai/ai_engine.py` | Claude API wrapper (`claude-sonnet-4-6`); in-memory cache; never raises — returns `None` on failure |
| `pipeline.py` | Stateless orchestrator; each `run()` call is independent |
| `app.py` | Streamlit UI with sidebar controls, mapping table, preview, download, and learn UI |
| `core/api.py` | FastAPI REST API; per-request job isolation via UUID dirs; in-memory job store (swap for Redis in multi-worker prod) |

### Mapping strategy cascade (`core/mapper.py`)

1. **Learned** (confidence 0.98) — from `data/mappings_db/learned.json`
2. **Fixed + Synonym** (1.0) — `MARKETPLACE_MAPPINGS` + `AMAZON_SYNONYMS` tables
3. **Similarity** (score) — `SequenceMatcher`, threshold 0.72
4. **AI** (variable) — `AIEngine.suggest_mapping()`, only when `use_ai=True`

`AMAZON_SYNONYMS` maps semantic keys (e.g. `"nome_produto"`) to raw column name variants. `MARKETPLACE_MAPPINGS` maps destination marketplace column names to those semantic keys.

### Adding a new marketplace

1. `core/mapper.py → MARKETPLACE_MAPPINGS` — add column→semantic-key mapping
2. `core/filler.py → MARKETPLACE_CONFIG` — add sheet name, header row, required fields
3. `core/mapper.py → REQUIRED_FIELDS` — add mandatory columns

### Mercado Livre sheet resolution

ML template sheet names vary by product category. The resolver uses **position** not name: take the sheet at index 2 (third sheet, after "Ajuda"). Header is at row 3.

### Key env var

`ANTHROPIC_API_KEY` — required only when `use_ai=True` or `enrich_ai=True`.

### `-correta.py` files

`*-correta.py` variants (e.g. `app-correta.py`, `pipeline-correta.py`) are reference/corrected versions kept alongside the active files. The active entry points are `app.py` and `pipeline.py`.
