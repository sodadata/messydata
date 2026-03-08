# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

QuasiData is a synthetic dirty data generator. It generates realistic datasets from a YAML config and injects configurable anomalies (missing values, duplicates, invalid categories, invalid dates).

## Commands

```bash
# Install dependencies
uv sync

# Run a script
uv run python my_script.py

# Add a dependency
uv add <package>

# Run tests (when added)
uv run pytest
```

## Public API

```python
from quasidata import Pipeline

df = Pipeline.from_config("examples/retail_config.yaml").run(n_rows=1000, seed=42)
```

`run()` returns a pandas DataFrame with anomalies already injected.

## Architecture

Three modules inside `quasidata/`:

- **`generator.py`** — `generate_data(config, n_rows, seed) -> DataFrame`. Reads the `schema.fields` list and dispatches on `distribution.type`: `uniform`, `lognormal`, `weighted_choice`, `weighted_choice_mapping`. For `unique_per_id=True` fields, one value is drawn per primary-key group. `id` and `date` are hardcoded outside the field loop (sequential types in schema are ignored by the generator currently).

- **`injector.py`** — Individual inject functions + `inject_anomalies(config, df) -> DataFrame`. Each anomaly in config has a `prob` (chance it applies to this run) and `rate` (fraction of rows/cells affected). `inject_invalid_category` and `inject_invalid_date` are stubs (TODO).

- **`pipeline.py`** — `Pipeline` class. `from_config(path)` loads the YAML, `run()` calls generator then injector.

## Configuration

`examples/retail_config.yaml` is the reference config. Top-level keys under `use_case`:
- `generation.records_per_primary_key.distribution` — how many rows per primary key group
- `anomalies` — list of anomaly types with `prob` and `rate`
- `schema.fields` — field definitions with `distribution`, `dtype`, `unique_per_id`, `nullable`
