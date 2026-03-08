---
name: messydata
description: >
  Use this skill when working with MessyData — a synthetic dirty data generator.
  Covers writing and validating YAML configs, using the CLI, and the Python API.
  Trigger on: "generate synthetic data", "messydata config", "create a dataset schema",
  "add anomalies", "fake data", "dirty data".
argument-hint: "[task description or config path]"
allowed-tools: Read, Bash(uv:*)
---

# MessyData Skill

MessyData generates realistic messy DataFrames from a YAML config. It produces structured data
with configurable anomalies (missing values, duplicates, invalid categories, bad dates, outliers).

## Workflow

Always follow this order:
1. Write or edit the YAML config
2. **Validate it** with the CLI before generating
3. Generate data

```bash
# 1. Validate (fast — no generation, exits 0/1)
uv run messydata validate my_config.yaml

# 2. Generate to stdout
uv run messydata generate my_config.yaml --rows 1000 --seed 42

# 3. Generate to file (format inferred from extension)
uv run messydata generate my_config.yaml --rows 1000 --output data.csv
uv run messydata generate my_config.yaml --rows 1000 --output data.parquet
uv run messydata generate my_config.yaml --rows 1000 --output data.json
uv run messydata generate my_config.yaml --rows 1000 --output data.jsonl
```

Always run `validate` after writing a config. Fix any errors before generating.

---

## YAML Config Structure

```yaml
name: <string>                          # required — dataset identifier
primary_key: <string>                   # optional, default: "id"
records_per_primary_key: <distribution> # required — rows per PK group (continuous only)
fields: [<field_spec>, ...]             # required — column definitions
anomalies: [<anomaly_spec>, ...]        # optional — data quality injections
```

**Row count:** `--rows N` is approximate. Each PK group is sampled from
`records_per_primary_key`; actual count may vary slightly. Each group has at least 1 row.

---

## Field Spec

```yaml
- name: <string>           # required — output column name
  dtype: <string>          # optional, default: "object"
                           # values: int32, int64, float32, float64, object, bool
  distribution: <dist>     # required — how values are sampled
  unique_per_id: <bool>    # optional, default: false
                           # true = one value drawn per PK group, repeated for all rows
  nullable: <bool>         # optional, default: true
```

**`unique_per_id: true`** — use for entity attributes that don't vary per row within a group:
store region, customer tier, payment method for an order, product category for a transaction.

---

## Distribution Reference

Every distribution block requires a `type` field. All other fields are parameters.

### Continuous

```yaml
distribution:
  type: uniform
  min: <float>    # required
  max: <float>    # required
```

```yaml
distribution:
  type: normal
  mean: <float>   # required
  std: <float>    # required
```

```yaml
distribution:
  type: lognormal
  mu: <float>     # required — good default for prices, durations, revenue
  sigma: <float>  # required
```

```yaml
distribution:
  type: weibull
  a: <float>      # required — shape
  scale: <float>  # optional, default: 1.0
```

```yaml
distribution:
  type: exponential
  scale: <float>  # optional, default: 1.0  (rate = 1/scale)
```

```yaml
distribution:
  type: beta
  a: <float>      # required — output in [0, 1], good for rates/probabilities
  b: <float>      # required
```

```yaml
distribution:
  type: gamma
  shape: <float>  # required
  scale: <float>  # optional, default: 1.0
```

```yaml
distribution:
  type: mixture                  # weighted blend of continuous distributions only
  components:
    - type: normal
      mean: 15.0
      std: 3.0
    - type: lognormal
      mu: 5.0
      sigma: 0.8
  weights: [0.6, 0.4]            # must sum to 1, one per component
```

`mixture` components must be continuous types only (not `weighted_choice`, `sequential`, etc.).

### Categorical

```yaml
distribution:
  type: weighted_choice
  values: [a, b, c]              # required — list of any type
  weights: [0.5, 0.3, 0.2]       # required — must sum to 1
```

```yaml
distribution:
  type: weighted_choice_mapping  # use when multiple columns are always correlated
  columns:                       # all lists must be the same length
    product_id:   [1001, 1002, 1003]
    product_name: [Widget, Gadget, Doohickey]
  weights: [0.5, 0.3, 0.2]       # required — must sum to 1
```

**Important for `weighted_choice_mapping`:** The field `name` is a placeholder — the actual
columns added to the DataFrame come from `columns:`. The placeholder name is not in the output.

### Sequential

```yaml
distribution:
  type: sequential
  start: 1          # int: increments by step per PK group
  step: 1           # optional, default: 1

distribution:
  type: sequential
  start: "2023-01-01"   # date string: advances by step days per PK group
  step: 1
```

---

## Anomaly Reference

Each anomaly fires with probability `prob`. When active, `rate` fraction of rows are affected.

```yaml
anomalies:
  - name: missing_values
    prob: <float>           # 0–1, chance this anomaly fires this run
    rate: <float>           # 0–1, fraction of cells set to NaN
    columns: any            # special string — targets all columns
    # columns: [col1, col2] # or explicit list

  - name: duplicate_values
    prob: <float>
    rate: <float>
    # no columns field — duplicates whole rows

  - name: invalid_category
    prob: <float>
    rate: <float>
    columns: [col1, col2]   # required — replaces with "INVALID"

  - name: invalid_date
    prob: <float>
    rate: <float>
    columns: [col1]         # required — replaces with "9999-99-99"

  - name: outliers
    prob: <float>
    rate: <float>
    columns: [col1]         # required — replaces with samples from distribution
    distribution:           # required for outliers
      type: lognormal
      mu: 6.0
      sigma: 0.5
```

**`columns: any`** is only valid for `missing_values`. All other anomaly types require an
explicit list. Keep `rate` below 0.3 for realistic data.

**prob/rate semantics:**
- `prob: 1.0, rate: 0.05` → always inject, 5% of rows affected (deterministic)
- `prob: 0.3, rate: 0.05` → 30% chance active; when active, 5% affected (non-deterministic)
- `prob: 1.0, rate: 1.0` → destroys the dataset — avoid

---

## Common Patterns

### Transaction dataset with entity attributes

```yaml
name: transactions
primary_key: transaction_id
records_per_primary_key:
  type: lognormal
  mu: 2.0
  sigma: 0.5

fields:
  - name: transaction_id
    dtype: int32
    unique_per_id: true
    nullable: false
    distribution: {type: sequential, start: 1}

  - name: transaction_date
    dtype: object
    unique_per_id: true
    nullable: false
    distribution: {type: sequential, start: "2024-01-01", step: 1}

  - name: customer_id        # entity attribute — same per transaction
    dtype: int32
    unique_per_id: true
    nullable: false
    distribution: {type: uniform, min: 1000, max: 9999}

  - name: amount
    dtype: float32
    nullable: false
    distribution: {type: lognormal, mu: 4.0, sigma: 0.8}

  - name: channel
    dtype: object
    unique_per_id: true
    nullable: false
    distribution:
      type: weighted_choice
      values: [web, mobile, in_store]
      weights: [0.5, 0.35, 0.15]
```

### Correlated product catalog

```yaml
- name: product
  dtype: object
  nullable: false
  distribution:
    type: weighted_choice_mapping
    columns:
      sku:           [SKU-001, SKU-002, SKU-003]
      product_name:  [Widget,  Gadget,  Doohickey]
      category:      [tools,   electronics, tools]
    weights: [0.5, 0.3, 0.2]
```

### Bimodal distribution (budget vs premium pricing)

```yaml
- name: price
  dtype: float32
  nullable: false
  distribution:
    type: mixture
    components:
      - type: normal
        mean: 12.0
        std: 3.0
      - type: lognormal
        mu: 5.5
        sigma: 0.6
    weights: [0.7, 0.3]
```

### Realistic anomaly set

```yaml
anomalies:
  - name: missing_values
    prob: 1.0
    rate: 0.05
    columns: any
  - name: duplicate_values
    prob: 0.3
    rate: 0.02
  - name: outliers
    prob: 0.5
    rate: 0.03
    columns: [amount, price]
    distribution: {type: lognormal, mu: 7.0, sigma: 0.5}
```

---

## Python API

```python
from messydata import Pipeline

# From YAML
df = Pipeline.from_config("my_config.yaml").run(n_rows=1000, seed=42)

# Python-first (same result, full IDE support)
from messydata import (
    DatasetSchema, Pipeline, FieldSpec, AnomalySpec,
    Lognormal, Uniform, Normal, WeightedChoice, WeightedChoiceMapping,
    Sequential, Mixture, Beta, Gamma, Weibull, Exponential,
)

schema = DatasetSchema(
    name="dataset",
    primary_key="id",
    records_per_primary_key=Lognormal(mu=2.0, sigma=0.5),
    fields=[
        FieldSpec(
            name="id", dtype="int32",
            distribution=Sequential(start=1),
            unique_per_id=True, nullable=False
        ),
        FieldSpec(
            name="amount", dtype="float32",
            distribution=Lognormal(mu=3.5, sigma=0.75)
        ),
    ],
    anomalies=[
        AnomalySpec(name="missing_values", prob=1.0, rate=0.05),
    ],
)

df = Pipeline(schema).run(n_rows=1000, seed=42)
```

---

## CLI Reference

```
messydata generate CONFIG [OPTIONS]
  --rows  -n  INT     Number of rows (default: 1000)
  --seed  -s  INT     Random seed (default: 42)
  --output -o PATH    Output file (default: stdout)
  --format -f FMT     csv | parquet | json | jsonl
                      Inferred from output extension if omitted

messydata validate CONFIG
  Exits 0 on success, 1 on error. Prints field/anomaly count or error message.
  Use this after every config edit.

messydata schema
  Prints the full JSON Schema for the config format to stdout.
  Useful for understanding the exact spec or passing to another model.
```

---

## Inspecting Output

```python
df.info()              # column names, dtypes, non-null counts
df.isna().sum()        # injected nulls by column
df.duplicated().sum()  # injected duplicate rows
df.describe()          # distribution summary
```

---

## Key Rules for Config Writing

- Use `lognormal` for prices, revenue, durations — reflects real-world skew
- Use `weighted_choice_mapping` when columns are always correlated (not separate `weighted_choice`)
- Set `unique_per_id: true` for entity attributes (customer region, store tier, etc.)
- Always validate before generating: `uv run messydata validate config.yaml`
- `columns: any` is only valid on `missing_values`
- `mixture` components must be continuous (not `weighted_choice` or `sequential`)
- All `weights` lists must sum to 1
- All lists under `weighted_choice_mapping.columns` must have equal length
