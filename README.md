# QuasiData

**Synthetic dirty data generator.** Define a schema in YAML, get a realistic messy DataFrame.

QuasiData generates structured datasets from a declarative config and injects configurable anomalies — missing values, duplicates, invalid categories, bad dates, and outliers. Designed for testing data pipelines, validating data quality tooling, and feeding AI/ML workflows that need realistic imperfect data.

---

## Install

```bash
uv add quasidata
# or
pip install quasidata
```

---

## Quick Start

### YAML (recommended)

```yaml
# my_config.yaml
name: orders
primary_key: order_id

records_per_primary_key:
  type: lognormal
  mu: 2.0
  sigma: 0.5

anomalies:
  - name: missing_values
    prob: 1.0   # always inject
    rate: 0.05  # 5% of cells set to NaN

fields:
  - name: order_id
    dtype: int32
    unique_per_id: true
    nullable: false
    distribution:
      type: sequential
      start: 1

  - name: amount
    dtype: float32
    nullable: false
    distribution:
      type: lognormal
      mu: 3.5
      sigma: 0.75

  - name: status
    dtype: object
    nullable: false
    distribution:
      type: weighted_choice
      values: [pending, shipped, delivered, cancelled]
      weights: [0.1, 0.3, 0.5, 0.1]
```

```python
from quasidata import Pipeline

df = Pipeline.from_config("my_config.yaml").run(n_rows=1000, seed=42)
```

### Python-first

All distribution and anomaly types are importable as Python classes with full IDE support:

```python
from quasidata import (
    DatasetSchema, Pipeline,
    FieldSpec, AnomalySpec,
    Lognormal, WeightedChoice, Sequential,
)

schema = DatasetSchema(
    name="orders",
    primary_key="order_id",
    records_per_primary_key=Lognormal(mu=2.0, sigma=0.5),
    fields=[
        FieldSpec(name="order_id", dtype="int32",
                  distribution=Sequential(start=1),
                  unique_per_id=True, nullable=False),
        FieldSpec(name="amount", dtype="float32",
                  distribution=Lognormal(mu=3.5, sigma=0.75),
                  nullable=False),
        FieldSpec(name="status", dtype="object",
                  distribution=WeightedChoice(
                      values=["pending", "shipped", "delivered"],
                      weights=[0.2, 0.5, 0.3])),
    ],
    anomalies=[AnomalySpec(name="missing_values", prob=1.0, rate=0.05)],
)

df = Pipeline(schema).run(n_rows=1000, seed=42)
```

---

## YAML Config Reference

### Top-level keys

| Key | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Dataset identifier |
| `primary_key` | string | no (default: `id`) | Field used as the primary grouping key |
| `records_per_primary_key` | distribution block | yes | How many rows to generate per primary key value — accepts any continuous distribution |
| `fields` | list of field specs | yes | Column definitions |
| `anomalies` | list of anomaly specs | no | Data quality issues to inject |

> **Row count:** `run(n_rows=N)` generates approximately N rows. Because each primary key group is sampled from `records_per_primary_key`, the actual count may differ slightly. Each group always has at least 1 row.

---

### Field spec properties

| Property | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | yes | — | Column name in the output DataFrame |
| `dtype` | string | no | `object` | Pandas dtype: `int32`, `int64`, `float32`, `float64`, `object`, `bool` |
| `distribution` | distribution block | yes | — | How values are sampled (see [Distribution Reference](#distribution-reference)) |
| `unique_per_id` | bool | no | `false` | If `true`, one value is drawn per primary key group and repeated for all rows in that group |
| `nullable` | bool | no | `true` | Marks the field as nullable — used by anomaly injection |

`unique_per_id: true` is appropriate for entity-level attributes that don't vary per transaction — e.g., a customer's region, a store's tier, a payment method for an order.

---

### Distribution reference

Each `distribution` block requires a `type` key. All other keys are parameters for that distribution type.

#### Continuous distributions

| `type` | Parameters | Notes |
|---|---|---|
| `uniform` | `min`, `max` | Uniform over [min, max] |
| `normal` | `mean`, `std` | Gaussian |
| `lognormal` | `mu`, `sigma` | Log-normal — good default for prices, quantities, durations |
| `weibull` | `a`, `scale` (default `1.0`) | Parametrised by shape `a` |
| `exponential` | `scale` (default `1.0`) | Rate = 1 / scale |
| `beta` | `a`, `b` | Output in [0, 1] — useful for rates and probabilities |
| `gamma` | `shape`, `scale` (default `1.0`) | General-purpose skewed positive |
| `mixture` | `components`, `weights` | Weighted blend of continuous distributions — see below |

#### Categorical distributions

| `type` | Parameters | Notes |
|---|---|---|
| `weighted_choice` | `values`, `weights` | Draws from a fixed list. `weights` must sum to 1. |
| `weighted_choice_mapping` | `columns`, `weights` | Draws correlated multi-column outcomes from a joint table — see below |

#### Special distributions

| `type` | Parameters | Notes |
|---|---|---|
| `sequential` | `start`, `step` (default `1`) | Auto-incrementing. `start` can be an integer or a date string (`"2023-01-01"`). Each primary key group advances by `step`. |

---

#### `weighted_choice` — categorical with probabilities

```yaml
distribution:
  type: weighted_choice
  values: [north, south, east, west]
  weights: [0.4, 0.3, 0.2, 0.1]
```

---

#### `weighted_choice_mapping` — correlated multi-column categorical

When two or more columns are always correlated (e.g., `product_id` and `product_name` always appear together), use a single `weighted_choice_mapping` field. All lists under `columns` must have the same length — each index is one joint outcome.

```yaml
- name: product        # field name is a placeholder; actual columns come from `columns:`
  dtype: object
  distribution:
    type: weighted_choice_mapping
    columns:
      product_id:   [1001,        1002,    1003,      1004,         1005]
      product_name: [Widget,      Gadget,  Doohickey, Thingamajig,  Whatsit]
    weights: [0.4, 0.2, 0.2, 0.1, 0.1]
```

This adds `product_id` and `product_name` as separate columns — guaranteed consistent. The placeholder `name: product` is not added to the DataFrame.

---

#### `sequential` — auto-incrementing integers or dates

```yaml
# Integer sequence starting at 1
distribution:
  type: sequential
  start: 1
  step: 1

# Date sequence — start must be a YYYY-MM-DD string
distribution:
  type: sequential
  start: "2023-01-01"
  step: 1           # advances by 1 day per primary key group
```

---

#### `mixture` — weighted blend of continuous distributions

```yaml
# Bimodal price distribution: budget items + premium items
distribution:
  type: mixture
  components:
    - type: normal
      mean: 15.0
      std: 3.0
    - type: lognormal
      mu: 5.0
      sigma: 0.8
  weights: [0.6, 0.4]
```

`mixture` only supports continuous component types (`uniform`, `normal`, `lognormal`, `weibull`, `exponential`, `beta`, `gamma`). Categorical and sequential types cannot be used as components.

---

### Anomaly reference

Each anomaly has two required fields:

| Field | Type | Description |
|---|---|---|
| `prob` | float [0–1] | Probability this anomaly fires on a given run. `1.0` = always inject. |
| `rate` | float [0–1] | Fraction of eligible rows or cells affected when the anomaly fires. |

> **Example:** `prob: 0.3, rate: 0.05` means a 30% chance the anomaly is active; when active, 5% of eligible rows are affected. Use `prob: 1.0` for deterministic injection.

#### Anomaly types

| `name` | `columns` | Extra params | Description |
|---|---|---|---|
| `missing_values` | `any` or list | — | Sets values to `NaN`. `any` targets all columns. |
| `duplicate_values` | — | — | Duplicates a fraction of rows and appends them. |
| `invalid_category` | list | — | Replaces values in the listed columns with `"INVALID"`. |
| `invalid_date` | list | — | Replaces values in the listed columns with `"9999-99-99"`. |
| `outliers` | list | `distribution` | Replaces values with samples from the specified distribution. |

```yaml
anomalies:
  - name: missing_values
    prob: 1.0
    rate: 0.08
    columns: any

  - name: duplicate_values
    prob: 0.5
    rate: 0.03

  - name: invalid_category
    prob: 0.3
    rate: 0.05
    columns: [product_name, region]

  - name: invalid_date
    prob: 0.4
    rate: 0.02
    columns: [order_date]

  - name: outliers
    prob: 0.2
    rate: 0.05
    columns: [unit_price]
    distribution:
      type: lognormal
      mu: 6.0
      sigma: 0.5
```

> **`columns: any`** is a special string value (not a YAML list). It is accepted by `missing_values` and tells the injector to target all columns. All other anomaly types require an explicit column list.

---

## Full Example Config

```yaml
# examples/retail_config.yaml
name: retail
primary_key: id

# Average ~33 rows per transaction group (exp(3.5) ≈ 33)
records_per_primary_key:
  type: lognormal
  mu: 3.5
  sigma: 0.75

anomalies:
  - name: missing_values
    prob: 1.0
    rate: 0.05
    columns: any

  - name: duplicate_values
    prob: 0.3
    rate: 0.02

  - name: invalid_category
    prob: 0.2
    rate: 0.03
    columns: [product_name, payment_method]

  - name: invalid_date
    prob: 0.2
    rate: 0.02
    columns: [date]

  - name: outliers
    prob: 0.2
    rate: 0.05
    columns: [unit_price]
    distribution:
      type: lognormal
      mu: 6.0
      sigma: 0.5

fields:
  # Transaction ID — sequential integer, one per primary key group
  - name: id
    unique_per_id: true
    dtype: int32
    nullable: false
    distribution:
      type: sequential
      start: 1

  # Transaction date — one date per group, advancing daily
  - name: date
    unique_per_id: true
    dtype: object
    nullable: false
    distribution:
      type: sequential
      start: "2023-01-01"
      step: 1

  # Store — entity attribute, fixed per transaction
  - name: store_id
    unique_per_id: true
    dtype: int32
    nullable: false
    distribution:
      type: weighted_choice
      values: [1, 2, 3, 4, 5]
      weights: [0.5, 0.2, 0.1, 0.1, 0.1]

  # Customer — entity attribute
  - name: customer_id
    unique_per_id: true
    dtype: int32
    nullable: false
    distribution:
      type: uniform
      min: 1000
      max: 9999

  # Product — correlated ID + name from a fixed catalog
  - name: product
    unique_per_id: false
    dtype: object
    nullable: false
    distribution:
      type: weighted_choice_mapping
      columns:
        product_id:   [1001, 1002, 1003, 1004, 1005]
        product_name: [A,    B,    C,    D,    E]
      weights: [0.4, 0.2, 0.2, 0.1, 0.1]

  # Quantity — uniform integer per line item
  - name: quantity
    unique_per_id: false
    dtype: int32
    nullable: false
    distribution:
      type: uniform
      min: 1
      max: 10

  # Unit price — log-normal, typical for retail prices
  - name: unit_price
    unique_per_id: false
    dtype: float32
    nullable: false
    distribution:
      type: lognormal
      mu: 3.5
      sigma: 0.75

  # Payment method — entity attribute for the transaction
  - name: payment_method
    unique_per_id: true
    dtype: object
    nullable: false
    distribution:
      type: weighted_choice
      values: [credit_card, cash, store_credit]
      weights: [0.8, 0.15, 0.05]
```

---

## Working with AI Agents

QuasiData's YAML format is designed to be written by language models without any procedural code. The config is declarative, self-describing, and maps directly to real-world data concepts.

### Why it works well for agents

- **Small fixed vocabulary** — 11 distribution types with 1–3 parameters each; an agent can enumerate them all from this README
- **Domain-transparent** — field names, distribution types, and anomaly names use standard data engineering language
- **Composable** — anomalies are independent specs; an agent can add, remove, or tune one without touching the rest of the config
- **No procedural logic** — the agent describes the schema, not the generation procedure

### Prompt template

```
Generate a QuasiData YAML config for a [domain] dataset.

Dataset requirements:
- Primary entity: [e.g., customer_id, order_id]
- Fields: [describe each field — name, expected distribution, whether it varies per row or per entity]
- Target ~[N] rows per primary key group on average
- Anomalies to inject: [list types and approximate rates]

Distribution types available:
  Continuous: uniform, normal, lognormal, weibull, exponential, beta, gamma, mixture
  Categorical: weighted_choice, weighted_choice_mapping
  Special: sequential

Rules:
- Use lognormal for prices, durations, and revenue
- Use weighted_choice for any field with a fixed set of categories
- Use weighted_choice_mapping when two columns are always correlated (e.g. product_id + product_name)
- Set unique_per_id: true for entity attributes that don't vary per row within a group
- Use prob: 1.0 on anomalies that should always be present; lower values for probabilistic injection
- Keep rate below 0.3 — above that, data becomes mostly noise
```

### Patterns to follow

| Do | Avoid |
|---|---|
| Use `lognormal` for prices, durations, and counts | Using `uniform` for everything |
| Use `weighted_choice_mapping` for correlated column pairs | Separate `weighted_choice` fields that can produce inconsistent pairs |
| Set `unique_per_id: true` on entity-level attributes | Per-row variation on fields that belong to the entity |
| Use `prob < 1.0` for realistic non-determinism | `prob: 1.0, rate: 1.0` — destroys the dataset |
| Target specific `columns` on category/date anomalies | `columns: any` on anomalies that should only touch specific fields |
| Use `mixture` for bimodal distributions | Using a single distribution when the real data has two regimes |

---

## Output

`Pipeline.run()` returns a `pandas.DataFrame`.

- Column names and dtypes match the field specs
- Row count is approximately `n_rows` — may vary slightly due to the `records_per_primary_key` distribution
- The `seed` parameter makes generation fully reproducible
- Anomaly injection happens in-place; no indicator columns are added

```python
df = Pipeline.from_config("my_config.yaml").run(n_rows=1000, seed=42)

df.info()               # column names, dtypes, non-null counts
df.isna().sum()         # verify injected nulls
df.duplicated().sum()   # verify injected duplicates
df.describe()           # distribution summary
```