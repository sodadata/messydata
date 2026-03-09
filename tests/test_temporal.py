"""Tests for date-aware generation modes (temporal field + run_for_date / run_date_range)."""

from datetime import date

import pytest

from messydata import (
    AnomalySpec,
    DatasetSchema,
    FieldSpec,
    Lognormal,
    Pipeline,
    Sequential,
    Uniform,
    WeightedChoice,
)


def _make_schema(with_temporal: bool = True) -> DatasetSchema:
    fields = [
        FieldSpec(
            name="id",
            dtype="int32",
            distribution=Sequential(start=1),
            unique_per_id=True,
            nullable=False,
        ),
        FieldSpec(
            name="amount",
            dtype="float32",
            distribution=Uniform(min=1.0, max=100.0),
            nullable=False,
        ),
    ]
    if with_temporal:
        fields.insert(
            1,
            FieldSpec(
                name="event_date",
                dtype="object",
                distribution=Sequential(start="2024-01-01"),
                unique_per_id=True,
                nullable=False,
                temporal=True,
            ),
        )
    return DatasetSchema(
        name="test",
        primary_key="id",
        records_per_primary_key=Lognormal(mu=1.0, sigma=0.2),
        fields=fields,
    )


# ---------------------------------------------------------------------------
# FieldSpec.temporal flag
# ---------------------------------------------------------------------------


def test_temporal_defaults_false():
    f = FieldSpec(name="x", distribution=Uniform(min=0, max=1))
    assert f.temporal is False


def test_temporal_true_preserved():
    schema = _make_schema(with_temporal=True)
    temporal = [f for f in schema.fields if f.temporal]
    assert len(temporal) == 1
    assert temporal[0].name == "event_date"


# ---------------------------------------------------------------------------
# run_for_date
# ---------------------------------------------------------------------------


def test_run_for_date_pins_date_object():
    pipeline = Pipeline(_make_schema())
    target = date(2025, 6, 15)
    df = pipeline.run_for_date(target, n_rows=200, seed=0)
    assert (df["event_date"] == "2025-06-15").all()


def test_run_for_date_pins_date_string():
    pipeline = Pipeline(_make_schema())
    df = pipeline.run_for_date("2025-06-15", n_rows=200, seed=0)
    assert (df["event_date"] == "2025-06-15").all()


def test_run_for_date_non_temporal_fields_unaffected():
    pipeline = Pipeline(_make_schema())
    df = pipeline.run_for_date("2025-06-15", n_rows=200, seed=0)
    assert "amount" in df.columns
    assert df["amount"].notna().any()


def test_run_for_date_raises_without_temporal_field():
    pipeline = Pipeline(_make_schema(with_temporal=False))
    with pytest.raises(ValueError, match="temporal"):
        pipeline.run_for_date("2025-06-15", n_rows=100)


# ---------------------------------------------------------------------------
# run_date_range
# ---------------------------------------------------------------------------


def test_run_date_range_contains_all_dates():
    pipeline = Pipeline(_make_schema())
    df = pipeline.run_date_range("2025-01-01", "2025-01-03", rows_per_day=100, seed=0)
    dates = set(df["event_date"].unique())
    assert dates == {"2025-01-01", "2025-01-02", "2025-01-03"}


def test_run_date_range_single_day():
    pipeline = Pipeline(_make_schema())
    df = pipeline.run_date_range("2025-03-10", "2025-03-10", rows_per_day=50, seed=0)
    assert set(df["event_date"].unique()) == {"2025-03-10"}


def test_run_date_range_date_objects():
    pipeline = Pipeline(_make_schema())
    df = pipeline.run_date_range(date(2025, 2, 1), date(2025, 2, 3), rows_per_day=50, seed=0)
    assert set(df["event_date"].unique()) == {"2025-02-01", "2025-02-02", "2025-02-03"}


def test_run_date_range_seed_varies_per_day():
    """Different days should produce different amount distributions (seed offset per day)."""
    pipeline = Pipeline(_make_schema())
    df = pipeline.run_date_range("2025-01-01", "2025-01-02", rows_per_day=200, seed=0)
    mean_day1 = df[df["event_date"] == "2025-01-01"]["amount"].mean()
    mean_day2 = df[df["event_date"] == "2025-01-02"]["amount"].mean()
    assert mean_day1 != mean_day2


def test_run_date_range_invalid_range():
    pipeline = Pipeline(_make_schema())
    with pytest.raises(ValueError, match="end"):
        pipeline.run_date_range("2025-01-05", "2025-01-01", rows_per_day=100)


# ---------------------------------------------------------------------------
# Example config round-trip
# ---------------------------------------------------------------------------


def test_retail_config_temporal_field():
    """retail_config.yaml has temporal=true on 'date'; run_for_date pins the date.
    Anomalies in that config may inject NaN (missing_values) or '9999-99-99' (invalid_date),
    so we only check the non-anomaly values are correct.
    """
    import pathlib

    config = pathlib.Path(__file__).parent.parent / "examples" / "retail_config.yaml"
    pipeline = Pipeline.from_config(str(config))
    df = pipeline.run_for_date("2025-06-01", n_rows=100, seed=42)
    valid_dates = df["date"].dropna()
    valid_dates = valid_dates[valid_dates != "9999-99-99"]
    assert (valid_dates == "2025-06-01").all()


def test_retail_config_date_range():
    import pathlib

    config = pathlib.Path(__file__).parent.parent / "examples" / "retail_config.yaml"
    pipeline = Pipeline.from_config(str(config))
    df = pipeline.run_date_range("2025-06-01", "2025-06-03", rows_per_day=50, seed=42)
    # Drop anomaly-injected values (NaN, invalid_date sentinel) before checking
    valid = df["date"].dropna()
    valid = valid[valid != "9999-99-99"]
    assert set(valid.unique()) == {"2025-06-01", "2025-06-02", "2025-06-03"}
