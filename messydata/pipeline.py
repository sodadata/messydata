from __future__ import annotations

from datetime import date, timedelta
from typing import Union

import pandas as pd

from .generator import generate_data
from .injector import inject_anomalies
from .schema import DatasetSchema


class Pipeline:
    def __init__(self, schema: DatasetSchema):
        self._schema = schema

    @classmethod
    def from_config(cls, path: str) -> "Pipeline":
        return cls(DatasetSchema.from_yaml(path))

    def run(self, n_rows: int = 1000, seed: int = 42) -> pd.DataFrame:
        df = generate_data(self._schema, n_rows=n_rows, seed=seed)
        df = inject_anomalies(self._schema, df)
        return df

    def run_for_date(
        self,
        target_date: Union[date, str],
        n_rows: int = 1000,
        seed: int = 42,
    ) -> pd.DataFrame:
        """Generate data pinned to a single date.

        Requires exactly one field with ``temporal=True`` in the schema.
        That field's distribution is overridden to emit ``target_date`` for every row.
        """
        temporal_fields = [f for f in self._schema.fields if f.temporal]
        if not temporal_fields:
            raise ValueError(
                "run_for_date requires at least one field with temporal=True in the schema."
            )
        if isinstance(target_date, str):
            target_date = date.fromisoformat(target_date)
        df = generate_data(self._schema, n_rows=n_rows, seed=seed, date_override=target_date)
        df = inject_anomalies(self._schema, df)
        return df

    def run_date_range(
        self,
        start: Union[date, str],
        end: Union[date, str],
        rows_per_day: int = 1000,
        seed: int = 42,
    ) -> pd.DataFrame:
        """Generate data for every calendar day in [start, end] inclusive.

        Each day gets its own generation pass with the temporal field pinned to that date.
        The seed is offset per day so anomaly patterns vary across days.
        """
        if isinstance(start, str):
            start = date.fromisoformat(start)
        if isinstance(end, str):
            end = date.fromisoformat(end)
        if end < start:
            raise ValueError(f"end ({end}) must be >= start ({start})")

        frames: list[pd.DataFrame] = []
        current = start
        day_idx = 0
        while current <= end:
            day_seed = seed + day_idx if seed is not None else None
            frames.append(self.run_for_date(current, n_rows=rows_per_day, seed=day_seed))
            current += timedelta(days=1)
            day_idx += 1

        return pd.concat(frames, ignore_index=True)
