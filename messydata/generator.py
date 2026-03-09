from __future__ import annotations

from datetime import date
from typing import Optional

import numpy as np
import pandas as pd

from .distributions import Sequential, WeightedChoice, WeightedChoiceMapping
from .schema import DatasetSchema


def generate_data(
    schema: DatasetSchema,
    n_rows: int = 1000,
    seed: int = 42,
    date_override: Optional[date] = None,
) -> pd.DataFrame:
    if seed is not None:
        np.random.seed(seed)

    # When date_override is set, pin temporal fields to that date
    if date_override is not None:
        target_str = str(date_override)
        fields = [
            f.model_copy(update={"distribution": WeightedChoice(values=[target_str], weights=[1.0])})
            if f.temporal
            else f
            for f in schema.fields
        ]
    else:
        fields = schema.fields

    # Initialise sequential state for each sequential field
    seq_state: dict[str, object] = {}
    for field in fields:
        if isinstance(field.distribution, Sequential):
            seq_state[field.name] = field.distribution.initial()

    n_generated = 0
    all_batches: list[pd.DataFrame] = []

    while n_generated < n_rows:
        n = max(1, int(schema.records_per_primary_key.sample(1)[0]))
        batch: dict[str, object] = {}

        for field in fields:
            dist = field.distribution
            unique = field.unique_per_id

            if isinstance(dist, Sequential):
                val = seq_state[field.name]
                batch[field.name] = np.repeat(val, n)
                seq_state[field.name] = dist.advance(val)

            elif isinstance(dist, WeightedChoiceMapping):
                # Returns dict of column_name -> list; ignore field.name
                for col, values in dist.sample(n).items():
                    batch[col] = values

            else:
                sample = dist.sample(1 if unique else n)
                if unique:
                    sample = np.repeat(sample, n)
                # Apply dtype where possible
                try:
                    sample = np.array(sample).astype(field.dtype)
                except (TypeError, ValueError):
                    pass
                batch[field.name] = sample

        all_batches.append(pd.DataFrame(batch))
        n_generated += n

    return pd.concat(all_batches, ignore_index=True)
