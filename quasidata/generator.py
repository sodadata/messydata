import numpy as np
import pandas as pd

from .distributions import Sequential, WeightedChoiceMapping
from .schema import DatasetSchema


def generate_data(schema: DatasetSchema, n_rows: int = 1000, seed: int = 42) -> pd.DataFrame:
    if seed is not None:
        np.random.seed(seed)

    # Initialise sequential state for each sequential field
    seq_state: dict[str, object] = {}
    for field in schema.fields:
        if isinstance(field.distribution, Sequential):
            seq_state[field.name] = field.distribution.initial()

    n_generated = 0
    all_batches: list[pd.DataFrame] = []

    while n_generated < n_rows:
        n = max(1, int(schema.records_per_primary_key.sample(1)[0]))
        batch: dict[str, object] = {}

        for field in schema.fields:
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
