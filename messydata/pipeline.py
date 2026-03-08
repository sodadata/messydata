from .generator import generate_data
from .injector import inject_anomalies
from .schema import DatasetSchema


class Pipeline:
    def __init__(self, schema: DatasetSchema):
        self._schema = schema

    @classmethod
    def from_config(cls, path: str) -> "Pipeline":
        return cls(DatasetSchema.from_yaml(path))

    def run(self, n_rows: int = 1000, seed: int = 42) -> "pd.DataFrame":
        import pandas as pd  # noqa: F401 — imported for type hint context

        df = generate_data(self._schema, n_rows=n_rows, seed=seed)
        df = inject_anomalies(self._schema, df)
        return df
