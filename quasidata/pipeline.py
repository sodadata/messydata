import yaml

from .generator import generate_data
from .injector import inject_anomalies


class Pipeline:
    def __init__(self, config):
        self._config = config

    @classmethod
    def from_config(cls, path):
        with open(path, 'r') as f:
            config = yaml.safe_load(f)['use_case']
        return cls(config)

    def run(self, n_rows=1000, seed=42):
        df = generate_data(self._config, n_rows=n_rows, seed=seed)
        df = inject_anomalies(self._config, df)
        return df
