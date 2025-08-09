import yaml
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime


def load_config(path=USE_CASE_CONFIG_PATH):
    with open(path, 'r') as f:
        return yaml.safe_load(f)['use_case']

def generate_data(config, n_rows=1000, seed=42):
    fake = Faker()
    if seed is not None:
        np.random.seed(seed)
        fake.seed_instance(seed)
    fields = config['schema']['fields']
    n_generated_rows = 0
    current_batch = {}
    all_batches = []
    while n_generated_rows < n_rows:
        n_records_per_primary_key = 0
        primary_key_distribution = config['generation']['records_per_primary_key']['distribution']
        if primary_key_distribution['type'] == 'lognormal':
            n_records_per_primary_key = int(np.random.lognormal(
                primary_key_distribution['parameters']['mu'],
                primary_key_distribution['parameters']['sigma']))
        else:
            raise ValueError(f"Unsupported distribution type: {primary_key_distribution['type']}")
        
        # Generate a fake row identifier and transaction date
        id = fake.uuid4()
        date = datetime.today().date()
        current_batch['id'] = np.repeat(id, n_records_per_primary_key)
        current_batch['date'] = np.repeat(date, n_records_per_primary_key)

        for field in fields:
            name = field['name']
            distribution = field['distribution']
            dtype = field['dtype']
            if distribution['type'] == 'uniform':
                min = distribution['parameters']['min']
                max = distribution['parameters']['max']
                if field['unique_per_id'] == True:
                    current_batch[name] = np.repeat(np.random.uniform(low=min,
                                                                      high=max,
                                                                      size=1).astype(dtype),
                                                                    n_records_per_primary_key)
                else:
                    current_batch[name] = np.random.uniform(low=min,
                                        high=max,
                                        size=n_records_per_primary_key).astype(dtype)
            elif distribution['type'] == 'lognormal':
                mu = distribution['parameters']['mu']
                sigma = distribution['parameters']['sigma']
                if field['unique_per_id'] == True:
                    current_batch[name] = np.repeat(np.random.lognormal(mean=mu,
                                                                        sigma=sigma,
                                                                        size=1).astype(dtype),
                                                                    n_records_per_primary_key)
                else:
                    current_batch[name] = np.random.lognormal(mean=mu,
                                                              sigma=sigma,
                                                              size=n_records_per_primary_key).astype(dtype)
            elif distribution['type'] == 'weighted_choice':
                values = distribution['parameters']['values']
                weights = distribution['parameters']['weights']
                if field['unique_per_id'] == True:
                    current_batch[name] = np.repeat(np.random.choice(a=values,
                                                                     p=weights,
                                                                     size=1).astype(dtype),
                                                                    n_records_per_primary_key)
                else:
                    current_batch[name] = np.random.choice(a=values,
                                                          p=weights,
                                                          size=n_records_per_primary_key).astype(dtype)
            elif distribution['type'] == 'weighted_choice_mapping':
                weights = distribution['parameters']['weights']
                values_index = [i for i in range(0, len(weights))]
                choosen_indexes = np.random.choice(a=values_index, p=weights, size=n_records_per_primary_key)
                for name in distribution['parameters']['columns']:
                    values = [
                        distribution['parameters']['columns'][name][index]
                        for index in choosen_indexes
                    ]
                    current_batch[name] = values
        all_batches.append(pd.DataFrame(current_batch))
        n_generated_rows += n_records_per_primary_key
    data = pd.concat(all_batches).reset_index(drop=True)
    return data


def persist_data(data):
    # TODO: this function should the daily generated data in a DB
    data.to_csv('data/data.csv', index=False)

if __name__ == "__main__":
    USE_CASE_CONFIG_PATH = 'use_case_config.yaml'
    print("Loading config file")
    config = load_config(USE_CASE_CONFIG_PATH)
    print("Generating data...")
    data = generate_data(config)
    print(f"Generated rows: {len(data)}")
    persist_data(data)