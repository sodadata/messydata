import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime


def generate_data(config, n_rows=1000, seed=42):
    fake = Faker()
    if seed is not None:
        np.random.seed(seed)
        fake.seed_instance(seed)

    fields = config['schema']['fields']
    n_generated_rows = 0
    all_batches = []

    while n_generated_rows < n_rows:
        pk_dist = config['generation']['records_per_primary_key']['distribution']
        if pk_dist['type'] == 'lognormal':
            n = int(np.random.lognormal(
                pk_dist['parameters']['mu'],
                pk_dist['parameters']['sigma']))
        else:
            raise ValueError(f"Unsupported distribution type: {pk_dist['type']}")

        batch = {
            'id': np.repeat(fake.uuid4(), n),
            'date': np.repeat(datetime.today().date(), n),
        }

        for field in fields:
            name = field['name']
            dist = field['distribution']
            dtype = field['dtype']
            unique = field.get('unique_per_id', False)

            if dist['type'] == 'uniform':
                lo, hi = dist['parameters']['min'], dist['parameters']['max']
                sample = np.random.uniform(low=lo, high=hi, size=1 if unique else n).astype(dtype)
                batch[name] = np.repeat(sample, n) if unique else sample

            elif dist['type'] == 'lognormal':
                mu, sigma = dist['parameters']['mu'], dist['parameters']['sigma']
                sample = np.random.lognormal(mean=mu, sigma=sigma, size=1 if unique else n).astype(dtype)
                batch[name] = np.repeat(sample, n) if unique else sample

            elif dist['type'] == 'weighted_choice':
                values = dist['parameters']['values']
                weights = dist['parameters']['weights']
                sample = np.random.choice(a=values, p=weights, size=1 if unique else n).astype(dtype)
                batch[name] = np.repeat(sample, n) if unique else sample

            elif dist['type'] == 'weighted_choice_mapping':
                weights = dist['parameters']['weights']
                indexes = np.random.choice(len(weights), p=weights, size=n)
                for col, col_values in dist['parameters']['columns'].items():
                    batch[col] = [col_values[i] for i in indexes]

            elif dist['type'] in ('sequential',):
                pass  # handled outside the field loop (id, date)

        all_batches.append(pd.DataFrame(batch))
        n_generated_rows += n

    return pd.concat(all_batches).reset_index(drop=True)
