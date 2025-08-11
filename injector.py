import yaml
import pandas as pd
import numpy as np

def load_config(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)['use_case']

def load_data(path):
    data = pd.read_csv(path)
    return data

def inject_duplicates(df, rate):
    n_dup = int(len(df) * rate)
    dup_rows = df.sample(n=n_dup, replace=True)
    return pd.concat([df, dup_rows], ignore_index=True)


def inject_missing(df, rate):
    n_cells = int(df.size * rate)
    for _ in range(n_cells):
        i = np.random.randint(0, len(df))
        j = np.random.randint(0, len(df.columns))
        df.iat[i, j] = np.nan
    return df


def inject_invalid_category(df, rate, cat_cols=[], bad_value='Z'):
    return df
    # TODO: rewrite this to make it make sense
    for category_col in cat_cols:
        n_bad = int(len(df) * rate)
        idx = np.random.choice(df.index, n_bad, replace=False)
        df.loc[idx, category_col] = bad_value
    return df



def inject_anomalies(config, df):
    for anomaly in config['anomalies']:
        name = anomaly['name']
        print(name)
        prob = anomaly['distribution']['parameters']['prob']
        rate = anomaly['distribution']['parameters']['rate']
        cols = anomaly['columns']
        
        if name == 'missing_values':
            if np.random.random() >= 1 - prob:
                df = inject_missing(df, rate)
        elif name == 'duplicate_values':
            if np.random.random() >= 1 - prob:
                df = inject_duplicates(df, rate)
        elif name == 'invalid_category':
            if np.random.random() >= 1 - prob:
                df = inject_invalid_category(df, cols, rate)
        elif name == 'invalid_date':
            pass
    return df


USE_CASE_CONFIG_PATH = 'use_case_config.yaml'
config = load_config(USE_CASE_CONFIG_PATH)
PATH = "data/data.csv"
data = load_data(PATH)

print(data.shape)
bad_data = inject_anomalies(config, data)
print(bad_data.head())