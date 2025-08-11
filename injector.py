import pandas as pd
import numpy as np

def load_data(path):
    data = pd.read_csv(path)
    return data

def inject_duplicates(df):
    rate = np.random.random()
    n_dup = int(len(df) * rate)
    dup_rows = df.sample(n=n_dup, replace=True)
    return pd.concat([df, dup_rows], ignore_index=True)


def inject_missing(df):
    rate = np.random.random()
    n_cells = int(df.size * rate)
    for _ in range(n_cells):
        i = np.random.randint(0, len(df))
        j = np.random.randint(0, len(df.columns))
        df.iat[i, j] = np.nan
    return df


def inject_invalid_category(df, category_cols=[], bad_value='Z'):
    rate = np.random.random()
    for category_col in category_cols:
        n_bad = int(len(df) * rate)
        idx = np.random.choice(df.index, n_bad, replace=False)
        df.loc[idx, category_col] = bad_value
    return df



def inject_anomalies(config, df):
    duplicates_config = config['anomalies']['duplicate_values']
    missing_config = config['anomalies']['missing_values']
    invalid_cat_config = config['anomalies']['invalid_category']

    duplicates_prob = duplicates_config['distribution']['parameters']['prob']
    missing_prob = missing_config['distribution']['parameters']['prob']
    invalid_cat_prob = invalid_cat_config['distribution']['parameters']['prob']

    duplicates_rate = duplicates_config['distribution']['parameters']['rate']
    missing_rate = missing_config['distribution']['parameters']['rate']
    invalid_cat_rate = invalid_cat_config['distribution']['parameters']['rate']

    if np.random.random() >= 1 -duplicates_prob:
        df = inject_duplicates(df, duplicates_rate)
    if np.random.random() >= 1 - missing_prob:
        df = inject_missing(df, missing_rate)
    if np.random.random() >= 1 - invalid_cat_prob:
        df = inject_invalid_category(df, invalid_cat_rate)

PATH = "data/data.csv"
data = load_data(PATH)

print(data.shape)
bad_data = inject_missing(data, 0.1)
print(bad_data.head())