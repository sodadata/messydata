import numpy as np
import pandas as pd


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


def inject_invalid_category(df, rate, cat_cols, bad_value='INVALID'):
    df = df.copy()
    n_bad = int(len(df) * rate)
    for col in cat_cols:
        if col not in df.columns:
            continue
        idx = np.random.choice(df.index, size=n_bad, replace=False)
        df[col] = df[col].astype(object)
        df.loc[idx, col] = bad_value
    return df


def inject_invalid_date(df, rate, date_cols, bad_value='9999-99-99'):
    df = df.copy()
    n_bad = int(len(df) * rate)
    for col in date_cols:
        if col not in df.columns:
            continue
        df[col] = df[col].astype(str)
        idx = np.random.choice(df.index, size=n_bad, replace=False)
        df.loc[idx, col] = bad_value
    return df


def inject_anomalies(config, df):
    for anomaly in config['anomalies']:
        name = anomaly['name']
        prob = anomaly['distribution']['parameters']['prob']
        rate = anomaly['distribution']['parameters']['rate']
        cols = anomaly['columns']

        if np.random.random() > 1 - prob:
            if name == 'missing_values':
                df = inject_missing(df, rate)
            elif name == 'duplicate_values':
                df = inject_duplicates(df, rate)
            elif name == 'invalid_category':
                df = inject_invalid_category(df, rate, cat_cols=cols)
            elif name == 'invalid_date':
                df = inject_invalid_date(df, rate, date_cols=cols)

    return df
