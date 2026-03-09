import numpy as np
import pandas as pd

from .schema import DatasetSchema


def inject_duplicates(df: pd.DataFrame, rate: float) -> pd.DataFrame:
    n_dup = int(len(df) * rate)
    dup_rows = df.sample(n=n_dup, replace=True)
    return pd.concat([df, dup_rows], ignore_index=True)


def inject_missing(df: pd.DataFrame, rate: float, cols: list[str] | None = None) -> pd.DataFrame:
    target_cols = [c for c in (cols or df.columns) if c in df.columns]
    n_cells = int(len(df) * len(target_cols) * rate)
    col_indices = [df.columns.get_loc(c) for c in target_cols]
    for _ in range(n_cells):
        i = np.random.randint(0, len(df))
        j = col_indices[np.random.randint(0, len(col_indices))]
        df.iat[i, j] = np.nan
    return df


def inject_invalid_category(
    df: pd.DataFrame, rate: float, cat_cols: list[str], bad_value: str = "INVALID"
) -> pd.DataFrame:
    df = df.copy()
    n_bad = int(len(df) * rate)
    for col in cat_cols:
        if col not in df.columns:
            continue
        idx = np.random.choice(df.index, size=min(n_bad, len(df)), replace=False)
        df[col] = df[col].astype(object)
        df.loc[idx, col] = bad_value
    return df


def inject_invalid_date(
    df: pd.DataFrame, rate: float, date_cols: list[str], bad_value: str = "9999-99-99"
) -> pd.DataFrame:
    df = df.copy()
    n_bad = int(len(df) * rate)
    for col in date_cols:
        if col not in df.columns:
            continue
        df[col] = df[col].astype(str)
        idx = np.random.choice(df.index, size=min(n_bad, len(df)), replace=False)
        df.loc[idx, col] = bad_value
    return df


def inject_outliers(df: pd.DataFrame, rate: float, cols: list[str], distribution) -> pd.DataFrame:
    df = df.copy()
    n_bad = int(len(df) * rate)
    for col in cols:
        if col not in df.columns:
            continue
        idx = np.random.choice(df.index, size=min(n_bad, len(df)), replace=False)
        values = distribution.sample(len(idx))
        df.loc[idx, col] = values.astype(df[col].dtype)
    return df


def inject_anomalies(schema: DatasetSchema, df: pd.DataFrame) -> pd.DataFrame:
    for anomaly in schema.anomalies:
        if np.random.random() > 1 - anomaly.prob:
            cols = anomaly.columns
            if anomaly.name == "missing_values":
                missing_cols = None if cols == "any" else cols
                df = inject_missing(df, anomaly.rate, cols=missing_cols)
            elif anomaly.name == "duplicate_values":
                df = inject_duplicates(df, anomaly.rate)
            elif anomaly.name == "invalid_category":
                if cols == "any":
                    cols = list(df.select_dtypes(include="object").columns)
                df = inject_invalid_category(df, anomaly.rate, cat_cols=cols)
            elif anomaly.name == "invalid_date":
                if cols == "any":
                    cols = [c for c in df.columns if "date" in c.lower()]
                df = inject_invalid_date(df, anomaly.rate, date_cols=cols)
            elif anomaly.name == "outliers":
                if cols == "any":
                    cols = list(df.select_dtypes(include="number").columns)
                df = inject_outliers(df, anomaly.rate, cols=cols, distribution=anomaly.distribution)
    return df
