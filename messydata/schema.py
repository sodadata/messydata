from __future__ import annotations

from typing import Literal, Optional, Union

import yaml
from pydantic import BaseModel

from .distributions import ContinuousDistribution, Distribution


class FieldSpec(BaseModel):
    name: str
    dtype: str = "object"
    distribution: Distribution
    unique_per_id: bool = False
    nullable: bool = True


class AnomalySpec(BaseModel):
    name: Literal[
        "missing_values", "duplicate_values", "invalid_category", "invalid_date", "outliers"
    ]
    prob: float
    rate: float
    columns: Union[Literal["any"], list[str]] = "any"
    distribution: Optional[Distribution] = None  # used by outliers


class DatasetSchema(BaseModel):
    name: str
    primary_key: str = "id"
    records_per_primary_key: ContinuousDistribution
    fields: list[FieldSpec]
    anomalies: list[AnomalySpec] = []

    @classmethod
    def from_yaml(cls, path: str) -> DatasetSchema:
        with open(path, "r") as f:
            raw = yaml.safe_load(f)
        # Strip legacy use_case wrapper if present
        if "use_case" in raw:
            raw = raw["use_case"]
        return cls.model_validate(raw)
