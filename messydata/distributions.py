from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Annotated, Literal, Union

import numpy as np
from pydantic import BaseModel, Field


class Uniform(BaseModel):
    type: Literal["uniform"] = "uniform"
    min: float
    max: float

    def sample(self, n: int) -> np.ndarray:
        return np.random.uniform(self.min, self.max, size=n)


class Normal(BaseModel):
    type: Literal["normal"] = "normal"
    mean: float
    std: float

    def sample(self, n: int) -> np.ndarray:
        return np.random.normal(self.mean, self.std, size=n)


class Lognormal(BaseModel):
    type: Literal["lognormal"] = "lognormal"
    mu: float
    sigma: float

    def sample(self, n: int) -> np.ndarray:
        return np.random.lognormal(self.mu, self.sigma, size=n)


class Weibull(BaseModel):
    type: Literal["weibull"] = "weibull"
    a: float
    scale: float = 1.0

    def sample(self, n: int) -> np.ndarray:
        return self.scale * np.random.weibull(self.a, size=n)


class Exponential(BaseModel):
    type: Literal["exponential"] = "exponential"
    scale: float = 1.0

    def sample(self, n: int) -> np.ndarray:
        return np.random.exponential(self.scale, size=n)


class Beta(BaseModel):
    type: Literal["beta"] = "beta"
    a: float
    b: float

    def sample(self, n: int) -> np.ndarray:
        return np.random.beta(self.a, self.b, size=n)


class Gamma(BaseModel):
    type: Literal["gamma"] = "gamma"
    shape: float
    scale: float = 1.0

    def sample(self, n: int) -> np.ndarray:
        return np.random.gamma(self.shape, self.scale, size=n)


class Mixture(BaseModel):
    """Weighted mixture of continuous distributions."""

    type: Literal["mixture"] = "mixture"
    components: list[ContinuousDistribution]  # resolved after model_rebuild()
    weights: list[float]

    def sample(self, n: int) -> np.ndarray:
        counts = np.random.multinomial(n, self.weights)
        parts = [comp.sample(int(c)) for comp, c in zip(self.components, counts)]
        result = np.concatenate(parts)
        np.random.shuffle(result)
        return result


class WeightedChoice(BaseModel):
    type: Literal["weighted_choice"] = "weighted_choice"
    values: list
    weights: list[float]

    def sample(self, n: int) -> np.ndarray:
        return np.random.choice(self.values, p=self.weights, size=n)


class WeightedChoiceMapping(BaseModel):
    """Samples multiple correlated columns from a joint categorical distribution."""

    type: Literal["weighted_choice_mapping"] = "weighted_choice_mapping"
    columns: dict[str, list]
    weights: list[float]

    def sample(self, n: int) -> dict[str, list]:
        indexes = np.random.choice(len(self.weights), p=self.weights, size=n)
        return {col: [vals[i] for i in indexes] for col, vals in self.columns.items()}


class Sequential(BaseModel):
    """Auto-incrementing integer or date sequence."""

    type: Literal["sequential"] = "sequential"
    start: Union[int, str]
    step: int = 1

    def initial(self) -> Union[int, str]:
        return self.start

    def advance(self, current: Union[int, str, date]) -> Union[int, str]:
        if isinstance(current, int):
            return current + self.step
        # date or date string
        if isinstance(current, date):
            return current + timedelta(days=self.step)
        d = datetime.strptime(str(current), "%Y-%m-%d").date()
        return str(d + timedelta(days=self.step))


ContinuousDistribution = Annotated[
    Union[Uniform, Normal, Lognormal, Weibull, Exponential, Beta, Gamma, Mixture],
    Field(discriminator="type"),
]

Distribution = Annotated[
    Union[
        Uniform,
        Normal,
        Lognormal,
        Weibull,
        Exponential,
        Beta,
        Gamma,
        Mixture,
        WeightedChoice,
        WeightedChoiceMapping,
        Sequential,
    ],
    Field(discriminator="type"),
]

# Resolve the forward reference in Mixture.components
Mixture.model_rebuild()
