from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    import polars as pl


class Severity(str, Enum):
    ERROR = "error"
    WARN = "warn"
    INFO = "info"


@dataclass(frozen=True)
class Finding:
    check: str
    severity: Severity
    domain: str
    dataset: str
    message: str
    locator: dict[str, Any] = field(default_factory=dict)
    expected: Any = None
    actual: Any = None
    metric: float | None = None
    needs_judgment: bool = False
    sample: list[dict[str, Any]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "check": self.check,
            "severity": self.severity.value,
            "domain": self.domain,
            "dataset": self.dataset,
            "message": self.message,
            "locator": self.locator,
            "expected": self.expected,
            "actual": self.actual,
            "metric": self.metric,
            "needs_judgment": self.needs_judgment,
            "sample": self.sample,
        }


@dataclass(frozen=True)
class Verdict:
    finding_ref: str
    status: str  # "confirmed" | "dismissed" | "uncertain"
    confidence: float
    rationale: str
    suggested_fix: str | None = None


@runtime_checkable
class OracleLike(Protocol):
    domain: str
    column_map: dict[str, str]
    thresholds: dict[str, float]

    def reference_frame(self, dataset: str, keys: pl.DataFrame) -> pl.DataFrame | None: ...


@dataclass(frozen=True)
class CheckContext:
    domain: str
    dataset: str
    schema: dict[str, str]
    required_columns: tuple[str, ...] = ()
    join_keys: tuple[str, ...] = ()
    prob_groups: tuple[tuple[str, ...], ...] = ()
    range_constraints: dict[str, tuple[float, float]] = field(default_factory=dict)
    oracle: OracleLike | None = None
    prior_frame: pl.DataFrame | None = None
    thresholds: dict[str, float] = field(default_factory=dict)
    lag_columns: tuple[str, ...] = ()
    cumulative_columns: tuple[str, ...] = ()
    group_key: str = "game_id"
