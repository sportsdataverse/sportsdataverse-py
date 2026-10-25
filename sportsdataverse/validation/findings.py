from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    import polars as pl


class Severity(str, Enum):
    """How serious a fired rule is: ``error`` fails a gate, ``warn`` needs judgment, ``info`` is coverage.

    Example:
        Compare by member, serialise by value::

            from sportsdataverse.validation.findings import Severity
            Severity("warn") is Severity.WARN, Severity.ERROR.value

    See Also:
        * :func:`~sportsdataverse.validation.pbp_invariants.run` -- the shipped check that records findings
        * :func:`~sportsdataverse.validation.validate_game` -- the per-game gate (its own ``Finding`` type)
    """

    ERROR = "error"
    WARN = "warn"
    INFO = "info"


@dataclass(frozen=True)
class Finding:
    """One result of an offline harness check (``tools/validation``) on one dataset.

    Attributes:
        check: the check that produced it (``"pbp_invariants"``, ``"boundary_leakage"``, ...).
        severity: its :class:`Severity`.
        domain: the league / domain (``"nfl"``, ``"cfb"``, ...).
        dataset: the dataset identifier.
        message: what broke, in one line.
        locator: where (rule, game, column, ...), as the check defines it.
        expected: the reference value, when the check has one.
        actual: the observed value, when the check has one.
        metric: the measured quantity (a violation count, a rate, a correlation).
        needs_judgment: True when a reviewer must decide whether it is real.
        sample: up to a handful of offending rows.

    Example:
        Record one and serialise it::

            from sportsdataverse.validation.findings import Finding, Severity
            f = Finding("pbp_invariants", Severity.ERROR, "nfl", "pbp_2024", "score.delta: 1/180 violate")
            f.to_dict()["severity"]  # "error"

    See Also:
        * :func:`~sportsdataverse.validation.pbp_invariants.run` -- the shipped check that records findings
        * :func:`~sportsdataverse.validation.validate_game` -- the per-game gate (its own ``Finding`` type)
    """

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
    """A reviewer's ruling on one :class:`Finding` (the harness triage output).

    Attributes:
        finding_ref: which finding it rules on.
        status: ``"confirmed"``, ``"dismissed"`` or ``"uncertain"``.
        confidence: 0-1.
        rationale: why.
        suggested_fix: a fix, when there is one.

    Example:
        Dismiss a benign drift::

            from sportsdataverse.validation.findings import Verdict
            Verdict("pbp_2024/score.delta", "dismissed", 0.9, "a scoring-change replay, not a parser bug")

    See Also:
        * :func:`~sportsdataverse.validation.pbp_invariants.run` -- the shipped check that records findings
        * :func:`~sportsdataverse.validation.validate_game` -- the per-game gate (its own ``Finding`` type)
    """

    finding_ref: str
    status: str  # "confirmed" | "dismissed" | "uncertain"
    confidence: float
    rationale: str
    suggested_fix: str | None = None


@runtime_checkable
class OracleLike(Protocol):
    """What a parity check needs from a reference implementation (an R or nflverse oracle).

    ``reference_frame(dataset, keys)`` returns the oracle's rows for those join keys (``None`` when it
    has none); ``column_map`` maps our column names to the oracle's and ``thresholds`` holds the
    per-column parity floors.

    Example:
        Any object with these members qualifies (a runtime-checkable Protocol)::

            from sportsdataverse.validation.findings import OracleLike
            isinstance(my_oracle, OracleLike)

    See Also:
        * :func:`~sportsdataverse.validation.pbp_invariants.run` -- the shipped check that records findings
        * :func:`~sportsdataverse.validation.validate_game` -- the per-game gate (its own ``Finding`` type)
    """

    domain: str
    column_map: dict[str, str]
    thresholds: dict[str, float]

    def reference_frame(self, dataset: str, keys: pl.DataFrame) -> pl.DataFrame | None: ...


@dataclass(frozen=True)
class CheckContext:
    """Everything a harness check may read about the dataset it runs on, beyond the frame itself.

    ``domain`` / ``dataset`` name it; ``schema`` and ``required_columns`` are its contract;
    ``join_keys``, ``prob_groups`` (columns that must sum to 1), ``range_constraints``,
    ``lag_columns`` / ``cumulative_columns`` (leakage checks, grouped by ``group_key``) and
    ``expected_constant_columns`` drive the individual checks; ``oracle``, ``prior_frame`` (the
    previous release) and ``thresholds`` feed the parity and drift checks. A check reads only the
    fields it needs.

    Example:
        The context :func:`~sportsdataverse.validation.pbp_invariants.run` needs (its league)::

            from sportsdataverse.validation.findings import CheckContext
            ctx = CheckContext(domain="nfl", dataset="pbp_2024", schema={})

    See Also:
        * :func:`~sportsdataverse.validation.pbp_invariants.run` -- the shipped check that records findings
        * :func:`~sportsdataverse.validation.validate_game` -- the per-game gate (its own ``Finding`` type)
    """

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
    expected_constant_columns: tuple[str, ...] = ()
