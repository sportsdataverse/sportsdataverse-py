"""Declarative per-dataset data contracts — expectations enforced at the wire.

A :class:`DataContract` states what a published dataset must look like:
per-column dtype / null-rate ceiling / value domain / numeric bounds, key
uniqueness, row floors (global and per partition, e.g. per season), all in a
plain JSON file that lives next to the producer. :func:`validate_frame`
checks a frame against it and splits findings into **blocking**
(completeness-class: missing/mistyped columns, duplicate keys, shrunk or
vanished partitions — the NBA nine-season silent-shortfall class) and
**warnings** (drift-class: null-rate creep, out-of-domain values — the
taxonomy-drift signal — and out-of-bounds numerics), mirroring the WS1 rule
that completeness blocks and drift warns.

Nobody hand-authors contracts: :func:`derive_contract` builds one from an
observed frame ("expectations from history") with slack, and the derived
contract always validates its own source frame cleanly. Producers block
before upload (``report.raise_if_blocking()`` — or thread the contract
through ``release.AuditSpec``); loaders warn after download.
"""

from __future__ import annotations

import dataclasses
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import polars as pl

from sportsdataverse.modeling.integrity.publish_audit import DEFAULT_SHRINK_TOLERANCE

#: Value domains are only recorded for columns at or under this cardinality.
DEFAULT_DOMAIN_MAX_CARD = 20
#: Slack added to an observed null rate when deriving a ceiling.
DEFAULT_NULL_SLACK = 0.05
#: Fractional widening of observed numeric bounds when deriving.
DEFAULT_BOUND_SLACK = 0.10


@dataclasses.dataclass(frozen=True)
class ColumnContract:
    """Expectations for one column.

    Attributes:
        dtype: Expected polars dtype string (``"Int64"``); None skips.
        required: Whether the column must exist (missing = blocking).
        null_rate_max: Null-fraction ceiling (over = warning).
        allowed_values: Closed value domain for low-cardinality columns;
            unseen values are the taxonomy-drift warning.
        min_value: Numeric floor (under = warning).
        max_value: Numeric ceiling (over = warning).
    """

    dtype: Optional[str] = None
    required: bool = True
    null_rate_max: Optional[float] = None
    allowed_values: Optional[List[Any]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None


@dataclasses.dataclass(frozen=True)
class DataContract:
    """The full expectation set for one published dataset.

    Attributes:
        name: Dataset name (manifest key / file stem).
        columns: Per-column expectations.
        key: Columns whose combination must be unique (duplicates block).
        partition_key: Partition column (season, game_id) for per-partition
            row floors; a partition present in the contract but absent from
            the frame is BLOCKING — a vanished season must never publish.
        min_rows: Global row floor (under = blocking).
        min_rows_per_partition: ``{str(partition value): floor}``.
    """

    name: str
    columns: Dict[str, ColumnContract]
    key: Optional[List[str]] = None
    partition_key: Optional[str] = None
    min_rows: Optional[int] = None
    min_rows_per_partition: Optional[Dict[str, int]] = None


@dataclasses.dataclass(frozen=True)
class ContractViolation:
    """One finding from :func:`validate_frame`.

    Attributes:
        kind: Violation kind (``missing_column`` / ``dtype_mismatch`` /
            ``duplicate_key`` / ``min_rows`` / ``missing_partition`` /
            ``partition_rows`` / ``unexpected_column`` / ``null_rate`` /
            ``domain`` / ``bounds``).
        column: The column (or partition) involved, when applicable.
        detail: Human-readable specifics.
        blocking: Completeness-class findings block; drift-class warn.
    """

    kind: str
    column: Optional[str]
    detail: str
    blocking: bool


@dataclasses.dataclass(frozen=True)
class ContractReport:
    """Outcome of validating one frame against one contract.

    Attributes:
        contract: The contract name.
        violations: Every finding, blocking and warning alike.
    """

    contract: str
    violations: List[ContractViolation]

    @property
    def blocking(self) -> List[ContractViolation]:
        """The completeness-class findings."""
        return [violation for violation in self.violations if violation.blocking]

    @property
    def warnings(self) -> List[ContractViolation]:
        """The drift-class findings."""
        return [violation for violation in self.violations if not violation.blocking]

    @property
    def ok(self) -> bool:
        """True when nothing blocks (warnings may still be present)."""
        return not self.blocking

    def to_frame(self) -> pl.DataFrame:
        """The violations as a frame (empty frame keeps the schema)."""
        schema = {"kind": pl.Utf8, "column": pl.Utf8, "detail": pl.Utf8, "blocking": pl.Boolean}
        if not self.violations:
            return pl.DataFrame(schema=schema)
        return pl.DataFrame([dataclasses.asdict(violation) for violation in self.violations], schema=schema)

    def raise_if_blocking(self) -> None:
        """Raise with every blocking finding (producers call this pre-upload).

        Raises:
            ValueError: Listing each blocking violation.
        """
        if self.blocking:
            lines = "\n  ".join(f"{v.kind}[{v.column}]: {v.detail}" for v in self.blocking)
            raise ValueError(f"data contract {self.contract!r} BLOCKED:\n  {lines}")


def validate_frame(df: pl.DataFrame, contract: DataContract, *, strict: bool = False) -> ContractReport:
    """Check a frame against a contract.

    Args:
        df: The frame about to be published (or just loaded).
        contract: The dataset's contract.
        strict: Upgrade every warning to blocking (loader-side stays False).

    Returns:
        The :class:`ContractReport`.

    Example:
        Producer-side gate::

            from sportsdataverse.modeling.integrity import read_contract, validate_frame
            report = validate_frame(frame, read_contract("pbp.contract.json"))
            report.raise_if_blocking()
    """
    found: List[ContractViolation] = []

    def _add(kind: str, column: Optional[str], detail: str, *, blocking: bool) -> None:
        found.append(ContractViolation(kind=kind, column=column, detail=detail, blocking=blocking or strict))

    for name, col in contract.columns.items():
        if name not in df.columns:
            if col.required:
                _add("missing_column", name, "required column absent", blocking=True)
            continue
        series = df.get_column(name)
        if col.dtype is not None and str(series.dtype) != col.dtype:
            _add("dtype_mismatch", name, f"expected {col.dtype}, got {series.dtype}", blocking=True)
            continue
        if col.null_rate_max is not None and df.height > 0:
            null_rate = series.null_count() / df.height
            if null_rate > col.null_rate_max:
                _add("null_rate", name, f"null rate {null_rate:.4f} > ceiling {col.null_rate_max:.4f}", blocking=False)
        if col.allowed_values is not None:
            allowed = set(col.allowed_values)
            unseen = sorted({v for v in series.drop_nulls().unique().to_list() if v not in allowed}, key=str)
            if unseen:
                _add("domain", name, f"unseen values (taxonomy drift?): {unseen[:10]}", blocking=False)
        if series.dtype.is_numeric() and df.height > 0 and series.null_count() < df.height:
            observed_min, observed_max = series.min(), series.max()
            if col.min_value is not None and float(observed_min) < col.min_value:
                _add("bounds", name, f"min {observed_min} < floor {col.min_value}", blocking=False)
            if col.max_value is not None and float(observed_max) > col.max_value:
                _add("bounds", name, f"max {observed_max} > ceiling {col.max_value}", blocking=False)
    for name in df.columns:
        if name not in contract.columns:
            _add("unexpected_column", name, "column not in contract (schema grew?)", blocking=False)

    if contract.key:
        missing_key = [k for k in contract.key if k not in df.columns]
        if not missing_key and df.height:
            dupes = df.height - df.select(contract.key).unique().height
            if dupes:
                _add("duplicate_key", ",".join(contract.key), f"{dupes} duplicate key rows", blocking=True)
    if contract.min_rows is not None and df.height < contract.min_rows:
        _add("min_rows", None, f"{df.height} rows < floor {contract.min_rows}", blocking=True)
    if contract.partition_key and contract.min_rows_per_partition:
        if contract.partition_key not in df.columns:
            _add("missing_column", contract.partition_key, "partition key absent", blocking=True)
        else:
            counts = {
                str(part): int(n)
                for part, n in df.group_by(contract.partition_key).agg(pl.len().alias("n")).iter_rows()
            }
            for part, floor in contract.min_rows_per_partition.items():
                if part not in counts:
                    _add("missing_partition", part, "partition vanished from the frame", blocking=True)
                elif counts[part] < floor:
                    _add("partition_rows", part, f"{counts[part]} rows < floor {floor}", blocking=True)
    return ContractReport(contract=contract.name, violations=found)


def derive_contract(
    df: pl.DataFrame,
    *,
    name: str,
    key: Optional[List[str]] = None,
    partition_key: Optional[str] = None,
    tolerance: float = DEFAULT_SHRINK_TOLERANCE,
    domain_max_card: int = DEFAULT_DOMAIN_MAX_CARD,
    null_slack: float = DEFAULT_NULL_SLACK,
    bound_slack: float = DEFAULT_BOUND_SLACK,
) -> DataContract:
    """Expectations from history: derive a contract from an observed frame.

    The derived contract always validates its own source frame cleanly —
    slack is built into every ceiling/floor so routine variation passes and
    only genuine regressions fire.

    Args:
        df: The observed (e.g. prior-release) frame.
        name: Dataset name.
        key: Uniqueness key to enforce (only recorded if currently unique).
        partition_key: Partition column for per-partition row floors.
        tolerance: Fractional row-shrink allowance for the floors.
        domain_max_card: Max cardinality for recording a value domain.
        null_slack: Slack added to observed null rates.
        bound_slack: Fractional widening of observed numeric bounds.

    Returns:
        The derived :class:`DataContract`.

    Raises:
        ValueError: On an empty frame (there is no history to derive from).

    Example:
        Quick start::

            from sportsdataverse.modeling.integrity import derive_contract, write_contract
            contract = derive_contract(prior, name="nba_pbp", key=["game_id", "event_id"],
                                       partition_key="season")
            write_contract("nba_pbp.contract.json", contract)
    """
    if df.height == 0:
        raise ValueError("cannot derive a contract from an empty frame")
    columns: Dict[str, ColumnContract] = {}
    for column in df.columns:
        series = df.get_column(column)
        null_rate_max = min(1.0, series.null_count() / df.height + null_slack)
        allowed: Optional[List[Any]] = None
        min_value = max_value = None
        if series.dtype.is_numeric() and series.null_count() < df.height:
            lo, hi = float(series.min()), float(series.max())
            pad = (hi - lo) * bound_slack or max(1.0, abs(hi) * bound_slack)
            min_value, max_value = lo - pad, hi + pad
        elif series.dtype in (pl.Utf8, pl.Boolean, pl.Categorical):
            uniques = series.drop_nulls().unique().to_list()
            if 0 < len(uniques) <= domain_max_card:
                allowed = sorted(uniques, key=str)
        columns[column] = ColumnContract(
            dtype=str(series.dtype),
            null_rate_max=round(null_rate_max, 6),
            allowed_values=allowed,
            min_value=min_value,
            max_value=max_value,
        )
    unique_key = None
    if key and all(k in df.columns for k in key):
        if df.select(key).unique().height == df.height:
            unique_key = list(key)
    per_partition = None
    if partition_key and partition_key in df.columns:
        per_partition = {
            str(part): max(1, math.floor(int(n) * (1.0 - tolerance)))
            for part, n in df.group_by(partition_key).agg(pl.len().alias("n")).iter_rows()
        }
    return DataContract(
        name=name,
        columns=columns,
        key=unique_key,
        partition_key=partition_key if per_partition else None,
        min_rows=max(1, math.floor(df.height * (1.0 - tolerance))),
        min_rows_per_partition=per_partition,
    )


def write_contract(path: Union[str, Path], contract: DataContract) -> None:
    """Persist a contract as deterministic, diff-friendly JSON (no timestamps).

    Args:
        path: Destination ``.json`` path.
        contract: The contract to persist.

    Example:
        Quick start::

            write_contract("nba_pbp.contract.json", contract)
    """
    payload = dataclasses.asdict(contract)
    Path(path).write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def read_contract(path: Union[str, Path]) -> DataContract:
    """Read a contract back from JSON.

    Args:
        path: The contract path.

    Returns:
        The :class:`DataContract`.

    Example:
        Quick start::

            contract = read_contract("nba_pbp.contract.json")
    """
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    columns = {name: ColumnContract(**entry) for name, entry in raw.pop("columns").items()}
    return DataContract(columns=columns, **raw)
