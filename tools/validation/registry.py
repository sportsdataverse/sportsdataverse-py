from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
import yaml

from tools.validation.findings import CheckContext
from tools.validation.oracles import ORACLES

_THRESHOLDS_PATH = Path(__file__).parent / "thresholds.yaml"


@dataclass(frozen=True)
class DatasetSpec:
    """Static contract for one validatable dataset."""

    name: str
    domain: str
    parquet_glob: str
    schema: dict[str, str]
    required_columns: tuple[str, ...] = ()
    join_keys: tuple[str, ...] = ()
    prob_groups: tuple[tuple[str, ...], ...] = ()
    range_constraints: dict[str, tuple[float, float]] = field(default_factory=dict)
    oracle_domain: str | None = None


DATASETS: dict[str, DatasetSpec] = {}  # registered incrementally; see spec §11


def load_thresholds(domain: str) -> dict[str, float]:
    """Load merged validation thresholds for a domain.

    Reads ``thresholds.yaml`` and overlays the domain-specific section on the
    ``default`` section.

    Args:
        domain: Domain key (e.g. "nfl", "cfb").

    Returns:
        A dict of threshold name -> float, domain values overriding defaults.
    """
    data = yaml.safe_load(_THRESHOLDS_PATH.read_text()) or {}
    merged = dict(data.get("default", {}))
    merged.update(data.get(domain, {}) or {})
    return merged


def resolve(dataset: str, release: str | None = None) -> tuple[pl.DataFrame, CheckContext]:
    """Resolve a registered dataset to its frame and check context.

    Looks the dataset up in ``DATASETS``, reads its parquet, and assembles the
    CheckContext the checks consume.

    Args:
        dataset: Registered dataset name (key of ``DATASETS``).
        release: Optional release tag (reserved; not yet used — see spec §11).

    Returns:
        A ``(frame, CheckContext)`` tuple.

    Raises:
        KeyError: If ``dataset`` is not registered in ``DATASETS``.
    """
    spec = DATASETS[dataset]
    frame = pl.read_parquet(spec.parquet_glob)  # release-tag resolution = spec §11 follow-up
    ctx = CheckContext(
        domain=spec.domain,
        dataset=spec.name,
        schema=spec.schema,
        required_columns=spec.required_columns,
        join_keys=spec.join_keys,
        prob_groups=spec.prob_groups,
        range_constraints=dict(spec.range_constraints),
        oracle=ORACLES.get(spec.oracle_domain) if spec.oracle_domain else None,
        prior_frame=None,
        thresholds=load_thresholds(spec.domain),
    )
    return frame, ctx
