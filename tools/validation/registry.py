from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
import yaml

from tools.validation.findings import CheckContext
from tools.validation.oracles import ORACLES

_THRESHOLDS_PATH = Path(__file__).parent / "thresholds.yaml"
_SCHEMAS_DIR = Path(__file__).parent / "schemas"


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
    lag_columns: tuple[str, ...] = ()
    cumulative_columns: tuple[str, ...] = ()
    group_key: str = "game_id"


DATASETS: dict[str, DatasetSpec] = {}  # registered incrementally; see spec §11


@dataclass(frozen=True)
class LintTarget:
    """A source tree to lint: where it is and what language it is."""

    name: str
    path: str
    language: str  # "python" | "r"


LINT_TARGETS: dict[str, LintTarget] = {}  # registered incrementally (follow-up)


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


def load_schema(name: str) -> dict[str, str]:
    """Load a column-dtype schema snapshot for a dataset.

    Reads ``tools/validation/schemas/<name>.json`` (relative to this module's
    directory) and returns its contents as a ``{column: dtype_str}`` dict.

    Args:
        name: Dataset name, e.g. ``"cfb_model_pbp"``.  The file
            ``schemas/<name>.json`` must exist alongside this module.

    Returns:
        A ``dict[str, str]`` mapping column name to its polars dtype string.

    Raises:
        FileNotFoundError: If the schema snapshot file does not exist.
    """
    path = _SCHEMAS_DIR / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"Schema snapshot not found: {path}. Run the schema-capture tool to generate it.")
    return json.loads(path.read_text())


def _resolve_spec(spec: DatasetSpec, release: str | None = None) -> tuple[pl.DataFrame, CheckContext]:
    """Resolve a DatasetSpec to its frame and check context.

    Reads the parquet glob (with ``$``-prefixed env-var expansion) and
    assembles the ``CheckContext`` the checks consume.

    Args:
        spec: The DatasetSpec to resolve.
        release: Optional release tag (reserved; not yet used — see spec §11).

    Returns:
        A ``(frame, CheckContext)`` tuple.

    Raises:
        FileNotFoundError: If the resolved parquet glob matches no files.
    """
    glob_expanded = os.path.expandvars(spec.parquet_glob)
    frame = pl.read_parquet(glob_expanded)
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
        lag_columns=spec.lag_columns,
        cumulative_columns=spec.cumulative_columns,
        group_key=spec.group_key,
    )
    return frame, ctx


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
    return _resolve_spec(DATASETS[dataset], release)


DATASETS["cfb_model_pbp"] = DatasetSpec(
    name="cfb_model_pbp",
    domain="cfb",
    parquet_glob="${SDV_VALIDATION_DATA_ROOT}/cfb/model_pbp/parquet/model_pbp_*.parquet",
    schema=load_schema("cfb_model_pbp"),
    required_columns=("game_id", "id"),
    join_keys=("game_id", "id"),
    range_constraints={
        "wp_before": (0.0, 1.0),
        "wp_after": (0.0, 1.0),
        "completion_prob": (0.0, 1.0),
        "ep_before": (-8.0, 8.0),
        "ep_after": (-8.0, 8.0),
    },
    oracle_domain="cfb",
    cumulative_columns=("game_play_number",),
)
