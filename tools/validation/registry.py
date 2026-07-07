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
    expected_constant_columns: tuple[str, ...] = ()


DATASETS: dict[str, DatasetSpec] = {}  # registered incrementally; see spec §11


@dataclass(frozen=True)
class LintTarget:
    """A source tree to lint: where it is and what language it is."""

    name: str
    path: str
    language: str  # "python" | "r"


LINT_TARGETS: dict[str, LintTarget] = {}  # registered incrementally (follow-up)

LINT_TARGETS["nfl_native_pbp"] = LintTarget(
    name="nfl_native_pbp",
    path="${SDV_VALIDATION_NFL_DATA_ROOT}/python/native_pbp",
    language="python",
)
LINT_TARGETS["sdv_nfl_ep_wp"] = LintTarget(
    name="sdv_nfl_ep_wp",
    path="sportsdataverse/nfl/ep_wp.py",
    language="python",
)
LINT_TARGETS["cfb_data_r"] = LintTarget(
    name="cfb_data_r",
    # cfb-data is the R producer of the public CFB datasets; SDV_VALIDATION_DATA_ROOT
    # is the cfb-data repo root, so its data-prep R lives under R/.
    path="${SDV_VALIDATION_DATA_ROOT}/R",
    language="r",
)


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
    if not isinstance(data, dict):
        raise ValueError(f"thresholds.yaml must be a YAML mapping at the top level; got {type(data).__name__!r}")
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
        oracle=ORACLES[spec.oracle_domain] if spec.oracle_domain else None,
        prior_frame=None,
        thresholds=load_thresholds(spec.domain),
        lag_columns=spec.lag_columns,
        cumulative_columns=spec.cumulative_columns,
        group_key=spec.group_key,
        expected_constant_columns=spec.expected_constant_columns,
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
    # season (partition id) + the build-metadata version stamps are constant by
    # design, not dead data — allowlist so constant_column adds no noise. `week`
    # is deliberately NOT listed: it is real play data (constant only in a
    # single-week local sample; it varies across a full published season).
    expected_constant_columns=(
        "season",
        "model_pbp_version",
        "cp_model_version",
        "ep_model_version",
        "wp_model_version",
    ),
)

DATASETS["nfl_model_pbp"] = DatasetSpec(
    name="nfl_model_pbp",
    domain="nfl",
    parquet_glob="${SDV_VALIDATION_NFL_DATA_ROOT}/out/model_pbp_*.parquet",
    schema=load_schema("nfl_model_pbp"),
    required_columns=("game_id", "play_id"),
    join_keys=("game_id", "play_id"),
    range_constraints={
        "wp": (0.0, 1.0),
        "vegas_wp": (0.0, 1.0),
        "cp": (0.0, 1.0),
        "ep": (-10.0, 10.0),
        "epa": (-15.0, 15.0),
    },
    oracle_domain="nfl",
    # season is a partition id, constant only when the glob resolves to one
    # season. nfl_model_pbp has no *_version stamp columns and its data root is
    # not locally auditable; a fuller allowlist can follow if the cron surfaces
    # a genuine build-metadata constant.
    expected_constant_columns=("season",),
)

_CFB_SEASON_STAT_CONST = ("season", "division")

DATASETS["cfb_passing"] = DatasetSpec(
    name="cfb_passing",
    domain="cfb",
    parquet_glob="${SDV_VALIDATION_DATA_ROOT}/cfb/passing/parquet/cfb_passing_*.parquet",
    schema=load_schema("cfb_passing"),
    join_keys=("player_id", "season"),
    oracle_domain="cfb",
    expected_constant_columns=_CFB_SEASON_STAT_CONST,
)

DATASETS["cfb_rushing"] = DatasetSpec(
    name="cfb_rushing",
    domain="cfb",
    parquet_glob="${SDV_VALIDATION_DATA_ROOT}/cfb/rushing/parquet/cfb_rushing_*.parquet",
    schema=load_schema("cfb_rushing"),
    join_keys=("player_id", "season"),
    oracle_domain="cfb",
    expected_constant_columns=_CFB_SEASON_STAT_CONST,
)

DATASETS["cfb_receiving"] = DatasetSpec(
    name="cfb_receiving",
    domain="cfb",
    parquet_glob="${SDV_VALIDATION_DATA_ROOT}/cfb/receiving/parquet/cfb_receiving_*.parquet",
    schema=load_schema("cfb_receiving"),
    join_keys=("player_id", "season"),
    oracle_domain="cfb",
    expected_constant_columns=_CFB_SEASON_STAT_CONST,
)

# cfb_percentiles schema has no "season" column (only "pctile" as a row-level
# quantile key); the plan's ("season", "pctile") join_keys are adjusted to
# ("pctile",) and the expected_constant_columns allowlist is empty.
DATASETS["cfb_percentiles"] = DatasetSpec(
    name="cfb_percentiles",
    domain="cfb",
    parquet_glob="${SDV_VALIDATION_DATA_ROOT}/cfb/percentiles/parquet/cfb_percentiles_*.parquet",
    schema=load_schema("cfb_percentiles"),
    join_keys=("pctile",),
    oracle_domain="cfb",
    expected_constant_columns=(),
)

# The passrate/rushrate/line_yards/opportunity_rate split cells are tautological
# (pass-rate among pass plays == 1.0, etc.) — legitimately constant, not dead.
_TEAM_SUMMARY_CONST = (
    "season",
    "division",
    "passrate_off_pass",
    "rushrate_off_pass",
    "line_yards_off_pass",
    "opportunity_rate_off_pass",
    "passrate_off_pass_rank",
    "rushrate_off_pass_rank",
    "opportunity_rate_off_pass_rank",
    "passrate_def_pass",
    "rushrate_def_pass",
    "line_yards_def_pass",
    "opportunity_rate_def_pass",
    "passrate_def_pass_rank",
    "rushrate_def_pass_rank",
    "opportunity_rate_def_pass_rank",
    "passrate_off_rush",
    "rushrate_off_rush",
    "passrate_off_rush_rank",
    "rushrate_off_rush_rank",
    "passrate_def_rush",
    "rushrate_def_rush",
    "passrate_def_rush_rank",
    "rushrate_def_rush_rank",
)

DATASETS["cfb_team_summaries"] = DatasetSpec(
    name="cfb_team_summaries",
    domain="cfb",
    parquet_glob="${SDV_VALIDATION_DATA_ROOT}/cfb/team_summaries/parquet/cfb_team_summaries_*.parquet",
    schema=load_schema("cfb_team_summaries"),
    join_keys=("team_id", "season"),
    oracle_domain="cfb",
    expected_constant_columns=_TEAM_SUMMARY_CONST,
)

# The crosswalk grain is person x team (no season column; a transferred player
# appears under several espn_team_ids), so (person_key, espn_team_id) is the key —
# person_key alone collides on 2088 multi-team players. 3 genuinely-duplicate
# person x team rows remain (a real crosswalk dedup gap for the cfb-data producer,
# surfaced by the sweep uniqueness check). yahoo_* are an unwired source
# (documented dead placeholders); fox_jersey is a real extraction gap (track a
# fixes it) and is DELIBERATELY left un-allowlisted so the harness keeps flagging it.
DATASETS["cfb_rosters_crosswalk"] = DatasetSpec(
    name="cfb_rosters_crosswalk",
    domain="cfb",
    parquet_glob="${SDV_VALIDATION_DATA_ROOT}/cfb/crosswalk/parquet/cfb_rosters_crosswalk.parquet",
    schema=load_schema("cfb_rosters_crosswalk"),
    join_keys=("person_key", "espn_team_id"),
    oracle_domain="cfb",
    expected_constant_columns=("yahoo_athlete_id", "yahoo_position"),
)

DATASETS["cfb_rb_eval"] = DatasetSpec(
    name="cfb_rb_eval",
    domain="cfb",
    parquet_glob="${SDV_VALIDATION_DATA_ROOT}/cfb/rb_eval/calibration.parquet",
    schema=load_schema("cfb_rb_eval"),
    oracle_domain="cfb",
)
