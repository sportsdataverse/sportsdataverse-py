"""Experiment ledger — persisted model-run records (WS2).

Every model retrain appends one row: config hash + git SHA + features +
metrics + baseline delta + calibration buckets + data fingerprint + release
tag. The ledger is the queryable promotions record (and the de-facto model
registry) behind every model-zoo release; ``beat_baseline=False`` releases
require a logged override note.

Two surfaces, per the program decision:

* :func:`log_run` -- local parquet append-log (one row per run).
* :func:`push_run` -- POST the run to the sdv-web platform ingest endpoint
  (``SDV_PLATFORM_INGEST_URL`` + bearer ``SDV_PLATFORM_INGEST_TOKEN``),
  surfaced on ``/platform/models`` + ``/platform/eval``. The transport is
  injectable so tests and offline runs never touch the network.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package;
retrain scripts import from here.
"""

from __future__ import annotations

import dataclasses
import datetime
import hashlib
import json
import os
import warnings
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import polars as pl

from sportsdataverse.modeling.integrity.publish_audit import _git_sha

INGEST_URL_ENV = "SDV_PLATFORM_INGEST_URL"
INGEST_TOKEN_ENV = "SDV_PLATFORM_INGEST_TOKEN"


@dataclasses.dataclass(frozen=True)
class ExperimentRun:
    """One model-training / evaluation run.

    Attributes:
        sport: League/sport slug (``nfl``, ``nba``, ...).
        model_name: Model identifier (``wp_spread``, ``xg_ev``, ...).
        metric: Evaluation metric name (lower is better).
        model_metric: Model score on ``metric``.
        baseline_name: Named baseline the model must beat (``vegas``,
            ``naive_fp``); empty when no baseline applies.
        baseline_metric: Baseline score on the SAME metric.
        beat_baseline: Whether the model beat the baseline (None = no
            baseline comparison ran).
        override_note: Non-empty to record a deliberate ship despite
            ``beat_baseline=False`` (the logged override).
        config: Arbitrary run configuration (hashed into ``config_hash``).
        features: Feature names the model consumed.
        depends_on: Upstream model names whose OUTPUTS feed this model
            (the model-on-model dependency graph — never leave implicit).
        calibration: Calibration-table rows (``calibration_table(...).to_dicts()``).
        data_fingerprint: Fingerprint digest of the training data asset
            (``file_sha256`` from the publish audit).
        release_tag: Release the resulting artifact ships under.
        notes: Free-form context.
    """

    sport: str
    model_name: str
    metric: str
    model_metric: float
    baseline_name: str = ""
    baseline_metric: Optional[float] = None
    beat_baseline: Optional[bool] = None
    override_note: str = ""
    config: Dict[str, Any] = dataclasses.field(default_factory=dict)
    features: Tuple[str, ...] = ()
    depends_on: Tuple[str, ...] = ()
    calibration: Optional[List[Dict[str, Any]]] = None
    data_fingerprint: Optional[str] = None
    release_tag: Optional[str] = None
    notes: str = ""

    @property
    def config_hash(self) -> str:
        """Deterministic 12-hex digest of the run configuration."""
        payload = json.dumps(self.config, sort_keys=True, default=str)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def run_row(run: ExperimentRun) -> Dict[str, Any]:
    """Flatten a run into one JSON-safe ledger row (adds git SHA + timestamp).

    Args:
        run: The run to flatten.

    Returns:
        A flat dict; nested fields (``config`` / ``features`` /
        ``depends_on`` / ``calibration``) are JSON-encoded strings.

    Example:
        Quick start::

            from sportsdataverse.modeling.eval.experiment_ledger import ExperimentRun, run_row
            row = run_row(ExperimentRun("nfl", "wp_spread", "brier", 0.181))
            print(row["config_hash"], row["git_sha"])
    """
    return {
        "sport": run.sport,
        "model_name": run.model_name,
        "metric": run.metric,
        "model_metric": float(run.model_metric),
        "baseline_name": run.baseline_name,
        "baseline_metric": None if run.baseline_metric is None else float(run.baseline_metric),
        "beat_baseline": run.beat_baseline,
        "override_note": run.override_note,
        "config_hash": run.config_hash,
        "config": json.dumps(run.config, sort_keys=True, default=str),
        "features": json.dumps(list(run.features)),
        "depends_on": json.dumps(list(run.depends_on)),
        "calibration": json.dumps(run.calibration) if run.calibration is not None else None,
        "data_fingerprint": run.data_fingerprint,
        "release_tag": run.release_tag,
        "notes": run.notes,
        "git_sha": _git_sha(),
        "logged_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


def log_run(ledger_path: Union[str, Path], run: ExperimentRun) -> pl.DataFrame:
    """Append one run to the parquet ledger append-log.

    Args:
        ledger_path: Ledger parquet path (created when absent).
        run: The run to append.

    Returns:
        The full ledger frame after the append.

    Example:
        Retrain close-out::

            from sportsdataverse.modeling.eval.experiment_ledger import ExperimentRun, log_run
            log_run("dev/experiment_ledger.parquet",
                    ExperimentRun("nfl", "wp_spread", "brier", 0.181,
                                  baseline_name="vegas", baseline_metric=0.185,
                                  beat_baseline=True, release_tag="nfl_model_artifacts"))
    """
    row = pl.DataFrame([run_row(run)])
    path = Path(ledger_path)
    if path.exists():
        ledger = pl.concat([pl.read_parquet(path), row], how="diagonal_relaxed")
    else:
        ledger = row
    path.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_parquet(path)
    return ledger


def push_run(
    run: ExperimentRun,
    *,
    url: Optional[str] = None,
    token: Optional[str] = None,
    poster: Optional[Callable[..., Any]] = None,
) -> bool:
    """POST one run to the sdv-web platform ingest endpoint.

    Resolution: explicit args win, then the ``SDV_PLATFORM_INGEST_URL`` /
    ``SDV_PLATFORM_INGEST_TOKEN`` env vars. With no URL configured this is a
    warning no-op (offline retrains still work; the parquet ledger remains
    the local record).

    Args:
        run: The run to push.
        url: Ingest endpoint; default from env.
        token: Bearer token; default from env.
        poster: Injectable transport with the ``requests.post`` signature
            (tests / offline). Defaults to ``requests.post``.

    Returns:
        True when the platform accepted the run (2xx), False otherwise.

    Example:
        Retrain close-out::

            from sportsdataverse.modeling.eval.experiment_ledger import push_run
            push_run(run)  # no-op warning unless SDV_PLATFORM_INGEST_URL is set
    """
    url = url or os.environ.get(INGEST_URL_ENV)
    token = token or os.environ.get(INGEST_TOKEN_ENV)
    if not url:
        warnings.warn(
            f"experiment ledger: {INGEST_URL_ENV} not set — run not pushed to the platform",
            stacklevel=2,
        )
        return False
    if poster is None:  # pragma: no cover - exercised via injection in tests
        import requests

        poster = requests.post
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        response = poster(url, json=run_row(run), headers=headers, timeout=30)
    except Exception as exc:  # noqa: BLE001 - a push failure must never kill a retrain
        warnings.warn(f"experiment ledger push failed: {exc}", stacklevel=2)
        return False
    ok = bool(getattr(response, "status_code", 599) < 300)
    if not ok:
        warnings.warn(
            f"experiment ledger push rejected: HTTP {getattr(response, 'status_code', '?')}",
            stacklevel=2,
        )
    return ok
