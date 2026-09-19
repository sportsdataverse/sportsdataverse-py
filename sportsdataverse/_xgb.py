"""One place for the xgboost thread count sdv-py's predictors use.

Every ``Booster`` the processors load used to be built with ``nthread=4``, and a
``Booster()`` built with no parameter takes every core. Inside a multi-worker
server (Game on Paper runs four gunicorn workers) that oversubscribes the host:
each request's predictions fan out across all cores at once, and the workers
contend. Measured on 2026-09-19 (CFB game 401858455 + 401858226, current main):
50 user CPU-seconds per two games with the default, 16.5 with one thread, at the
same wall time -- the per-game work is small enough that threading buys nothing
and costs a saturated host.

``SDV_XGB_THREADS`` overrides the default of 1 for batch jobs that want more.
"""

from __future__ import annotations

import os


def apply_thread_env() -> None:
    """Default the native thread pools to ``xgb_threads()`` unless the process already set them.

    xgboost 3's ``predict`` sizes its work by the OpenMP pool, not by the Booster's
    ``nthread`` param, so the env is the lever that actually works (measured: 50 -> 16.5
    user CPU-seconds per two games). ``setdefault`` only -- an operator's explicit
    ``OMP_NUM_THREADS`` always wins. Must run before xgboost / numpy import.
    """
    n = str(xgb_threads())
    for var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
        os.environ.setdefault(var, n)


def xgb_threads() -> int:
    """Thread count for every xgboost Booster sdv-py builds (env ``SDV_XGB_THREADS``, default 1)."""
    raw = os.environ.get("SDV_XGB_THREADS", "1")
    try:
        n = int(raw)
    except ValueError:
        return 1
    return n if n >= 1 else 1
