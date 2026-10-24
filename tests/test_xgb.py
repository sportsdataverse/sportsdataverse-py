"""First tests reaching `sportsdataverse._xgb` (supp-013): the thread count every bundled Booster uses.

`xgb_threads()` is the default nthread for every Booster the processors build (`SDV_XGB_THREADS`, default 1:
measured 50 -> 16.5 user CPU-seconds per two CFB games, `_xgb.py` docstring) and `apply_thread_env()` sets
the OpenMP/BLAS pools only where the operator has not. The last test reads the nthread back from a real
bundled booster as the NFL processor loads it.
"""

from __future__ import annotations

import json

import pytest

from sportsdataverse import _xgb

_POOLS = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")


@pytest.mark.parametrize(("raw", "want"), [(None, 1), ("6", 6), ("1", 1), ("0", 1), ("-3", 1), ("four", 1), ("", 1)])
def test_xgb_threads_reads_the_env_and_never_goes_below_one(monkeypatch, raw, want):
    if raw is None:
        monkeypatch.delenv("SDV_XGB_THREADS", raising=False)
    else:
        monkeypatch.setenv("SDV_XGB_THREADS", raw)
    assert _xgb.xgb_threads() == want


def test_apply_thread_env_sets_only_the_unset_pools(monkeypatch):
    monkeypatch.setenv("SDV_XGB_THREADS", "3")
    monkeypatch.setenv("OMP_NUM_THREADS", "8")  # an operator's explicit choice wins
    monkeypatch.delenv("OPENBLAS_NUM_THREADS", raising=False)
    monkeypatch.delenv("MKL_NUM_THREADS", raising=False)
    _xgb.apply_thread_env()
    import os

    assert [os.environ[v] for v in _POOLS] == ["8", "3", "3"]


def test_the_nfl_processor_builds_its_boosters_with_xgb_threads():
    from sportsdataverse.nfl import nfl_pbp

    cfg = json.loads(nfl_pbp.qbr_model.save_config())
    assert int(cfg["learner"]["generic_param"]["nthread"]) == _xgb.xgb_threads()
    print("XGB_THREADS_OK", _xgb.xgb_threads())
