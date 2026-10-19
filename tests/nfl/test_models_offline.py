"""Offline model availability for `enrich_nfl_pbp` (C-M15; ledger gate offline-model-tests).

The wheel does not ship three of the boosters the default enrichment uses: `xyac_model.ubj` (xYAC) and the
nfl4th `fd_model.ubj` / `wp_model.ubj` (fourth-down step); they download on first use. Offline with an
empty cache, the owner's ruling is WARN (one warning per missing artifact) plus a strict mode that RAISES
(task 4.6: `strict=True` / `SDV_PY_MODELS_STRICT=1`, which the producer cron turns on). Before 4.6 xYAC warned
once but the fourth-down step emitted nothing and returned all-null `go_wp` / `fg_wp` / `punt_wp` (the audit
measured 0/163 on 2012_01_ATL_KC), and there was no strict mode.

Input: one real nflverse game (`tests/nfl/fixtures/nflverse/2022_01_BUF_LA_full.parquet`, every column of
the nflverse-data `play_by_play_2022` release rows for that game; README there). The model cache is an
empty temp dir and `dl_utils.download` raises, so no test here can reach the network or the user's cache.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.dl_utils as dl_utils
from sportsdataverse.nfl import ep_wp, nfl_fourth_down
from sportsdataverse.nfl.config import get_config

GAME = Path(__file__).resolve().parent / "fixtures" / "nflverse" / "2022_01_BUF_LA_full.parquet"
DOWNLOADED = ("xyac_model.ubj", "fd_model.ubj", "wp_model.ubj")
# Upstream model outputs the release frame already carries; dropped so every value below is ours.
_MODEL_OUTPUTS = (
    "xyac_epa",
    "xyac_mean_yardage",
    "xyac_median_yardage",
    "xyac_success",
    "xyac_fd",
    "go_wp",
    "fg_wp",
    "punt_wp",
    "go_boost",
    "first_down_prob",
    "wp_succeed",
    "wp_fail",
)


@pytest.fixture
def offline(monkeypatch, tmp_path):
    """Empty model cache + a download that raises (records the URLs it was asked for)."""
    calls: list[str] = []

    def _no_network(url, *args, **kwargs):
        calls.append(url)
        raise ConnectionError(f"offline test: refused {url}")

    monkeypatch.setattr(get_config(), "cache_dir", tmp_path)
    monkeypatch.setattr(dl_utils, "download", _no_network)
    monkeypatch.setattr(get_config(), "verbose", False)
    monkeypatch.setattr(nfl_fourth_down, "_WARNED_MISSING", set())
    monkeypatch.delenv("SDV_PY_MODELS_STRICT", raising=False)
    ep_wp._load_model.cache_clear()
    yield calls
    ep_wp._load_model.cache_clear()


def _game() -> pl.DataFrame:
    df = pl.read_parquet(GAME)
    return df.drop([c for c in _MODEL_OUTPUTS if c in df.columns])


def _enrich_recording(df: pl.DataFrame):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = ep_wp.enrich_nfl_pbp(df)
    return out, [str(w.message) for w in caught]


def _naming(messages: list[str], artifact: str) -> list[str]:
    return [m for m in messages if artifact in m]


def test_offline_xyac_warns_once_and_no_model_is_reached(offline):
    out, messages = _enrich_recording(_game())
    assert set(offline) and all(u.endswith(DOWNLOADED) for u in offline), offline
    assert any(u.endswith("xyac_model.ubj") for u in offline)
    assert len(_naming(messages, "xyac_model.ubj")) == 1, messages
    fourth = out.filter(pl.col("down") == 4)
    assert fourth.height == 8
    # The documented degrade path: the decision columns exist and are null, never a crash.
    assert fourth["go_wp"].null_count() == fourth.height
    print("OFFLINE_XYAC_WARNS_ONCE_OK", len(offline), "download attempts refused")


@pytest.mark.parametrize("artifact", ["fd_model.ubj", "wp_model.ubj"])
def test_offline_fourth_down_warns_once_per_missing_model(offline, artifact):
    _, messages = _enrich_recording(_game())
    assert len(_naming(messages, artifact)) == 1, f"{artifact}: {messages}"


@pytest.mark.parametrize("how", ["env", "kwarg"])
def test_offline_strict_mode_raises(offline, monkeypatch, how):
    if how == "env":
        monkeypatch.setenv("SDV_PY_MODELS_STRICT", "1")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            ep_wp.enrich_nfl_pbp(_game(), **({"strict": True} if how == "kwarg" else {}))
    except Exception as exc:  # noqa: BLE001 - any raise naming the missing model satisfies the contract
        assert any(a in str(exc) for a in DOWNLOADED), exc
        return
    raise AssertionError("strict mode returned a frame with a missing model")


@pytest.mark.parametrize("artifact", ["fd_model.ubj", "wp_model.ubj"])
def test_offline_strict_fourth_down_names_its_model(offline, monkeypatch, artifact):
    """With xYAC present, strict mode still refuses the fourth-down step, naming the nfl4th model."""
    monkeypatch.setattr(ep_wp, "calculate_xyac", lambda raw, models_dir=None: raw)
    calls_before = list(offline)
    with pytest.raises(FileNotFoundError, match="fd_model.ubj"):
        ep_wp.enrich_nfl_pbp(_game(), strict=True)
    assert any(u.endswith("fd_model.ubj") for u in offline[len(calls_before) :])
    # the ESPN path and direct get_4th_down_probs callers read the env flag through _try_load
    monkeypatch.setenv("SDV_PY_MODELS_STRICT", "1")
    with pytest.raises(FileNotFoundError, match=artifact):
        nfl_fourth_down._try_load(artifact)


def test_offline_strict_xyac_names_its_model(offline):
    """Strict mode refuses the xYAC step on its own (the fourth-down step off)."""
    with pytest.raises(FileNotFoundError, match="xyac_model.ubj"):
        ep_wp.enrich_nfl_pbp(_game(), strict=True, add_fourth_down=False)


ESPN_SUMMARY = Path(__file__).resolve().parent / "fixtures" / "summary_401872922.json"


def _espn_process(monkeypatch):
    """The ESPN path (``NFLPlayProcess``) on a real committed summary, every request refused."""
    import json

    import sportsdataverse.nfl.nfl_pbp as nfl_pbp

    def _no_network(*a, **k):
        raise AssertionError("network call on the offline ESPN path")

    monkeypatch.setattr(nfl_pbp, "download", _no_network)
    proc = nfl_pbp.NFLPlayProcess(gameId=401872922, join_participants=False)
    proc.espn_nfl_pbp(summary=json.loads(ESPN_SUMMARY.read_text(encoding="utf-8")))
    return proc


def test_espn_path_strict_env_raises_on_missing_fourth_down_model(offline, monkeypatch):
    """SDV_PY_MODELS_STRICT=1 reaches NFLPlayProcess: its fourth-down step re-raises instead of nulling."""
    proc = _espn_process(monkeypatch)
    monkeypatch.setenv("SDV_PY_MODELS_STRICT", "1")
    with pytest.raises(FileNotFoundError, match="fd_model.ubj"):
        proc.run_processing_pipeline()


def test_espn_path_strict_env_raises_on_missing_xpass_model(offline, monkeypatch):
    """The bundled xpass booster cannot go missing in the wheel, so its absence is simulated at the call."""
    import sportsdataverse.nfl.nfl_pbp as nfl_pbp

    def _missing(*a, **k):
        raise FileNotFoundError("NFL model 'xpass_model.ubj' not found")

    proc = _espn_process(monkeypatch)
    monkeypatch.setattr(nfl_pbp, "calculate_xpass", _missing)
    monkeypatch.setenv("SDV_PY_MODELS_STRICT", "1")
    with pytest.raises(FileNotFoundError, match="xpass_model.ubj"):
        proc.run_processing_pipeline()
