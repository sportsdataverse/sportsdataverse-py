"""WNBA EPV (BB-D24) on real 2025 games (the holdout), its dated scoring level, and its gates.

Fixtures: ``tests/fixtures/bb_epv/wnba_{pbp,possessions,game_lineups,schedule}_2025.parquet``, the first
20 regular-season games of the ``wnba_stats_*`` releases (README beside them). The gate tests read the
committed fit report (``tools/models/nba_epv/epv_wnba_holdout.json``) and the bundled booster.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
from functools import lru_cache
from typing import Any

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_epv import (
    EPV_FEATURES,
    add_epv,
    epv_level,
    epv_lineup_games,
    epv_player_games,
    epv_states,
    epv_team_games,
    load_epv_model,
)
from sportsdataverse.wnba import wnba_epv

ROOT = pathlib.Path(__file__).resolve().parents[2]
D = ROOT / "tests" / "fixtures" / "bb_epv"
REPORT = ROOT / "tools" / "models" / "nba_epv" / "epv_wnba_holdout.json"
CARD = ROOT / "sportsdataverse" / "nba" / "models" / "wnba_epv.card.json"
HOLDOUT = 2025
KEY = ["game_id", "possession_number"]


@lru_cache(maxsize=1)
def _train_epv() -> Any:
    spec = importlib.util.spec_from_file_location("train_epv", ROOT / "tools" / "models" / "nba_epv" / "train_epv.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _f(name: str) -> pl.DataFrame:
    return pl.read_parquet(D / f"wnba_{name}_{HOLDOUT}.parquet")


@lru_cache(maxsize=1)
def _states() -> pl.DataFrame:
    return epv_states(_f("possessions"), _f("pbp"))


def _order() -> pl.DataFrame:
    """The games' dates, parsed from the schedule release's own rows."""
    return wnba_epv._game_order(_f("schedule"))


@lru_cache(maxsize=1)
def _plays() -> pl.DataFrame:
    return add_epv(_states(), league_id="10", game_order=_order())


def test_labels_sum_to_the_engine_points() -> None:
    poss = _f("possessions")
    got = _states().filter(pl.col("step") == 0).select(*KEY, "remaining_points")
    j = got.join(poss.select(*KEY, "points"), on=KEY)
    assert j.height == poss.height and (j["remaining_points"] == j["points"]).all()


def test_epv_added_telescopes_and_teams_reconcile() -> None:
    p = _plays()
    r = p.group_by(KEY).agg(
        (
            pl.col("epv_added").sum()
            - pl.col("points").sum()
            + pl.col("epv_start").first()
            - pl.col("epv_after").filter(pl.col("is_last")).first()
        ).abs()
    )
    assert r["epv_added"].max() < 1e-9
    t = epv_team_games(p)
    assert t.height == 2 * p["game_id"].n_unique()
    assert t.group_by("game_id").agg(pl.col("net_epv").sum().abs())["net_epv"].max() < 1e-9


def test_the_level_reads_the_schedule_dates() -> None:
    """Same-day games share a level and never see each other; the first date is the training level."""
    booster = load_epv_model("10")
    order = _order()
    assert order.height == _f("schedule").height and order.schema["game_order"] == pl.Date
    lv = epv_level(_states(), booster, order).join(order, on="game_id")
    by_day = lv.group_by("game_order").agg(pl.col("level_scale").n_unique())
    assert (by_day["level_scale"] == 1).all()
    assert (lv.filter(pl.col("game_order") == lv["game_order"].min())["level_scale"] == 1.0).all()
    # a hand walk over the possessions of strictly earlier dates
    ppp, k = float(booster.attr("level_ppp")), float(booster.attr("level_k"))
    starts = _states().filter(pl.col("step") == 0).group_by("game_id").agg(pl.col("remaining_points").sum(), pl.len())
    per = {g: (float(p), n) for g, p, n in starts.iter_rows()}
    date = dict(order.iter_rows())
    for g, scale in lv.select("game_id", "level_scale").iter_rows():
        before = [h for h in per if date[h] < date[g]]
        want = (k * ppp + sum(per[h][0] for h in before)) / (k + sum(per[h][1] for h in before)) / ppp
        assert abs(scale - want) < 1e-12
    assert lv["level_scale"].n_unique() > 1
    with pytest.raises(ValueError, match="needs game_order"):
        epv_level(_states(), booster)  # no ordering by game_id


def test_the_level_scales_every_value() -> None:
    booster = load_epv_model("10")
    s = _states().filter(pl.col("step") > 0).sort("game_id", "possession_number", "step")
    raw = booster.inplace_predict(s.select(pl.col(EPV_FEATURES).cast(pl.Float64)).to_numpy())
    lv = s.select("game_id").join(
        epv_level(_states(), booster, _order()), on="game_id", how="left", maintain_order="left"
    )
    p = _plays().sort("game_id", "possession_number", "step")
    assert np.allclose(p["epv"].to_numpy(), raw * lv["level_scale"].to_numpy())


def test_the_booster_is_out_of_sample_and_calibrated() -> None:
    booster = load_epv_model("10")
    train = {int(x) for x in booster.attr("train_seasons").split(",")}
    held = {int(booster.attr("holdout_season")), int(booster.attr("confirmation_season"))}
    assert not train & held and int(booster.attr("holdout_season")) == HOLDOUT
    p = _plays()
    y = p["remaining_points"].to_numpy()
    const = float(booster.attr("train_label_mean"))
    skill = 1 - np.mean((p["epv"].to_numpy() - y) ** 2) / np.mean((const - y) ** 2)
    assert skill > 0.20  # observed 0.24 on the 2025 season (the report); these 20 games likewise
    g = (
        p.with_columns((pl.col("epv") - pl.col("remaining_points")).alias("e"))
        .group_by("game_id")
        .agg(pl.col("e").sum(), pl.len().alias("n"))
    )
    e, n = g["e"].to_numpy(), g["n"].to_numpy()
    bias = e.sum() / n.sum()
    se = np.sqrt(np.sum((e - bias * n) ** 2) / (len(n) * (len(n) - 1))) / n.mean()
    assert abs(bias) < 3 * se


def _report() -> dict[str, Any]:
    return json.loads(REPORT.read_text(encoding="utf-8"))


def test_gate_two_is_the_wnba_drift_rule() -> None:
    """0.01 plus the SD of the WNBA's own year-over-year points per possession, through the holdout."""
    rule = _report()["gates"]["max_large_bias_rule"]
    ppp = rule["season_ppp"]
    yoy = [ppp[str(s)] - ppp[str(s - 1)] for s in range(rule["drift_seasons"][0], rule["drift_seasons"][1] + 1)]
    assert rule["drift_seasons"] == [2011, HOLDOUT]  # never the confirmation season
    assert abs(rule["drift_sd"] - float(np.std(yoy, ddof=1))) < 1e-12
    assert abs(rule["max_large_bias"] - (0.01 + rule["drift_sd"])) < 1e-12
    assert abs(_report()["gates"]["max_large_bias"] - rule["max_large_bias"]) < 1e-12


def test_the_accepted_bucket_is_excused_and_no_other() -> None:
    """The one accepted gate-3 bucket passes only as recorded; any other bucket over the gate still fails."""
    T = _train_epv()
    r = _report()
    m, thr = r["seasons"]["2026"], r["gates"]["max_large_bias"]
    exc = T.ACCEPTED_BUCKET_EXCEPTIONS["10"]
    assert [(b["last_event"], b["transition"], b["n"]) for b in m["failing_buckets"]] == [(13, 2, 1041)]
    assert T.gate("c", m, thr, 2026, exc) == []
    assert any("buckets over the bias gate" in f for f in T.gate("c", m, thr, 2026, ()))  # it is not a pass
    assert T.gate("c", m, thr, 2025, exc)  # excused in its recorded season only
    other = {**m, "failing_buckets": [*m["failing_buckets"], {**m["failing_buckets"][0], "last_event": 12}]}
    assert T.gate("c", other, thr, 2026, exc)  # a second failing bucket fails the run
    moved = {**m, "failing_buckets": [{**m["failing_buckets"][0], "bias": 0.13}]}
    assert T.gate("c", moved, thr, 2026, exc)  # the accepted bucket getting worse fails the run
    regrown = {**m, "failing_buckets": [{**m["failing_buckets"][0], "n": 1040}]}
    assert T.gate("c", regrown, thr, 2026, exc)  # a different sample of that bucket is not the one accepted
    assert r["failed_gates"] == []
    assert [e["observed"]["n"] for e in r["accepted_bucket_exceptions"]] == [1041]


def test_the_card_is_built_from_the_committed_artefacts() -> None:
    T = _train_epv()
    cfg = T.LEAGUES["10"]
    card = json.loads(CARD.read_text(encoding="utf-8"))
    assert card == json.loads(json.dumps(T.epv_card(cfg, _report(), cfg["models"] / cfg["artifact"])))
    assert card["accepted_bucket_exceptions"] and "not a pass" in card["caveats"][0]


def _same(a: pl.DataFrame, b: pl.DataFrame) -> None:
    keys = [c for c in ("game_id", "team_id", "player_id", "lineup_id") if c in a.columns]
    assert a.sort(keys).equals(b.sort(keys))


def test_public_functions_read_the_season_and_its_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.wnba import wnba_loaders

    seen: dict[str, list[int]] = {}

    def fake(which: str):  # type: ignore[no-untyped-def]
        def f(seasons: list[int]) -> pl.DataFrame:
            seen[which] = list(seasons)
            return _f(which)

        return f

    for name, which in (
        ("load_wnba_stats_pbp", "pbp"),
        ("load_wnba_stats_possessions", "possessions"),
        ("load_wnba_stats_game_lineups", "game_lineups"),
        ("load_wnba_stats_schedules", "schedule"),
    ):
        monkeypatch.setattr(wnba_loaders, name, fake(which))
    t = wnba_epv.wnba_epv_team_games(HOLDOUT)
    assert seen["pbp"] == seen["schedule"] == [HOLDOUT] and "game_lineups" not in seen
    _same(t, epv_team_games(_plays()))
    _same(wnba_epv.wnba_epv_player_games(HOLDOUT), epv_player_games(_plays(), _f("game_lineups")))
    _same(wnba_epv.wnba_epv_lineup_games(HOLDOUT), epv_lineup_games(_plays(), _f("game_lineups")))
    assert wnba_epv.wnba_epv_plays(HOLDOUT, return_as_pandas=True).shape[0] == _plays().height
    first = min(int(x) for x in load_epv_model("10").attr("train_seasons").split(","))
    with pytest.raises(ValueError, match=f"served from the {first} season"):
        wnba_epv.wnba_epv_plays(first - 1)
