"""Ridge vintage engine + Axis E map tests on the real NFL fixture.

Observed at gate-setting time (2026-08-06, 821-game fixture, seasons
2009/2020/2024): lam=10 margin_normal brier 0.2239 / acc 0.6425 on n=772
(weeks 2+); isotonic brier 0.2276 on n=672 (iso_min_fit=100 warm-up).
Floors carry margin off those values — never lower them to pass.

The fixture pools 2024, which sits inside the sweep's 2022+ holdout.
These numbers are STRUCTURAL sanity gates only and must never inform
variant selection — tuning reads results/wexp (tune window <= 2021).
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.wexp.backtest import run_backtest
from sportsdataverse.wexp.engines import ratings_predictor, ridge_margin_vintages
from sportsdataverse.wexp.oracle_market import nfl_market_oracle_from_schedule
from sportsdataverse.wexp.store import VintageStore

FIXDIR = Path(__file__).resolve().parents[1] / "fixtures" / "wexp"

LAM = 10.0


@pytest.fixture(scope="module")
def nfl_oracle() -> pl.DataFrame:
    return nfl_market_oracle_from_schedule(pl.read_parquet(FIXDIR / "nfl_schedule_sample.parquet"))


@pytest.fixture(scope="module")
def store(nfl_oracle) -> VintageStore:
    s = VintageStore()
    s.register("ridge", ridge_margin_vintages(nfl_oracle, lam=LAM), entity_key="team_id")
    return s


def test_vintage_table_shape(nfl_oracle, store):
    vint = store.table("ridge")
    # every vintage carries the full 32-team league (observed: always 32)
    assert vint.group_by("season", "as_of_week").agg(pl.len())["len"].unique().to_list() == [32]
    # week 1 emits no vintage (no prior completed games in-season)
    assert vint.filter(pl.col("as_of_week") == 1).height == 0
    assert vint.schema["team_id"] == pl.Utf8
    assert vint.schema["as_of_week"] == pl.Int32


def test_vintage_fit_identifies_hfa_and_spread(store):
    """The symmetrized fit must identify HFA and spread the team coefs.

    Observed final-vintage values (lam=10): hfa 5.05 (2009), 3.78 (2024),
    0.24 (2020 — the COVID no-fans season, a real-world identification
    check); off_coef std 4.44 / 4.02 / 3.68. A collapsed fit (hfa -> 0,
    coefs shrunk flat) fails these without failing the brier gate.
    """
    vint = store.table("ridge")
    finals = {s: vint.filter(pl.col("season") == s).sort("as_of_week").tail(32) for s in (2009, 2020, 2024)}
    for s in (2009, 2024):
        assert 1.0 < finals[s]["hfa"][0] < 7.0
    # 2020 played without fans: fitted HFA collapses toward zero
    assert finals[2020]["hfa"][0] < finals[2009]["hfa"][0]
    assert finals[2020]["hfa"][0] < finals[2024]["hfa"][0]
    for s, f in finals.items():
        assert f["off_coef"].std() > 2.0, s


def test_lam_mismatch_refused(nfl_oracle, store):
    """A variant claiming a different lam than the served table must error."""
    from sportsdataverse.wexp.engines import build_predictor
    from sportsdataverse.wexp.variants import VariantConfig

    variant = VariantConfig(
        core="ridge_epa",
        response="raw",
        opponent_adjust="ridge",
        prior="flat",
        wp_map="margin_normal",
        hfa="fixed",
        params=(("lam", 25.0),),  # store fixture was built with lam=10
    )
    with pytest.raises(ValueError, match="lam"):
        run_backtest(nfl_oracle, build_predictor(variant, table="ridge"), model_id="r", variant=variant, store=store)


def test_vintage_builder_ignores_future_games(nfl_oracle):
    """A vintage at as_of_week W must be invariant to games in weeks >= W."""
    season = nfl_oracle.filter(pl.col("season") == 2024)
    tampered = season.with_columns(
        pl.when(pl.col("week") >= 10)
        .then(pl.col("home_margin") * 3 + 7)
        .otherwise(pl.col("home_margin"))
        .alias("home_margin")
    )
    a = ridge_margin_vintages(season, lam=LAM).filter(pl.col("as_of_week") <= 10)
    b = ridge_margin_vintages(tampered, lam=LAM).filter(pl.col("as_of_week") <= 10)
    assert a.height == b.height > 0
    assert a.sort("as_of_week", "team_id").equals(b.sort("as_of_week", "team_id"))


def test_ridge_margin_normal_backtest(nfl_oracle, store):
    probs, rows = run_backtest(nfl_oracle, ratings_predictor("ridge"), model_id="ridge_margin", store=store)
    pooled = {r["metric"]: r["value"] for r in rows.filter(pl.col("season") == -1).iter_rows(named=True)}
    n = rows.filter(pl.col("season") == -1)["n"][0]
    assert n >= 770  # observed 772 (821 minus week-1 games); min-size guard
    assert pooled["brier"] < 0.24  # observed 0.2239; clearly beats coin flip 0.25
    assert pooled["winner_accuracy"] > 0.62  # observed 0.6425
    # week-1 games are uncovered (no vintage) and stay null, never imputed
    week1 = probs.filter((pl.col("week") == 1) & (pl.col("season_type") == "REG"))
    assert week1["p_home"].null_count() == week1.height


def test_ridge_isotonic_backtest(nfl_oracle, store):
    _, rows = run_backtest(nfl_oracle, ratings_predictor("ridge", wp_map="isotonic"), model_id="ridge_iso", store=store)
    pooled = {r["metric"]: r["value"] for r in rows.filter(pl.col("season") == -1).iter_rows(named=True)}
    n = rows.filter(pl.col("season") == -1)["n"][0]
    assert n >= 650  # observed 672 (iso_min_fit=100 warm-up per season)
    assert pooled["brier"] < 0.24  # observed 0.2276


def test_build_predictor_elo_matches_direct(nfl_oracle):
    from sportsdataverse.wexp.backtest import elo_predictor
    from sportsdataverse.wexp.elo import EloConfig
    from sportsdataverse.wexp.engines import build_predictor
    from sportsdataverse.wexp.variants import VariantConfig

    variant = VariantConfig(
        core="elo_margin",
        response="raw",
        opponent_adjust="none",
        prior="carryover",
        wp_map="elo_logistic",
        hfa="fixed",
        params=(("k", 9.1), ("z", 402.0), ("carryover", 0.53)),
    )
    got, _ = run_backtest(nfl_oracle, build_predictor(variant), model_id="elo", variant=variant)
    want, _ = run_backtest(
        nfl_oracle,
        elo_predictor(EloConfig(k=9.1, z=402.0, carryover=0.53)),
        model_id="elo",
        variant=variant,
    )
    assert (got["p_home"] - want["p_home"]).abs().max() < 1e-12
    # prior="flat" means full season reset (carryover 0), a different walk
    flat = VariantConfig(
        core="elo_margin",
        response="raw",
        opponent_adjust="none",
        prior="flat",
        wp_map="elo_logistic",
        hfa="fixed",
    )
    got_flat, _ = run_backtest(nfl_oracle, build_predictor(flat), model_id="elo", variant=flat)
    assert (got_flat["p_home"] - got["p_home"]).abs().max() > 1e-6


def test_build_predictor_ridge_dispatch(nfl_oracle, store):
    from sportsdataverse.wexp.engines import build_predictor
    from sportsdataverse.wexp.variants import VariantConfig

    variant = VariantConfig(
        core="ridge_epa",
        response="raw",
        opponent_adjust="ridge",
        prior="flat",
        wp_map="margin_normal",
        hfa="fixed",
    )
    probs, _ = run_backtest(
        nfl_oracle, build_predictor(variant, table="ridge"), model_id="r", variant=variant, store=store
    )
    direct, _ = run_backtest(nfl_oracle, ratings_predictor("ridge"), model_id="r", store=store)
    assert probs["p_home"].fill_null(-1.0).equals(direct["p_home"].fill_null(-1.0))


def test_build_predictor_unimplemented_combo():
    from sportsdataverse.wexp.engines import build_predictor
    from sportsdataverse.wexp.variants import VariantConfig

    with pytest.raises(NotImplementedError):
        build_predictor(
            VariantConfig(
                core="glickman_stern",
                response="raw",
                opponent_adjust="none",
                prior="flat",
                wp_map="elo_logistic",
                hfa="fixed",
            )
        )
    with pytest.raises(NotImplementedError):
        # valid config cell, engine for the wepa response not yet landed
        build_predictor(
            VariantConfig(
                core="ridge_epa",
                response="wepa",
                opponent_adjust="ridge",
                prior="flat",
                wp_map="margin_normal",
                hfa="fixed",
            )
        )
    with pytest.raises(NotImplementedError):
        # unbuilt priors on the elo core must NOT silently serve carryover Elo
        build_predictor(
            VariantConfig(
                core="elo_margin",
                response="raw",
                opponent_adjust="none",
                prior="carryover_continuity",
                wp_map="elo_logistic",
                hfa="fixed",
            )
        )


def test_unknown_wp_map_refused():
    with pytest.raises(ValueError, match="wp_map"):
        ratings_predictor("ridge", wp_map="monte_carlo")


def test_predictor_requires_store(nfl_oracle):
    with pytest.raises(ValueError, match="VintageStore"):
        run_backtest(nfl_oracle, ratings_predictor("ridge"), model_id="ridge_margin", store=None)
