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


def test_capped_response_changes_fit_and_scores(nfl_oracle):
    """Axis B2: the cap provably alters the fit (observed max coef delta
    1.85 at cap=28, brier 0.2233) and mismatched cap claims are refused
    both ways."""
    capped = ridge_margin_vintages(nfl_oracle, lam=LAM, cap=28.0)
    raw = ridge_margin_vintages(nfl_oracle, lam=LAM)
    assert (capped["off_coef"] - raw["off_coef"]).abs().max() > 1.0  # not a silent no-op
    store = VintageStore()
    store.register("ridge_capped", capped, entity_key="team_id")
    _, rows = run_backtest(
        nfl_oracle, ratings_predictor("ridge_capped", cap=28.0), model_id="ridge_capped", store=store
    )
    pooled = {r["metric"]: r["value"] for r in rows.filter(pl.col("season") == -1).iter_rows(named=True)}
    assert pooled["brier"] < 0.24  # observed 0.2233
    # a raw-claiming predictor must refuse the capped table, and vice versa
    with pytest.raises(ValueError, match="cap"):
        run_backtest(nfl_oracle, ratings_predictor("ridge_capped", cap=None), model_id="r", store=store)
    with pytest.raises(ValueError, match="cap"):
        run_backtest(nfl_oracle, ratings_predictor("ridge_capped", cap=21.0), model_id="r", store=store)


def test_drive_ep_responses_extraction():
    """Drive-EP extraction on real 2019 pbp (2 games, 324 plays).

    Per drive: sum of play EPA (the perspective-corrected start-to-end EP
    change — the naive EP_end(last) - EP_start(first) flips sign on
    punt/turnover drives because raw EP_end switches to the NEW possession
    team's perspective, and fit a NEGATIVE margin scale on real data).
    resp_pct = delta / (7 - drive start EP), the share of available drive
    EP, floored at 0.5 available. Every game emits BOTH offenses, so the
    downstream ridge rates offense and defense for every team.
    """
    from sportsdataverse.wexp.engines import cfb_drive_ep_responses

    pbp = pl.read_parquet(FIXDIR / "cfb_pbp_drive_sample.parquet")
    rows = cfb_drive_ep_responses(pbp)
    assert rows.height == 4  # 2 games x 2 offenses
    assert rows.schema["off_team_id"] == pl.Utf8 and rows.schema["game_id"] == pl.Utf8
    assert set(rows["game_id"].unique().to_list()) == {"401110720", "401112224"}
    # hand-check against the raw plays for one offense of 401110720
    g = pbp.filter(pl.col("game_id") == 401110720).sort("game_play_number")
    off = str(g["pos_team"][0])
    team_row = rows.filter((pl.col("game_id") == "401110720") & (pl.col("off_team_id") == off))
    assert team_row.height == 1
    assert team_row["drives"][0] >= 5  # real game: several drives per side
    per_drive = (
        g.drop_nulls(["EPA", "EP_start"])
        .group_by("drive.id", maintain_order=True)
        .agg(off=pl.col("pos_team").first(), delta=pl.col("EPA").sum(), start=pl.col("EP_start").first())
        .filter(pl.col("off") == int(off))
        .with_columns(pct=pl.col("delta") / (7.0 - pl.col("start")).clip(lower_bound=0.5))
    )
    assert abs(team_row["resp"][0] - per_drive["delta"].mean()) < 1e-12
    assert abs(team_row["resp_pct"][0] - per_drive["pct"].mean()) < 1e-12
    # winner sanity on real data: Alabama (333) blew out Duke 42-3 in
    # 401110720 — its per-drive EPA efficiency must exceed Duke's
    resp_by_team = {
        r["off_team_id"]: r["resp"] for r in rows.filter(pl.col("game_id") == "401110720").iter_rows(named=True)
    }
    assert resp_by_team["333"] > resp_by_team["150"]


def test_response_ridge_vintages_walk(nfl_oracle):
    """The response-ridge walk is EXCLUSIVE and future-invariant.

    Reuses the NFL fixture oracle as the game spine with a margin-derived
    response (real data; the drive-EP response plugs in identically):
    tampering weeks >= 10 must not change earlier vintages.
    """
    from sportsdataverse.wexp.engines import response_ridge_vintages

    season = nfl_oracle.filter(pl.col("season") == 2024)
    resp = pl.concat(
        [
            season.select(
                game_id=pl.col("game_id"),
                off_team_id=pl.col("home_team_id"),
                def_team_id=pl.col("away_team_id"),
                resp=pl.col("home_margin") / 10.0,
            ),
            season.select(
                game_id=pl.col("game_id"),
                off_team_id=pl.col("away_team_id"),
                def_team_id=pl.col("home_team_id"),
                resp=-pl.col("home_margin") / 10.0,
            ),
        ]
    ).drop_nulls("resp")
    vint = response_ridge_vintages(resp, season, lam=1.0)
    assert vint.filter(pl.col("as_of_week") == 1).height == 0  # exclusive: week 1 empty
    assert "adj_net" in vint.columns and vint.schema["team_id"] == pl.Utf8
    tampered = season.with_columns(
        pl.when(pl.col("week") >= 10)
        .then(pl.col("home_margin") * 3 + 7)
        .otherwise(pl.col("home_margin"))
        .alias("home_margin")
    )
    resp_t = resp  # responses unchanged; the oracle's margins feed only close_filter
    a = response_ridge_vintages(resp_t, season, lam=1.0, close_filter=24.0).filter(pl.col("as_of_week") <= 10)
    b = response_ridge_vintages(resp_t, tampered, lam=1.0, close_filter=24.0).filter(pl.col("as_of_week") <= 10)
    assert a.height == b.height > 0
    assert a.sort("as_of_week", "team_id").equals(b.sort("as_of_week", "team_id"))


def test_close_filter_changes_fit_and_is_stamped(nfl_oracle):
    """Axis B4 game-level garbage filter: blowouts leave the fit entirely.

    Observed (lam=5, close_filter=24): fit keeps 723/821 fixture games,
    brier 0.2213 (~neutral vs raw on NFL). The filter provably changes
    the fit, and mismatched claims are refused both ways.
    """
    filtered = ridge_margin_vintages(nfl_oracle, lam=5.0, close_filter=24.0)
    raw = ridge_margin_vintages(nfl_oracle, lam=5.0)
    # keyed join — the filtered table legitimately loses vintage rows for
    # teams whose entire early history was blowouts (observed 1942 vs 1952)
    joined = filtered.select("season", "as_of_week", "team_id", fc=pl.col("off_coef")).join(
        raw.select("season", "as_of_week", "team_id", rc=pl.col("off_coef")),
        on=["season", "as_of_week", "team_id"],
    )
    assert joined.height >= 1900  # observed 1942
    assert (joined["fc"] - joined["rc"]).abs().max() > 2.0  # observed 8.76 — not a silent no-op
    store = VintageStore()
    store.register("ridge_cf", filtered, entity_key="team_id")
    _, rows = run_backtest(
        nfl_oracle, ratings_predictor("ridge_cf", close_filter=24.0), model_id="ridge_cf", store=store
    )
    pooled = {r["metric"]: r["value"] for r in rows.filter(pl.col("season") == -1).iter_rows(named=True)}
    assert pooled["brier"] < 0.24  # observed 0.2213
    with pytest.raises(ValueError, match="close_filter"):
        run_backtest(nfl_oracle, ratings_predictor("ridge_cf"), model_id="r", store=store)
    from sportsdataverse.wexp.engines import build_predictor
    from sportsdataverse.wexp.variants import VariantConfig

    base = dict(core="ridge_epa", opponent_adjust="ridge", prior="flat", wp_map="margin_normal", hfa="fixed")
    with pytest.raises(ValueError, match="close_filter"):
        build_predictor(VariantConfig(response="garbage_filtered", **base))


def test_build_predictor_capped_param_contract():
    from sportsdataverse.wexp.engines import build_predictor
    from sportsdataverse.wexp.variants import VariantConfig

    base = dict(core="ridge_epa", opponent_adjust="ridge", prior="flat", wp_map="margin_normal", hfa="fixed")
    with pytest.raises(ValueError, match="cap"):
        build_predictor(VariantConfig(response="capped", **base))  # capped needs a cap param
    with pytest.raises(ValueError, match="cap"):
        build_predictor(VariantConfig(response="raw", params=(("cap", 28.0),), **base))


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
                prior="market_open_informed",
                wp_map="elo_logistic",
                hfa="fixed",
            )
        )


CONTINUITY_VARIANT_KW = dict(
    core="elo_margin", response="raw", opponent_adjust="none", wp_map="elo_logistic", hfa="fixed"
)


def test_continuity_prior_shifts_change_predictions():
    """Axis D3: shifts provably move preseason predictions; zero-beta == plain carryover.

    Real fixtures: CFB oracle (2015 + 2024) + captured talent/returning.
    """
    from sportsdataverse.wexp.engines import build_predictor, cfb_continuity_shifts
    from sportsdataverse.wexp.oracle_market import cfb_market_oracle_from_lines
    from sportsdataverse.wexp.variants import VariantConfig

    oracle = cfb_market_oracle_from_lines(
        pl.read_parquet(FIXDIR / "cfb_line_odds_sample.parquet"),
        pl.read_parquet(FIXDIR / "cfb_schedule_sample.parquet"),
    )
    talent = pl.read_parquet(FIXDIR / "cfb_talent_sample.parquet")
    returning = pl.read_parquet(FIXDIR / "cfb_returning_sample.parquet")
    shifts = cfb_continuity_shifts(oracle, talent, returning, beta_talent=50.0, beta_returning=100.0)
    assert shifts.height > 200  # observed 429 (215 oracle teams x 2 seasons, minus unmatched)
    assert shifts["prior_shift"].abs().max() > 10.0  # the betas actually move ratings

    cont = VariantConfig(prior="carryover_continuity", **CONTINUITY_VARIANT_KW)
    plain = VariantConfig(prior="carryover", **CONTINUITY_VARIANT_KW)
    p_cont, _ = run_backtest(oracle, build_predictor(cont, season_priors=shifts), model_id="e", variant=cont)
    p_plain, _ = run_backtest(oracle, build_predictor(plain), model_id="e", variant=plain)
    assert (p_cont["p_home"] - p_plain["p_home"]).abs().max() > 1e-3  # not a silent no-op
    # zero-beta shifts reduce exactly to plain carryover
    zero = cfb_continuity_shifts(oracle, talent, returning)
    p_zero, _ = run_backtest(oracle, build_predictor(cont, season_priors=zero), model_id="e", variant=cont)
    assert (p_zero["p_home"] - p_plain["p_home"]).abs().max() < 1e-12


def test_glickman_stern_backtest(nfl_oracle):
    """Axis A6: walk-forward Kalman filter on the real NFL fixture.

    Observed (default seeds sw=0.9 ss=2.35 shrink=0.82): brier 0.2233 /
    acc 0.639, full 821-game coverage (the prior rates week 1). The
    frozen no-dynamics config (sw=0, ss=0, shrink=1) scored 0.2425 —
    the dynamics must keep beating it (silent-no-op ordering gate).
    """
    from sportsdataverse.wexp.engines import GSConfig, build_predictor, glickman_stern_predictor
    from sportsdataverse.wexp.variants import VariantConfig

    probs, rows = run_backtest(nfl_oracle, glickman_stern_predictor(), model_id="gs")
    pooled = {r["metric"]: r["value"] for r in rows.filter(pl.col("season") == -1).iter_rows(named=True)}
    assert probs["p_home"].null_count() == 0 and probs.height == 821
    assert pooled["brier"] < 0.235  # observed 0.2233
    assert pooled["winner_accuracy"] > 0.62  # observed 0.639
    _, frozen_rows = run_backtest(
        nfl_oracle,
        glickman_stern_predictor(GSConfig(sigma_w=0.0, sigma_s=0.0, season_shrink=1.0)),
        model_id="gs_frozen",
    )
    frozen = {r["metric"]: r["value"] for r in frozen_rows.filter(pl.col("season") == -1).iter_rows(named=True)}
    assert pooled["brier"] < frozen["brier"] - 0.01  # observed gap 0.019

    variant = VariantConfig(
        core="glickman_stern",
        response="raw",
        opponent_adjust="none",
        prior="carryover",
        wp_map="margin_normal",
        hfa="fixed",
    )
    dispatched, _ = run_backtest(nfl_oracle, build_predictor(variant), model_id="gs", variant=variant)
    assert (dispatched["p_home"] - probs["p_home"]).abs().max() < 1e-12


def test_per_era_hfa_changes_2020_only(nfl_oracle):
    """Axis F per_era: the COVID override moves ONLY 2020 non-neutral games."""
    from sportsdataverse.wexp.engines import build_predictor
    from sportsdataverse.wexp.variants import VariantConfig

    base = dict(core="elo_margin", response="raw", opponent_adjust="none", prior="carryover", wp_map="elo_logistic")
    per_era = VariantConfig(hfa="per_era", params=(("hfa_covid", 0.0),), **base)
    fixed = VariantConfig(hfa="fixed", **base)
    p_era, _ = run_backtest(nfl_oracle, build_predictor(per_era), model_id="e", variant=per_era)
    p_fix, _ = run_backtest(nfl_oracle, build_predictor(fixed), model_id="e", variant=fixed)
    diff = (p_era["p_home"] - p_fix["p_home"]).abs()
    joined = nfl_oracle.with_columns(diff=diff)
    assert joined.filter(pl.col("season") == 2020)["diff"].max() > 0.01  # override applied
    # pre-2020 seasons are untouched (post-2020 ratings may drift via updates)
    assert joined.filter(pl.col("season") < 2020)["diff"].max() == 0.0
    with pytest.raises(ValueError, match="hfa_covid"):
        build_predictor(VariantConfig(hfa="per_era", **base))


def test_true_epa_ridge_arm(nfl_oracle):
    """A3 true-EPA arm: published nfl_ratings_weekly vintages via net_vintages_view.

    Observed (scale 26.4 / hfa 1.57, OLS on this fixture): margin_normal
    brier 0.2212 on n=727 — better than the margin-ridge's 0.2239, the
    EPA-response signal the RESEARCH doc predicted. Isotonic 0.2216 on
    n=622. Under isotonic with hfa=0 the scale is provably irrelevant
    (monotone transform) — a property gate, not a tuned floor.
    """
    from sportsdataverse.wexp.engines import net_vintages_view

    vintages = pl.read_parquet(FIXDIR / "nfl_ratings_weekly_sample.parquet")
    view = net_vintages_view(vintages, scale=26.4, hfa=1.57)
    assert view.schema["season"] == pl.Int32 and view.schema["team_id"] == pl.Utf8
    store = VintageStore()
    store.register("epa", view, entity_key="team_id")
    _, rows = run_backtest(nfl_oracle, ratings_predictor("epa"), model_id="ridge_epa", store=store)
    pooled = {r["metric"]: r["value"] for r in rows.filter(pl.col("season") == -1).iter_rows(named=True)}
    assert rows.filter(pl.col("season") == -1)["n"][0] >= 720  # observed 727
    assert pooled["brier"] < 0.23  # observed 0.2212

    # isotonic is invariant to the scale tunable (monotone in the net diff)
    s1, s2 = VintageStore(), VintageStore()
    s1.register("epa", net_vintages_view(vintages, scale=1.0), entity_key="team_id")
    s2.register("epa", net_vintages_view(vintages, scale=26.4), entity_key="team_id")
    p1, _ = run_backtest(nfl_oracle, ratings_predictor("epa", wp_map="isotonic"), model_id="e", store=s1)
    p2, _ = run_backtest(nfl_oracle, ratings_predictor("epa", wp_map="isotonic"), model_id="e", store=s2)
    assert (p1["p_home"] - p2["p_home"]).abs().max() < 1e-9


def test_cfb_epa_and_fei_arms_join_by_id():
    """CFB vintage arms join by ESPN team ID — the name join is a silent
    zero-match trap (same dtype, disjoint values) the driver must refuse.

    Real fixtures: cfb_ratings_weekly (2015 + 2024) through the store's
    guarded through->as_of shift. Observed (scale 8.7 / hfa 2.71 from the
    tune fit): fei_net margin_normal brier 0.1844 / acc 0.7331 on n=487;
    the wrong (name) join raises the driver's no-predictions error.
    """
    from sportsdataverse.wexp.engines import net_vintages_view
    from sportsdataverse.wexp.oracle_market import CFB_MARGIN_SIGMA, cfb_market_oracle_from_lines

    oracle = cfb_market_oracle_from_lines(
        pl.read_parquet(FIXDIR / "cfb_line_odds_sample.parquet"),
        pl.read_parquet(FIXDIR / "cfb_schedule_sample.parquet"),
    )
    raw = pl.read_parquet(FIXDIR / "cfb_ratings_weekly_sample.parquet")
    view = net_vintages_view(raw.rename({"through_week": "as_of_week"}), net_col="fei_net", scale=8.7, hfa=2.71)
    store = VintageStore()
    store.register("fei", view.rename({"as_of_week": "through_week"}), entity_key="team_id", week_semantics="through")

    probs, rows = run_backtest(
        oracle,
        ratings_predictor("fei", sigma=CFB_MARGIN_SIGMA, join_on=("home_team_id", "away_team_id")),
        model_id="fei",
        store=store,
    )
    pooled = {r["metric"]: r["value"] for r in rows.filter(pl.col("season") == -1).iter_rows(named=True)}
    assert rows.filter(pl.col("season") == -1)["n"][0] >= 480  # observed 487
    assert pooled["brier"] < 0.20  # observed 0.1844 (fixture, favorite-heavy sample)

    # the default NAME join matches nothing on CFB — must fail loudly
    with pytest.raises(ValueError, match="no predictions"):
        run_backtest(oracle, ratings_predictor("fei", sigma=CFB_MARGIN_SIGMA), model_id="fei", store=store)


def test_gs_continuity_composition():
    """A6 + D3: continuity shifts as season-boundary state means in the filter.

    Real CFB fixtures (2015 + 2024 — exercises both the start-season prior
    and the multi-season boundary path). Observed: GS plain 0.1984; composed
    bt=2/br=4 brier 0.1841 (gate < 0.19 and strictly better than plain).
    Zero-beta shifts reduce exactly to the plain filter.
    """
    from sportsdataverse.wexp.engines import (
        GSConfig,
        build_predictor,
        cfb_continuity_shifts,
        glickman_stern_predictor,
    )
    from sportsdataverse.wexp.oracle_market import CFB_MARGIN_SIGMA, cfb_market_oracle_from_lines
    from sportsdataverse.wexp.variants import VariantConfig

    oracle = cfb_market_oracle_from_lines(
        pl.read_parquet(FIXDIR / "cfb_line_odds_sample.parquet"),
        pl.read_parquet(FIXDIR / "cfb_schedule_sample.parquet"),
    )
    talent = pl.read_parquet(FIXDIR / "cfb_talent_sample.parquet")
    returning = pl.read_parquet(FIXDIR / "cfb_returning_sample.parquet")
    cfg = GSConfig(sigma_w=1.5, sigma_y=CFB_MARGIN_SIGMA, hfa=3.0)

    plain, plain_rows = run_backtest(oracle, glickman_stern_predictor(cfg), model_id="gs")
    shifts = cfb_continuity_shifts(oracle, talent, returning, beta_talent=2.0, beta_returning=4.0)
    composed, comp_rows = run_backtest(oracle, glickman_stern_predictor(cfg, shifts), model_id="gs_cont")
    brier = {
        r["model_id"]: r["value"]
        for r in pl.concat([plain_rows, comp_rows])
        .filter((pl.col("season") == -1) & (pl.col("metric") == "brier"))
        .iter_rows(named=True)
    }
    assert brier["gs_cont"] < 0.19  # observed 0.1841
    assert brier["gs_cont"] < brier["gs"]  # composition must help here (0.1984 plain)

    zero = cfb_continuity_shifts(oracle, talent, returning)
    zeroed, _ = run_backtest(oracle, glickman_stern_predictor(cfg, zero), model_id="gs")
    assert (zeroed["p_home"] - plain["p_home"]).abs().max() < 1e-12

    variant = VariantConfig(
        core="glickman_stern",
        response="raw",
        opponent_adjust="none",
        prior="carryover_continuity",
        wp_map="margin_normal",
        hfa="fixed",
        params=(("hfa", 3.0), ("sigma_w", 1.5), ("sigma_y", CFB_MARGIN_SIGMA)),
    )
    dispatched, _ = run_backtest(
        oracle, build_predictor(variant, season_priors=shifts), model_id="gs_cont", variant=variant
    )
    assert (dispatched["p_home"] - composed["p_home"]).abs().max() < 1e-12
    with pytest.raises(ValueError, match="season_priors"):
        build_predictor(variant)


def test_continuity_prior_requires_table():
    from sportsdataverse.wexp.engines import build_predictor
    from sportsdataverse.wexp.variants import VariantConfig

    with pytest.raises(ValueError, match="season_priors"):
        build_predictor(VariantConfig(prior="carryover_continuity", **CONTINUITY_VARIANT_KW))


def test_unknown_wp_map_refused():
    with pytest.raises(ValueError, match="wp_map"):
        ratings_predictor("ridge", wp_map="monte_carlo")


def test_predictor_requires_store(nfl_oracle):
    with pytest.raises(ValueError, match="VintageStore"):
        run_backtest(nfl_oracle, ratings_predictor("ridge"), model_id="ridge_margin", store=None)


def test_qb_change_events_and_variance_knob(nfl_oracle):
    """QB-change events are within-season, opener-exempt, and only widen
    variance (zero/no events reduce exactly to the plain filter).

    Real fixture: 156 starter changes across 2009/2020/2024.
    """
    from sportsdataverse.wexp.engines import GSConfig, glickman_stern_predictor, nfl_qb_change_events

    sch = pl.read_parquet(FIXDIR / "nfl_schedule_sample.parquet")
    events = nfl_qb_change_events(sch, extra_var=9.0)
    assert events.height == 156  # observed on the fixture
    # opener-exempt: no event at any (season, team)'s first game week
    firsts = (
        pl.concat(
            [
                sch.select(season="season", week="week", team="home_team"),
                sch.select(season="season", week="week", team="away_team"),
            ]
        )
        .group_by("season", "team")
        .agg(first_week=pl.col("week").min())
    )
    joined = events.join(firsts, left_on=["season", "team"], right_on=["season", "team"], how="inner")
    assert joined.filter(pl.col("week") == pl.col("first_week")).height == 0

    plain, _ = run_backtest(nfl_oracle, glickman_stern_predictor(GSConfig()), model_id="gs")
    with_events, _ = run_backtest(
        nfl_oracle, glickman_stern_predictor(GSConfig(), variance_events=events), model_id="gs"
    )
    assert (plain["p_home"] - with_events["p_home"]).abs().max() > 1e-6  # knob does something
    none_events, _ = run_backtest(
        nfl_oracle, glickman_stern_predictor(GSConfig(), variance_events=events.head(0)), model_id="gs"
    )
    assert (plain["p_home"] - none_events["p_home"]).abs().max() < 1e-12  # empty == plain
    with pytest.raises(ValueError, match="duplicate"):
        glickman_stern_predictor(GSConfig(), variance_events=pl.concat([events.head(1), events.head(1)]))


def test_drive_classifiers_on_real_fixture():
    """Garbage + clock-kill flags on the real 2-game fixture.

    Alabama 42-3 Duke necessarily produced garbage-time drives (Q4 margin
    39 > the 21-point Connelly band); the competitive Wisconsin-Michigan
    first half produced none in Q1.
    """
    from sportsdataverse.wexp.engines import cfb_drive_deltas

    drives = cfb_drive_deltas(pl.read_parquet(FIXDIR / "cfb_pbp_drive_sample.parquet"))
    assert {"garbage", "clock_kill", "period", "margin_start", "plays"} <= set(drives.columns)
    bama = drives.filter(pl.col("game_id") == 401110720)  # deltas keep the raw Int64 id
    assert bama.filter(pl.col("garbage") == True).height > 0  # noqa: E712
    q1 = drives.filter(pl.col("period") == 1)
    assert q1.filter(pl.col("garbage") == True).height == 0  # noqa: E712


def test_cross_season_history_is_leak_free_and_widens_coverage(nfl_oracle):
    """history_seasons folds PRIOR seasons in — never the current one's future.

    Tampering the current season's later weeks must leave earlier
    vintages byte-identical (the walk stays exclusive); and week 2 of a
    later season must now carry ratings for teams that have played no
    current-season games yet (the coverage win: NFL tune coverage rose
    5,758 -> 6,105 games).
    """
    two = nfl_oracle.filter(pl.col("season").is_in([2020, 2024]))
    base = ridge_margin_vintages(two, lam=LAM, history_seasons=1)
    tampered = ridge_margin_vintages(
        two.with_columns(
            pl.when((pl.col("season") == 2024) & (pl.col("week") >= 10))
            .then(pl.col("home_margin") * 3 + 7)
            .otherwise(pl.col("home_margin"))
            .alias("home_margin")
        ),
        lam=LAM,
        history_seasons=1,
    )
    early = base.filter((pl.col("season") == 2024) & (pl.col("as_of_week") <= 10))
    early_t = tampered.filter((pl.col("season") == 2024) & (pl.col("as_of_week") <= 10))
    assert early.height == early_t.height > 0
    assert early.sort("as_of_week", "team_id").equals(early_t.sort("as_of_week", "team_id"))

    # coverage: with history, 2024 week 2 exists AND the fit is larger
    without = ridge_margin_vintages(two, lam=LAM)
    assert base.filter(pl.col("season") == 2024).height >= without.filter(pl.col("season") == 2024).height
    # the fit actually changed (history is not a silent no-op)
    j = base.join(without, on=["season", "as_of_week", "team_id"], suffix="_w")
    assert (j["off_coef"] - j["off_coef_w"]).abs().max() > 0.1


def test_market_prior_shifts_contract_and_seed_isolation():
    """D4 market priors: seeded ONLY by the seed weeks, shaped like D3 shifts.

    Real CFB fixture (2015 + 2024). Tampering the market lines of weeks
    AFTER the seed window must leave the priors byte-identical — the
    prior is a season seed, not a rolling market tracker.
    """
    from sportsdataverse.wexp.engines import market_prior_shifts
    from sportsdataverse.wexp.oracle_market import cfb_market_oracle_from_lines

    oracle = cfb_market_oracle_from_lines(
        pl.read_parquet(FIXDIR / "cfb_line_odds_sample.parquet"),
        pl.read_parquet(FIXDIR / "cfb_schedule_sample.parquet"),
    )
    shifts = market_prior_shifts(oracle, seed_weeks=3)
    assert set(shifts.columns) == {"season", "team", "prior_shift"}
    assert shifts.schema["team"] == pl.Utf8 and shifts.schema["season"] == pl.Int32
    assert shifts.height > 0
    # one row per (season, team) — the GS prior loader refuses duplicates
    assert shifts.unique(subset=["season", "team"]).height == shifts.height
    # market strengths must actually spread teams apart, not collapse
    assert shifts["prior_shift"].std() > 1.0

    tampered = oracle.with_columns(
        pl.when(pl.col("week") > 3)
        .then(pl.col("spread_close") * 5 - 20)
        .otherwise(pl.col("spread_close"))
        .alias("spread_close")
    )
    after = market_prior_shifts(tampered, seed_weeks=3)
    assert shifts.sort("season", "team").equals(after.sort("season", "team"))


def test_market_prior_dispatch_requires_table():
    from sportsdataverse.wexp.engines import build_predictor
    from sportsdataverse.wexp.variants import VariantConfig

    v = VariantConfig(
        core="glickman_stern",
        response="raw",
        opponent_adjust="none",
        prior="market_open_informed",
        wp_map="margin_normal",
        hfa="fixed",
    )
    with pytest.raises(ValueError, match="season_priors"):
        build_predictor(v)


def test_season_priors_wrong_entity_space_refused(nfl_oracle):
    """A prior keyed on ids (not the filter's name space) matches nothing.

    This is the silent-degrade bug that shipped in the first D4 draft:
    market priors keyed on ESPN team ids applied to zero teams and the
    arm scored identically to no-prior. It must fail loudly instead.
    """
    from sportsdataverse.wexp.engines import GSConfig, glickman_stern_predictor

    bogus = pl.DataFrame(
        {
            "season": pl.Series([2024, 2024], dtype=pl.Int32),
            "team": ["99999", "88888"],  # ids, not the oracle's team names
            "prior_shift": [3.0, -3.0],
        }
    )
    with pytest.raises(ValueError, match="state space"):
        run_backtest(nfl_oracle, glickman_stern_predictor(GSConfig(), bogus), model_id="gs")
