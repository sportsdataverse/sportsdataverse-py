"""The starting-QB layer (``nfl_qb_layer``) on a real nflverse 2025 slice, weeks 1-6 (fixtures/qb_layer)."""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import importlib
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl import nfl_qb_layer as mod

ratings_mod = importlib.import_module("sportsdataverse.nfl.nfl_ratings")  # the package re-exports the function
from sportsdataverse.nfl.nfl_market import nfl_predict_games, predict_margin
from sportsdataverse.nfl.nfl_prediction_constants import QB_LAYER_CONSTANTS, QBLayerConfig, get_constants
from sportsdataverse.nfl.nfl_ratings import efficiency_ratings

FIX = Path(__file__).parent / "fixtures" / "qb_layer"
JSON = Path(__file__).parents[2] / "tools" / "models" / "nfl_prediction" / "qb_layer_constants.json"
CFG = QBLayerConfig(
    k=200.0,
    gamma=1.0,
    league=0.05,
    replacement=-0.05,
    p_start={"none": 0.98, "Questionable": 0.8, "Doubtful": 0.3, "Out": 0.05},
)
BURROW, BROWNING, FLACCO = "00-0036442", "00-0035100", "00-0026158"
MCCARTHY, WENTZ = "00-0039923", "00-0032950"


@pytest.fixture(scope="module")
def data() -> dict[str, pl.DataFrame]:
    return {n: pl.read_parquet(FIX / f"{n}_2025_w1_6.parquet") for n in ("schedule", "pbp", "injuries", "depth_charts")}


@pytest.fixture(scope="module")
def starters(data) -> pl.DataFrame:
    return mod.nfl_expected_starters(
        data["schedule"], depth_charts=data["depth_charts"], injuries=data["injuries"], config=CFG
    )


def _pick(st: pl.DataFrame, game_id: str, team: str) -> dict:
    return st.filter((pl.col("game_id") == game_id) & (pl.col("team") == team)).row(0, named=True)


def test_mixture_reduces_to_one_normal_and_widens_when_the_starters_differ():
    sd = get_constants().margin_sd
    one = mod.nfl_qb_margin_mixture([3.0], [1.0], sigma=sd)
    assert one["lo80"] == pytest.approx(3.0 - 1.2816 * sd, abs=1e-3)
    assert one["hi80"] == pytest.approx(3.0 + 1.2816 * sd, abs=1e-3)
    assert one["win_prob"] == pytest.approx(0.5904, abs=1e-3)
    mix = mod.nfl_qb_margin_mixture([3.0, -7.0], [0.6, 0.4], sigma=sd)
    assert mix["mean"] == pytest.approx(-1.0)
    width = mix["hi80"] - mix["lo80"]
    assert width > 2 * 1.2816 * sd  # an uncertain starter widens the interval
    with pytest.raises(ValueError):
        mod.nfl_qb_margin_mixture([1.0], [0.0], sigma=sd)


def test_qb_values_shrink_career_dropbacks_strictly_before_the_date(data):
    db = mod.nfl_qb_dropbacks(data["pbp"])
    day = dt.date(2025, 10, 5)
    vals = mod.nfl_qb_values(db, as_of_date=day, config=CFG)
    raw = db.filter((pl.col("qb_id") == BROWNING) & (pl.col("game_date") < day) & pl.col("is_dropback"))
    n, total = raw.height, raw["qb_epa"].sum()
    row = vals.filter(pl.col("qb_id") == BROWNING).row(0, named=True)
    prior = CFG.league if n >= CFG.new_qb_dropbacks else CFG.replacement
    assert row["dropbacks"] == n
    assert row["qb_value"] == pytest.approx((total + CFG.k * prior) / (n + CFG.k))
    # the game on the cutoff date is not in the value: one more day takes it in
    later = mod.nfl_qb_values(db, as_of_date=day + dt.timedelta(days=1), config=CFG)
    assert later.filter(pl.col("qb_id") == BROWNING)["dropbacks"].item() > n


def test_expected_starter_is_the_last_depth_chart_snapshot_before_kickoff(starters):
    # CIN's chart moved QB1 from Burrow to Browning on 2025-09-17 and to Flacco on 2025-10-08
    assert _pick(starters, "2025_02_JAX_CIN", "CIN")["expected_qb_id"] == BURROW
    assert _pick(starters, "2025_03_CIN_MIN", "CIN")["expected_qb_id"] == BROWNING
    week6 = _pick(starters, "2025_06_CIN_GB", "CIN")
    assert (week6["expected_qb_id"], week6["starter_source"]) == (FLACCO, "depth_chart")


def test_a_snapshot_stamped_at_kickoff_is_not_used(data):
    depth = data["depth_charts"].filter(~((pl.col("team") == "CIN") & (pl.col("dt") >= "2025-10-08")))
    flacco = data["depth_charts"].filter((pl.col("team") == "CIN") & (pl.col("dt").str.starts_with("2025-10-08")))
    at_kickoff = flacco.with_columns(dt=pl.lit("2025-10-12T20:25:00Z"))  # 16:25 US Eastern
    st = mod.nfl_expected_starters(data["schedule"], depth_charts=pl.concat([depth, at_kickoff]), config=CFG)
    assert _pick(st, "2025_06_CIN_GB", "CIN")["expected_qb_id"] == BROWNING
    # and a snapshot one second after kickoff, naming another QB, is never read (no look-ahead)
    after = at_kickoff.with_columns(dt=pl.lit("2025-10-12T20:25:01Z"), gsis_id=pl.lit(BURROW))
    st = mod.nfl_expected_starters(data["schedule"], depth_charts=pl.concat([depth, after]), config=CFG)
    assert _pick(st, "2025_06_CIN_GB", "CIN")["expected_qb_id"] == BROWNING


def test_every_snapshot_used_is_older_than_kickoff(starters):
    charted = starters.filter(pl.col("starter_source") == "depth_chart")
    assert charted.height > 150
    assert (charted["chart_dt"] < charted["kickoff"]).all()


def test_report_status_sets_the_start_probability_and_the_backup(starters):
    # McCarthy stayed QB1 on the chart while Out; Wentz, QB2, started
    row = _pick(starters, "2025_04_MIN_PIT", "MIN")
    assert (row["expected_qb_id"], row["backup_qb_id"], row["status"]) == (MCCARTHY, WENTZ, "Out")
    assert row["p_start"] == CFG.p_start["Out"]
    # the report is matched to its own week: McCarthy played week 2 off no status
    week2 = _pick(starters, "2025_02_ATL_MIN", "MIN")
    assert (week2["expected_qb_id"], week2["status"], week2["p_start"]) == (MCCARTHY, "none", CFG.p_start["none"])
    assert set(starters["status"].unique()) <= set(mod.STATUSES)


def test_previous_start_is_the_fallback_without_a_chart(data):
    st = mod.nfl_expected_starters(data["schedule"], config=CFG)
    assert _pick(st, "2025_03_CIN_MIN", "CIN")["expected_qb_id"] == BURROW  # he started week 2
    assert _pick(st, "2025_01_CIN_CLE", "CIN")["expected_qb_id"] is None  # no earlier game


def _week5(data):
    return data["schedule"].filter(pl.col("week") == 5)


def test_base_arm_is_the_shipped_spine(data, starters):
    preds = mod.nfl_qb_predict_games(_week5(data), data["pbp"], starters=starters, config=CFG)
    assert preds.height == _week5(data).height
    g = preds.filter(pl.col("game_id") == "2025_05_DET_CIN").row(0, named=True)
    plays = (
        data["pbp"]
        .filter(pl.col("game_date") < "2025-10-05")
        .with_columns(pl.col("game_id").cast(pl.Utf8), gameday=pl.col("game_date").str.to_date())
    )
    eff = efficiency_ratings(plays)
    net = dict(zip(eff["team_id"].to_list(), eff["adj_net"].to_list()))
    assert g["base_margin"] == pytest.approx(predict_margin(net["CIN"], net["DET"], False))


def test_the_layer_changes_the_margin_and_is_a_no_op_at_gamma_zero(data, starters):
    week5 = _week5(data)
    on = mod.nfl_qb_predict_games(week5, data["pbp"], starters=starters, config=CFG)
    assert (on["qb_margin"] - on["base_margin"]).abs().max() > 0.5
    # MIN (McCarthy Out, Wentz the backup) is an uncertain side: its interval is wider than base
    mn = on.filter(pl.col("game_id") == "2025_05_MIN_CLE").row(0, named=True)
    assert mn["away_status"] == "Out" and mn["qb_hi80"] - mn["qb_lo80"] > mn["base_hi80"] - mn["base_lo80"]
    certain = dataclasses.replace(CFG, gamma=0.0, p_start={s: 1.0 for s in mod.STATUSES})
    off = mod.nfl_qb_predict_games(week5, data["pbp"], starters=starters, config=certain)
    for a, b in (("qb_margin", "base_margin"), ("qb_lo80", "base_lo80"), ("qb_win_prob", "base_win_prob")):
        assert (off[a] - off[b]).abs().max() == pytest.approx(0.0, abs=1e-6)


def test_week5_prices_from_pre_kickoff_plays_only(data, starters):
    week5 = _week5(data)
    full = mod.nfl_qb_predict_games(week5, data["pbp"], starters=starters, config=CFG)
    first = week5["gameday"].min()
    cut = mod.nfl_qb_predict_games(
        week5, data["pbp"].filter(pl.col("game_date") < first), starters=starters, config=CFG
    )
    # games on the first week-5 date see the same inputs whether or not later plays are in the frame
    a = full.filter(pl.col("game_id").is_in(week5.filter(pl.col("gameday") == first)["game_id"].to_list()))
    b = cut.filter(pl.col("game_id").is_in(a["game_id"].to_list()))
    assert a.height > 0 and a.sort("game_id").equals(b.sort("game_id"))


def test_realized_arm_reads_the_schedule_starter_with_certainty(data):
    preds = mod.nfl_qb_predict_games(_week5(data), data["pbp"], starter="realized", config=CFG)
    assert (preds["home_p_start"] == 1.0).all() and (preds["away_p_start"] == 1.0).all()
    sched = _week5(data).select("game_id", "home_qb_id", "away_qb_id")
    joined = preds.join(sched, on="game_id", suffix="_sched")
    assert (joined["home_qb_id"] == joined["home_qb_id_sched"]).all()
    assert (preds["home_shift"] != 0).any() and preds["home_layer_applied"].all()
    with pytest.raises(ValueError):
        mod.nfl_qb_predict_games(_week5(data), data["pbp"], config=CFG)


def test_committed_constants_match_the_fit_output():
    fit = json.loads(JSON.read_text(encoding="utf-8"))["constants"]
    assert dataclasses.asdict(QB_LAYER_CONSTANTS["modern"]) == fit


def test_the_expected_arm_never_reads_the_games_own_starter(data, starters):
    week5 = _week5(data)
    scrambled = data["schedule"].with_columns(
        home_qb_id=pl.when(pl.col("week") == 5).then(pl.lit("00-0000000")).otherwise(pl.col("home_qb_id")),
        away_qb_id=pl.when(pl.col("week") == 5).then(pl.lit("00-0000000")).otherwise(pl.col("away_qb_id")),
    )
    st = mod.nfl_expected_starters(scrambled, depth_charts=data["depth_charts"], injuries=data["injuries"], config=CFG)
    ids = week5["game_id"].to_list()
    keep = ["game_id", "team", "expected_qb_id", "backup_qb_id", "status", "p_start"]
    assert (
        st.filter(pl.col("game_id").is_in(ids))
        .select(keep)
        .sort("game_id", "team")
        .equals(starters.filter(pl.col("game_id").is_in(ids)).select(keep).sort("game_id", "team"))
    )
    a = mod.nfl_qb_predict_games(week5, data["pbp"], starters=starters, config=CFG)
    b = mod.nfl_qb_predict_games(week5, data["pbp"], starters=st, config=CFG)
    assert a.sort("game_id").equals(b.sort("game_id"))
    # the previous-start rule does read week 5's starters for week 6
    fallback_a = mod.nfl_expected_starters(data["schedule"], config=CFG)
    fallback_b = mod.nfl_expected_starters(scrambled, config=CFG)
    w6 = data["schedule"].filter(pl.col("week") == 6)["game_id"].to_list()
    assert (
        not fallback_a.filter(pl.col("game_id").is_in(w6))
        .sort("game_id", "team")
        .equals(fallback_b.filter(pl.col("game_id").is_in(w6)).sort("game_id", "team"))
    )


def test_base_arm_equals_nfl_ratings_and_nfl_predict_games(data, starters, monkeypatch):
    monkeypatch.setattr(ratings_mod, "load_nfl_pbp", lambda seasons: data["pbp"], raising=True)
    monkeypatch.setattr(ratings_mod, "load_nfl_schedule", lambda seasons: data["schedule"], raising=True)
    week5 = _week5(data)
    preds = mod.nfl_qb_predict_games(week5, data["pbp"], starters=starters, config=CFG)
    for day, games in week5.group_by("gameday"):
        ratings = ratings_mod.nfl_ratings(2025, as_of_date=dt.date.fromisoformat(day[0]))
        shipped = nfl_predict_games(
            games.select(
                "game_id",
                pl.col("home_team").alias("home_team_id"),
                pl.col("away_team").alias("away_team_id"),
                (pl.col("location") == "Neutral").alias("neutral_site"),
            ),
            ratings,
        )
        both = shipped.join(preds, on="game_id")
        assert both.height == games.height
        assert (both["exp_margin"] - both["base_margin"]).abs().max() < 1e-9
        assert (both["home_win_prob"] - both["base_win_prob"]).abs().max() < 1e-9


def test_both_prior_branches_are_applied(data):
    db = mod.nfl_qb_dropbacks(data["pbp"])
    vals = mod.nfl_qb_values(db, as_of_date=dt.date(2025, 10, 13), config=CFG)
    for row in (
        vals.filter(pl.col("dropbacks") < 100).row(0, named=True),
        vals.filter(pl.col("dropbacks") >= 100).row(0, named=True),
    ):
        prior = CFG.replacement if row["dropbacks"] < CFG.new_qb_dropbacks else CFG.league
        n = row["dropbacks"]
        assert row["qb_value"] == pytest.approx((n * row["epa_per_dropback"] + CFG.k * prior) / (n + CFG.k))


def test_mixture_quantiles_are_the_mixture_cdf():
    from scipy.stats import norm

    sd = get_constants().margin_sd
    m, w = [3.0, -7.0, 1.0], [0.5, 0.3, 0.2]
    mix = mod.nfl_qb_margin_mixture(m, w, sigma=sd)
    cdf = lambda x: sum(wi * norm.cdf(x, loc=mi, scale=sd) for mi, wi in zip(m, w))  # noqa: E731
    assert cdf(mix["lo80"]) == pytest.approx(0.10, abs=1e-6)
    assert cdf(mix["hi80"]) == pytest.approx(0.90, abs=1e-6)
    assert mix["win_prob"] == pytest.approx(1 - cdf(0.0))


def test_inputs_that_cannot_be_matched_raise(data):
    with pytest.raises(ValueError, match="injury team codes"):
        mod.nfl_expected_starters(
            data["schedule"], injuries=data["injuries"].with_columns(team=pl.lit("XXX")), config=CFG
        )
    with pytest.raises(ValueError, match="dt timestamp"):
        mod.nfl_expected_starters(data["schedule"], depth_charts=data["depth_charts"].drop("dt"), config=CFG)
    with pytest.raises(ValueError, match="gametime"):
        mod.nfl_expected_starters(data["schedule"].with_columns(gametime=pl.lit(None, dtype=pl.Utf8)), config=CFG)
    with pytest.raises(ValueError, match="p_start"):
        mod.nfl_expected_starters(data["schedule"], config=dataclasses.replace(CFG, p_start={"none": 1.0}))


def test_the_constants_json_has_its_committed_sha256():
    sha, name = JSON.with_name(JSON.name + ".sha256").read_text(encoding="utf-8").split()
    assert name == JSON.name and hashlib.sha256(JSON.read_bytes()).hexdigest() == sha
