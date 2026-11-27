"""nfl_adjusted_epa_ledger on real NFL play-by-play (see tests/fixtures/nfl_adjusted_epa/README.md)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse.cfb import cfb_adjusted_epa, cfb_adjusted_epa_ledger
from sportsdataverse.cfb.cfb_adjusted_epa import _ADJ_REQUIRED, _prepare
from sportsdataverse.nfl import nfl_adjusted_epa_ledger
from sportsdataverse.nfl.nfl_adjusted_epa import _nfl_plays

_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_adjusted_epa"


@pytest.fixture(scope="module")
def plays_2024() -> pl.DataFrame:
    return pl.read_parquet(_DIR / "nfl_plays_2024_reg.parquet")


@pytest.fixture(scope="module")
def ledger_2024(plays_2024) -> pl.DataFrame:
    return nfl_adjusted_epa_ledger(plays_2024)


@pytest.fixture(scope="module")
def espn_plays() -> pl.DataFrame:
    # all three games were at the home team's stadium (nflverse schedule location "Home")
    return pl.read_parquet(_DIR / "espn_processed_plays_2024_wk1_2.parquet").with_columns(neutral_site=pl.lit(False))


def _same(a: pl.DataFrame, b: pl.DataFrame) -> None:
    # polars' threaded group-by means can differ in the last bit between two runs of the same fit
    assert_frame_equal(a, b, check_exact=False, rel_tol=0, abs_tol=1e-12)


def test_contributions_sum_to_the_season_value_for_every_team(plays_2024, ledger_2024) -> None:
    season = cfb_adjusted_epa(_nfl_plays(plays_2024)).select("team_id", "net_adj_epa")
    sums = ledger_2024.group_by("team_id").agg(total=pl.col("contribution").sum(), net=pl.col("net_adj_epa").first())
    joined = season.join(sums, on="team_id", how="full", coalesce=True)
    assert joined.height == 32 and joined.null_count().sum_horizontal().item() == 0
    assert (joined["total"] - joined["net_adj_epa"]).abs().max() < 1e-9
    assert (joined["net"] - joined["net_adj_epa"]).abs().max() < 1e-9
    assert ledger_2024.height == 2 * 272  # every 2024 regular-season game, both teams
    assert ledger_2024["season_type"].unique().to_list() == ["REG"]


def test_leave_one_game_out_equals_refitting_without_the_game(plays_2024, ledger_2024) -> None:
    # every KC game against the public season fit run without it (the full season, all 272 games, was checked
    # the same way when the fixture was added: max gap 5.6e-17)
    norm = _nfl_plays(plays_2024)
    rows = ledger_2024.filter(pl.col("team_id") == "KC")
    assert rows.height == 17
    for row in rows.iter_rows(named=True):
        refit = cfb_adjusted_epa(norm.filter(pl.col("game_id") != row["game_id"]))
        expected = refit.filter(pl.col("team_id") == "KC")["net_adj_epa"].item()
        assert row["loo_net_adj_epa"] == pytest.approx(expected, abs=1e-9)
        assert row["game_influence"] == pytest.approx(row["net_adj_epa"] - expected, abs=1e-9)


def test_matches_the_validated_team_summaries_frame(plays_2024, ledger_2024) -> None:
    # Values from the frame the 2014-2023 walk-forward scored: nfl-data's nfl_team_summaries.input.prepare_plays
    # on the same release asset (plus period = qtr), run through cfb_adjusted_epa there, independently of
    # _nfl_plays. A wrong score_differential / qtr / home_team / play mapping moves these.
    base, clean = _prepare(_nfl_plays(plays_2024), _ADJ_REQUIRED, None)
    assert base.height == 33_335  # pass / rush plays with EPA
    assert clean.height == 32_086  # of which not garbage time
    golden = {
        "BAL": 0.3253703242007089,
        "CAR": -0.2789117200219179,
        "DET": 0.2615145993558845,
        "KC": 0.06691249112746177,
    }
    got = dict(ledger_2024.group_by("team_id").agg(pl.col("net_adj_epa").first()).rows())
    for team, value in golden.items():
        assert got[team] == pytest.approx(value, abs=1e-9)
    # who was home, and who was played
    opener = ledger_2024.filter(pl.col("game_id") == "2024_01_BAL_KC").sort("team_id")
    assert opener.select("team_id", "opponent_id", "site").rows() == [("BAL", "KC", "away"), ("KC", "BAL", "home")]


def test_only_scrimmage_plays_with_a_down_are_kept(plays_2024) -> None:
    kept = _nfl_plays(plays_2024)
    assert set(kept["play_type"].unique().to_list()) == {"pass", "run"}
    assert kept["down"].null_count() == 0
    # the fixture holds the plays that must not count: kneels, spikes, no-plays, kicks, conversions
    dropped = set(plays_2024["play_type"].drop_nulls().unique().to_list()) - {"pass", "run"}
    assert {"qb_kneel", "qb_spike", "no_play", "kickoff", "punt"} <= dropped
    assert (kept["pass"] + kept["rush"] == 1).all()


def test_neutral_sites_come_from_the_schedule_location(ledger_2024, plays_2024) -> None:
    # 2024 had five neutral-site regular-season games (Sao Paulo, three in London, Munich)
    neutral = ledger_2024.filter(pl.col("site") == "neutral")["game_id"].unique().sort().to_list()
    assert neutral == ["2024_01_GB_PHI", "2024_05_NYJ_MIN", "2024_06_JAX_CHI", "2024_07_NE_JAX", "2024_10_NYG_CAR"]
    flagged = nfl_adjusted_epa_ledger(
        plays_2024.with_columns(neutral_site=pl.col("location") == "Neutral").drop("location"), influence=False
    )
    _same(flagged, nfl_adjusted_epa_ledger(plays_2024, influence=False))


def test_missing_columns_raise(plays_2024, espn_plays) -> None:
    with pytest.raises(KeyError, match="neutral_site or location"):
        nfl_adjusted_epa_ledger(plays_2024.drop("location"))
    with pytest.raises(KeyError, match="score_differential"):
        nfl_adjusted_epa_ledger(plays_2024.drop("score_differential"))
    with pytest.raises(KeyError, match="neutral_site"):
        nfl_adjusted_epa_ledger(espn_plays.drop("neutral_site"))


def test_espn_processor_frames_live_and_released_names_agree(espn_plays) -> None:
    # live NFLPlayProcess: pos_team / def_pos_team are the numeric ids; a released frame names them
    # pos_team_id / def_pos_team_id and makes pos_team the team name
    live = nfl_adjusted_epa_ledger(espn_plays)
    released = nfl_adjusted_epa_ledger(
        espn_plays.rename({"pos_team": "pos_team_id", "def_pos_team": "def_pos_team_id"}).with_columns(
            pos_team=pl.col("start.pos_team.name"), def_pos_team=pl.col("start.def_pos_team.name")
        )
    )
    _same(live, released)
    # BAL (33) and KC (12) each played two of the three games; LV and CIN one, so are not rated
    assert sorted(live["team_id"].unique().to_list()) == ["12", "33"]
    assert live.schema["team_id"] == pl.Utf8 and live["season_type"].unique().to_list() == [2]
    # the same plays straight into the college ledger (kneels dropped, ids as strings) give the same frame
    not_scrimmage = pl.col("text").str.contains(r"(?i)\bspiked? the ball\b|\bno play\b")
    direct = espn_plays.filter((pl.col("kneel_down") == False) & (not_scrimmage == False)).with_columns(  # noqa: E712
        pl.col("game_id").cast(pl.Utf8),
        pos_team_id=pl.col("pos_team").cast(pl.Utf8),
        def_pos_team_id=pl.col("def_pos_team").cast(pl.Utf8),
        pos_team=pl.col("pos_team").cast(pl.Utf8),
        home=pl.col("homeTeamId").cast(pl.Utf8),
        **{"pass": pl.col("pass").cast(pl.Int8), "rush": pl.col("rush").cast(pl.Int8)},
    )
    assert espn_plays["kneel_down"].sum() > 0  # the fixture has kneels to drop
    assert espn_plays.filter(pl.col("text").str.contains("spiked the ball")).height == 2  # and spikes
    _same(cfb_adjusted_epa_ledger(direct), live)


def test_pandas_in_and_out(plays_2024) -> None:
    plain = nfl_adjusted_epa_ledger(plays_2024, influence=False)
    assert "game_influence" not in plain.columns
    pdf = nfl_adjusted_epa_ledger(plays_2024.to_pandas(), influence=False, return_as_pandas=True)
    assert list(pdf.columns) == plain.columns and len(pdf) == plain.height


def test_espn_and_nflverse_shapes_keep_the_same_plays_per_game(plays_2024, espn_plays) -> None:
    # the same three games from both sources: once kneels, spikes and no-play penalties are out, the play counts agree
    ids = {"401671789": "2024_01_BAL_KC", "401671624": "2024_02_LV_BAL", "401671670": "2024_02_CIN_KC"}

    def per_game(frame: pl.DataFrame) -> dict[str, int]:
        base, _ = _prepare(_nfl_plays(frame), _ADJ_REQUIRED, None)
        return dict(base.group_by("game_id").len().rows())

    espn = {ids[g]: n for g, n in per_game(espn_plays).items()}
    nflverse = per_game(plays_2024.filter(pl.col("game_id").is_in(list(ids.values()))))
    assert espn == nflverse == {"2024_01_BAL_KC": 123, "2024_02_LV_BAL": 120, "2024_02_CIN_KC": 120}
