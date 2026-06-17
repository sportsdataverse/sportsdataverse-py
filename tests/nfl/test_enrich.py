"""Offline tests for ``sportsdataverse.nfl.ep_wp.enrich_nfl_pbp`` (Task 4a).

``enrich_nfl_pbp(method="lead_diff")`` is a NFLVERSE-NATIVE orchestrator that
derives EP/EPA/WP/WPA/CP/CPOE/xYAC on an nflverse-shape play-by-play frame by
mirroring nflfastR's ``helper_add_ep_wp.R``.  It must NOT call
``calculate_epa`` / ``calculate_wpa`` (those consume ESPN-internal columns that
don't exist on a nflverse frame).

Why these tests monkeypatch the scorers
---------------------------------------
The bundled ``sportsdataverse/nfl/models/*.ubj`` files are CFB placeholders, not
real NFL models: ``ep_model.ubj`` is an 8-feature CFB tree (the NFL code feeds
18 features -> XGBoostError), ``wp_naive.ubj`` / ``cp_model.ubj`` don't exist,
and ``wp_spread.ubj`` is a 13-feature placeholder.  Real-model structural and
numeric assertions are therefore impossible in-env.  Instead we monkeypatch the
four scorer functions (``calculate_expected_points`` /
``calculate_win_probability`` / ``calculate_completion_probability`` /
``calculate_xyac``) with deterministic, model-free stubs and assert the
ORCHESTRATION WIRING:

* nflfastR pipeline order (EP -> EPA -> WP -> WPA -> CP -> CPOE -> xYAC),
* lead-over-``game_id`` grouping (no cross-game leak in ``epa`` / ``wpa``),
* the kickoff / down-NA (PAT, 2pt) feature substitution is applied before EP
  scoring,
* ``ep`` is the start-of-play estimate,
* idempotency (enrich twice == enrich once),
* method dispatch (``snapshot`` -> NotImplementedError, invalid -> ValueError),
* input-column validation against the frame contract.

The stub EP score is a deterministic function of ``yardline_100`` so that the
test can verify the substituted feature (touchback yardline, down=1, ydstogo=10)
actually reached the scorer for kickoff / down-NA rows.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.nfl import ep_wp
from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp


# ---------------------------------------------------------------------------
# Synthetic nflverse-shape frame: 2 games, mixed play types.
# ---------------------------------------------------------------------------
# Each game has: a normal play, a scoring (TD) play, a kickoff, a PAT
# (extra_point, down NA), and an end-of-half play.  Two distinct games verify
# the lead/shift never leaks across the game boundary.


def _synthetic_frame() -> pl.DataFrame:
    rows: list[dict] = []

    def game(gid: str, home: str, away: str, season: int) -> list[dict]:
        return [
            # play_id 1: normal 1st-down run, posteam == home
            dict(
                game_id=gid,
                play_id=1,
                season=season,
                game_half="Half1",
                posteam=home,
                defteam=away,
                home_team=home,
                away_team=away,
                home_score=0,
                away_score=0,
                qtr=1,
                half_seconds_remaining=1800.0,
                game_seconds_remaining=3600.0,
                yardline_100=75,
                ydstogo=10,
                down=1,
                posteam_timeouts_remaining=3,
                defteam_timeouts_remaining=3,
                score_differential=0,
                spread_line=-3.0,
                receive_2h_ko=0,
                roof="outdoors",
                play_type="run",
                desc="(15:00) run up the middle for 5 yards",
                kickoff_attempt=0,
                two_point_attempt=0,
                extra_point_attempt=0,
                td_team=None,
                field_goal_made=0,
                extra_point_good=0,
                air_yards=None,
                complete_pass=0,
                sp=0,
                touchdown=0,
            ),
            # play_id 2: passing TD by posteam == home
            dict(
                game_id=gid,
                play_id=2,
                season=season,
                game_half="Half1",
                posteam=home,
                defteam=away,
                home_team=home,
                away_team=away,
                home_score=7,
                away_score=0,
                qtr=1,
                half_seconds_remaining=1700.0,
                game_seconds_remaining=3500.0,
                yardline_100=8,
                ydstogo=8,
                down=1,
                posteam_timeouts_remaining=3,
                defteam_timeouts_remaining=3,
                score_differential=0,
                spread_line=-3.0,
                receive_2h_ko=0,
                roof="outdoors",
                play_type="pass",
                desc="(13:20) pass complete for 8 yards, TOUCHDOWN",
                kickoff_attempt=0,
                two_point_attempt=0,
                extra_point_attempt=0,
                td_team=home,
                field_goal_made=0,
                extra_point_good=0,
                air_yards=8.0,
                complete_pass=1,
                sp=1,
                touchdown=1,
            ),
            # play_id 3: extra point (PAT) — down is NA
            dict(
                game_id=gid,
                play_id=3,
                season=season,
                game_half="Half1",
                posteam=home,
                defteam=away,
                home_team=home,
                away_team=away,
                home_score=7,
                away_score=0,
                qtr=1,
                half_seconds_remaining=1700.0,
                game_seconds_remaining=3500.0,
                yardline_100=15,
                ydstogo=0,
                down=None,
                posteam_timeouts_remaining=3,
                defteam_timeouts_remaining=3,
                score_differential=7,
                spread_line=-3.0,
                receive_2h_ko=0,
                roof="outdoors",
                play_type="extra_point",
                desc="extra point GOOD",
                kickoff_attempt=0,
                two_point_attempt=0,
                extra_point_attempt=1,
                td_team=None,
                field_goal_made=0,
                extra_point_good=1,
                air_yards=None,
                complete_pass=0,
                sp=1,
                touchdown=0,
            ),
            # play_id 4: kickoff — down is NA
            dict(
                game_id=gid,
                play_id=4,
                season=season,
                game_half="Half1",
                posteam=home,
                defteam=away,
                home_team=home,
                away_team=away,
                home_score=8,
                away_score=0,
                qtr=1,
                half_seconds_remaining=1690.0,
                game_seconds_remaining=3490.0,
                yardline_100=35,
                ydstogo=0,
                down=None,
                posteam_timeouts_remaining=3,
                defteam_timeouts_remaining=3,
                score_differential=8,
                spread_line=-3.0,
                receive_2h_ko=0,
                roof="outdoors",
                play_type="kickoff",
                desc="kicks 65 yards, Kick formation",
                kickoff_attempt=1,
                two_point_attempt=0,
                extra_point_attempt=0,
                td_team=None,
                field_goal_made=0,
                extra_point_good=0,
                air_yards=None,
                complete_pass=0,
                sp=0,
                touchdown=0,
            ),
            # play_id 5: end of half play, posteam == away
            dict(
                game_id=gid,
                play_id=5,
                season=season,
                game_half="Half1",
                posteam=away,
                defteam=home,
                home_team=home,
                away_team=away,
                home_score=8,
                away_score=0,
                qtr=2,
                half_seconds_remaining=2.0,
                game_seconds_remaining=1802.0,
                yardline_100=50,
                ydstogo=10,
                down=1,
                posteam_timeouts_remaining=0,
                defteam_timeouts_remaining=1,
                score_differential=-8,
                spread_line=-3.0,
                receive_2h_ko=1,
                roof="outdoors",
                play_type="pass",
                desc="(0:02) pass incomplete END QUARTER 2",
                kickoff_attempt=0,
                two_point_attempt=0,
                extra_point_attempt=0,
                td_team=None,
                field_goal_made=0,
                extra_point_good=0,
                air_yards=12.0,
                complete_pass=0,
                sp=0,
                touchdown=0,
            ),
        ]

    rows.extend(game("2023_01_AAA_BBB", "BBB", "AAA", 2023))
    rows.extend(game("2014_01_CCC_DDD", "DDD", "CCC", 2014))  # pre-2016 -> touchback 80
    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Deterministic, model-free scorer stubs.
# ---------------------------------------------------------------------------
# Recorder captures the yardline_100 the EP scorer actually received, per
# game_id+play_id, so the substitution test can verify the touchback yardline
# reached the model for kickoff / down-NA rows.


def _stub_ep_factory(recorder: dict) -> object:
    def _stub_ep(df: pl.DataFrame, *, return_as_pandas: bool = False):  # noqa: ANN001
        for r in df.select("game_id", "play_id", "yardline_100", "down", "ydstogo").iter_rows(named=True):
            recorder[(r["game_id"], r["play_id"])] = (r["yardline_100"], r["down"], r["ydstogo"])
        # ep is a deterministic linear function of (substituted) yardline_100 so
        # callers can verify which features reached the scorer.
        out = df.with_columns(((100.0 - pl.col("yardline_100").cast(pl.Float64)) / 10.0).alias("ep"))
        # add the 7 class-prob columns nflfastR EP emits (dummy uniform)
        for name in ep_wp._EP_CLASS_NAMES:
            out = out.with_columns(pl.lit(1.0 / 7.0).alias(name))
        return out.to_pandas() if return_as_pandas else out

    return _stub_ep


def _stub_wp(df: pl.DataFrame, *, return_as_pandas: bool = False):  # noqa: ANN001
    # wp = naive function of score_differential, vegas_wp shifted slightly.
    out = df.with_columns(
        (0.5 + pl.col("score_differential").cast(pl.Float64) / 100.0).clip(0.0, 1.0).alias("wp"),
    )
    out = out.with_columns((pl.col("wp") * 0.99).alias("vegas_wp"))
    return out.to_pandas() if return_as_pandas else out


def _stub_cp(df: pl.DataFrame, *, return_as_pandas: bool = False):  # noqa: ANN001
    out = df.with_columns(
        pl.when(pl.col("air_yards").is_not_null()).then(0.6).otherwise(None).alias("cp"),
    )
    if "complete_pass" in out.columns:
        out = out.with_columns((pl.col("complete_pass").cast(pl.Float64) - pl.col("cp")).alias("cpoe"))
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Float64).alias("cpoe"))
    return out.to_pandas() if return_as_pandas else out


def _stub_xyac(df: pl.DataFrame, *, return_as_pandas: bool = False):  # noqa: ANN001
    out = df.with_columns(
        pl.when(pl.col("air_yards").is_not_null()).then(4.5).otherwise(None).alias("xyac_mean_yardage"),
        pl.when(pl.col("air_yards").is_not_null()).then(4.0).otherwise(None).alias("xyac_median_yardage"),
        pl.when(pl.col("air_yards").is_not_null()).then(2.0).otherwise(None).alias("xyac_sd_yardage"),
        pl.when(pl.col("air_yards").is_not_null()).then(0.6).otherwise(None).alias("xyac_prob_complete"),
    )
    return out.to_pandas() if return_as_pandas else out


@pytest.fixture()
def patched(monkeypatch):  # noqa: ANN001
    recorder: dict = {}
    monkeypatch.setattr(ep_wp, "calculate_expected_points", _stub_ep_factory(recorder))
    monkeypatch.setattr(ep_wp, "calculate_win_probability", _stub_wp)
    monkeypatch.setattr(ep_wp, "calculate_completion_probability", _stub_cp)
    monkeypatch.setattr(ep_wp, "calculate_xyac", _stub_xyac)
    return recorder


# ---------------------------------------------------------------------------
# Dispatch + validation
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_snapshot_not_implemented(self) -> None:
        df = _synthetic_frame()
        with pytest.raises(NotImplementedError):
            enrich_nfl_pbp(df, method="snapshot")

    def test_invalid_method_raises_valueerror(self) -> None:
        df = _synthetic_frame()
        with pytest.raises(ValueError):
            enrich_nfl_pbp(df, method="bogus")

    def test_missing_required_columns_raises(self) -> None:
        df = _synthetic_frame().drop("yardline_100")
        with pytest.raises((ValueError, KeyError)) as exc:
            enrich_nfl_pbp(df, method="lead_diff")
        # error references the contract / the missing column
        assert "yardline_100" in str(exc.value) or "contract" in str(exc.value).lower()


# ---------------------------------------------------------------------------
# lead_diff orchestration wiring (monkeypatched scorers)
# ---------------------------------------------------------------------------


class TestLeadDiffWiring:
    def test_all_output_columns_present(self, patched) -> None:  # noqa: ANN001
        out = enrich_nfl_pbp(_synthetic_frame(), method="lead_diff")
        for col in ("ep", "epa", "wp", "vegas_wp", "def_wp", "home_wp", "away_wp", "wpa", "cp", "cpoe"):
            assert col in out.columns, f"missing output column {col}"
        for col in ("xyac_mean_yardage", "xyac_median_yardage", "xyac_sd_yardage", "xyac_prob_complete"):
            assert col in out.columns, f"missing xyac column {col}"

    def test_returns_polars_by_default(self, patched) -> None:  # noqa: ANN001
        out = enrich_nfl_pbp(_synthetic_frame(), method="lead_diff")
        assert isinstance(out, pl.DataFrame)

    def test_return_as_pandas(self, patched) -> None:  # noqa: ANN001
        import pandas as pd

        out = enrich_nfl_pbp(_synthetic_frame(), method="lead_diff", return_as_pandas=True)
        assert isinstance(out, pd.DataFrame)

    def test_ep_is_start_of_play(self, patched) -> None:  # noqa: ANN001
        # The stub ep = (100 - yardline_100)/10 for NORMAL plays (no substitution).
        out = enrich_nfl_pbp(_synthetic_frame(), method="lead_diff")
        normal = out.filter((pl.col("play_id") == 1) & (pl.col("game_id") == "2023_01_AAA_BBB"))
        # yardline_100 == 75 -> ep == 2.5  (start-of-play estimate, not lead)
        assert abs(normal["ep"][0] - 2.5) < 1e-9

    def test_kickoff_feature_substitution_post_2016(self, patched) -> None:  # noqa: ANN001
        # 2023 game kickoff (play_id 4) must be scored with yardline_100=75, down=1, ydstogo=10.
        enrich_nfl_pbp(_synthetic_frame(), method="lead_diff")
        yard, down, ytg = patched[("2023_01_AAA_BBB", 4)]
        assert yard == 75, f"post-2016 kickoff should score at touchback 75, got {yard}"
        assert down == 1 and ytg == 10

    def test_kickoff_feature_substitution_pre_2016(self, patched) -> None:  # noqa: ANN001
        # 2014 game kickoff (play_id 4) must be scored with yardline_100=80.
        enrich_nfl_pbp(_synthetic_frame(), method="lead_diff")
        yard, down, ytg = patched[("2014_01_CCC_DDD", 4)]
        assert yard == 80, f"pre-2016 kickoff should score at touchback 80, got {yard}"
        assert down == 1 and ytg == 10

    def test_down_na_pat_substitution(self, patched) -> None:  # noqa: ANN001
        # PAT (play_id 3) has down NA -> substituted to down=1, ydstogo=10.
        enrich_nfl_pbp(_synthetic_frame(), method="lead_diff")
        _, down, ytg = patched[("2023_01_AAA_BBB", 3)]
        assert down == 1 and ytg == 10

    def test_normal_play_not_substituted(self, patched) -> None:  # noqa: ANN001
        # Normal play (play_id 1) keeps raw features.
        enrich_nfl_pbp(_synthetic_frame(), method="lead_diff")
        yard, down, ytg = patched[("2023_01_AAA_BBB", 1)]
        assert yard == 75 and down == 1 and ytg == 10  # already 1st & 10 at the 25

    def test_no_cross_game_leak_in_epa(self, patched) -> None:  # noqa: ANN001
        # The last play of each game (play_id 5) must not derive epa from the
        # NEXT game's first play.  We verify by ensuring per-game last-row epa
        # is computed within the game (here the end-of-half overlay sets it
        # using only the in-game ep, never the other game's lead).
        out = enrich_nfl_pbp(_synthetic_frame(), method="lead_diff")
        # Build a single-game enrich and compare the same play_id rows; if the
        # lead leaked across games, the two-game frame would differ from the
        # per-game frame on the boundary row.
        g1 = _synthetic_frame().filter(pl.col("game_id") == "2023_01_AAA_BBB")
        out_g1 = enrich_nfl_pbp(g1, method="lead_diff")
        merged = out.filter(pl.col("game_id") == "2023_01_AAA_BBB").sort("play_id")
        solo = out_g1.sort("play_id")
        # epa column must match between the two-game and single-game runs
        assert merged["epa"].to_list() == solo["epa"].to_list(), "epa leaked across game boundary"

    def test_idempotent(self, patched) -> None:  # noqa: ANN001
        once = enrich_nfl_pbp(_synthetic_frame(), method="lead_diff")
        twice = enrich_nfl_pbp(once, method="lead_diff")
        # Enriching an already-enriched frame must reproduce the same outputs.
        for col in ("ep", "epa", "wp", "vegas_wp", "wpa", "cp", "cpoe"):
            a = once[col].to_list()
            b = twice[col].to_list()
            assert a == b, f"column {col} not idempotent under re-enrich"

    def test_default_method_is_lead_diff(self, patched) -> None:  # noqa: ANN001
        out_default = enrich_nfl_pbp(_synthetic_frame())
        out_explicit = enrich_nfl_pbp(_synthetic_frame(), method="lead_diff")
        assert out_default["ep"].to_list() == out_explicit["ep"].to_list()

    def test_wpa_perspective_posteam(self, patched) -> None:  # noqa: ANN001
        # wpa must be from the POSSESSION team's perspective:
        # wpa = home_wpa when posteam == home_team, else -home_wpa.
        out = enrich_nfl_pbp(_synthetic_frame(), method="lead_diff").sort("game_id", "play_id")
        g = out.filter(pl.col("game_id") == "2023_01_AAA_BBB").sort("play_id")
        # home_wp lead difference for play 1 -> play 2 (both posteam == home)
        hw = g["home_wp"].to_list()
        wpa = g["wpa"].to_list()
        # play_id 1 posteam == home -> wpa == home_wp[1] - home_wp[0]
        expected = hw[1] - hw[0]
        assert abs(wpa[0] - expected) < 1e-9
