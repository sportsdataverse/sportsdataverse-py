"""Processor-invariant checks (``tools/validation/checks/pbp_invariants.py``).

Synthetic frames validate the CHECK logic: each rule is shown clean on a
well-formed game and firing on a single mutated value. One smoke test runs the
real NFL processor offline on the committed fixture and evaluates every group.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from tools.validation.checks import pbp_invariants as inv
from tools.validation.findings import CheckContext, Severity

FIX = Path(__file__).resolve().parents[1] / "nfl" / "fixtures" / "summary_401872922.json"
HOME, AWAY = 1, 2


def _game(**overrides) -> pl.DataFrame:
    """A 6-play, internally consistent game: home drive (rush, rush TD), away drive (pass, sack), timeout, 2H rush."""
    cols = {
        "id": [11, 12, 13, 14, 15, 16],
        "period.number": [1, 1, 2, 2, 2, 3],
        "clock.displayValue": ["10:00", "9:30", "5:00", "4:30", "4:00", "15:00"],
        "type.text": ["Rush", "Rushing Touchdown", "Pass Reception", "Sack", "Timeout", "Rush"],
        "text": [
            "A.Back run for 5 yards",
            "A.Back run for 20 yards, TOUCHDOWN (K.Icker kick)",
            "Q.B pass complete to W.R for 8 yards",
            "Q.B sacked by D.End for -6 yards",
            "Timeout #1 by HOM",
            "A.Back run for 3 yards",
        ],
        "drive.id": ["d1", "d1", "d2", "d2", "d2", "d3"],
        "drive.team.abbreviation": ["HOM", "HOM", "AWY", "AWY", "AWY", "HOM"],
        "homeTeamAbbrev": ["HOM"] * 6,
        "awayTeamAbbrev": ["AWY"] * 6,
        "homeTeamId": [HOME] * 6,
        "awayTeamId": [AWAY] * 6,
        "start.pos_team.id": [HOME, HOME, AWAY, AWAY, AWAY, HOME],
        "end.pos_team.id": [HOME, HOME, AWAY, AWAY, AWAY, HOME],
        "pos_team": [HOME, HOME, AWAY, AWAY, AWAY, HOME],
        "start.down": [1, 2, 1, 2, None, 1],
        "start.distance": [10, 5, 10, 2, None, 10],
        "start.downDistanceText": ["1st & 10", "2nd & 5", "1st & 10", "2nd & 2", None, "1st & 10"],
        "start.yardsToEndzone": [25, 20, 75, 67, 73, 75],
        "end.yardsToEndzone": [20, 0, 67, 73, 73, 72],
        "statYardage": [5, 20, 8, -6, 0, 3],
        "yds_rushed": [5, 20, None, None, None, 3],
        "yds_receiving": [None, None, 8, None, None, None],
        "yds_sacked": [None, None, None, -6, None, None],
        "scoringPlay": [False, True, False, False, False, False],
        "scoring_play": [False, True, False, False, False, False],
        "td_play": [False, True, False, False, False, False],
        "start.homeScore": [0, 0, 7, 7, 7, 7],
        "end.homeScore": [0, 7, 7, 7, 7, 7],
        "start.awayScore": [0] * 6,
        "end.awayScore": [0] * 6,
        "start.homeTeamTimeouts": [3, 3, 3, 3, 3, 3],
        "end.homeTeamTimeouts": [3, 3, 3, 3, 2, 3],
        "start.awayTeamTimeouts": [3] * 6,
        "end.awayTeamTimeouts": [3] * 6,
        "rush": [True, True, False, False, False, True],
        "pass": [False, False, True, True, False, False],
        "sack": [False, False, False, True, False, False],
        "pass_attempt": [False, False, True, False, False, False],
        "completion": [False, False, True, False, False, False],
        "int": [False] * 6,
        "rush_td": [False, True, False, False, False, False],
        "pass_td": [False] * 6,
        "penalty_flag": [False] * 6,
        "penalty_no_play": [False] * 6,
        "penalty_in_text": [False] * 6,
        "end_of_half": [False] * 6,
        "fumble_vec": [False] * 6,
        "fg_attempt": [False] * 6,
        "punt": [False] * 6,
        "punt_blocked": [False] * 6,
        "kickoff_play": [False] * 6,
        "scrimmage_play": [True, True, True, True, False, True],
        "EP_start": [3.0, 3.5, 0.5, 0.2, -0.5, 0.5],
        "EP_end": [3.5, 7.0, 0.8, -0.5, -0.5, 0.6],
        "EPA": [0.5, 3.5, 0.3, -0.7, 0.0, 0.1],
        "wp_before": [0.5, 0.55, 0.3, 0.32, 0.3, 0.8],
        "wp_after": [0.55, 0.7, 0.32, 0.3, 0.3, 0.9],
        "home_wp_before": [0.5, 0.55, 0.7, 0.68, 0.7, 0.8],
        "home_wp_after": [0.55, 0.7, 0.68, 0.7, 0.8, 1.0],
        "wpa": [0.05, 0.15, 0.02, -0.02, 0.0, 0.25],
        "passer_player_name": [None, None, "Q.B", "Q.B", None, None],
        "receiver_player_name": [None, None, "W.R", None, None, None],
        "rusher_player_name": ["A.Back", "A.Back", None, None, None, "A.Back"],
        "sack_player_name": [None, None, None, "D.End", None, None],
    }
    for key, (idx, value) in overrides.items():
        cols[key.replace("__", ".")][idx] = value
    return pl.DataFrame(cols, strict=False)


def _summary(home=7, away=0, raw_ids=(11, 12, 13, 14, 15, 16)):
    return {
        "header": {
            "competitions": [
                {
                    "competitors": [
                        {"homeAway": "home", "score": str(home), "team": {"id": str(HOME)}},
                        {"homeAway": "away", "score": str(away), "team": {"id": str(AWAY)}},
                    ]
                }
            ]
        },
        "drives": {
            "previous": [{"plays": [{"id": str(i), "text": f"play {i}", "type": {"text": "Rush"}} for i in raw_ids]}]
        },
    }


def _by_rule(df, **kw):
    return {r.rule: r for r in inv.evaluate(df, **kw)}


def _fires(df, rule, **kw):
    r = _by_rule(df, **kw)[rule]
    assert r.n_checked > 0, rule
    return r.n_violations


def test_clean_game_fires_no_error_rules():
    fired = [
        r.rule for r in inv.evaluate(_game(), summary=_summary()) if r.n_violations and r.severity is Severity.ERROR
    ]
    assert fired == []


# 1 --------------------------------------------------------------------------


def test_timeouts():
    assert _fires(_game(), "timeouts.timeout_row_not_charged") == 0
    assert _fires(_game(**{"end__homeTeamTimeouts": (4, 3)}), "timeouts.timeout_row_not_charged") == 1
    both = _game(**{"end__awayTeamTimeouts": (4, 2)})
    assert _fires(both, "timeouts.timeout_row_charged_both") == 1
    assert _fires(_game(**{"end__homeTeamTimeouts": (0, 2)}), "timeouts.charged_on_non_timeout_row") == 1
    assert _fires(_game(**{"start__awayTeamTimeouts": (2, -1)}), "timeouts.range") == 1
    overtime = _game(**{"period__number": (5, 5), "start__awayTeamTimeouts": (5, -1), "end__awayTeamTimeouts": (5, -1)})
    assert _fires(overtime, "timeouts.range_overtime") == 1
    assert _fires(overtime, "timeouts.range") == 0
    # no second-half reset: the 2H opener still carries the 1H's 2
    no_reset = _game(**{"start__homeTeamTimeouts": (5, 2), "end__homeTeamTimeouts": (5, 2)})
    assert _fires(no_reset, "timeouts.second_half_reset") == 1
    grow = _game(**{"end__awayTeamTimeouts": (2, 2)})
    assert _fires(grow, "timeouts.increase_within_half") == 1
    # the timeout row (2 left) is Q4, the next row opens overtime with 3: a re-allotment, not an increase
    ot_realloc = _game().with_columns(pl.Series("period.number", [1, 1, 2, 2, 4, 5]))
    assert _fires(ot_realloc, "timeouts.increase_within_half") == 0


# 2 --------------------------------------------------------------------------


def test_field_position():
    assert _fires(_game(**{"end__yardsToEndzone": (0, 101)}), "ytg.end_range") == 1
    assert _fires(_game(), "ytg.continuity") == 0
    assert _fires(_game(**{"start__yardsToEndzone": (1, 22)}), "ytg.continuity") == 1
    assert _fires(_game(), "ytg.td_start_eq_yards_gained") == 0
    assert _fires(_game(**{"yds_rushed": (1, 19)}), "ytg.td_start_eq_yards_gained") == 1
    assert _fires(_game(**{"start__yardsToEndzone": (5, 0)}), "ytg.scrimmage_start_1_99") == 1
    # row 0: HOM offense 25 yards from the endzone == "at AWY 25"; on its own 25 it would be 75
    spot = "ytg.start_matches_down_distance_text"
    assert _fires(_game(**{"start__downDistanceText": (0, "1st & 10 at AWY 25")}), spot) == 0
    assert _fires(_game(**{"start__downDistanceText": (0, "1st & 10 at HOM 25")}), spot) == 1
    # an unrecognized side code only admits the two mirror values
    assert _fires(_game(**{"start__downDistanceText": (0, "1st & 10 at XYZ 25")}), spot) == 0
    assert _fires(_game(**{"start__downDistanceText": (0, "1st & 10 at XYZ 30")}), spot) == 1
    placeholder = _game(**{"start__downDistanceText": (0, "1st & 10 at HOM 0")})
    assert _by_rule(placeholder)[spot].n_checked == 0  # "at HOM 0" is a placeholder, not a spot


# 3 --------------------------------------------------------------------------


def test_score():
    assert _fires(_game(), "score.final_matches_header", summary=_summary()) == 0
    assert _fires(_game(), "score.final_matches_header", summary=_summary(home=14)) == 1
    dip = _game(**{"end__homeScore": (3, 0)})
    assert _fires(dip, "score.monotone") == 1
    assert _fires(_game(**{"end__awayScore": (0, 3)}), "score.change_on_non_scoring_play") == 1
    assert _fires(_game(**{"end__homeScore": (1, 5)}), "score.delta_value") == 1
    assert _fires(_game(**{"end__homeScore": (1, 0)}), "score.scoring_play_without_change") == 1


# 4 --------------------------------------------------------------------------


def test_possession():
    assert _fires(_game(), "poss.end_team_flips_without_change") == 0
    assert _fires(_game(**{"end__pos_team__id": (0, AWAY)}), "poss.end_team_flips_without_change") == 1
    assert _fires(_game(), "poss.offense_matches_drive_team") == 0
    assert _fires(_game(**{"drive__team__abbreviation": (2, "HOM")}), "poss.offense_matches_drive_team") == 1
    assert _fires(_game(**{"start__pos_team__id": (3, HOME)}), "poss.offense_constant_within_drive") == 1
    punt = _game(
        **{
            "punt": (3, True),
            "type__text": (3, "Punt"),
            "pass": (3, False),
            "sack": (3, False),
            "start__pos_team__id": (5, AWAY),
            "period__number": (5, 2),
        }
    )
    assert _fires(punt, "poss.punt_changes_possession") == 1


# 5 --------------------------------------------------------------------------


def test_down_distance():
    assert _fires(_game(**{"start__down": (0, 5)}), "down.scrimmage_down_1_4") == 1
    assert _fires(_game(**{"start__distance": (0, 0)}), "down.scrimmage_distance_ge_1") == 1
    assert _fires(_game(**{"start__distance": (1, 21)}), "down.distance_le_ytg") == 1
    goal = _game(**{"start__downDistanceText": (1, "2nd & Goal"), "start__distance": (1, 5)})
    assert _fires(goal, "down.goal_to_go_distance_eq_ytg") == 1
    assert (
        _fires(
            _game(**{"start__downDistanceText": (1, "2nd & Goal"), "start__distance": (1, 20)}),
            "down.goal_to_go_distance_eq_ytg",
        )
        == 0
    )


# 6 --------------------------------------------------------------------------


def test_ep_wp():
    assert _fires(_game(**{"EP_start": (0, 7.5)}), "ep.start_range") == 1
    assert _fires(_game(), "ep.offense_td_end_not_realized") == 0
    assert _fires(_game(**{"EP_end": (1, 5.1)}), "ep.offense_td_end_not_realized") == 1
    # CFB emits EP_end as Float32: 6.92 arrives as 6.920000076..., still a realized value
    f32 = _game(**{"EP_end": (1, 6.92)}).with_columns(pl.col("EP_end").cast(pl.Float32))
    assert _fires(f32, "ep.offense_td_end_not_realized") == 0
    assert _fires(f32, "ep.offense_td_pat_in_text_unresolved") == 1
    # the TD text carries "(K.Icker kick)": the try is known, 6.92 is the unknown-PAT fallback
    assert _fires(_game(), "ep.offense_td_pat_in_text_unresolved") == 0
    assert _fires(_game(**{"EP_end": (1, 6.92)}), "ep.offense_td_pat_in_text_unresolved") == 1
    missed = _game(**{"text": (1, "A.Back run for 20 yards, TOUCHDOWN. K.Icker extra point is No Good, Wide Right")})
    assert _fires(missed, "ep.offense_td_failed_try_scored_as_made") == 1
    assert (
        _fires(missed.with_columns(pl.col("EP_end").replace(7.0, 6.0)), "ep.offense_td_failed_try_scored_as_made") == 0
    )
    # possession change on row 1 (HOM TD -> AWY): home_wp_after must not be 1 - next home_wp_before
    change = _game(**{"end__pos_team__id": (1, AWAY), "home_wp_after": (1, 0.3)})
    assert _fires(change, "wp.home_wp_after_complemented") == 1
    assert _fires(_game(**{"end__pos_team__id": (1, AWAY)}), "wp.home_wp_after_complemented") == 0
    assert _fires(_game(**{"EPA": (0, 0.9)}), "ep.epa_identity") == 1
    assert _fires(_game(**{"wp_after": (2, 1.2)}), "wp.wp_after_range") == 1
    assert _fires(_game(**{"home_wp_before": (3, 0.9)}), "wp.home_wp_continuity") == 1
    assert _fires(_game(), "wp.final_home_wp_matches_result", summary=_summary()) == 0
    assert _fires(_game(), "wp.final_home_wp_matches_result", summary=_summary(home=0, away=7)) == 1
    # home-signed wpa: +.05 +.15 -.02 +.02 -0 +.25 = .45; result 1 - first .5 = .5 -> drift -.05
    assert _fires(_game(), "wp.wpa_sums_to_result", summary=_summary()) == 0
    assert _fires(_game(**{"wpa": (5, 0.6)}), "wp.wpa_sums_to_result", summary=_summary()) == 1


# 7 --------------------------------------------------------------------------


def test_flags():
    assert _fires(_game(**{"rush": (2, True)}), "flags.rush_and_pass") == 1
    assert _fires(_game(**{"pass": (2, False)}), "flags.pass_type_without_pass") == 1
    assert _fires(_game(**{"rush": (0, False)}), "flags.rush_type_without_rush") == 1
    assert _fires(_game(**{"pass_attempt": (3, True)}), "flags.sack_counted_as_pass_attempt") == 1
    assert _fires(_game(**{"pass_attempt": (2, False)}), "flags.completion_without_attempt") == 1
    pick_six = _game(**{"type__text": (2, "Interception Return Touchdown"), "pass_td": (2, True)})
    assert _fires(pick_six, "flags.offense_td_flag_on_return_td") == 1
    assert (
        _fires(_game(**{"type__text": (2, "Interception Return Touchdown")}), "flags.offense_td_flag_on_return_td") == 0
    )
    wiped = _game(**{"penalty_no_play": (2, True)})
    assert _fires(wiped, "flags.no_play_yardage_credited") == 1
    fg = _game(**{"type__text": (5, "Extra Point Missed"), "fg_attempt": (5, False)}).with_columns(
        orig_play_type=pl.Series(["Rush", "Rush", "Pass", "Sack", "Timeout", "Blocked Field Goal"])
    )
    assert _fires(fg, "flags.fg_relabeled_extra_point") == 1
    assert _fires(fg, "flags.fg_type_without_fg_attempt") == 1


# 8 --------------------------------------------------------------------------


def test_dropped_and_duplicated_plays():
    assert _fires(_game(), "plays.unexplained_drop", summary=_summary()) == 0
    extra = _summary(raw_ids=(10, 11, 12, 13, 14, 15, 16))
    assert _fires(_game(), "plays.unexplained_drop", summary=extra) == 1
    documented = _summary(raw_ids=(10, 11, 12, 13, 14, 15, 16))
    documented["drives"]["previous"][0]["plays"][0]["type"]["text"] = "End of Quarter"
    assert _fires(_game(), "plays.unexplained_drop", summary=documented) == 0
    assert _fires(_game(**{"id": (1, 11)}), "plays.duplicate_ids") == 2
    assert _fires(_game(), "plays.rows_not_in_raw", summary=_summary(raw_ids=(11, 12, 13, 14, 15))) == 1


# 9 --------------------------------------------------------------------------


def test_box_score_nfl_conventions():
    summary = _summary()
    summary["boxscore"] = {
        "teams": [
            {
                "team": {"id": str(HOME)},
                "statistics": [
                    {"name": "rushingYards", "displayValue": "28"},
                    {"name": "rushingAttempts", "displayValue": "3"},
                    {"name": "netPassingYards", "displayValue": "0"},
                    {"name": "completionAttempts", "displayValue": "0/0"},
                    {"name": "sacksYardsLost", "displayValue": "0-0"},
                ],
            },
            {
                "team": {"id": str(AWAY)},
                "statistics": [
                    {"name": "rushingYards", "displayValue": "0"},
                    {"name": "rushingAttempts", "displayValue": "0"},
                    {"name": "netPassingYards", "displayValue": "2"},  # 8 gross - 6 sack
                    {"name": "completionAttempts", "displayValue": "1/1"},
                    {"name": "sacksYardsLost", "displayValue": "1-6"},
                ],
            },
        ]
    }
    box = {"team": [{"pos_team": HOME, "rush_yards": 28.0, "pass_yards": 0.0}, {"pos_team": AWAY, "pass_yards": 9.0}]}
    rules = _by_rule(_game(), summary=summary, box=box, league="nfl")
    for rule in (
        "box.plays_pass_yards_vs_espn",
        "box.plays_rush_yards_vs_espn",
        "box.plays_completions_vs_espn",
        "box.plays_pass_attempts_vs_espn",
        "box.plays_rush_attempts_vs_espn",
        "box.plays_sacks_vs_espn",
    ):
        assert (rules[rule].n_checked, rules[rule].n_violations) == (2, 0), rule
    assert rules["box.adv_pass_yards_vs_espn"].n_violations == 1
    assert rules["box.adv_pass_yards_vs_espn"].samples[0]["delta"] == 1.0
    # CFB charges sacks to rushing: ESPN's 3 rushing attempts would include the sack
    cfb = _by_rule(_game(), summary=summary, box=box, league="cfb")
    assert "box.plays_rush_yards_vs_espn" not in cfb


# 10 -------------------------------------------------------------------------


def test_attribution_coverage():
    assert _fires(_game(), "attr.passer") == 0
    r = _by_rule(_game(**{"passer_player_name": (2, " ")}))["attr.passer"]
    assert (r.n_checked, r.n_violations, r.severity) == (1, 1, Severity.INFO)
    assert _fires(_game(**{"sack_player_name": (3, None)}), "attr.sacker") == 1


def test_run_emits_findings_only_for_fired_rules_and_skips_missing_columns():
    ctx = CheckContext(domain="nfl", dataset="nfl_espn_pbp", schema={})
    assert [f for f in inv.run("nfl_espn_pbp", _game(), ctx, summary=_summary()) if f.severity is Severity.ERROR] == []
    findings = inv.run("nfl_espn_pbp", _game(**{"start__down": (0, 5)}), ctx)
    hit = [f for f in findings if f.locator["rule"] == "down.scrimmage_down_1_4"]
    assert hit and hit[0].metric == 1.0 and hit[0].locator["n_checked"] == 5
    # a frame without the rules' columns skips them rather than raising
    assert [r.rule for r in inv.evaluate(pl.DataFrame({"id": [1]}))] == ["plays.duplicate_ids"]


def test_nfl_processor_fixture_smoke(monkeypatch):
    """Real processor output, offline: every invariant group evaluates on the committed fixture."""
    import sportsdataverse.nfl.nfl_pbp as mod

    def _boom(*a, **k):
        raise AssertionError("network call on the offline path")

    monkeypatch.setattr(mod, "download", _boom)
    summary = json.loads(FIX.read_text())
    proc = mod.NFLPlayProcess(gameId=401872922, join_participants=False)
    proc.espn_nfl_pbp(summary=json.loads(FIX.read_text()))
    result = proc.run_processing_pipeline()
    rules = _by_rule(proc.plays_frame, summary=summary, box=result["advBoxScore"], league="nfl")
    assert {r.invariant for r in rules.values()} == set(range(1, 11))
    for rule in ("score.final_matches_header", "plays.unexplained_drop", "plays.duplicate_ids", "ytg.start_range"):
        assert rules[rule].n_checked > 0 and rules[rule].n_violations == 0, rule
    assert rules["attr.passer"].n_checked > 20


def test_sweep_report_survives_a_late_error_game(tmp_path):
    """The status frame's "error" column is None for 100+ games before the first failure."""
    from tools.validation.pbp_invariant_sweep import report

    games = tmp_path / "games" / "nfl" / "2020"
    games.mkdir(parents=True)
    meta = {"league": "nfl", "season": 2020, "stratum": "REG", "seconds": 1.0, "rules": []}
    for i in range(101):
        (games / f"{i}.json").write_text(json.dumps({**meta, "game_id": i, "status": "ok"}))
    (games / "999.json").write_text(json.dumps({**meta, "game_id": 999, "status": "error", "error": "KeyError: 'x'"}))
    report(tmp_path)
    assert pl.read_csv(tmp_path / "game_status.csv").height == 102


@pytest.mark.parametrize("season,era", [(2002, "2002-2009"), (2014, "2010-2014"), (2026, "2025-2026")])
def test_sweep_era_buckets(season, era):
    from tools.validation.pbp_invariant_sweep import era_of

    assert era_of(season) == era
