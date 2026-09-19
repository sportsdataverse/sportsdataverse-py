"""The packaged per-game gate (``sportsdataverse.validation.validate_game``).

Scoping (severity per league / source / era) and the report contract are checked on
small frames; ``test_real_games_pass`` runs the real processors offline on committed
fixtures and asserts the gate's verdict on main's current output. 0 network.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.validation import (
    NOT_APPLICABLE,
    RULE_SCOPE,
    SOURCE_COLUMNS,
    Finding,
    GameReport,
    Rule,
    validate_game,
)
from sportsdataverse.validation import pbp_invariants as inv
from sportsdataverse.validation.findings import Severity

FIXTURES = Path(__file__).resolve().parents[1]

#: Games whose processed output carries no ``error``-severity finding on main.
CLEAN = (
    ("nfl", 401872922, "nfl/fixtures/summary_401872922.json"),
    ("nfl", 401220341, "nfl/fixtures/summary_401220341_trimmed.json.gz"),
    ("nfl", 400951676, "nfl/fixtures/summary_400951676_trimmed.json.gz"),
    ("cfb", 401856682, "cfb/fixtures/summary_401856682.json"),
    ("cfb", 401858439, "cfb/fixtures/summary_401858439.json"),
    ("cfb", 401754598, "cfb/fixtures/summary_401754598.json"),
)


def _frame(**cols) -> pl.DataFrame:
    """A minimal frame carrying only what the rules under test read."""
    base = {
        "id": [1, 2, 3],
        "game_id": [42, 42, 42],
        "season": [2023, 2023, 2023],
        "period.number": [1, 1, 1],
        "type.id": ["5", "5", "5"],
        "type.abbreviation": ["RUSH", "RUSH", "RUSH"],
    }
    base.update(cols)
    return pl.DataFrame(base, strict=False)


def _load(rel: str) -> dict:
    path = FIXTURES / rel
    return json.loads(gzip.open(path, "rt").read()) if path.suffix == ".gz" else json.loads(path.read_text())


def _process(league: str, game_id: int, rel: str):
    """Run the real processor offline; returns ``(plays_frame, summary, game)``."""
    if league == "nfl":
        import sportsdataverse.nfl.nfl_pbp as mod

        cls, fetch = mod.NFLPlayProcess, "espn_nfl_pbp"
    else:
        import sportsdataverse.cfb.cfb_pbp as mod

        cls, fetch = mod.CFBPlayProcess, "espn_cfb_pbp"

    def _boom(*a, **k):
        raise AssertionError("network call on the offline path")

    mod.download = _boom
    proc = cls(gameId=game_id, join_participants=False)
    getattr(proc, fetch)(summary=_load(rel))
    game = proc.run_processing_pipeline(validate=True)
    return proc, game


# --- report contract --------------------------------------------------------


def test_report_is_json_serialisable_and_stable():
    report = validate_game(_frame(**{"type.id": ["5", None, "5"]}), "nfl")
    assert isinstance(report, GameReport)
    assert report.game_id == 42 and report.season == 2023 and report.n_rows == 3
    payload = report.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "game_id",
        "league",
        "source",
        "season",
        "processing_version",
        "n_rows",
        "ok",
        "n_errors",
        "n_warnings",
        "n_not_applicable",
        "errors",
        "warnings",
        "counts_by_rule",
        "not_applicable",
    ]
    assert payload["counts_by_rule"]["type.null_id"] == 1
    assert not payload["ok"] and payload["n_errors"] == 1
    row = report.to_row()
    assert row["failed_rule_ids"] == ["type.null_id"]
    assert all(not isinstance(v, (dict, list)) or k.endswith("_ids") for k, v in row.items())
    assert json.loads(json.dumps(row)) == row


def test_finding_carries_the_denominator_and_samples():
    report = validate_game(_frame(**{"type.id": ["5", None, "5"]}), "nfl")
    (finding,) = report.errors
    assert isinstance(finding, Finding)
    assert (finding.rule_id, finding.severity, finding.n_rows, finding.n_checked) == ("type.null_id", "error", 1, 3)
    assert finding.sample_row_ids == [2]
    assert finding.message


def test_unknown_league_is_rejected():
    with pytest.raises(ValueError, match="league must be"):
        validate_game(_frame(), "mbb")


def test_empty_frame_is_ok_not_a_crash():
    report = validate_game(pl.DataFrame({"id": []}), "cfb")
    assert report.ok and report.n_rows == 0 and report.counts_by_rule == {}


# --- scoping ----------------------------------------------------------------


def test_era_scope_demotes_outside_its_window():
    """``type.null_id`` is an error from 2010; the 2002-09 summaries carry typeless rows."""
    modern = _frame(**{"type.id": ["5", None, "5"]})
    assert [f.rule_id for f in validate_game(modern, "nfl").errors] == ["type.null_id"]
    legacy = modern.with_columns(pl.Series("season", [2006] * 3))
    report = validate_game(legacy, "nfl")
    assert report.ok
    assert [f.rule_id for f in report.warnings] == ["type.null_id"]
    # the count is kept whatever the severity -- scoping never hides a rule
    assert report.counts_by_rule["type.null_id"] == 1


def test_era_scope_is_per_league():
    """``ytg.start_matches_down_distance_text`` is an NFL-2025+ error; CFB's residual is ESPN's."""
    rule = RULE_SCOPE["ytg.start_matches_down_distance_text"]
    assert rule.severity_for("nfl", 2025) is Severity.ERROR
    assert rule.severity_for("nfl", 2019) is Severity.WARN
    assert rule.severity_for("cfb", 2025) is Severity.WARN


def test_source_scope_skips_espn_only_rules():
    """A rule that judges ESPN's own feed says nothing about an adapted source."""
    assert RULE_SCOPE["poss.end_team_flips_without_change"].sources == ("espn",)
    assert RULE_SCOPE["poss.end_team_flips_without_change"].applies("nfl", "espn")
    assert not RULE_SCOPE["poss.end_team_flips_without_change"].applies("nfl", "shield")
    frame = _frame(**{"type.abbreviation": [None, None, None]})
    espn = validate_game(frame, "nfl", source="espn")
    assert "type.null_abbreviation" in espn.counts_by_rule
    # ... and a rule with no source restriction still runs for every source
    cbs = validate_game(frame, "nfl", source="cbs")
    assert cbs.counts_by_rule == espn.counts_by_rule


def test_every_scoped_rule_id_exists_in_the_rule_table():
    """RULE_SCOPE cannot drift away from the rules it scopes (a typo would silently stop scoping)."""
    known = {
        r.rule
        for r in inv.evaluate(_full_frame(), summary=None, box=None, league="nfl")
        + inv.evaluate(_full_frame(), summary=None, box=None, league="cfb")
    }
    # the rules RULE_SCOPE names that a bare frame cannot produce are named here explicitly
    frame_only = set(RULE_SCOPE) - known
    assert frame_only <= {
        "box.usage_shares_sum_to_one",  # needs an advBoxScore
        "plays.unexplained_drop",  # needs a summary
        "score.scoring_play_without_change",
        "wp.wpa_sums_to_result",
        "wp.home_wp_continuity",
        "wp.home_wp_after_complemented",
    }, sorted(frame_only)


def test_feed_order_rules_are_warnings_not_errors():
    """``period.monotone`` measures ESPN's own row order, so it must not fail a build."""
    assert RULE_SCOPE["period.monotone"].severity is Severity.WARN
    out_of_order = _frame(**{"period.number": [1, 2, 1]})
    report = validate_game(out_of_order, "cfb")
    assert report.ok and [f.rule_id for f in report.warnings] == ["period.monotone"]
    assert report.counts_by_rule["period.monotone"] == 1


def test_unresolvable_season_fails_closed():
    """An era-scoped error rule keeps its severity when the season cannot be resolved."""
    frame = _frame(**{"type.id": ["5", None, "5"]}).drop("season")
    report = validate_game(frame, "nfl")
    assert not report.ok and [f.rule_id for f in report.errors] == ["type.null_id"]
    assert report.season is None


def test_adapter_defects_are_not_excused_by_source_scope():
    """``down.scrimmage_distance_ge_1`` reads only ``start.distance`` -- every source is judged."""
    rule = RULE_SCOPE["down.scrimmage_distance_ge_1"]
    assert rule.applies("nfl", "cbs") and rule.applies("cfb", "shield")
    assert rule.severity_for("nfl", 2024) is Severity.ERROR


def test_default_rule_keeps_the_tables_severity():
    rule = Rule("made.up")
    assert rule.severity_for("nfl", 2002) is Severity.ERROR
    assert rule.applies("cfb", "yahoo")


def _full_frame() -> pl.DataFrame:
    """Enough columns that most rule groups evaluate (values are internally consistent)."""
    from tests.contracts.test_pbp_invariants import _game

    return _game()


# --- real games -------------------------------------------------------------


@pytest.mark.parametrize(("league", "game_id", "rel"), CLEAN, ids=[f"{lg}-{gid}" for lg, gid, _ in CLEAN])
def test_real_games_pass(league, game_id, rel):
    """main's current output on six committed fixtures carries no error-severity finding."""
    proc, game = _process(league, game_id, rel)
    report = validate_game(proc.plays_frame, league, header=game["header"], summary=game, box=game.get("advBoxScore"))
    assert report.errors == [], [f.to_dict() for f in report.errors]
    assert report.ok and report.game_id == game_id and report.league == league
    assert report.n_rows == proc.plays_frame.height
    # the processor hook produces the same verdict as the direct call
    assert game["validation"]["ok"] is True
    assert game["validation"]["counts_by_rule"] == report.counts_by_rule


def test_validate_is_opt_in():
    """``validate=False`` (the default) attaches nothing."""
    league, game_id, rel = CLEAN[3]
    import sportsdataverse.cfb.cfb_pbp as mod

    def _boom(*a, **k):
        raise AssertionError("network call on the offline path")

    mod.download = _boom
    proc = mod.CFBPlayProcess(gameId=game_id, join_participants=False)
    proc.espn_cfb_pbp(summary=_load(rel))
    assert "validation" not in proc.run_processing_pipeline()


# --- adapted sources: column aliases + not-applicable rules ------------------


def _ncaa_frame() -> pl.DataFrame:
    """A real stats.ncaa.org game mapped to cfbfastR names, from a committed page. 0 network."""
    from sportsdataverse.cfb import to_cfbfastr
    from sportsdataverse.cfb.cfb_ncaa_box import (
        parse_cfb_ncaa_drives,
        parse_cfb_ncaa_linescore,
        parse_cfb_ncaa_scoring_summary,
    )
    from sportsdataverse.cfb.cfb_ncaa_pbp import parse_cfb_ncaa_drive_titles, parse_cfb_ncaa_pbp

    fix = FIXTURES / "fixtures" / "cfb_ncaa"
    pbp_html = (fix / "mfb_play_by_play_6386512.html").read_text(encoding="utf-8")
    box_html = (fix / "mfb_box_score_6386512.html").read_text(encoding="utf-8")
    drives = parse_cfb_ncaa_drives((fix / "mfb_drives_6386512.html").read_text(encoding="utf-8"), contest_id="6386512")
    return to_cfbfastr(
        parse_cfb_ncaa_pbp(pbp_html, contest_id="6386512"),
        season=2025,
        drives=drives,
        linescore=parse_cfb_ncaa_linescore(box_html, contest_id="6386512"),
        drive_titles=parse_cfb_ncaa_drive_titles(pbp_html),
        ot_drives=drives,
        scoring_summary=parse_cfb_ncaa_scoring_summary(box_html, contest_id="6386512"),
    )


def test_ncaa_aliases_carry_the_mappers_own_values():
    """``SOURCE_COLUMNS["ncaa"]`` is renames and arithmetic identities, nothing invented."""
    from sportsdataverse.validation.report import _as_source

    frame = _ncaa_frame()
    view = _as_source(frame, "ncaa")
    assert set(SOURCE_COLUMNS["ncaa"]) <= set(view.columns)
    for espn, native in (
        ("start.yardsToEndzone", "yards_to_goal"),
        ("end.yardsToEndzone", "yards_to_goal_end"),
        ("start.down", "down"),
        ("start.distance", "distance"),
        ("period.number", "period"),
        ("type.text", "play_type"),
        ("text", "play_text"),
        ("id", "id_play"),
        ("drive.id", "drive_id"),
        ("scoringPlay", "scoring_play"),
        ("fg_attempt", "fg_inds"),
    ):
        assert view.get_column(espn).to_list() == frame.get_column(native).to_list(), espn
    # the source's own orig_play_type is the raw NCAA structural type, so the alias
    # must WIN over it -- otherwise the field-goal rules read "field_goal" and never scope
    assert frame.get_column("orig_play_type").to_list() != frame.get_column("play_type").to_list()
    assert view.get_column("orig_play_type").to_list() == frame.get_column("play_type").to_list()
    # "M:SS" -- the shape clock.monotone_within_period parses
    clock = view.filter(pl.col("clock.seconds") < 10).get_column("clock.displayValue").drop_nulls()
    assert clock.len() and all(len(v.split(":")[1]) == 2 for v in clock)
    # the score aliases resolve the offense/defense pair onto home/away, and start is
    # the previous row's end (0-0 before the first row)
    home_end = view.get_column("end.homeScore")
    assert view.get_column("start.homeScore").to_list() == [0, *home_end.to_list()[:-1]]
    resolved = view.select(
        (
            pl.when(pl.col("pos_team") == pl.col("home"))
            .then(pl.col("pos_team_score"))
            .otherwise(pl.col("def_pos_team_score"))
            == pl.col("end.homeScore")
        ).all()
    ).item()
    assert resolved
    assert frame.columns == _ncaa_frame().columns  # the caller's frame is not mutated


def test_ncaa_alias_is_skipped_when_its_inputs_are_missing():
    """A slim frame keeps validating: an alias whose source columns are absent is dropped."""
    from sportsdataverse.validation.report import _as_source

    slim = pl.DataFrame({"yards_to_goal": [40, 30], "play_text": ["a", "b"]})
    view = _as_source(slim, "ncaa")
    assert view.get_column("start.yardsToEndzone").to_list() == [40, 30]
    assert "end.homeScore" not in view.columns and "type.text" not in view.columns


def test_every_ncaa_rule_is_evaluated_or_explicitly_not_applicable():
    """The gate's whole point: no rule silently disappears on the NCAA mapper path."""
    from sportsdataverse.validation.report import _as_source

    proc, game = _process("cfb", 401856682, "cfb/fixtures/summary_401856682.json")
    catalog = {
        r.rule for r in inv.evaluate(proc.plays_frame, summary=game, box=game.get("advBoxScore"), league="cfb")
    } | {
        # NFL-only box comparisons: ESPN charges sacks to passing, NCAA to rushing,
        # so _box never emits these three for a cfb game
        "box.plays_rush_yards_vs_espn",
        "box.plays_sacks_vs_espn",
        "box.adv_rush_yards_vs_espn",
    }
    unsupported = set(NOT_APPLICABLE["ncaa"])
    assert unsupported <= catalog, sorted(unsupported - catalog)  # no typo'd rule id
    results = inv.evaluate(_as_source(_ncaa_frame(), "ncaa"), league="cfb")
    evaluated = {r.rule for r in results} - unsupported
    assert evaluated | unsupported == catalog, sorted(catalog - evaluated - unsupported)
    # an evaluated rule with an empty denominator is a silent skip wearing a hat,
    # unless its scope waits on an event this one game did not produce
    empty = {r.rule for r in results if r.rule in evaluated and not r.n_checked}
    assert empty <= {"flags.offense_td_flag_on_return_td"}, sorted(empty)


def test_not_applicable_is_reported_and_suppresses_the_rule():
    """A not-applicable rule is counted, listed, and never contributes a finding."""
    report = validate_game(_ncaa_frame(), "cfb", source="ncaa")
    assert report.not_applicable == sorted(NOT_APPLICABLE["ncaa"])
    assert report.to_dict()["n_not_applicable"] == len(NOT_APPLICABLE["ncaa"])
    assert report.to_row()["n_not_applicable"] == len(NOT_APPLICABLE["ncaa"])
    assert not set(report.counts_by_rule) & set(NOT_APPLICABLE["ncaa"])
    # ytg.continuity_next_snap DOES evaluate once the aliases land -- being
    # not-applicable is what keeps its (double-flipped) verdict out of the report
    from sportsdataverse.validation.report import _as_source

    fired = {r.rule for r in inv.evaluate(_as_source(_ncaa_frame(), "ncaa"), league="cfb") if r.n_violations}
    assert "ytg.continuity_next_snap" in fired
    assert "ytg.continuity_next_snap" not in report.counts_by_rule
    # an ESPN game is untouched by the ncaa map
    assert validate_game(_frame(), "cfb").not_applicable == []


def test_a_challenge_quote_is_not_an_incomplete_pass():
    """``flags.completion_on_incomplete_text`` read the text of the *challenge*, not the play."""
    frame = _frame(
        completion=[True, True, True],
        text=[
            "pass complete to Olson for 26 yards. FST is challenging the ruling on the field - "
            '"Incomplete pass". PLAY STANDS.',
            "pass incomplete deep left to Brown",
            "pass complete short right to Hester",
        ],
    )
    (result,) = [r for r in inv.evaluate(frame, league="cfb") if r.rule == "flags.completion_on_incomplete_text"]
    assert (result.n_checked, result.n_violations) == (3, 1)
    assert result.samples[0]["id"] == 2
