"""League-neutral Fox adapter behaviour, on trimmed real captures. Offline: zero network.

The three mutation tests at the bottom are the point of this file: each one re-runs the
projection with one repair disabled and asserts the output goes wrong. A gate that cannot go
red is not a gate.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from sportsdataverse.football import fox_common
from sportsdataverse.football.sources.contract import _validate_summary
from sportsdataverse.nfl.fox_pbp.to_espn_summary import _fox_nfl_to_espn_summary

NFL_FIXTURES = pathlib.Path(__file__).resolve().parents[1] / "nfl" / "fixtures" / "fox"
CFB_FIXTURES = pathlib.Path(__file__).resolve().parents[1] / "cfb" / "fixtures" / "fox"


def _load(path):
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def nfl_final():
    return _load(NFL_FIXTURES / "fox_nfl_401671775.json")


@pytest.fixture(scope="module")
def nfl_row():
    meta = _load(NFL_FIXTURES / "fox_nfl_401671775_meta.json")
    return {
        k: meta[k] for k in ("espn_event_id", "season", "season_type", "week", "home_espn_team_id", "away_espn_team_id")
    }


@pytest.fixture(scope="module")
def nfl_summary(nfl_final, nfl_row):
    summary, _ = _fox_nfl_to_espn_summary(nfl_final, nfl_row)
    return summary


def _plays(summary):
    drives = summary["drives"]
    groupings = list(drives["previous"]) + ([drives["current"]] if drives.get("current") else [])
    return [p for d in groupings for p in d["plays"]]


def test_the_adapted_summary_satisfies_the_processor_and_gop_contract(nfl_summary):
    report = _validate_summary(nfl_summary, "nfl")
    assert report.ok, (report.missing, report.invalid)
    assert report.gop_ok, report.gop_missing
    assert report.n_plays > 100


def test_field_position_is_read_as_an_absolute_coordinate_for_both_offences(nfl_summary):
    """``yardsToEndzone = 100 - pos`` for the away team and ``pos`` for the home team.

    The shipped CFB Fox adapter reads ``yardStart`` *as* yards-to-goal, which is right for one
    side of the ball and mirrored for the other; the tell is that every row's
    ``yardLine`` and ``yardsToEndzone`` would then be equal on both sides.
    """
    home_id = nfl_summary["header"]["competitions"][0]["competitors"][0]["team"]["id"]
    scrimmage = [p for p in _plays(nfl_summary) if p["start"]["down"] and p["start"]["yardLine"] is not None]
    home_rows = [p for p in scrimmage if p["start"]["team"]["id"] == home_id]
    away_rows = [p for p in scrimmage if p["start"]["team"]["id"] != home_id]
    assert home_rows and away_rows
    for play in scrimmage:
        assert 0 <= play["start"]["yardsToEndzone"] <= 100
    assert all(p["start"]["yardsToEndzone"] == 100 - p["start"]["yardLine"] for p in home_rows)
    assert all(p["start"]["yardsToEndzone"] == p["start"]["yardLine"] for p in away_rows)


def test_a_kickoff_is_credited_to_the_kicking_team(nfl_summary):
    """ESPN's convention; the processor flips it back with ``kickoff_vec``. Fox's drive group
    names the **receiving** team, so an unflipped kickoff puts the ball on the wrong side."""
    plays = _plays(nfl_summary)
    kickoffs = [(i, p) for i, p in enumerate(plays) if p["type"]["id"] == "53"]
    assert kickoffs
    for index, kick in kickoffs:
        nxt = next((p for p in plays[index + 1 :] if p["start"]["down"]), None)
        if nxt is not None and nxt["type"]["id"] not in ("8",):
            assert kick["start"]["team"]["id"] != nxt["start"]["team"]["id"]


def test_the_try_is_folded_into_its_touchdown_and_reads_in_espn_grammar(nfl_summary):
    """``NFLPlayProcess`` reads the extra point off the TEXT, case-sensitively."""
    folded = [p for p in _plays(nfl_summary) if p.get("pointAfterAttempt")]
    assert folded
    for play in folded:
        assert play["type"]["abbreviation"] == "TD"
        if play["pointAfterAttempt"]["abbreviation"] == "Extra Point Good":
            assert "extra point is GOOD" in play["text"]
    assert not [p for p in _plays(nfl_summary) if p["type"]["id"] == "PAT"]


def test_period_end_rows_carry_down_and_distance_zero(nfl_summary):
    """Copying the previous snap's state makes ``nfl_pbp``'s duplicate-text filter drop it."""
    enders = [p for p in _plays(nfl_summary) if p["type"]["id"] in ("2", "65", "66")]
    assert enders
    assert all(p["start"]["down"] == 0 and p["start"]["distance"] == 0 for p in enders)


def test_a_tv_timeout_is_an_official_timeout_not_a_charged_one(nfl_summary):
    texts = {p["type"]["text"] for p in _plays(nfl_summary)}
    charged = [p for p in _plays(nfl_summary) if p["type"]["text"] == "Timeout"]
    assert "Official Timeout" in texts
    # every charged timeout names the club that called it, which is how the processor debits it
    assert charged and all("Timeout #" in p["text"] for p in charged)


def test_every_play_team_id_is_one_of_the_two_espn_ids(nfl_summary):
    ids = {c["team"]["id"] for c in nfl_summary["header"]["competitions"][0]["competitors"]}
    for play in _plays(nfl_summary):
        assert str(play["start"]["team"]["id"]) in ids
        assert play["end"].get("team", {}).get("id") is None or str(play["end"]["team"]["id"]) in ids


def test_the_mascot_is_never_empty(nfl_summary):
    """An empty ``team.name`` is contained in every string, so it charges every timeout to both."""
    for competitor in nfl_summary["header"]["competitions"][0]["competitors"]:
        assert competitor["team"]["name"]
        assert competitor["team"]["abbreviation"]


# ------------------------------------------------------------------ live / newest-first order
@pytest.mark.parametrize("label", ["early", "mid", "late"])
def test_a_live_poll_is_re_sorted_and_its_open_drive_states_no_outcome(label, nfl_row):
    """Fox serves the whole pbp tree newest-first until FINAL (291 captured polls).

    These fixtures are those raw polls, unmodified.
    """
    payload = _load(NFL_FIXTURES / f"fox_nfl_live_{label}.json")
    feed_order = [int(p["id"]) for s in payload["pbp"]["sections"] for g in s["groups"] for p in g["plays"]]
    assert feed_order != sorted(feed_order), "fixture is not newest-first; it cannot prove the re-sort"
    row = dict(nfl_row, espn_event_id="401872932", home_espn_team_id="2", away_espn_team_id="8")
    summary, notes = _fox_nfl_to_espn_summary(payload, row)
    plays = _plays(summary)
    assert [int(p["sequenceNumber"]) for p in plays] == sorted(int(p["sequenceNumber"]) for p in plays)
    assert _validate_summary(summary, "nfl").ok
    assert summary["drives"].get("current"), "an unfinished game must expose an open drive"
    assert summary["drives"]["current"]["result"] == "In Progress"
    assert summary["drives"]["current"]["isScore"] is False
    assert any("drives.current" in n for n in notes)
    assert summary["header"]["competitions"][0]["status"]["type"]["completed"] is False
    last = plays[-1]
    if last["type"]["id"] in fox_common._KEEPS_THE_BALL and last["statYardage"]:
        # the newest row is the one Game on Paper renders; its end must come from its own
        # yardage, not from its own start (which would say the ball never moved)
        assert last["end"]["yardsToEndzone"] == last["start"]["yardsToEndzone"] - last["statYardage"]


def test_the_final_of_the_same_game_is_served_oldest_first(nfl_final):
    feed_order = [int(p["id"]) for s in nfl_final["pbp"]["sections"] for g in s["groups"] for p in g["plays"]]
    assert feed_order == sorted(feed_order)


# --------------------------------------------------------------------------------- mutations
def test_mutation_pat_folded_onto_the_preceding_row_breaks_the_scoreboard(nfl_final, nfl_row, monkeypatch):
    """The #540 defect: anchoring the try on ``emitted[-1]`` instead of the newest touchdown.

    This fixture is chosen because a **penalty row sits between** a touchdown and its try -- on
    a game whose tries are all adjacent the mutation passes, which is exactly how the original
    defect shipped green.
    """
    good = _fox_nfl_to_espn_summary(nfl_final, nfl_row)[0]
    non_adjacent = [
        i
        for i, p in enumerate(_plays(good))
        if p.get("pointAfterAttempt") and i and _plays(good)[i - 1]["type"]["abbreviation"] != "TD"
    ]
    assert non_adjacent, "fixture no longer carries a try whose touchdown is not the row before it"
    for play in _plays(good):
        if play.get("pointAfterAttempt"):
            assert play["scoringPlay"] is True and play["type"]["abbreviation"] == "TD"

    monkeypatch.setattr(fox_common, "_fold_target", lambda emitted, last_touchdown: emitted[-1])
    mutated = _fox_nfl_to_espn_summary(nfl_final, nfl_row)[0]
    landed_on_a_non_touchdown = [
        p for p in _plays(mutated) if p.get("pointAfterAttempt") and p["type"]["abbreviation"] != "TD"
    ]
    assert landed_on_a_non_touchdown, "the mutation did not move a try off its touchdown"


def test_mutation_not_sorting_the_feed_wrecks_a_live_poll(nfl_row, monkeypatch):
    """Disable the newest-first re-sort; a live poll must then come out wrong."""
    payload = _load(NFL_FIXTURES / "fox_nfl_live_mid.json")
    row = dict(nfl_row, espn_event_id="401872932", home_espn_team_id="2", away_espn_team_id="8")
    good = _fox_nfl_to_espn_summary(payload, row)[0]

    original = fox_common._flatten

    def unsorted_flatten(fox):
        rows = original(fox)
        return list(reversed(rows))  # feed order for a live payload

    monkeypatch.setattr(fox_common, "_flatten", unsorted_flatten)
    mutated = _fox_nfl_to_espn_summary(payload, row)[0]
    good_order = [int(p["sequenceNumber"]) for p in _plays(good)]
    mutated_order = [int(p["sequenceNumber"]) for p in _plays(mutated)]
    assert good_order == sorted(good_order)
    assert mutated_order != good_order


def test_mutation_taking_the_period_from_the_section_misfiles_the_quarter_end_row(nfl_final, nfl_row, monkeypatch):
    """Fox files a drive under the quarter it ENDS in, so the section title is wrong for the
    "End Quarter N" row of every quarter -- which is what zeroes the lagged end clock."""
    good = _fox_nfl_to_espn_summary(nfl_final, nfl_row)[0]
    good_periods = [(p["type"]["id"], p["period"]["number"]) for p in _plays(good) if p["type"]["id"] in ("2", "65")]
    assert good_periods

    def section_period(play, previous, section_period_value):
        return section_period_value

    monkeypatch.setattr(fox_common, "_period_of", section_period)
    mutated = _fox_nfl_to_espn_summary(nfl_final, nfl_row)[0]
    mutated_periods = [
        (p["type"]["id"], p["period"]["number"]) for p in _plays(mutated) if p["type"]["id"] in ("2", "65")
    ]
    assert mutated_periods != good_periods
