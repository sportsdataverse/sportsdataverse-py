"""``advBoxScore`` reconciliation rules (V1b, ``sportsdataverse.validation.box_reconcile``).

One real committed fixture per league is processed offline (0 network) and supplies
the PASSING case for every rule; each rule's FAILING case mutates one section total
of that same real box, so a rule that could not fail is visible as a red test.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.validation import RULE_SCOPE, validate_game
from sportsdataverse.validation.box_reconcile import _is_full_box, evaluate_box, game_teams
from sportsdataverse.validation.findings import Severity

FIXTURES = Path(__file__).resolve().parents[1]

RULES = (
    "box.team_totals_match_plays",
    "box.player_sums_match_team",
    "box.rates_recompute_from_counts",
    "box.sections_mirror_off_def",
    "box.turnovers_match_flags",
    "box.drives_match_drive_rows",
    "box.usage_shares_sum_to_one",
    "box.team_ids_in_game",
)


def _process(league: str, game_id: int, rel: str):
    if league == "nfl":
        import sportsdataverse.nfl.nfl_pbp as mod

        cls, fetch = mod.NFLPlayProcess, "espn_nfl_pbp"
    else:
        import sportsdataverse.cfb.cfb_pbp as mod

        cls, fetch = mod.CFBPlayProcess, "espn_cfb_pbp"

    def _boom(*a, **k):
        raise AssertionError("network call on the offline path")

    mod.download = _boom
    summary = json.loads((FIXTURES / rel).read_text())
    proc = cls(gameId=game_id, join_participants=False)
    getattr(proc, fetch)(summary=summary)
    game = proc.run_processing_pipeline()
    return proc.plays_frame, summary, game["advBoxScore"]


@pytest.fixture(scope="module")
def nfl_game():
    return _process("nfl", 401872922, "nfl/fixtures/summary_401872922.json")


@pytest.fixture(scope="module")
def cfb_game():
    return _process("cfb", 401856682, "cfb/fixtures/summary_401856682.json")


def _by_rule(plays, box, league="nfl"):
    return {r.rule: r for r in evaluate_box(plays, box, league)}


def _fires(plays, box, rule, league="nfl"):
    """Violations for one rule, with the denominator asserted real (never a vacuous pass)."""
    result = _by_rule(plays, box, league)[rule]
    assert result.n_checked > 0, f"{rule} checked nothing"
    return result.n_violations


# --- passing case: the real processor output reconciles ----------------------


@pytest.mark.parametrize("league", ["nfl", "cfb"])
def test_real_game_reconciles_on_every_rule(league, nfl_game, cfb_game):
    plays, _summary, box = nfl_game if league == "nfl" else cfb_game
    results = _by_rule(plays, box, league)
    assert set(results) == set(RULES)
    for rule in RULES:
        assert results[rule].n_checked > 0, rule
        assert results[rule].n_violations == 0, (rule, results[rule].samples)


def test_every_rule_id_is_scoped_or_deliberately_default():
    """A ``box.*`` rule whose severity is not the table default must be in RULE_SCOPE."""
    for rule in RULES:
        scoped = RULE_SCOPE.get(rule)
        if scoped is not None:
            assert scoped.note, f"{rule}: a scoped severity must cite its finding"


# --- failing case: one mutated section total per rule ------------------------


def _mutate(box: dict, fn) -> dict:
    out = copy.deepcopy(box)
    fn(out)
    return out


def test_team_total_that_disagrees_with_the_plays_fires(nfl_game):
    plays, _s, box = nfl_game
    bad = _mutate(box, lambda b: b["team"][0].update(scrimmage_plays=b["team"][0]["scrimmage_plays"] + 3))
    assert _fires(plays, bad, "box.team_totals_match_plays") == 1


def test_usage_team_total_that_disagrees_with_the_plays_fires(nfl_game):
    plays, _s, box = nfl_game
    bad = _mutate(box, lambda b: b["team_usage"][0].update(epa=b["team_usage"][0]["epa"] + 5))
    assert _fires(plays, bad, "box.team_totals_match_plays") == 1


def test_a_section_missing_from_a_full_box_is_a_finding_not_a_skip(nfl_game):
    """The #553 lesson: an absent section must not read as a clean pass."""
    plays, _s, box = nfl_game
    assert _is_full_box(box)
    bad = _mutate(box, lambda b: b.pop("team"))
    assert _is_full_box(bad)
    assert _fires(plays, bad, "box.team_totals_match_plays") > 0


def test_a_partial_box_is_judged_only_on_what_it_carries(nfl_game):
    """The offline sweep passes ``{"team": ...}``; the sections it never supplied are not failures."""
    plays, _s, box = nfl_game
    partial = {"team": copy.deepcopy(box["team"])}
    assert not _is_full_box(partial)
    assert _by_rule(plays, partial)["box.team_totals_match_plays"].n_violations == 0
    assert _by_rule(plays, partial)["box.drives_match_drive_rows"].n_violations == 0


def test_a_float_origin_team_id_fails_rather_than_reconciling(nfl_game):
    """``194.0`` must not coerce into a match (#553 finding 4)."""
    plays, _s, box = nfl_game
    bad = _mutate(box, lambda b: [r.update(pos_team=float(r["pos_team"])) for r in b["team"]])
    assert _fires(plays, bad, "box.team_totals_match_plays") > 0


def test_player_section_that_no_longer_sums_to_the_plays_fires(nfl_game):
    plays, _s, box = nfl_game
    bad = _mutate(box, lambda b: b["rush"].pop(0))
    assert _fires(plays, bad, "box.player_sums_match_team") > 0


def test_rate_that_does_not_recompute_from_its_counts_fires(nfl_game):
    plays, _s, box = nfl_game
    bad = _mutate(box, lambda b: b["team"][0].update(EPA_per_play=b["team"][0]["EPA_per_play"] * 2 + 1))
    assert _fires(plays, bad, "box.rates_recompute_from_counts") == 1


def test_offence_defence_mirror_break_fires(nfl_game):
    plays, _s, box = nfl_game
    bad = _mutate(box, lambda b: b["turnover"][0].update(takeaways=b["turnover"][0]["takeaways"] + 2))
    assert _fires(plays, bad, "box.sections_mirror_off_def") == 1


def test_turnover_counts_that_disagree_with_the_flags_fire(nfl_game):
    plays, _s, box = nfl_game
    bad = _mutate(box, lambda b: b["turnover"][0].update(Int_pbp=b["turnover"][0]["Int_pbp"] + 1))
    assert _fires(plays, bad, "box.turnovers_match_flags") == 1


def test_turnovers_are_judged_on_the_pbp_keys_not_the_espn_sourced_ones(nfl_game):
    """``turnovers`` / ``Int`` / ``fumbles_lost`` are overwritten from ESPN's box, so they say
    nothing about the plays frame; only the ``*_pbp`` twins are a claim about it."""
    plays, _s, box = nfl_game
    bad = _mutate(box, lambda b: [r.update(turnovers=r["turnovers"] + 9, Int=r["Int"] + 9) for r in b["turnover"]])
    assert _fires(plays, bad, "box.turnovers_match_flags") == 0


def test_drive_count_that_disagrees_with_the_frame_fires(nfl_game):
    plays, _s, box = nfl_game
    bad = _mutate(box, lambda b: b["drives"][0].update(drives=b["drives"][0]["drives"] + 1))
    assert _fires(plays, bad, "box.drives_match_drive_rows") == 1


def test_emptied_drive_scripting_fires(nfl_game):
    plays, _s, box = nfl_game
    bad = _mutate(box, lambda b: b.update(drive_scripting=[]))
    assert _fires(plays, bad, "box.drives_match_drive_rows") > 0


def test_usage_share_that_is_not_its_count_over_the_team_fires(nfl_game):
    plays, _s, box = nfl_game
    bad = _mutate(box, lambda b: b["player_usage"][0].update(target_share=0.9))
    assert _fires(plays, bad, "box.usage_shares_sum_to_one") > 0


def test_a_team_id_the_game_was_not_played_by_fires(nfl_game):
    plays, _s, box = nfl_game
    assert len(game_teams(plays)) == 2
    bad = _mutate(box, lambda b: b["drives"][0].update(pos_team=99999))
    assert _fires(plays, bad, "box.team_ids_in_game") == 1


# --- wiring -----------------------------------------------------------------


def test_rules_reach_validate_game_and_carry_the_denominator(nfl_game):
    plays, summary, box = nfl_game
    clean = validate_game(plays, "nfl", summary=summary, box=box)
    assert not any(f.rule_id.startswith("box.") and f.rule_id in RULES for f in clean.errors)
    bad = _mutate(box, lambda b: b["team"][0].update(scrimmage_plays=b["team"][0]["scrimmage_plays"] + 3))
    report = validate_game(plays, "nfl", summary=summary, box=bad)
    hit = [f for f in report.errors if f.rule_id == "box.team_totals_match_plays"]
    assert hit and hit[0].n_rows == 1 and hit[0].n_checked > 1
    assert report.counts_by_rule["box.team_totals_match_plays"] == 1
    assert json.loads(json.dumps(report.to_dict())) == report.to_dict()


def test_no_box_means_no_box_rules():
    frame = pl.DataFrame({"id": [1, 2], "pos_team": [1, 2], "def_pos_team": [2, 1]})
    assert evaluate_box(frame, None) == []
    assert evaluate_box(frame, {}) == []


def test_warn_rules_are_warn_everywhere():
    """Every ``box.*`` rule that is not an error must say so in RULE_SCOPE, with a note."""
    warn = [r for r in RULES if (RULE_SCOPE.get(r) or None) and RULE_SCOPE[r].severity is Severity.WARN]
    for rule in warn:
        assert RULE_SCOPE[rule].note


def test_emptied_usage_box_fires_although_the_sections_are_present(nfl_game):
    """``create_box_score`` empties all eleven usage sections when the usage box raises;
    that must not read as "this game had no rushers or receivers"."""
    plays, _s, box = nfl_game
    bad = _mutate(box, lambda b: b.update(player_usage=[]))
    assert _fires(plays, bad, "box.usage_shares_sum_to_one") > 0
    emptied = _mutate(box, lambda b: b.update(team_usage=[], player_usage=[], drive_scripting=[]))
    assert _fires(plays, emptied, "box.team_totals_match_plays") > 0
    assert _fires(plays, emptied, "box.drives_match_drive_rows") > 0
