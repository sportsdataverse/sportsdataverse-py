"""Contract validator: passes on every real ESPN fixture, names the exact gaps on a gutted one."""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import pytest

from sportsdataverse.football.sources.contract import _validate_summary

from .conftest import CFB_FIX, NFL_FIX, NFL_GAME_ID

# 401761665's pickcenter has no total on any provider: it warns DefaultOddsWarning incidentally;
# tests/cfb/test_cfb_pbp_offline.py::test_pickcenter_without_a_total_defaults_it_and_warns asserts it.
pytestmark = pytest.mark.filterwarnings(
    "ignore:CFB game 401761665. no pregame total:sportsdataverse.errors.DefaultOddsWarning"
)

_CFB_FIXTURES = sorted(CFB_FIX.glob("summary_*.json"))


@pytest.mark.parametrize(
    ("league", "path"),
    [("nfl", NFL_FIX / f"summary_{NFL_GAME_ID}.json")] + [("cfb", p) for p in _CFB_FIXTURES],
    ids=lambda v: v.name if isinstance(v, Path) else v,
)
def test_real_espn_summaries_satisfy_the_contract(league, path):
    report = _validate_summary(json.loads(path.read_text(encoding="utf-8")), league)
    assert report.ok, (report.missing, report.invalid)
    assert report.gop_ok, report.gop_missing
    assert report.n_plays > 100 and report.n_drives > 10
    # A real ESPN payload only ever produces the documented warnings: its own late inserts
    # and duplicate play ids, and the fields ESPN's pre-2010 CFB feeds simply do not carry
    # (``repaired`` / ``gop_soft`` levels -- the processor or Game on Paper covers those).
    allowed = ("feed order", "duplicate", "absent: the processor fills it", "absent: Game on Paper falls back")
    assert all(any(a in w for a in allowed) for w in report.warnings), report.warnings


def _all_plays(summary):
    return [p for d in summary["drives"]["previous"] for p in d["plays"]]


def test_gutted_summary_reports_the_exact_missing_fields(nfl_summary):
    s = copy.deepcopy(nfl_summary)
    comp = s["header"]["competitions"][0]
    del s["header"]["week"]
    del comp["status"]
    for c in comp["competitors"]:
        c["team"]["name"] = ""  # empty mascot: every Timeout row matches both teams
    for p in _all_plays(s):
        del p["type"]["id"]
        p["type"].pop("abbreviation", None)  # ESPN omits the key on Sack / Pass Incompletion
        del p["statYardage"]

    report = _validate_summary(s, "nfl")

    assert not report.ok and not report.gop_ok
    assert report.missing == [
        "header.week",
        "header.competitions[0].status.type.completed",
        "plays[].statYardage",
    ]
    assert report.gop_missing == [
        "header.competitions[0].status.type.name",
        "header.competitions[0].status.type.state",
        "header.competitions[0].status.type.detail",
        "header.competitions[0].status.type.description",
        "plays[].type.id",
        "plays[].type.abbreviation",
    ]
    assert report.invalid == [
        "competitors[0].team.name is empty: every Timeout row would match both teams",
        "competitors[1].team.name is empty: every Timeout row would match both teams",
    ]
    summary = report.summary()
    assert summary["ok"] is False and summary["missing"] == report.missing


def test_value_rules(nfl_summary):
    s = copy.deepcopy(nfl_summary)
    plays = _all_plays(s)
    plays[3]["start"]["team"]["id"] = "9999"
    plays[4]["clock"]["displayValue"] = "bad"
    plays[5]["id"] = "not-an-int"
    s["header"]["competitions"][0]["competitors"].reverse()  # away first: GOP assumes home-first
    for p in plays:
        p["scoringPlay"] = None  # a value-level column that is present but all-null

    report = _validate_summary(s, "nfl")

    assert "plays[].scoringPlay: present but null on every row" in report.invalid
    assert "play id 'not-an-int' is not int-castable" in report.invalid
    assert 'clock.displayValue not "MM:SS" on 1 plays' in report.invalid
    assert "play team ids not in header competitors: ['9999']" in report.invalid
    assert any("home-first" in w for w in report.warnings)
    assert report.null_rate["plays[].scoringPlay"] == 1.0


def test_late_inserts_and_sparse_feeds_only_warn(nfl_summary):
    s = copy.deepcopy(nfl_summary)
    plays = _all_plays(s)
    plays[10]["id"], plays[11]["id"] = plays[11]["id"], plays[10]["id"]
    s["drives"]["previous"] = s["drives"]["previous"][:3]  # 3 drives of a completed game
    report = _validate_summary(s, "nfl")
    assert report.ok
    assert any("feed order" in w for w in report.warnings)
    assert any("corrupt_pbp_check" in w for w in report.warnings)


def test_no_drives_and_bad_inputs():
    assert _validate_summary({"header": {}, "drives": {}}, "cfb").missing[:1] == ["header.season.year"]
    r = _validate_summary({"header": {"competitions": [{}]}, "drives": {}}, "cfb")
    assert "drives.previous" in r.missing and "drives[].plays[]" in r.missing
    assert _validate_summary("nope", "nfl").missing == ["<summary is not a dict>"]
    with pytest.raises(ValueError):
        _validate_summary({}, "nhl")


def test_all_null_required_column_is_invalid(nfl_summary):
    """An all-null ``required`` path keeps the column (no raise) but zeroes the output.

    ``ok`` must be False so the dispatcher fails over instead of serving garbage.
    """
    gutted = copy.deepcopy(nfl_summary)
    for d in gutted["drives"]["previous"]:
        for p in d["plays"]:
            p["statYardage"] = None
    report = _validate_summary(gutted, "nfl")
    assert report.missing == []  # the key is there: no ColumnNotFoundError
    assert "plays[].statYardage: present but null on every row" in report.invalid
    assert report.ok is False
    assert report.null_rate["plays[].statYardage"] == 1.0


def test_a_feed_with_no_end_team_still_satisfies_the_contract(nfl_summary):
    """ESPN's own pre-2010 CFB feeds carry no ``end.team``; both processors rebuild it.

    Rejecting those payloads made ``_process_game`` raise ``AllSourcesFailed`` for a game
    ESPN serves and the processor handles (252532751, 2005: 163 plays, 0 null
    ``end.team.id``, 0 null ``EPA`` straight through ``CFBPlayProcess``).
    """
    stripped = copy.deepcopy(nfl_summary)
    for d in stripped["drives"]["previous"]:
        for p in d["plays"]:
            p["end"].pop("team", None)
    report = _validate_summary(stripped, "nfl")
    assert report.ok, (report.missing, report.invalid)
    assert "plays[].end.team.id absent: the processor fills it from the next play's start team" in report.warnings
    assert "plays[].end.team.id" not in report.missing


def test_a_feed_with_no_drive_result_fields_still_renders(nfl_summary):
    """ESPN's pre-2010 CFB feeds carry no drive ``displayResult`` / ``result`` / ``description``.

    Game on Paper reads all three with a fallback (``DriveRow.astro:81,98``,
    ``latestDrive.ts:41``), so their absence costs a label, not a 404 — ``gop_ok`` must
    survive it. Game 252532751 (2005) has 27 such drives.
    """
    stripped = copy.deepcopy(nfl_summary)
    for d in stripped["drives"]["previous"]:
        for k in ("displayResult", "result", "description"):
            d.pop(k, None)
        if isinstance(d.get("team"), dict):
            d["team"].pop("shortDisplayName", None)
    report = _validate_summary(stripped, "nfl")
    assert report.ok and report.gop_ok, (report.missing, report.invalid, report.gop_missing)
    assert sum("Game on Paper falls back" in w for w in report.warnings) == 4


def test_mirrored_field_position_is_invalid(cfb_summary):
    """An adapted summary whose ``yardLine`` is mirrored against ``yardsToEndzone`` fails (B-GB-08).

    The Fox backup (B-H3) mirrored every away-offense snap; the contract checked presence and
    null rate only, so a real ESPN summary with every ``yardLine`` replaced by ``100 - yardLine``
    still came back ``ok``. ``yardLine`` is measured from the home goal line, so
    ``yardsToEndzone == 100 - yardLine`` when the home team has the ball and ``== yardLine``
    when the away team does. ESPN's own feed is not checked (its orientation varies, see
    ``test_espn_feeds_keep_their_own_yardline_orientation``).
    """
    mirrored = copy.deepcopy(cfb_summary)
    for p in _all_plays(mirrored):
        for side in ("start", "end"):
            if p[side].get("yardLine") is not None:
                p[side]["yardLine"] = 100 - p[side]["yardLine"]
    assert _validate_summary(cfb_summary, "cfb", source="fox").ok
    report = _validate_summary(mirrored, "cfb", source="fox")
    assert not report.ok
    assert any("yardsToEndzone disagrees with yardLine" in i for i in report.invalid), report.invalid
    assert _validate_summary(mirrored, "cfb", source="espn").ok  # never a veto on ESPN


def _committed_espn_summaries():
    out = []
    for path in sorted(Path(__file__).parents[2].rglob("summary_*.json*")):
        if path.suffix not in (".json", ".gz"):
            continue
        text = gzip.open(path, "rt", encoding="utf-8").read() if path.suffix == ".gz" else path.read_text("utf-8")
        summary = json.loads(text)
        if isinstance(summary, dict) and "drives" in summary:  # football only
            out.append(("nfl" if "nfl" in path.parts else "cfb", path, summary))
    return out


def test_every_committed_espn_summary_passes_the_contract():
    """No committed real ESPN football summary is vetoed (a veto drops ESPN in ``_process_game``)."""
    summaries = _committed_espn_summaries()
    assert len(summaries) > 150
    failing = [
        (p.name, r.missing, r.invalid) for lg, p, s in summaries if not (r := _validate_summary(s, lg, "espn")).ok
    ]
    assert failing == []


@pytest.mark.parametrize("game_id", [400876049, 401761665])
def test_espn_feeds_keep_their_own_yardline_orientation(game_id, no_network):
    """ESPN runs ``yardLine`` from the other goal line on 188 of 192 plays of 400876049 and switches
    mid-game on 401761665 (87 of 192); both still serve from ESPN, never ``AllSourcesFailed``."""
    from sportsdataverse.football.sources.dispatch import _process_game

    path = CFB_FIX / f"summary_{game_id}_trimmed.json.gz"
    summary = json.loads(gzip.open(path, "rt", encoding="utf-8").read())
    out = _process_game("cfb", game_id, source="espn", fallthrough=False, payloads={"espn": summary})
    assert out.provenance["served"] == "espn" and out.provenance["contract"]["ok"]
