"""NCAA -> ESPN-summary adapter (``source="ncaa"`` for CFB), offline.

Every fixture is a **real** stats.ncaa.org contest bundle from ``ncaa-mfb-football-raw``, trimmed
to the three pages the adapter reads (``play_by_play`` / ``box_score`` / ``drives``) and, for the
in-progress cases, cut mid-drive. Provenance is in ``fixtures/README.md``.

**Zero network.** No test resolves an id, fetches a page or reads a release asset: every bundle is
injected as ``payloads={"ncaa": ...}``, which is the branch that skips resolution entirely. A
module-level autouse fixture makes that structural by making ``socket.socket`` raise.
"""

from __future__ import annotations

import gzip
import json
import socket
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.ncaa_pbp.fetch import _archive_bundle, _has_plays, _resolve_contest_id
from sportsdataverse.cfb.ncaa_pbp.to_espn_summary import _fill_end_state, _ncaa_to_espn_summary
from sportsdataverse.football.sources.contract import _validate_summary
from sportsdataverse.football.sources.dispatch import AllSourcesFailed, SourceUnavailable, _process_game

FIXTURES = Path(__file__).parent / "fixtures"

#: ``{fixture: (espn_event_id, season, home espn team id, away espn team id)}``. The team ids are
#: ESPN's own, read off the vendored crosswalk for these contests.
GAMES = {
    "final_fcs_6386315": ("401767513", 2025, "2449", "2754"),  # Youngstown St. at North Dakota St.
    "final_fbs_6386337": ("401762505", 2025, "2226", "202"),  # Tulsa at Florida Atlantic
}


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """Any socket this module opens is a bug: the whole suite runs on injected payloads."""

    def boom(*args, **kwargs):
        raise AssertionError("a test in this module tried to open a socket")

    monkeypatch.setattr(socket, "socket", boom)
    monkeypatch.setattr(socket, "create_connection", boom)


def bundle(name: str) -> dict:
    with gzip.open(FIXTURES / f"{name}.json.gz", "rt", encoding="utf-8") as fh:
        return json.load(fh)


def idmap_row(name: str) -> dict:
    espn_id, season, home, away = GAMES[name]
    return {
        "espn_event_id": espn_id,
        "season": season,
        "week": 3,
        "home_espn_team_id": home,
        "away_espn_team_id": away,
        "ncaa_game_id": name.rsplit("_", 1)[-1],
    }


def summary(name: str, **kwargs) -> dict:
    return _ncaa_to_espn_summary(bundle(name), idmap_row(name), **kwargs)[0]


def plays(summary_dict: dict) -> list[dict]:
    served = summary_dict["drives"]["previous"] + [d for d in (summary_dict["drives"].get("current"),) if d]
    return [play for drive in served for play in drive["plays"]]


# --- G1: a final projects, satisfies the contract and drives the unmodified processor ----------


@pytest.mark.parametrize("name", list(GAMES))
def test_a_real_final_satisfies_the_summary_contract(name):
    report = _validate_summary(summary(name), "cfb")
    assert report.ok, report.summary()
    assert report.summary()["n_plays"] > 50


@pytest.mark.parametrize("name", list(GAMES))
def test_the_unmodified_processor_runs_and_leaves_no_null_epa(name):
    espn_id, *_ = GAMES[name]
    processed = _process_game(
        "cfb",
        int(espn_id),
        source="ncaa",
        fallthrough=False,
        payloads={"ncaa": bundle(name)},
        idmap_row=idmap_row(name),
    )
    frame = processed.plays_frame
    assert frame.height > 50
    assert frame.select(pl.col("EPA").is_null().sum()).item() == 0
    assert frame.select(pl.col("wp_before").is_null().sum()).item() == 0
    assert processed.provenance["served"] == "ncaa"
    assert processed.provenance["native_ids"]["ncaa_contest_id"] == name.rsplit("_", 1)[-1]


def test_an_fcs_hosted_game_is_servable():
    """The reason this source exists: Fox, CBS and Yahoo are all empty on FCS-hosted CFB."""
    served = summary("final_fcs_6386315")
    competitors = served["header"]["competitions"][0]["competitors"]
    assert [c["team"]["location"] for c in competitors] == ["North Dakota St.", "Youngstown St."]
    assert len(plays(served)) > 100


def test_competitors_carry_the_mascot_the_page_states_not_an_invented_one():
    """An empty ``team.name`` charges every Timeout row to BOTH clubs (the mascot is matched
    against the text), and an invented one charges the wrong club."""
    competitors = summary("final_fbs_6386337")["header"]["competitions"][0]["competitors"]
    assert [c["team"]["name"] for c in competitors] == ["Owls", "Golden Hurricane"]
    assert all(c["team"]["name"] for c in competitors)


@pytest.mark.parametrize(("name", "expected"), [("final_fcs_6386315", [30, 38]), ("final_fbs_6386337", [21, 40])])
def test_the_final_score_matches_the_pages_own_linescore(name, expected):
    """The header score is the last play's running score; the linescore is the official final.
    They agree, which is what says the event-sourced score walked the whole game correctly."""
    competitors = summary(name)["header"]["competitions"][0]["competitors"]
    assert sorted(int(c["score"]) for c in competitors) == expected


# --- the review checklist: PAT fold target, scoring end state, no-next-snap end state ----------


def test_the_try_folds_onto_its_touchdown_not_onto_the_row_before_it():
    """MUTATION TARGET. Both fixtures carry a try that is **not** adjacent to its touchdown (a
    penalty sits between), so folding onto ``emitted[-1]`` hangs ``pointAfterAttempt`` on a
    Penalty row and leaves the touchdown stepping the scoreboard by 6."""
    for name in GAMES:
        with_try = [p for p in plays(summary(name)) if p.get("pointAfterAttempt")]
        assert with_try, name
        for play in with_try:
            assert play["type"]["text"].endswith("Touchdown"), (name, play["type"], play["text"][:80])


def test_a_defensive_touchdown_ends_in_the_scoring_teams_frame():
    """A fumble returned by the defence (``score_pts`` is negative) must be credited to the
    defence: crediting possession flips the end spot 100 yards and ~14 EPA."""
    served = summary("final_fbs_6386337")
    home_id, away_id = GAMES["final_fbs_6386337"][2], GAMES["final_fbs_6386337"][3]
    returns = [p for p in plays(served) if p["type"]["text"] == "Fumble Recovery (Opponent) Touchdown"]
    assert returns, "fixture no longer carries a defensive-return touchdown"
    for play in returns:
        assert play["end"]["team"]["id"] != play["start"]["team"]["id"]
        assert play["end"]["yardsToEndzone"] == 0
        assert play["end"]["yardLine"] == (100 if play["end"]["team"]["id"] == home_id else 0)
    assert {home_id, away_id} == {p["start"]["team"]["id"] for p in plays(served)} | {home_id, away_id}


def test_espns_own_end_down_conventions_are_stated_not_the_next_snaps():
    """MUTATION TARGET. Counted over 60 captured ESPN CFB summaries: every touchdown label and
    every made field goal carries ``end.down = -1, end.distance = -1`` (171/171 Passing Touchdown,
    128/128 Field Goal Good), and a **returned** kickoff carries ``(-1, 10)`` -- not the 1st and 10
    the receiving team actually faces. The EP model reads ``down``, so emitting the next snap's
    state moved EP_end by about a point on every kickoff return."""
    for name in GAMES:
        served = plays(summary(name))
        returns = [p for p in served if p["type"]["text"] == "Kickoff Return (Offense)"]
        assert returns, name
        for play in returns:
            assert (play["end"]["down"], play["end"]["distance"]) == (-1, 10), play["text"][:70]
        for play in served:
            if play["type"]["text"].endswith("Touchdown") or play["type"]["text"] == "Field Goal Good":
                assert (play["end"]["down"], play["end"]["distance"]) == (-1, -1), play["text"][:70]


def test_a_play_with_no_next_snap_ends_where_its_own_yardage_puts_it():
    """MUTATION TARGET. The last row of the page -- and, on a truncated page, the newest row,
    which is the one Game on Paper renders at the top -- has no next snap to read an end state
    from. Taking its own start says the ball never moved, so a 6-yard gain is scored as a 0-yard
    one against a spot no feed reports."""
    last = plays(summary_for_truncated("truncated_6386337"))[-1]
    assert last["type"]["text"] == "Rush"
    assert last["statYardage"] == 6
    assert last["start"]["yardsToEndzone"] == 34
    assert last["end"]["yardsToEndzone"] == 28


def test_a_trailing_kick_takes_the_end_spot_the_page_states():
    """The NCAA text usually states the end spot itself ("punt 39 yards to the YSU05"), which is
    a better answer than any inference -- and still not the play's own start."""
    served = _ncaa_to_espn_summary(
        bundle("truncated_6386315"),
        {
            "espn_event_id": GAMES["final_fcs_6386315"][0],
            "season": 2025,
            "home_espn_team_id": GAMES["final_fcs_6386315"][2],
            "away_espn_team_id": GAMES["final_fcs_6386315"][3],
        },
    )[0]
    punt = [p for p in plays(served) if p["type"]["text"] == "Punt"][-1]
    assert punt["start"]["yardsToEndzone"] == 44
    assert punt["end"]["yardsToEndzone"] == 5


def test_the_no_next_snap_end_state_is_derived_not_copied():
    """The same rule, exercised directly: a scrimmage snap with no successor moves the ball by
    its own yardage; a kick or a turnover, where the yardage says nothing about the next spot,
    keeps the start."""

    def play(type_text: str, gained: int) -> dict:
        return {
            "type": {"text": type_text},
            "statYardage": gained,
            "scoringPlay": False,
            "start": {"down": 2, "distance": 7, "yardLine": 40, "yardsToEndzone": 60, "team": {"id": "1"}},
            "end": {},
            "_yards_to_goal_end": None,
            "_touchdown": False,
            "_scoring_team": None,
            "_end_is_home": False,
        }

    rush, punt = play("Rush", 12), play("Punt", 45)
    _fill_end_state([rush], "2")
    _fill_end_state([punt], "2")
    assert rush["end"]["yardsToEndzone"] == 48
    assert punt["end"]["yardsToEndzone"] == 60


def test_a_kickoff_is_framed_on_the_kicking_team_like_espns_own_feed():
    """MUTATION TARGET. ESPN's raw summary puts a kickoff's ``start.team.id`` and yard line on the
    team that **kicked**; ``CFBPlayProcess`` derives ``pos_team`` as the return team from it.
    ``to_cfbfastr`` states the same spot but leaves possession on the drive's team, which on a
    stats.ncaa.org page is whichever club the row was printed under -- so passing it through made
    the processor read the kicking team backwards on 50 of 52 kickoff rows.

    The spot is unchanged (``yards_to_goal`` is already measured in the kicker's direction); only
    the club and the absolute yard line move with it.
    """
    from sportsdataverse.cfb.ncaa_pbp.to_espn_summary import _parse_bundle

    name = "final_fcs_6386315"
    home, away = GAMES[name][2], GAMES[name][3]
    cfbfastr, _, _, _ = _parse_bundle(bundle(name), 2025)
    spots = cfbfastr.filter(pl.col("play_type") == "Kickoff").get_column("yards_to_goal").to_list()
    kickoffs = [p for p in plays(summary(name)) if p["type"]["text"].startswith("Kickoff")]
    assert kickoffs
    for play in kickoffs:
        expected = 100 - play["start"]["yardsToEndzone"] if play["start"]["team"]["id"] == home else None
        if expected is not None:
            assert play["start"]["yardLine"] == expected
        else:
            assert play["start"]["yardLine"] == play["start"]["yardsToEndzone"]
    # the spot itself is the mapper's, untouched
    assert [p["start"]["yardsToEndzone"] for p in kickoffs if p["type"]["text"] == "Kickoff"] == spots
    assert {p["start"]["team"]["id"] for p in kickoffs} <= {home, away}


def test_the_ensuing_kickoff_is_kicked_by_the_team_that_just_scored():
    """The derivation, checked against the game's own narrative: the club that does not have the
    ball when play resumes is the one that kicked."""
    served = summary("final_fbs_6386337")
    ordered = plays(served)
    for index, play in enumerate(ordered):
        if not play["type"]["text"].startswith("Kickoff"):
            continue
        snap = next((p for p in ordered[index + 1 :] if (p["start"]["down"] or 0) >= 1), None)
        if snap is None or play["type"]["text"].endswith("Touchdown"):
            continue
        assert play["start"]["team"]["id"] != snap["start"]["team"]["id"], play["text"][:70]


# --- G2: a truncated page is an in-progress game ----------------------------------------------


@pytest.mark.parametrize("name", ["truncated_6386337", "truncated_6386315", "truncated_early_6386337"])
def test_a_truncated_page_opens_a_current_drive_with_no_outcome(name):
    espn_id, season, home, away = GAMES["final_fbs_6386337" if "6386337" in name else "final_fcs_6386315"]
    served, notes = _ncaa_to_espn_summary(
        bundle(name),
        {
            "espn_event_id": espn_id,
            "season": season,
            "home_espn_team_id": home,
            "away_espn_team_id": away,
        },
    )
    current = served["drives"].get("current")
    assert current is not None, name
    assert current["result"] == current["displayResult"] == "In Progress"
    assert current["isScore"] is False
    assert current["plays"]
    assert served["header"]["competitions"][0]["status"]["type"]["completed"] is False
    assert any("drives.current" in note for note in notes)
    assert _validate_summary(served, "cfb").ok


def test_the_open_drives_plays_still_reach_the_play_presence_guard():
    """The #540 class: a guard that reads ``drives.previous`` only refuses the whole opening
    drive of a live game and reports an empty chart."""
    served = summary_for_truncated("truncated_early_6386337")
    assert not served["drives"]["previous"] or served["drives"]["current"]["plays"]
    assert plays(served)


def summary_for_truncated(name: str) -> dict:
    espn_id, season, home, away = GAMES["final_fbs_6386337"]
    return _ncaa_to_espn_summary(
        bundle(name),
        {"espn_event_id": espn_id, "season": season, "home_espn_team_id": home, "away_espn_team_id": away},
    )[0]


# --- G3: a game NCAA does not carry fails closed -----------------------------------------------


class _Ctx:
    def __init__(self, payload=None, idmap_row=None):
        self.payload, self.idmap_row = payload, idmap_row
        self.participants = self.odds_override = None


def adapter(espn_id: int, ctx: _Ctx):
    from sportsdataverse.cfb.ncaa_pbp.to_espn_summary import _ncaa_adapter

    return _ncaa_adapter("cfb", espn_id, ctx)


def test_a_contest_page_with_no_drive_markup_is_source_unavailable_by_shape():
    """stats.ncaa.org answers HTTP 200 with a real contest page for a game it holds no
    play-by-play for. The capture gate means the archive can never store one (a pbp page under
    40 KB or without ``drives`` is refused), so the shape only reaches the live path -- and the
    box-score page of a real contest is exactly that shape."""
    real = bundle("final_fbs_6386337")
    no_pbp = {"contest_id": real["contest_id"], "play_by_play": real["box_score"], "box_score": real["box_score"]}
    assert _has_plays(no_pbp) is False
    with pytest.raises(SourceUnavailable, match="carries no play-by-play"):
        adapter(401762505, _Ctx(payload=no_pbp, idmap_row=idmap_row("final_fbs_6386337")))


def test_a_game_with_no_contest_id_is_source_unavailable_never_a_guessed_id():
    """An FBS game NCAA does not carry has no ``ncaa_game_id``, and there is no formula that
    could produce one -- the contest-id namespace is opaque."""
    assert _resolve_contest_id({"espn_event_id": "401752665"}) == (None, "unresolved")
    with pytest.raises(SourceUnavailable, match="no ncaa_game_id"):
        adapter(401752665, _Ctx(idmap_row={"espn_event_id": "401752665", "season": 2025}))
    with pytest.raises(SourceUnavailable, match="no ncaa_game_id"):
        adapter(401752665, _Ctx())


def test_a_non_bundle_payload_is_source_unavailable_not_a_traceback():
    with pytest.raises(SourceUnavailable, match="not a contest bundle"):
        adapter(401762505, _Ctx(payload=[], idmap_row=idmap_row("final_fbs_6386337")))


def test_an_id_map_row_with_no_team_ids_falls_back_to_the_vendored_crosswalk():
    """The crosswalk resolves both clubs for these contests, so the adapter serves the game
    without inventing an id -- and without reaching ESPN (the autouse socket guard proves it)."""
    row = {k: v for k, v in idmap_row("final_fbs_6386337").items() if not k.endswith("espn_team_id")}
    adapted = adapter(401762505, _Ctx(payload=bundle("final_fbs_6386337"), idmap_row=row))
    competitors = adapted.summary["header"]["competitions"][0]["competitors"]
    assert [c["team"]["id"] for c in competitors] == ["2226", "202"]
    assert adapted.native_ids["ncaa_id_resolved_by"].endswith("crosswalk")


def test_a_club_the_crosswalk_does_not_carry_is_source_unavailable(monkeypatch):
    """No ESPN team id must ever be invented: ``CFBPlayProcess`` casts ``team.id`` to ``int`` and
    keys possession, logos and the box score off it."""
    import sportsdataverse.cfb.ncaa_pbp.fetch as fetch_module

    monkeypatch.setattr(fetch_module, "_crosswalk", dict)
    row = {k: v for k, v in idmap_row("final_fbs_6386337").items() if not k.endswith("espn_team_id")}
    with pytest.raises(SourceUnavailable, match="no ESPN team id for both clubs"):
        adapter(401762505, _Ctx(payload=bundle("final_fbs_6386337"), idmap_row=row))


def test_dispatch_falls_through_to_the_next_source_when_ncaa_has_nothing():
    with pytest.raises(AllSourcesFailed) as excinfo:
        _process_game("cfb", 401752665, source="ncaa", fallthrough=False, idmap_row={"espn_event_id": "401752665"})
    assert "no ncaa_game_id" in str(excinfo.value)


# --- plumbing ---------------------------------------------------------------------------------


def test_ncaa_is_registered_for_cfb_and_only_for_cfb():
    from sportsdataverse.football.sources.dispatch import SOURCE_ORDER, _adapter_for

    assert _adapter_for("cfb", "ncaa") is not None
    assert _adapter_for("nfl", "ncaa") is None
    assert "ncaa" in SOURCE_ORDER["cfb"]


def test_an_archive_miss_is_a_miss_not_a_raise(tmp_path):
    assert _archive_bundle("6386337", season=2025, root=tmp_path) is None
    assert _archive_bundle("6386337", season=2025, root=tmp_path / "nope") is None
    (tmp_path / "mfb" / "raw" / "2026").mkdir(parents=True)
    (tmp_path / "mfb" / "raw" / "2026" / "6386337.json.gz").write_bytes(b"not gzip")
    assert _archive_bundle("6386337", season=2025, root=tmp_path) is None


def test_the_vendored_crosswalk_ships_with_the_package():
    from sportsdataverse.cfb.ncaa_pbp.fetch import _crosswalk

    table = _crosswalk()
    assert len(table) > 3000
    assert all(isinstance(k, str) and isinstance(v, str) for k, v in list(table.items())[:50])
