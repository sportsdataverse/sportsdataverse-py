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
from sportsdataverse.errors import NoDataError
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
    # MUTATION TARGET. ``to_cfbfastr`` flips ``yards_to_goal_end`` into the new offence's frame
    # only on the arm that can see the next snap, so on the trailing row it hands back **5** --
    # the spot in the PUNTING team's frame. Emitting that verbatim, credited to the punting
    # team, says North Dakota St. ended 5 yards from scoring immediately after punting the ball
    # away. The same contest's complete page is the oracle: it states 95, for Youngstown St.
    home, away = GAMES["final_fcs_6386315"][2], GAMES["final_fcs_6386315"][3]
    assert punt["start"]["team"]["id"] == home
    assert punt["end"]["team"]["id"] == away
    assert punt["end"]["yardsToEndzone"] == 95
    full = [p for p in plays(summary("final_fcs_6386315")) if p["type"]["text"] == "Punt"]
    twin = next(p for p in full if p["text"] == punt["text"])
    assert (twin["end"]["yardsToEndzone"], twin["end"]["team"]["id"]) == (95, away)


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
        }

    rush, punt = play("Rush", 12), play("Punt", 45)
    _fill_end_state([rush], "2", "1")
    _fill_end_state([punt], "2", "1")
    assert rush["end"]["yardsToEndzone"] == 48
    assert rush["end"]["team"]["id"] == "1"
    # the punt states no end spot, so the ball keeps the start spot -- but it does NOT keep the
    # punting team: the club in start.team.id no longer has it.
    assert punt["end"]["yardsToEndzone"] == 60


def test_a_kickoff_is_framed_on_the_kicking_team_like_espns_own_feed():
    """The frame's internal consistency only -- ``start.yardLine`` agrees with whichever club
    ``start.team.id`` names, and the spot is the mapper's. That holds with or without the
    re-frame, so this is NOT the mutation target for it;
    ``test_the_ensuing_kickoff_is_kicked_by_the_team_that_just_scored`` is, and it is the one
    that goes red when ``_kickoff_frame`` is turned off.

    ESPN's raw summary puts a kickoff's ``start.team.id`` and yard line on the
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


@pytest.mark.parametrize("name", list(GAMES))
def test_the_ensuing_kickoff_is_kicked_by_the_team_that_just_scored(name):
    """MUTATION TARGET for ``_kickoff_frame``. The derivation, checked against the game's own
    narrative: the club that does not have the ball when play resumes is the one that kicked.
    Turning the re-frame off drops pooled ``wp_before`` against ESPN from .9999 to .8725 over six
    gate games."""
    served = summary(name)
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


def test_a_same_prefix_matchup_resolves_two_different_clubs_from_the_crosswalk():
    """MUTATION TARGET. The page links "Texas A&M Aggies", the linescore says "Texas" and
    "Texas A&M", and crediting **every** linescore name the label starts with gave the shorter
    club's side the longer club's ESPN id. Both sides then collapsed to one id and the
    ``home_id == away_id`` guard refused the game -- on all 38 same-prefix matchups in the
    2024-25 archive, including the FCS rivalries this source exists for (North Dakota, South
    Dakota, Montana, Idaho). The longest matching name wins, and only it.
    """
    from sportsdataverse.cfb.ncaa_pbp.fetch import _espn_team_ids_from_bundle

    html = '<a href="/teams/100">Texas Longhorns</a><a href="/teams/200">Texas A&amp;M Aggies</a>'
    crosswalk = {"100": "251", "200": "245"}
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("sportsdataverse.cfb.ncaa_pbp.fetch._crosswalk", lambda: crosswalk)
        resolved = _espn_team_ids_from_bundle(html, {"Texas": "home", "Texas A&M": "away"})
    assert resolved == {"home": "251", "away": "245"}
    # and with the anchors in the other order, which is what made the bug order-dependent
    flipped = '<a href="/teams/200">Texas A&amp;M Aggies</a><a href="/teams/100">Texas Longhorns</a>'
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("sportsdataverse.cfb.ncaa_pbp.fetch._crosswalk", lambda: crosswalk)
        assert _espn_team_ids_from_bundle(flipped, {"Texas": "home", "Texas A&M": "away"}) == resolved


def test_a_linescore_with_no_game_date_is_a_miss_not_an_indexerror():
    """``_espn_team_ids_from_schedule`` is documented "never raises" and is the last leg of the
    cascade, reached exactly when the payload is degraded -- which is also when the linescore
    states no ``game_date``. ``"".split()[0]`` was an IndexError, not a miss."""
    from sportsdataverse.cfb.ncaa_pbp.fetch import _espn_team_ids_from_schedule

    for stamp in (None, "", "   ", "not/a/date", "11/2025"):
        assert _espn_team_ids_from_schedule("401762505", stamp) == {}


def test_the_live_path_is_opt_in_and_never_taken_by_a_request_path_by_default(monkeypatch):
    """MUTATION TARGET. With no archive configured every ``source="ncaa"`` call reaches
    ``_fetch_bundle``; unguarded that is three paced pages behind a 45 s-navigation, 8 s-challenge,
    3-attempt browser transport, and dispatch imposes no per-source time budget. It fails fast
    today only because ``patchright`` is an optional extra nobody installed, which is not a
    timeout. The guard must fire before any transport is touched."""
    from sportsdataverse.cfb.ncaa_pbp.fetch import LIVE_FETCH_ENV, _fetch_bundle

    monkeypatch.delenv(LIVE_FETCH_ENV, raising=False)
    sentinel = object()

    class _Boom:
        def fetch_html(self, path):  # pragma: no cover - the guard must run first
            raise AssertionError("the live transport was touched with the opt-in unset")

    with pytest.raises(RuntimeError, match=LIVE_FETCH_ENV):
        _fetch_bundle("6386337", fetcher=_Boom())
    assert sentinel is not None

    # and the adapter turns it into a hand-over, naming the opt-in rather than a traceback
    row = {"espn_event_id": "401762505", "season": 2025, "ncaa_game_id": "6386337"}
    monkeypatch.delenv("SDV_NCAA_MFB_ARCHIVE", raising=False)
    with pytest.raises(SourceUnavailable, match=LIVE_FETCH_ENV):
        adapter(401762505, _Ctx(payload=None, idmap_row=row))


@pytest.mark.parametrize("name", list(GAMES))
def test_rows_the_page_states_no_clock_for_carry_a_real_clock_not_a_0_00_fill(name):
    """MUTATION TARGET, and the one the gate moved most. stats.ncaa.org prints ``(MM:SS)`` only
    where the stat crew entered one; filling the rest with ``0:00`` puts those plays at the end of
    their quarter, and the clock is an EP/WP input. Reverting to a ``0:00`` fill drops pooled
    EP_start against ESPN from .9986 to .9487 over six gate games -- with, before this test, every
    test in this module still green."""
    from sportsdataverse.cfb.ncaa_pbp.to_espn_summary import _clocks, _parse_bundle

    cfbfastr, _, _, drive_titles = _parse_bundle(bundle(name), GAMES[name][1])
    rows = cfbfastr.to_dicts()
    unstated = [i for i, r in enumerate(rows) if r.get("clock.minutes") is None and r.get("clock.seconds") is None]
    assert unstated, "fixture no longer exercises the clockless branch"
    clocks, carried = _clocks(rows, drive_titles)
    assert carried == len(unstated)
    # every clockless row takes a real clock from its own drive, never the 0:00 fill
    assert [clocks[i] for i in unstated if clocks[i] == "0:00"] == []
    # and it is the last clock the page actually stated, not an interpolation
    for i in unstated:
        stated_before = [_clock_text(rows[j]) for j in range(i) if rows[j].get("period") == rows[i].get("period")]
        stated_before = [c for c in stated_before if c]
        if stated_before:
            assert clocks[i] in {stated_before[-1], *_drive_start_clocks(drive_titles)}
    # the served summary carries them through
    served = plays(summary(name))
    assert sum(1 for p in served if p["clock"]["displayValue"] == "0:00") < len(unstated)


def _clock_text(row: dict) -> str | None:
    minutes, seconds = row.get("clock.minutes"), row.get("clock.seconds")
    if minutes is None and seconds is None:
        return None
    return f"{int(minutes or 0)}:{int(seconds or 0):02d}"


def _drive_start_clocks(drive_titles: pl.DataFrame) -> set:
    if not drive_titles.height or "start_clock" not in drive_titles.columns:
        return set()
    out = set()
    for value in drive_titles.get_column("start_clock").to_list():
        parts = str(value or "").split(":")
        if len(parts) == 2 and all(p.strip().isdigit() for p in parts):
            out.add(f"{int(parts[0])}:{int(parts[1]):02d}")
    return out


def test_one_club_from_the_id_map_and_one_from_the_crosswalk_is_enough():
    """MUTATION TARGET. ``_ncaa_to_espn_summary`` resolves each side from the id-map row first
    and the crosswalk second, so counting only the crosswalk's hits refused a game whose id map
    carried one club and whose crosswalk carried the other -- a servable game, handed to the next
    source for nothing. The completeness test is per side, across both sources."""
    row = {k: v for k, v in idmap_row("final_fbs_6386337").items() if k != "away_espn_team_id"}
    with pytest.MonkeyPatch.context() as mp:
        # the crosswalk carries ONLY the away club -- the shape that made ``len(team_ids) < 2``
        # refuse a game both sources together can key
        mp.setattr(
            "sportsdataverse.cfb.ncaa_pbp.to_espn_summary._espn_team_ids_from_bundle",
            lambda html, sides: {"away": "202"},
        )
        adapted = adapter(401762505, _Ctx(payload=bundle("final_fbs_6386337"), idmap_row=row))
    competitors = adapted.summary["header"]["competitions"][0]["competitors"]
    assert [c["team"]["id"] for c in competitors] == ["2226", "202"]


# --- the Data API leg: archive -> Data API -> live (opt-in) ------------------------------------


class _Resp:
    """The two attributes ``_api_bundle`` touches on a ``download`` response."""

    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body


def _transport(body=None, raises=None, calls=None):
    """A ``download`` stand-in recording its kwargs; ``raises`` is the exception to throw."""

    def fake(**kwargs):
        if calls is not None:
            calls.append(kwargs)
        if raises is not None:
            raise raises
        return _Resp(body)

    return fake


def test_the_data_api_leg_returns_the_same_bundle_the_archive_would_have(monkeypatch):
    """The whole point of NCAA-PROD: the consumer that cannot mount the checkout reads the
    identical payload over HTTP."""
    from sportsdataverse.cfb.ncaa_pbp.fetch import CONTEST_ROUTE, _api_bundle

    calls: list[dict] = []
    served = _api_bundle(
        "6386337",
        season=2025,
        base_url="http://127.0.0.1:8000/",
        transport=_transport(bundle("final_fbs_6386337"), calls=calls),
    )
    assert served == bundle("final_fbs_6386337")
    assert calls[0]["url"] == "http://127.0.0.1:8000" + CONTEST_ROUTE.format(contest_id="6386337")
    assert calls[0]["params"] == {"season": 2025}


def test_the_data_api_leg_is_bounded_and_never_retries(monkeypatch):
    """MUTATION TARGET. It runs on Game on Paper's request path and dispatch imposes no
    per-source budget, so the ceiling has to live here: ``download``'s defaults are a 30 s
    timeout and **15** retries with backoff — minutes of retry storm per unservable game."""
    from sportsdataverse.cfb.ncaa_pbp.fetch import API_TIMEOUT, _api_bundle

    calls: list[dict] = []
    _api_bundle("6386337", base_url="http://x", transport=_transport({}, calls=calls))
    assert calls[0]["timeout"] == API_TIMEOUT <= 5.0
    assert calls[0]["num_retries"] == 0


@pytest.mark.parametrize(
    "failure",
    [
        NoDataError("404: not archived"),  # the game is not in the archive
        RuntimeError("503: no archive on this deployment"),
        TimeoutError("read timed out"),
        ValueError("not json"),  # a proxy's HTML error page
    ],
)
def test_every_data_api_failure_is_a_miss_not_a_raise(failure):
    """404, 503, a timeout and a junk body are one answer -- None -- so the caller falls to the
    opt-in live leg and then to ``SourceUnavailable``. An HTTP exception must not escape onto a
    request path from here."""
    from sportsdataverse.cfb.ncaa_pbp.fetch import _api_bundle

    assert _api_bundle("6386337", base_url="http://x", transport=_transport(raises=failure)) is None


@pytest.mark.parametrize("body", [None, {}, [], "html", 7])
def test_a_non_bundle_body_is_a_miss(body):
    from sportsdataverse.cfb.ncaa_pbp.fetch import _api_bundle

    assert _api_bundle("6386337", base_url="http://x", transport=_transport(body)) is None


def test_no_data_api_configured_means_the_leg_is_skipped_entirely(monkeypatch):
    from sportsdataverse.cfb.ncaa_pbp.fetch import _api_bundle
    from sportsdataverse.football.sources.dispatch import IDMAP_BASE_URL_ENV

    monkeypatch.delenv(IDMAP_BASE_URL_ENV, raising=False)
    calls: list[dict] = []
    assert _api_bundle("6386337", transport=_transport({}, calls=calls)) is None
    assert calls == []  # not "called and ignored": never called


def test_the_read_key_is_sent_when_one_is_configured(monkeypatch):
    """Every ``/v1/`` route on the Data API needs a bearer key; without one the leg 401s."""
    from sportsdataverse.cfb.ncaa_pbp.fetch import _api_bundle
    from sportsdataverse.football.sources.idmap import API_KEY_ENV

    calls: list[dict] = []
    monkeypatch.setenv(API_KEY_ENV, "sdv_test_key")
    _api_bundle("6386337", base_url="http://x", transport=_transport({}, calls=calls))
    assert calls[0]["headers"] == {"Authorization": "Bearer sdv_test_key"}
    monkeypatch.delenv(API_KEY_ENV)
    _api_bundle("6386337", base_url="http://x", transport=_transport({}, calls=calls))
    assert calls[1]["headers"] == {}


def test_a_local_archive_wins_and_the_data_api_is_never_called(monkeypatch, tmp_path):
    """Order matters for latency, not correctness: the droplet must keep reading its own disk."""
    import gzip as _gzip

    from sportsdataverse.cfb.ncaa_pbp.fetch import ARCHIVE_DIR_ENV
    from sportsdataverse.football.sources.dispatch import IDMAP_BASE_URL_ENV

    year = tmp_path / "mfb" / "raw" / "2026"
    year.mkdir(parents=True)
    with _gzip.open(year / "6386337.json.gz", "wt", encoding="utf-8") as fh:
        json.dump(bundle("final_fbs_6386337"), fh)
    monkeypatch.setenv(ARCHIVE_DIR_ENV, str(tmp_path))
    monkeypatch.setenv(IDMAP_BASE_URL_ENV, "http://127.0.0.1:8000")
    calls: list[dict] = []
    monkeypatch.setattr("sportsdataverse.dl_utils.download", _transport({}, calls=calls))

    processed = _process_game(
        "cfb", 401762505, source="ncaa", fallthrough=False, idmap_row=idmap_row("final_fbs_6386337")
    )
    assert processed.provenance["native_ids"]["ncaa_bundle_source"] == "archive"
    assert calls == []


def test_with_no_archive_the_data_api_serves_the_game_and_stamps_its_provenance(monkeypatch):
    """The production shape: Game on Paper's API, which cannot mount the checkout."""
    from sportsdataverse.cfb.ncaa_pbp.fetch import ARCHIVE_DIR_ENV
    from sportsdataverse.football.sources.dispatch import IDMAP_BASE_URL_ENV

    monkeypatch.delenv(ARCHIVE_DIR_ENV, raising=False)
    monkeypatch.setenv(IDMAP_BASE_URL_ENV, "http://127.0.0.1:8000")
    monkeypatch.setattr("sportsdataverse.dl_utils.download", _transport(bundle("final_fbs_6386337")))

    processed = _process_game(
        "cfb", 401762505, source="ncaa", fallthrough=False, idmap_row=idmap_row("final_fbs_6386337")
    )
    assert processed.provenance["native_ids"]["ncaa_bundle_source"] == "data_api"
    assert processed.plays_frame.height > 50


def test_the_data_api_frame_is_identical_to_the_archive_frame(monkeypatch, tmp_path):
    """Identity, not "looks right": the same game served by disk and by HTTP must produce the
    same plays frame, or the Data API leg is a second source wearing the first one's name."""
    import gzip as _gzip

    from sportsdataverse.cfb.ncaa_pbp.fetch import ARCHIVE_DIR_ENV
    from sportsdataverse.football.sources.dispatch import IDMAP_BASE_URL_ENV

    year = tmp_path / "mfb" / "raw" / "2026"
    year.mkdir(parents=True)
    with _gzip.open(year / "6386337.json.gz", "wt", encoding="utf-8") as fh:
        json.dump(bundle("final_fbs_6386337"), fh)

    def run() -> pl.DataFrame:
        return _process_game(
            "cfb", 401762505, source="ncaa", fallthrough=False, idmap_row=idmap_row("final_fbs_6386337")
        ).plays_frame

    monkeypatch.setenv(ARCHIVE_DIR_ENV, str(tmp_path))
    monkeypatch.delenv(IDMAP_BASE_URL_ENV, raising=False)
    from_disk = run()

    monkeypatch.delenv(ARCHIVE_DIR_ENV)
    monkeypatch.setenv(IDMAP_BASE_URL_ENV, "http://127.0.0.1:8000")
    monkeypatch.setattr("sportsdataverse.dl_utils.download", _transport(bundle("final_fbs_6386337")))
    over_http = run()

    assert from_disk.to_dicts() == over_http.to_dicts()


def test_neither_archive_nor_data_api_fails_closed_naming_both(monkeypatch):
    """MUTATION TARGET. With every leg empty the caller gets ``SourceUnavailable`` -- not a
    browser launch, not a hang -- and the message says which legs were tried."""
    import time as _time

    from sportsdataverse.cfb.ncaa_pbp.fetch import ARCHIVE_DIR_ENV, LIVE_FETCH_ENV
    from sportsdataverse.football.sources.dispatch import IDMAP_BASE_URL_ENV

    monkeypatch.delenv(ARCHIVE_DIR_ENV, raising=False)
    monkeypatch.delenv(LIVE_FETCH_ENV, raising=False)
    monkeypatch.setenv(IDMAP_BASE_URL_ENV, "http://127.0.0.1:8000")
    monkeypatch.setattr("sportsdataverse.dl_utils.download", _transport(raises=NoDataError("404: not archived")))

    t0 = _time.monotonic()
    with pytest.raises(SourceUnavailable, match="neither the local archive nor the Data API"):
        adapter(401762505, _Ctx(idmap_row=idmap_row("final_fbs_6386337")))
    assert _time.monotonic() - t0 < 1.0
