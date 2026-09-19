"""Shield player/team statistics -> ESPN ``boxscore`` (Phase 5), on real payloads only.

Fixtures are the trimmed real bodies for TB @ CIN, 2026 week 1
(``tests/fixtures/nfl_shield/2026_01_TB_CIN_*``) plus ESPN's own box for the same game
(``tests/nfl/fixtures/box_401872925_espn_trimmed.json.gz``), so every assertion below is a
comparison of two real feeds, never of a synthetic row.

The ``gsis_id -> espn_id`` map is the committed **slice of the nflverse players crosswalk**
this game needs (``players_crosswalk_slice.json.gz``), so the id join is exercised with real
nflverse ids without a network read -- and without the circularity of taking the ids from
ESPN's own box.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from sportsdataverse.football.espn_box import parse_espn_player_box, parse_espn_team_box
from sportsdataverse.nfl.shield_pbp import box as box_mod
from sportsdataverse.nfl.shield_pbp import shield_to_espn_summary

SHIELD_FIX = Path(__file__).resolve().parents[2] / "fixtures" / "nfl_shield"
ESPN_FIX = Path(__file__).resolve().parents[1] / "fixtures"

TB_CIN_ESPN_ID = 401872925
TB_CIN_ROW = {
    "league": "nfl",
    "espn_event_id": str(TB_CIN_ESPN_ID),
    "home_espn_team_id": "4",
    "away_espn_team_id": "27",
    "shield_game_id": "a8fc106b-4feb-11f1-abca-2c54536568a9",
    "nflverse_game_id": "2026_01_TB_CIN",
    "home_team": {"espn_abbr": "CIN"},
    "away_team": {"espn_abbr": "TB"},
}
#: ESPN-proprietary, no Shield counterpart (emitted as ESPN's own "--" token).
UNSOURCEABLE = {"adjQBR"}
#: The two rows on which THIS game's feeds genuinely disagree, both about special-teams tackle
#: credit. Pinned explicitly so a regression shows up as a NEW entry rather than a loosened
#: assertion. Over the 24-game gate (967 defensive rows) the feeds agree exactly everywhere.
#: * Kevin Knowles: ESPN counts his ST assist as a solo (TOT 2 / SOLO 2); Shield's own split is
#:   1 solo + 1 assist -- same total, different solo.
#: * Miles Killebrew: Shield credits him one ST tackle, ESPN lists no defensive row for him.
FEED_DIVERGENT_STATS = {((27, "defensive", "4602648"), "soloTackles")}
SHIELD_ONLY_ROWS = {(27, "defensive", "2575164")}


def _load(name: str) -> dict:
    with gzip.open(SHIELD_FIX / name, "rt", encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="module")
def ids() -> dict:
    with gzip.open(SHIELD_FIX / "players_crosswalk_slice.json.gz", "rt", encoding="utf-8") as fh:
        return {k: tuple(v) for k, v in json.load(fh).items()}


@pytest.fixture(autouse=True)
def _offline_crosswalk(monkeypatch, ids):
    """Every test reads the committed crosswalk slice, never nflverse over the wire."""
    monkeypatch.setattr(box_mod, "_gsis_to_espn", lambda: ids)


@pytest.fixture(scope="module")
def payloads() -> tuple[dict, dict, dict]:
    return (
        _load("2026_01_TB_CIN.json.gz"),
        _load("2026_01_TB_CIN_playerstats.json.gz"),
        _load("2026_01_TB_CIN_teamstats.json.gz"),
    )


@pytest.fixture
def summary(payloads) -> dict:
    game, player_stats, team_stats = payloads
    out, _ = shield_to_espn_summary(game, TB_CIN_ROW, player_stats=player_stats, team_stats=team_stats)
    return out


@pytest.fixture(scope="module")
def espn_box() -> dict:
    with gzip.open(ESPN_FIX / f"box_{TB_CIN_ESPN_ID}_espn_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        return json.load(fh)["boxscore"]


def _key(row):
    return (row["team_id"], row["category"], str(row["athlete_id"]) if row["athlete_id"] else None)


# ------------------------------------------------------------------ player box vs ESPN's own


def test_every_espn_player_box_row_is_reproduced_by_espn_athlete_id(summary, espn_box):
    """Category membership: every ESPN (team, category, athlete) is in the Shield box, and vice versa.

    Pinned from the 24-game 2025/2026 gate, which measured recall AND precision 1.000 on all
    ten categories (``s2-shield-box/out/box_finals.json``).
    """
    reference = {_key(r) for r in parse_espn_player_box(espn_box)}
    candidate = {_key(r) for r in parse_espn_player_box(summary["boxscore"])}
    assert None not in {k[2] for k in candidate}, "an athlete row reached the box with no ESPN id"
    assert reference - candidate == set()
    assert candidate - reference == SHIELD_ONLY_ROWS


def test_every_shield_sourced_stat_equals_espns_own_value(summary, espn_box):
    """Per-stat exactness on the paired rows -- only ``adjQBR`` may differ (ESPN-proprietary)."""
    reference = {_key(r): r for r in parse_espn_player_box(espn_box)}
    candidate = {_key(r): r for r in parse_espn_player_box(summary["boxscore"])}
    mismatched = [
        (k, stat, value, candidate[k].get(stat))
        for k, row in reference.items()
        if k in candidate
        for stat, value in row.items()
        if stat not in UNSOURCEABLE
        and stat not in ("team_id", "team_abbreviation", "category", "athlete_id", "athlete")
        and str(value) != str(candidate[k].get(stat))
    ]
    assert {(k, stat) for k, stat, _, _ in mismatched} == FEED_DIVERGENT_STATS


def test_defensive_totals_fold_special_teams_and_miscellaneous_tackles(summary, espn_box):
    """One category's aggregation, pinned to the shape that made it right.

    ESPN lists a player in ``defensive`` on a special-teams or miscellaneous tackle alone, and
    counts it in ``totalTackles`` / ``soloTackles``. This game must contain at least one such
    player, so dropping either family from :func:`box._solo` / :func:`box._assists` (or from the
    category predicate) fails here rather than only in the 24-game gate.
    """
    player_stats = _load("2026_01_TB_CIN_playerstats.json.gz")
    core = ("defensiveTackles", "defensiveTacklesAssists")
    st_only = [
        p
        for side in ("awayTeam", "homeTeam")
        for p in player_stats[side]["players"]
        if not any(p.get(c) for c in core)
        and any(
            p.get(f)
            for f in (
                "defensiveSpecialTeamsTackles",
                "defensiveSpecialTeamsTacklesAssists",
                "defensiveMiscellaneousTackles",
                "defensiveMiscellaneousTacklesAssists",
            )
        )
    ]
    assert st_only, "fixture no longer exercises the special-teams/miscellaneous tackle fold"
    reference = {_key(r): r for r in parse_espn_player_box(espn_box) if r["category"] == "defensive"}
    candidate = {_key(r): r for r in parse_espn_player_box(summary["boxscore"]) if r["category"] == "defensive"}
    ids = box_mod._gsis_to_espn()
    checked = 0
    for p in st_only:
        espn_id = ids[p["gsisPlayerId"]][0]
        for k in [k for k in reference if k[2] == espn_id]:
            # every such row must be in the candidate at all -- dropping the ST/miscellaneous
            # families from the predicate loses the row entirely
            assert k in candidate, f"{p['gsisPlayerName']} fell out of the defensive box"
            assert candidate[k]["totalTackles"] == reference[k]["totalTackles"]
            checked += 1
    assert checked, "no special-teams-only tackler is in ESPN's defensive box for this game"


# ------------------------------------------------------------------------------ team box


def test_team_box_reproduces_espns_countable_totals(summary, espn_box):
    """``create_box_score`` sources turnovers / interceptions / fumbles lost from this box."""
    reference = parse_espn_team_box(espn_box)
    candidate = parse_espn_team_box(summary["boxscore"])
    assert set(candidate) == set(reference)
    for tid, ref in reference.items():
        for stat in ("turnovers", "interceptions", "fumblesLost", "firstDowns", "totalYards", "totalDrives"):
            assert candidate[tid][stat] == ref[stat], (tid, stat)


# --------------------------------------------------------------- id mapping is fail-closed


def test_an_unmapped_gsis_id_gets_a_null_athlete_id_never_another_players(payloads, monkeypatch, ids):
    """Mutation on the fallback: drop one player from the crosswalk and he must go id-less.

    Never re-used, never guessed -- ``__attach_player_ids`` skips a row with no id, which is
    the pre-Phase-5 behaviour for that one player instead of a wrong-athlete attribution.
    """
    game, player_stats, team_stats = payloads
    dropped = next(
        p for side in ("awayTeam", "homeTeam") for p in player_stats[side]["players"] if p.get("passingAttempts")
    )
    gsis = dropped["gsisPlayerId"]
    monkeypatch.setattr(box_mod, "_gsis_to_espn", lambda: {k: v for k, v in ids.items() if k != gsis})
    out, _ = shield_to_espn_summary(game, TB_CIN_ROW, player_stats=player_stats, team_stats=team_stats)
    rows = [r for r in parse_espn_player_box(out["boxscore"]) if r["athlete"] == dropped["gsisPlayerName"]]
    assert rows, "the unmapped player fell out of the box entirely"
    assert {r["athlete_id"] for r in rows} == {None}
    # and nobody else inherited his id
    others = [r["athlete_id"] for r in parse_espn_player_box(out["boxscore"]) if r["athlete_id"]]
    assert ids[gsis][0] not in others
    assert len(others) == len(
        set(zip(others, [r["category"] for r in parse_espn_player_box(out["boxscore"]) if r["athlete_id"]]))
    )


def test_a_crosswalk_that_cannot_load_degrades_to_an_id_less_box(payloads, monkeypatch):
    game, player_stats, team_stats = payloads
    monkeypatch.setattr(box_mod, "_gsis_to_espn", dict)
    out, _ = shield_to_espn_summary(game, TB_CIN_ROW, player_stats=player_stats, team_stats=team_stats)
    rows = parse_espn_player_box(out["boxscore"])
    assert rows, "the box collapsed instead of degrading"
    assert {r["athlete_id"] for r in rows} == {None}


# ------------------------------------------------------------------------------- fail-open


def test_no_box_payload_leaves_the_summary_boxscore_empty(payloads):
    """The pre-Phase-5 shape, unchanged: an adapter with no statistics payload still runs."""
    game, _, _ = payloads
    out, notes = shield_to_espn_summary(game, TB_CIN_ROW)
    assert out["boxscore"] == {"teams": [], "players": []}
    assert not [n for n in notes if "box" in n]


def test_player_statistics_alone_still_fills_the_player_box(payloads):
    game, player_stats, _ = payloads
    out, _ = shield_to_espn_summary(game, TB_CIN_ROW, player_stats=player_stats)
    assert out["boxscore"]["teams"] == []
    assert len(out["boxscore"]["players"]) == 2


def test_fetch_shield_box_is_fail_open_per_route(monkeypatch):
    """A 404 on one route (measured: 2026_01_CLE_JAX) must not take the other one down."""
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        if "player-statistics" in url:
            raise RuntimeError("404")
        return {"gameId": "x", "homeTeam": {}}

    monkeypatch.setattr("sportsdataverse.nfl.nfl_api_runtime._get", fake_get)
    monkeypatch.setattr("sportsdataverse.nfl.nfl_games.nfl_headers_gen", lambda *a, **k: {})
    player_stats, team_stats = box_mod.fetch_shield_box("abc")
    assert player_stats is None
    assert team_stats == {"gameId": "x", "homeTeam": {}}
    assert len(calls) == 2


# --------------------------------------------------------------- what the processor gains


def test_the_processor_attaches_espn_athlete_ids_once_the_box_is_present(payloads):
    """The GOP-facing point of Phase 5: ``__attach_player_ids`` has something to read."""
    from sportsdataverse.nfl import NFLPlayProcess

    game, player_stats, team_stats = payloads
    boxed, _ = shield_to_espn_summary(game, TB_CIN_ROW, player_stats=player_stats, team_stats=team_stats)
    plain, _ = shield_to_espn_summary(game, TB_CIN_ROW)
    out = {}
    for name, summary in (("boxed", boxed), ("plain", plain)):
        proc = NFLPlayProcess(gameId=TB_CIN_ESPN_ID, join_participants=False)
        proc.espn_nfl_pbp(summary=summary)
        out[name] = proc.run_processing_pipeline()
    assert out["plain"]["advBoxScore"]["espn_players"] == []
    assert out["plain"]["advBoxScore"]["espn_team"] == []
    assert len(out["boxed"]["advBoxScore"]["espn_players"]) > 50
    assert len(out["boxed"]["advBoxScore"]["espn_team"]) == 2
    for section in ("pass", "rush", "receiver", "defensive", "specialists", "player_usage"):
        assert out["boxed"]["advBoxScore"][section], section
