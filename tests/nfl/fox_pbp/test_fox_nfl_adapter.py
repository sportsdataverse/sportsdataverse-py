"""NFL-side Fox adapter: id resolution, no-coverage detection and dispatch wiring. Zero network."""

from __future__ import annotations

import json
import pathlib

import pytest

from sportsdataverse.football.sources.dispatch import SourceUnavailable, _process_game
from sportsdataverse.nfl.fox_pbp.game_id import _FOX_TEAM_BY_ESPN, _resolve_fox_game_id, _segment_ids
from sportsdataverse.nfl.fox_pbp.to_espn_summary import _fox_adapter, _resolve_fox_event_id

FIXTURES = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "fox"


def _load(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def meta():
    return _load("fox_nfl_401671775_meta.json")


@pytest.fixture(scope="module")
def row(meta):
    return {
        k: meta[k] for k in ("espn_event_id", "season", "season_type", "week", "home_espn_team_id", "away_espn_team_id")
    }


class _Ctx:
    def __init__(self, payload=None, idmap_row=None, odds_override=None):
        self.payload = payload
        self.idmap_row = idmap_row
        self.participants = None
        self.odds_override = odds_override


# --------------------------------------------------------------------------- id resolution
def test_the_franchise_table_covers_every_club_and_maps_one_to_one():
    assert len(_FOX_TEAM_BY_ESPN) == 32
    assert len(set(_FOX_TEAM_BY_ESPN.values())) == 32


def test_segment_ids_follow_foxs_season_week_type_shape():
    assert _segment_ids(2026, 2, 1) == ["2026-1-1"]  # regular season
    assert _segment_ids(2026, 1, 2) == ["2026-2-3"]  # preseason
    # ESPN numbers the Pro Bowl 4 and the Super Bowl 5; Fox has no Pro Bowl and calls the
    # Super Bowl week 4, so the mapped week leads and the other postseason weeks follow
    assert _segment_ids(2025, 3, 5)[0] == "2025-4-2"
    assert set(_segment_ids(2025, 3, 5)) == {"2025-1-2", "2025-2-2", "2025-3-2", "2025-4-2"}
    assert _segment_ids(2026, 2, None) == []


def test_a_stored_fox_event_id_wins_and_costs_no_request():
    def explode(path):  # pragma: no cover - must never be called
        raise AssertionError(f"resolution fetched {path} although the id map stated one")

    assert _resolve_fox_event_id({"fox_event_id": "11187"}, transport=explode) == ("11187", "idmap")


def test_the_week_scoreboard_resolves_the_id_from_the_fox_team_pair(meta):
    """The leg that has to work in an ESPN outage: Fox's own schedule, joined on Fox team ids."""
    calls = []

    def transport(path):
        calls.append(path)
        return {
            "sectionList": [
                {
                    "id": "s",
                    "events": [
                        {
                            "contentUri": "football/nfl/events/99999",
                            "entityLink": {
                                "layout": {
                                    "tokens": {
                                        "id": "99999",
                                        "homeUri": "football/nfl/teams/1",
                                        "awayUri": "football/nfl/teams/2",
                                    }
                                }
                            },
                            "upperTeam": {"uri": "football/nfl/teams/2"},
                            "lowerTeam": {"uri": "football/nfl/teams/1"},
                        },
                        {
                            "contentUri": "football/nfl/events/10625",
                            "entityLink": {
                                "layout": {
                                    "tokens": {
                                        "id": "10625",
                                        "homeUri": f"football/nfl/teams/{_FOX_TEAM_BY_ESPN[meta['home_espn_team_id']]}",
                                        "awayUri": f"football/nfl/teams/{_FOX_TEAM_BY_ESPN[meta['away_espn_team_id']]}",
                                    }
                                }
                            },
                            "upperTeam": {"uri": f"football/nfl/teams/{_FOX_TEAM_BY_ESPN[meta['away_espn_team_id']]}"},
                            "lowerTeam": {"uri": f"football/nfl/teams/{_FOX_TEAM_BY_ESPN[meta['home_espn_team_id']]}"},
                        },
                    ],
                }
            ]
        }

    fox_id, provenance = _resolve_fox_event_id(
        {
            "season": meta["season"],
            "season_type": meta["season_type"],
            "week": meta["week"],
            "home_espn_team_id": meta["home_espn_team_id"],
            "away_espn_team_id": meta["away_espn_team_id"],
        },
        transport=transport,
    )
    assert (fox_id, provenance) == (meta["fox_event_id"], "scores_segment:2024-12-1")
    assert calls == ["nfl/league/scores-segment/2024-12-1"]


def test_a_failed_segment_is_a_miss_and_the_postseason_tries_its_neighbours():
    """ESPN's Super Bowl (postseason week 5) is Fox's week 4; a segment that raises is skipped."""
    calls = []

    def transport(path):
        calls.append(path)
        if path.endswith("2025-4-2"):
            raise TimeoutError("segment down")
        return (
            {
                "sectionList": [
                    {
                        "events": [
                            {
                                "contentUri": "football/nfl/events/11187",
                                "entityLink": {
                                    "layout": {
                                        "tokens": {
                                            "id": "11187",
                                            "homeUri": "football/nfl/teams/4",
                                            "awayUri": "football/nfl/teams/14",
                                        }
                                    }
                                },
                            }
                        ]
                    }
                ]
            }
            if path.endswith("2025-1-2")
            else {"sectionList": []}
        )

    assert _resolve_fox_game_id(2025, 3, 5, "17", "26", transport=transport) == ("11187", "scores_segment:2025-1-2")
    assert calls[:2] == ["nfl/league/scores-segment/2025-4-2", "nfl/league/scores-segment/2025-1-2"]


def test_an_unlisted_game_resolves_to_nothing_rather_than_a_guess(meta):
    """Fox's scoreboard not carrying the game is a miss, never an invented id."""
    fox_id, provenance = _resolve_fox_event_id(
        {
            "season": 2026,
            "season_type": 2,
            "week": 1,
            "home_espn_team_id": meta["home_espn_team_id"],
            "away_espn_team_id": meta["away_espn_team_id"],
        },
        transport=lambda path: {"sectionList": []},
    )
    assert (fox_id, provenance) == (None, "unresolved")


def test_a_row_with_no_team_ids_never_reaches_the_network():
    def explode(path):  # pragma: no cover
        raise AssertionError("resolution fetched a segment without knowing both clubs")

    assert _resolve_fox_event_id({"season": 2026, "season_type": 2, "week": 1}, transport=explode)[0] is None


# ------------------------------------------------------------------------------ no coverage
def test_a_game_below_the_2024_play_floor_hands_over_by_shape(row):
    """Fox answers HTTP 200 with the ``pbp`` key simply absent -- never a 404."""
    payload = _load("fox_nfl_no_coverage.json")
    meta = _load("fox_nfl_no_coverage_meta.json")
    assert payload["header"]["leftTeam"]["name"], "the no-coverage fixture must carry a real header"
    with pytest.raises(SourceUnavailable, match="carries no play-by-play"):
        _fox_adapter(
            "nfl",
            int(meta["espn_event_id"]),
            _Ctx(
                payload=payload,
                idmap_row=dict(
                    row,
                    **{
                        "espn_event_id": meta["espn_event_id"],
                        "home_espn_team_id": meta["home_espn_team_id"],
                        "away_espn_team_id": meta["away_espn_team_id"],
                    },
                ),
            ),
        )


def test_an_empty_body_is_a_failed_request_not_a_game_without_coverage(row):
    """An empty injected payload is not a game without coverage; the two must not read alike."""
    with pytest.raises(SourceUnavailable, match="no event payload"):
        _fox_adapter("nfl", 1, _Ctx(payload={}, idmap_row=row))


def test_a_row_without_espn_team_ids_hands_over(row):
    payload = _load("fox_nfl_401671775.json")
    stripped = {k: v for k, v in row.items() if not k.endswith("espn_team_id")}
    with pytest.raises(SourceUnavailable, match="no ESPN team ids"):
        _fox_adapter("nfl", int(row["espn_event_id"]), _Ctx(payload=payload, idmap_row=stripped))


# --------------------------------------------------------------------------------- dispatch
def test_dispatch_serves_the_game_from_fox_with_provenance(row):
    payload = _load("fox_nfl_401671775.json")
    served = _process_game(
        "nfl",
        int(row["espn_event_id"]),
        source="fox",
        fallthrough=False,
        payloads={"fox": payload},
        idmap_row=row,
        odds_override={"gameSpread": 1.5, "overUnder": 46.5, "homeFavorite": True, "gameSpreadAvailable": True},
    )
    assert served.provenance["served"] == "fox"
    assert served.provenance["contract"]["ok"]
    assert served.provenance["native_ids"]["fox_id_resolved_by"] == "payload"
    assert served.provenance["lossy_columns"], "the Fox NFL losses must be declared in the contract"
    assert served.plays_frame.height > 100
    assert served.plays_frame["EPA"].null_count() < served.plays_frame.height


def test_the_odds_payload_yields_the_closing_line(row):
    from sportsdataverse.football.fox_common import _fox_odds_override

    odds = _load("fox_nfl_401671775_odds.json")
    payload = _load("fox_nfl_401671775.json")
    home_name = payload["header"]["rightTeam"]["imageAltText"]
    resolved = _fox_odds_override(odds, home_name)
    assert resolved["gameSpreadAvailable"] is True
    assert resolved["gameSpread"] > 0 and resolved["overUnder"] > 0
    assert isinstance(resolved["homeFavorite"], bool)
    # a payload with no six-pack is a miss, never a made-up line
    assert _fox_odds_override({}, home_name) is None
    assert _fox_odds_override(odds, None) is None


#: supp-012: observed 2026-10-11 against ESPN's own summary of 401671775 (170 / 157 plays, 139 paired):
#: agreement type.* .9640, rush .9856, pass .9712, start.yardLine / end.pos_team.id / td_play 1.0; corr
#: EP_start 1.0, EP_end .9740, EPA .9308, wp_before .9998, wp_after .9938. Floors, never lowered.
ESPN_REFERENCE_PINNED = {
    "paired_share": 0.99,
    "agreement": {
        "type.id": 0.96,
        "type.text": 0.96,
        "type.abbreviation": 0.96,
        "start.yardLine": 0.99,
        "end.pos_team.id": 0.99,
        "rush": 0.98,
        "pass": 0.97,
        "td_play": 0.99,
    },
    "correlation": {"EP_start": 0.999, "EP_end": 0.97, "EPA": 0.93, "wp_before": 0.999, "wp_after": 0.99},
}


def test_fox_nfl_espn_reference_parity(row):
    """Fox NFL vs the ESPN summary of the same game (supp-012): per-play floors."""
    from tests.football.sources._espn_reference import espn_reference_report

    odds = {"gameSpread": 1.5, "overUnder": 46.5, "homeFavorite": True, "gameSpreadAvailable": True}
    report = espn_reference_report("nfl", int(row["espn_event_id"]), "fox", _load("fox_nfl_401671775.json"), row, odds)
    assert report.check(ESPN_REFERENCE_PINNED) == [], report.gates()
    print("FOX_NFL_ESPN_REFERENCE_OK", report.gates()["paired_share"])


# ------------------------------------------------------- classification and end-state edges (3.5)
def _adapted(name, espn_id, home, away):
    from sportsdataverse.nfl.fox_pbp.to_espn_summary import _fox_nfl_to_espn_summary

    row = {"espn_event_id": espn_id, "home_espn_team_id": home, "away_espn_team_id": away}
    summary, _ = _fox_nfl_to_espn_summary(_load(name), row)
    drives = summary["drives"]["previous"] + ([summary["drives"]["current"]] if "current" in summary["drives"] else [])
    return summary, [p for d in drives for p in d["plays"]]


def test_no_play_penalty_rows_classify_as_penalties():
    """NYG @ DAL 2025 wk 2 (Fox 10777): the two event-less Delay of Game rows are ESPN type 8.

    ESPN's own summary of 401772834 types both ``8`` and starts them where the flag fell
    (4th & 5 at DAL 10, and 4th & 5 with 57 to go in OT); the fallback used to type them
    ``2`` End Period with the NEXT snap's spot and down 0.
    """
    _, plays = _adapted("fox_nfl_401772834.json", "401772834", "6", "19")
    # the two rows Fox charges to the club itself ("NYG-NYG"), which carry no event geometry
    delay = [p for p in plays if "on NYG-NYG, Delay" in p["text"] or "on DAL-DAL, Delay" in p["text"]]
    assert [p["type"]["id"] for p in delay] == ["8", "8"]
    assert [(p["start"]["down"], p["start"]["distance"], p["start"]["yardsToEndzone"]) for p in delay] == [
        (4, 5, 10),
        (4, 5, 57),
    ]
    # the end is the next snap's start: the same 4th down, five yards back
    assert [(p["end"]["down"], p["end"]["distance"], p["end"]["yardsToEndzone"]) for p in delay] == [
        (4, 10, 15),
        (4, 10, 62),
    ]
    assert all(p["penalty"]["yards"] == 5 for p in delay)
    assert not any(p["type"]["id"] == "2" and p["period"]["number"] in (3, 5) and "PENALTY" in p["text"] for p in plays)


def test_a_sack_for_a_safety_carries_the_scoring_side():
    """CLE @ JAX 2024 wk 2 (Fox 10431): "Sacked at JAC End Zone ... SAFETY." is a Safety for CLE."""
    _, plays = _adapted("fox_nfl_401671635.json", "401671635", "30", "5")
    (safety,) = [p for p in plays if "SAFETY" in p["text"]]
    assert safety["type"]["id"] == "20"
    assert safety["scoringPlay"] is True
    assert safety["scoringType"]["name"] == "safety"
    before = plays[plays.index(safety) - 1]
    # the two points land on the safety row itself, for the away club (CLE), not on the next drive
    assert safety["awayScore"] == before["awayScore"] + 2
    assert safety["homeScore"] == before["homeScore"]


def test_overtime_boundary_end_state_is_read_from_regulation():
    """SEA @ NE 2024 wk 2 (Fox 10432): regulation ends on a kneel and Fox emits no end-of-Q4 row.

    Reading through the period change gave the kneel the OT kickoff's spot in the kicking
    team's frame (65 to go); ESPN's summary of 401671702 ends it at SEA 23, 77 to go, SEA ball.
    Over the 32 OT games of 2024-25, 23 regulation-ending snaps read into the OT kickoff.
    """
    _, plays = _adapted("fox_nfl_401671702.json", "401671702", "17", "26")
    first_ot = next(i for i, p in enumerate(plays) if p["period"]["number"] == 5)
    kneel = next(p for p in reversed(plays[:first_ot]) if "kneels" in p["text"])
    assert kneel["period"]["number"] == 4
    assert kneel["end"]["team"]["id"] == "26"
    assert kneel["end"]["yardsToEndzone"] == 77
    assert kneel["end"]["yardsToEndzone"] != plays[first_ot]["start"]["yardsToEndzone"]


def test_a_live_poll_has_no_winner_and_a_final_has_one():
    """Fox reads ``isLoser: false`` for both clubs on every live poll (B-L59)."""
    live, _ = _adapted("fox_nfl_live_mid.json", "401872932", "2", "8")
    final, _ = _adapted("fox_nfl_401671775.json", "401671775", "26", "22")
    assert [c["winner"] for c in live["header"]["competitions"][0]["competitors"]] == [False, False]
    assert sorted(c["winner"] for c in final["header"]["competitions"][0]["competitors"]) == [False, True]
