"""NFL-side Fox adapter: id resolution, no-coverage detection and dispatch wiring. Zero network."""

from __future__ import annotations

import json
import pathlib

import pytest

from sportsdataverse.football.sources.dispatch import SourceUnavailable, _process_game
from sportsdataverse.nfl.fox_pbp.to_espn_summary import (
    _FOX_TEAM_BY_ESPN,
    _fox_adapter,
    _resolve_fox_event_id,
    _segment_ids,
)

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
    """``_codegen_runtime._get`` returns ``{}`` on any failure; the two must not read alike."""
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
