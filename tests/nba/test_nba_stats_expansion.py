"""Offline fixture tests for the 2026-08 stats-surface expansion.

Real captures (see fixtures/README.md) exercise the newly generated wrappers
via the injectable transport, plus the v3 boxscore envelope synthesis that the
shared parser previously returned empty frames for.
"""

from pathlib import Path

import polars as pl

from sportsdataverse.nba import nba_stats
from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets
from sportsdataverse.wnba import wnba_stats

FIX = Path(__file__).resolve().parent / "fixtures"


def _transport(fixture: str, captured: dict):
    text = (FIX / fixture).read_text(encoding="utf-8")

    def transport(url, params, headers, proxy_url):
        captured["url"] = url
        return 200, text

    return transport


def test_playbyplayv2_wnba_wrapper():
    captured: dict = {}
    out = wnba_stats.wnba_stats_playbyplayv2(
        game_id="1022400050", transport=_transport("cap_playbyplayv2_wnba.json", captured)
    )
    assert captured["url"] == "https://stats.wnba.com/stats/playbyplayv2"
    assert isinstance(out, dict)  # PlayByPlay + AvailableVideo
    assert out["PlayByPlay"].height > 300
    assert "event_num" in out["PlayByPlay"].columns or "eventnum" in out["PlayByPlay"].columns


def test_scoreboardv2_nba_wrapper():
    captured: dict = {}
    out = nba_stats.nba_stats_scoreboardv2(
        game_date="2025-01-15", transport=_transport("cap_scoreboardv2_nba.json", captured)
    )
    assert captured["url"] == "https://stats.nba.com/stats/scoreboardv2"
    assert isinstance(out, dict)
    assert out["GameHeader"].height > 0
    assert out["LineScore"].height > 0


def test_homepagev2_wnba_wrapper():
    captured: dict = {}
    out = wnba_stats.wnba_stats_homepagev2(transport=_transport("cap_homepagev2_wnba.json", captured))
    assert captured["url"] == "https://stats.wnba.com/stats/homepagev2"
    assert isinstance(out, dict) and len(out) == 8


def test_playercareerbycollegerollup_wnba_wrapper():
    captured: dict = {}
    out = wnba_stats.wnba_stats_playercareerbycollegerollup(
        transport=_transport("cap_playercareerbycollegerollup_wnba.json", captured)
    )
    assert isinstance(out, dict) and set(out) == {"East", "South", "Midwest", "West"}
    assert sum(df.height for df in out.values()) > 0


def test_boxscoretraditionalv3_envelope_synthesis():
    """The v3 boxScore* nested envelope must yield PlayerStats/TeamStats, not (0,0)."""
    captured: dict = {}
    out = wnba_stats.wnba_stats_boxscoretraditionalv3(
        game_id="1022400001", transport=_transport("cap_boxscoretraditionalv3_wnba.json", captured)
    )
    assert isinstance(out, dict) and set(out) == {"PlayerStats", "TeamStats"}
    players, teams = out["PlayerStats"], out["TeamStats"]
    assert teams.height == 2
    assert players.height >= 10
    assert {"person_id", "team_id", "points"} <= set(players.columns)
    assert players.get_column("points").null_count() < players.height


def test_boxscoresummaryv3_envelope_synthesis():
    captured: dict = {}
    out = wnba_stats.wnba_stats_boxscoresummaryv3(
        game_id="1022400001", transport=_transport("cap_boxscoresummaryv3_wnba.json", captured)
    )
    # summaryv3 embeds home/away rosters plus summary sections
    assert isinstance(out, dict)
    assert {"PlayerStats", "TeamStats"} <= set(out)
    assert out["TeamStats"].height == 2
    assert all(isinstance(v, pl.DataFrame) for v in out.values())


def test_videoevents_nba_wrapper():
    # 2026-08-26 revival: video endpoints ship the dict envelope
    # {Meta: {videoUrls: [...]}, playlist: [...]} the sweep misread as dead
    captured: dict = {}
    out = nba_stats.nba_stats_videoevents(
        game_id="0022201086",
        game_event_id="7",
        transport=_transport("cap_videoevents_nba.json", captured),
    )
    assert captured["url"] == "https://stats.nba.com/stats/videoevents"
    assert isinstance(out, dict)
    assert set(out) == {"videoUrls", "playlist"}
    assert out["playlist"].height == 1 and "gi" in out["playlist"].columns


def test_result_sets_envelope_still_default():
    """A plain resultSets payload keeps its exact previous behavior."""
    raw = {"resultSets": [{"name": "A", "headers": ["GAME_ID", "PTS"], "rowSet": [["1", 10], ["2", 12]]}]}
    df = parse_nba_stats_result_sets(raw)
    assert isinstance(df, pl.DataFrame) and df.height == 2 and df.columns == ["game_id", "pts"]
