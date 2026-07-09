"""Unit tests for the WNBA play-type/impact by-reference shims (league_id="10")."""

import polars as pl

from sportsdataverse.wnba.wnba_playtype_impact import (
    wnba_expected_turnovers,
    wnba_foul_drawing,
    wnba_matchup_drapm,
    wnba_playtype_ratings,
)


def test_shims_bind_league_id_10_to_the_nba_core(monkeypatch):
    import sportsdataverse.wnba.wnba_playtype_impact as mod

    calls = {}

    def _capture(name):
        def _fn(season, *, league_id=None, **kw):
            calls[name] = league_id
            return pl.DataFrame()

        return _fn

    monkeypatch.setattr(mod, "_ratings", _capture("ratings"))
    monkeypatch.setattr(mod, "_drapm", _capture("drapm"))
    monkeypatch.setattr(mod, "_foul", _capture("foul"))
    monkeypatch.setattr(mod, "_tov", _capture("tov"))

    wnba_playtype_ratings("2024")
    wnba_matchup_drapm("2024")
    wnba_foul_drawing("2024")
    wnba_expected_turnovers("2024")

    assert calls == {"ratings": "10", "drapm": "10", "foul": "10", "tov": "10"}


def test_shims_run_offline_with_injected_frames():
    off = pl.DataFrame(
        {"team_id": [1, 2], "play_type": ["Isolation", "Isolation"], "poss": [50.0, 60.0], "pts": [45.0, 55.0]}
    )
    deff = pl.DataFrame(
        {"team_id": [1, 2], "play_type": ["Isolation", "Isolation"], "poss": [55.0, 50.0], "pts": [50.0, 44.0]}
    )
    sched = pl.DataFrame({"team_id": [1, 2], "opp_team_id": [2, 1]})
    r = wnba_playtype_ratings("2024", off_team=off, def_team=deff, schedule=sched)
    assert r.schema["team_id"] == pl.Int64

    matchups = pl.DataFrame(
        {"off_player_id": [10, 11], "def_player_id": [20, 20], "partial_poss": [30.0, 30.0], "player_pts": [30.0, 28.0]}
    )
    d = wnba_matchup_drapm("2024", matchups=matchups)
    assert set(d.columns) == {"player_id", "matchup_drapm", "matchup_poss"}


def test_shims_sparse_coverage_degrades_to_zero_row():
    assert (
        wnba_playtype_ratings("2024", off_team=pl.DataFrame(), def_team=pl.DataFrame(), schedule=pl.DataFrame()).height
        == 0
    )
    assert wnba_matchup_drapm("2024", matchups=pl.DataFrame()).height == 0
    assert wnba_foul_drawing("2024", base=pl.DataFrame(), player_mix=pl.DataFrame()).height == 0
    assert wnba_expected_turnovers("2024", base=pl.DataFrame(), player_mix=pl.DataFrame()).height == 0
