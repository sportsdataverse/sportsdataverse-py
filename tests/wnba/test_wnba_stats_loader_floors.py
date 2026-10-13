"""wnba_stats shots / player game logs start at 1997, and the NBA / WNBA player crosswalks at 2002:
the first seasons their releases publish (offline)."""

from __future__ import annotations

import polars as pl
import pytest

import sportsdataverse.nba.nba_loaders as nl
import sportsdataverse.wnba.wnba_loaders as wl
from sportsdataverse.errors import SeasonNotFoundError

# Key columns of the first row of each floor-season release asset, copied verbatim
# from sportsdataverse-data (read 2026-10-12): wnba_stats_shots/shots_1997.parquet,
# wnba_stats_player_game_logs/player_game_logs_1997.parquet,
# nba_crosswalk/nba_player_crosswalk_2002.parquet, wnba_crosswalk/wnba_player_crosswalk_2002.parquet.
REAL_ROWS = {
    "shots_": {"game_id": "1029700001", "season": 1997, "person_id": 100005, "shot_result": "Missed"},
    "player_game_logs_": {"season_id": "41997", "game_id": "1049700002", "team_abbreviation": "PHO", "season": 1997},
    "nba_player_crosswalk_": {"season": 2002, "espn_athlete_id": "322", "nba_player_id": "673"},
    "wnba_player_crosswalk_": {"season": 2002, "espn_athlete_id": "167", "wnba_player_id": "100170"},
}


def _serve_real_row(mod, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record each requested URL and serve that asset's real first row (no network)."""
    seen: list[str] = []

    def fake(url: str) -> pl.DataFrame:
        seen.append(url)
        name = url.rsplit("/", 1)[-1]
        row = next((row for stem, row in REAL_ROWS.items() if name.startswith(stem)), None)
        assert row is not None, f"no real row for asset {name}"
        return pl.DataFrame([row])

    monkeypatch.setattr(mod, "_read_release_parquet", fake)
    return seen


def _tail(urls: list[str]) -> list[str]:
    return [u.rsplit("/", 2)[-2] + "/" + u.rsplit("/", 1)[-1] for u in urls]


@pytest.mark.parametrize(
    ("loader", "stem"),
    [
        (wl.load_wnba_stats_shots, "wnba_stats_shots/shots_"),
        (wl.load_wnba_stats_player_game_logs, "wnba_stats_player_game_logs/player_game_logs_"),
    ],
)
def test_first_season_is_1997(loader, stem: str, monkeypatch: pytest.MonkeyPatch) -> None:
    seen = _serve_real_row(wl, monkeypatch)
    out = loader(seasons=[1997, 2005])
    assert _tail(seen) == [f"{stem}1997.parquet", f"{stem}2005.parquet"]
    assert out.height == 2 and out["season"][0] == 1997
    with pytest.raises(SeasonNotFoundError):
        loader(seasons=1996)


@pytest.mark.parametrize(
    ("mod", "loader", "stem"),
    [
        (nl, nl.load_nba_player_crosswalk, "nba_crosswalk/nba_player_crosswalk_"),
        (wl, wl.load_wnba_player_crosswalk, "wnba_crosswalk/wnba_player_crosswalk_"),
    ],
)
def test_player_crosswalk_first_season_is_2002(mod, loader, stem: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # D18: the nba_crosswalk / wnba_crosswalk releases publish player crosswalks from 2002.
    seen = _serve_real_row(mod, monkeypatch)
    out = loader(seasons=[2002, 2010])
    assert _tail(seen) == [f"{stem}2002.parquet", f"{stem}2010.parquet"]
    assert out.height == 2 and out["season"][0] == 2002
    with pytest.raises(SeasonNotFoundError):
        loader(seasons=2001)
