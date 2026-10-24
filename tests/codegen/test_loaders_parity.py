"""Loader manifest readiness: the generated 404-safe loaders fetch the same URL
the existing hand-written loaders do (faithful), and historical loaders are kept.
"""

import importlib
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from sportsdataverse import _codegen_runtime as runtime
from sportsdataverse.errors import NoDataError, SeasonNotFoundError
from sportsdataverse.nhl import nhl_loaders

from tools.codegen import generate, spec

REL = Path("tools/codegen/endpoints/releases.yaml")


def test_manifest_covers_historical_loaders():
    rel = spec.load_releases(REL)
    fns = {ld.fn for ld in rel.loaders}
    for f in ("load_wnba_pbp", "load_wnba_schedule", "load_wnba_player_boxscore", "load_wnba_team_boxscore"):
        assert f in fns, f"regressed historical loader {f}"


def _existing_url(fn_name: str, league: str):
    """Capture the URL the existing hand-written loader fetches for season 2024.

    Returns ``None`` when there is nothing to compare against -- a release-seeded
    dataset with no hand-written twin, or a new league (e.g. ``pwhl``) that has no
    ``{league}_loaders.py`` module yet. Those loaders' URLs are verified directly
    against their release assets at seed time, not here.
    """
    try:
        mod = importlib.import_module(f"sportsdataverse.{league}.{league}_loaders")
    except ModuleNotFoundError:
        return None
    if not hasattr(mod, fn_name) or not hasattr(mod, "pl"):
        return None
    box = {}

    def fake(url, *a, **k):
        box["url"] = url
        raise FileNotFoundError("404")  # short-circuit after first fetch

    with patch.object(runtime, "_release_bytes", side_effect=fake):  # the release fetch, before the parse (R898-9)
        # `_read_release_parquet` classifies a failed read by asking the transport
        # what the server said, so keep that lookup offline too.
        with patch.object(runtime, "download", side_effect=NoDataError("404")):
            try:
                getattr(mod, fn_name)(seasons=2024)
            except Exception:  # noqa: BLE001 -- the URL is captured before the stubbed 404; what follows is irrelevant
                pass
    return box.get("url")


# Every loader is called against a stubbed read that finds nothing, so each warns "no data"; the
# skip-and-warn contract itself is asserted in test_on_missing.py and test_load_module.py.
@pytest.mark.filterwarnings(r"ignore:load_\w+\x3a no (\w+ )?data for season:UserWarning")
def test_generated_loader_urls_match_existing():
    rel = spec.load_releases(REL)
    mismatches = []
    for ld in rel.loaders:
        if ld.stub:
            continue
        gen_url = spec.fill_season(f"{rel.bases[ld.base]}{ld.url}", 2024)
        existing = _existing_url(ld.fn, ld.league)
        if existing is None:
            continue  # historical loader not URL-capturable (different mechanism); skip
        if gen_url != existing:
            mismatches.append(f"{ld.fn}:\n   gen:      {gen_url}\n   existing: {existing}")
    assert not mismatches, f"{len(mismatches)} loader URL mismatch(es):\n" + "\n".join(mismatches[:20])


def test_loader_modules_render_valid_python():
    import ast

    rel = spec.load_releases(REL)
    by_league: dict = {}
    for ld in rel.loaders:
        by_league.setdefault(ld.league, []).append(ld)
    for league, loaders in by_league.items():
        src = generate.render_loader_module(league, loaders, rel.bases)
        ast.parse(src)  # raises on malformed


@pytest.mark.parametrize("season", [2010, 2015, 2024])
def test_nhl_game_rosters_accepts_published_seasons(season):
    roster = pl.DataFrame({"player_id": [8471675], "season": [season], "shoots_catches": ["L"]})
    with patch.object(nhl_loaders, "_read_release_parquet", return_value=roster) as reader:
        out = nhl_loaders.load_nhl_game_rosters(seasons=season)

    assert out.equals(roster)
    reader.assert_called_once_with(
        "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
        f"nhl_game_rosters/game_rosters_{season}.parquet"
    )


def test_nhl_game_rosters_rejects_seasons_before_first_release():
    with patch.object(nhl_loaders, "_read_release_parquet") as reader:
        with pytest.raises(SeasonNotFoundError):
            nhl_loaders.load_nhl_game_rosters(seasons=2009)

    reader.assert_not_called()


# Every NHL season release publishes from 2010 (the 2009-10 season); the asset
# paths are spelled out so a releases.yaml regression cannot pass by echo.
_NHL_FIRST_SEASON_ASSETS = {
    "load_nhl_game_info": "nhl_game_info/game_info_2010.parquet",
    "load_nhl_game_rosters": "nhl_game_rosters/game_rosters_2010.parquet",
    "load_nhl_goalie_boxscores": "nhl_goalie_boxscores/goalie_box_2010.parquet",
    "load_nhl_linescore": "nhl_linescore/linescore_2010.parquet",
    "load_nhl_officials": "nhl_officials/officials_2010.parquet",
    "load_nhl_pbp_full": "nhl_pbp_full/play_by_play_2010.parquet",
    "load_nhl_pbp_lite": "nhl_pbp_lite/play_by_play_lite_2010.parquet",
    "load_nhl_penalties": "nhl_penalties/penalties_2010.parquet",
    "load_nhl_player_boxscores": "nhl_player_boxscores/player_box_2010.parquet",
    "load_nhl_rosters": "nhl_rosters/rosters_2010.parquet",
    "load_nhl_schedules": "nhl_schedules/nhl_schedule_2010.parquet",
    "load_nhl_scoring": "nhl_scoring/scoring_2010.parquet",
    "load_nhl_scratches": "nhl_scratches/scratches_2010.parquet",
    "load_nhl_shifts": "nhl_shifts/shifts_2010.parquet",
    "load_nhl_shootout": "nhl_shootout/shootout_summary_2010.parquet",
    "load_nhl_shots_by_period": "nhl_shots_by_period/shots_by_period_2010.parquet",
    "load_nhl_skater_boxscores": "nhl_skater_boxscores/skater_box_2010.parquet",
    "load_nhl_team_boxscores": "nhl_team_boxscores/team_box_2010.parquet",
    "load_nhl_three_stars": "nhl_three_stars/three_stars_2010.parquet",
}


@pytest.mark.parametrize("fn,asset", sorted(_NHL_FIRST_SEASON_ASSETS.items()))
def test_nhl_release_loaders_start_at_first_published_season(fn, asset):
    loader = getattr(nhl_loaders, fn)
    frame = pl.DataFrame({"season": [2010]})
    with patch.object(nhl_loaders, "_read_release_parquet", return_value=frame) as reader:
        loader(seasons=2010)
    reader.assert_called_once_with("https://github.com/sportsdataverse/sportsdataverse-data/releases/download/" + asset)

    with patch.object(nhl_loaders, "_read_release_parquet") as reader:
        with pytest.raises(SeasonNotFoundError):
            loader(seasons=2009)
    reader.assert_not_called()
