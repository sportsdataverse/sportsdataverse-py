"""Season-identity regression for the NBA/WNBA stats loaders (offline).

The Program V release assets are keyed by the season's END year while the public
``seasons`` argument stayed the START year, so the loaders carry a ``{season + 1}``
token. An off-by-one there silently mislabels every season -- a row-count or
non-empty assertion would not notice. These tests pin the exact asset year each
public season resolves to.
"""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools"))
from codegen import spec  # noqa: E402

REL = ROOT / "tools" / "codegen" / "endpoints" / "releases.yaml"

# (loader, module, public season, asset basename it MUST resolve to).
# NBA: start year -> end-year asset (+1). WNBA seasons are single-calendar-year,
# so start == end and the asset year must equal the argument exactly.
CASES = [
    ("load_nba_stats_schedules", "nba", 1996, "nba_schedule_1997.parquet"),
    ("load_nba_stats_schedules", "nba", 2025, "nba_schedule_2026.parquet"),
    ("load_nba_stats_pbp", "nba", 1996, "nba_play_by_play_1997.parquet"),
    ("load_nba_stats_pbp", "nba", 2025, "nba_play_by_play_2026.parquet"),
    ("load_nba_stats_possessions", "nba", 1996, "nba_possessions_1997.parquet"),
    ("load_nba_stats_possessions", "nba", 2025, "nba_possessions_2026.parquet"),
    ("load_nba_stats_game_lineups", "nba", 1996, "nba_lineups_1997.parquet"),
    ("load_nba_stats_game_lineups", "nba", 2025, "nba_lineups_2026.parquet"),
    ("load_wnba_stats_schedules", "wnba", 1997, "wnba_schedule_1997.parquet"),
    ("load_wnba_stats_schedules", "wnba", 2025, "wnba_schedule_2025.parquet"),
    ("load_wnba_stats_pbp", "wnba", 1997, "wnba_play_by_play_1997.parquet"),
    ("load_wnba_stats_pbp", "wnba", 2025, "wnba_play_by_play_2025.parquet"),
    ("load_wnba_stats_possessions", "wnba", 1997, "wnba_possessions_1997.parquet"),
    ("load_wnba_stats_possessions", "wnba", 2025, "wnba_possessions_2025.parquet"),
    ("load_wnba_stats_game_lineups", "wnba", 1997, "wnba_lineups_1997.parquet"),
    ("load_wnba_stats_game_lineups", "wnba", 2025, "wnba_lineups_2025.parquet"),
    # Retired-tag shims. These used to read START-year-keyed ``*_v3_{season}``
    # assets off their own tags; they now forward to the production loaders. The
    # public ``seasons`` argument was START-year before and must stay START-year,
    # so 2025 has to keep meaning 2025-26 -- i.e. the SAME asset the target picks.
    ("load_nba_stats_pbp_v3", "nba", 2025, "nba_play_by_play_2026.parquet"),
    ("load_nba_stats_possessions_v3", "nba", 2025, "nba_possessions_2026.parquet"),
    ("load_nba_stats_lineups_v3", "nba", 2025, "nba_lineups_2026.parquet"),
]

# Deprecated shim -> the loader it forwards to.
SHIMS = [
    ("load_nba_stats_pbp_v3", "load_nba_stats_pbp"),
    ("load_nba_stats_possessions_v3", "load_nba_stats_possessions"),
    ("load_nba_stats_lineups_v3", "load_nba_stats_game_lineups"),
]


def _requested_url(fn_name: str, league: str, season: int) -> str:
    """Call the public loader and return the asset URL it actually asked for."""
    import importlib

    mod = importlib.import_module(f"sportsdataverse.{league}.{league}_loaders")
    box: dict = {}

    def fake(url, *a, **k):
        box["url"] = url
        raise FileNotFoundError("404 not found")  # 404-safe path: warn + skip

    with patch.object(mod.pl, "read_parquet", side_effect=fake):
        with pytest.warns(UserWarning):
            getattr(mod, fn_name)(seasons=season)
    return box["url"]


@pytest.mark.parametrize(("fn_name", "league", "season", "asset"), CASES)
def test_public_season_resolves_to_expected_asset(fn_name, league, season, asset):
    assert _requested_url(fn_name, league, season).endswith("/" + asset)


def test_fill_season_offset_and_identity():
    assert spec.fill_season("x_{season}.parquet", 2025) == "x_2025.parquet"
    assert spec.fill_season("x_{season + 1}.parquet", 2025) == "x_2026.parquet"
    # Whitespace-tolerant, and every occurrence is substituted.
    assert spec.fill_season("{season}/y_{season+1}.parquet", 1996) == "1996/y_1997.parquet"


def test_only_nba_stats_families_carry_the_end_year_offset():
    """Guard the blast radius: the +1 belongs to nba_stats, and to nothing else.

    Every nba_stats release loader carries it as of the 2026-08-13 republish,
    which moved the remaining START-year-named assets onto END-year names. Before
    it, only four did — one schema publishing two conventions.

    WNBA seasons are single-calendar-year, so an offset there would be an
    off-by-one. That is not theoretical: the first cut of the republish shifted
    8 wnba urls by accident, because ``wnba_stats_`` contains ``nba_stats_``.
    Asserting the exact set, rather than "all nba_stats", is what catches it.
    """
    # `== "1"`, not truthiness: SEASON_TOKEN captures the N in `{season + N}`, so
    # a bare truth test would accept `{season + 2}` as satisfying the END-year
    # contract. The contract is exactly one year.
    offset = {
        ld.fn
        for ld in spec.load_releases(REL).loaders
        if (m := spec.SEASON_TOKEN.search(ld.url)) and m.group(1) == "1"
    }
    assert offset == {
        "load_nba_stats_schedules",
        "load_nba_stats_pbp",
        "load_nba_stats_possessions",
        "load_nba_stats_game_lineups",
        # moved onto END-year assets by the 2026-08-13 republish
        "load_nba_stats_coaches",
        "load_nba_stats_game_rosters",
        "load_nba_stats_lineups",
        "load_nba_stats_officials",
        "load_nba_stats_player_boxscores",
        "load_nba_stats_player_game_logs",
        "load_nba_stats_player_season_stats",
        "load_nba_stats_rosters",
        "load_nba_stats_shots",
        "load_nba_stats_standings",
        "load_nba_stats_team_boxscores",
        "load_nba_stats_team_season_stats",
        # The retired-tag shims inherit their target's END-year asset path.
        "load_nba_stats_pbp_v3",
        "load_nba_stats_possessions_v3",
        "load_nba_stats_lineups_v3",
    }
    assert not any(fn.startswith("load_wnba") for fn in offset)


@pytest.mark.parametrize(("shim", "target"), SHIMS)
def test_shim_is_a_pure_pass_through(shim, target):
    """The shim must not apply its own year arithmetic.

    Both sides take the START year, so the shim forwards ``seasons`` untouched.
    Adding an offset at this boundary would double-count the ``{season + 1}``
    already in the target's asset path and shift every caller by a year.
    """
    for season in (1996, 2010, 2025):
        assert _requested_url(shim, "nba", season) == _requested_url(target, "nba", season)


@pytest.mark.parametrize(("shim", "target"), SHIMS)
def test_shim_warns_and_names_its_replacement(shim, target):
    import importlib

    mod = importlib.import_module("sportsdataverse.nba.nba_loaders")
    with patch.object(mod.pl, "read_parquet", side_effect=FileNotFoundError("404")):
        with pytest.warns(DeprecationWarning, match=target):
            getattr(mod, shim)(seasons=2025)


def test_shifted_loaders_document_the_season_column_divergence():
    """A ``{season + N}`` loader whose frame has a ``season`` column MUST say so.

    The asset is END-year keyed and the frame carries the asset's own stamp, so
    ``load_nba_stats_pbp(seasons=2024)`` returns rows reading ``season == 2025``
    while unshifted siblings read ``2024`` for that same real season. Silence
    here reads as "season means the same thing everywhere", which is false --
    and downstream partitioners key off this column, so the wrong reading
    overwrites the neighbouring season rather than duplicating it.
    """
    import importlib

    import yaml

    schemas = yaml.safe_load((ROOT / "tools" / "codegen" / "schemas" / "loader_schemas.yaml").read_text("utf-8"))
    offenders = []
    for ld in spec.load_releases(REL).loaders:
        m = spec.SEASON_TOKEN.search(ld.url)
        if not (m and m.group(1)):
            continue
        if not any(c["name"] == "season" for c in schemas.get(ld.fn) or []):
            continue
        mod = importlib.import_module(f"sportsdataverse.{ld.league}.{ld.league}_loaders")
        doc = getattr(mod, ld.fn).__doc__ or ""
        if "COLUMN carries the END year" not in doc:
            offenders.append(ld.fn)
    assert not offenders, f"shifted loaders missing the season-column warning: {offenders}"


def test_retired_v3_tags_are_unreferenced():
    """No loader may still point at a retired ``*_v3`` release tag."""
    retired = {"nba_stats_pbpv3", "nba_stats_possessions_v3", "nba_stats_lineups_v3"}
    for ld in spec.load_releases(REL).loaders:
        assert ld.tag not in retired, f"{ld.fn} still reads retired tag {ld.tag}"
        assert not any(t in ld.url for t in retired), f"{ld.fn} url still reads a retired tag: {ld.url}"
