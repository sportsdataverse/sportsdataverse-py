"""Offline + live tests for the multi-asset ``nba_stats_*`` release loaders.

Offline tests monkeypatch ``_read_release_parquet`` so the file-name fan-out, the
START -> END year translation, the discriminator stamping and the filter guards
run without the network. Live tests (``SDV_PY_LIVE_TESTS=1``) read the release.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse._codegen_runtime import SeasonNotFoundError
from sportsdataverse.nba import nba_loaders_extra as x
from tests.conftest import skip_if_no_live

BASE = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"


@pytest.fixture
def urls(monkeypatch):
    """Record every requested URL; serve a 1-row frame with no discriminator columns."""
    seen: list[str] = []

    def fake(url, *args, **kwargs):
        seen.append(url)
        return pl.DataFrame({"player_id": [1]})

    monkeypatch.setattr(x, "_read_release_parquet", fake)
    return seen


def test_synergy_reads_all_72_files_of_a_season_by_end_year(urls):
    out = x.load_nba_stats_synergy(2024)
    assert len(urls) == 72 == len(set(urls))
    assert all(u.startswith(BASE + "nba_stats_synergy/") and u.endswith("_2025.parquet") for u in urls)
    assert BASE + "nba_stats_synergy/playoffs_isolation_defensive_totals_2025.parquet" in urls
    assert not any("cut_defensive" in u for u in urls), "cut has no defensive split on the tag"
    # discriminators the rows lacked are stamped from the file name, season as END year
    assert sorted(out["season_type"].unique()) == ["playoffs", "regular-season"]
    assert sorted(out["per_mode"].unique()) == ["pergame", "totals"]
    assert out["season"].unique().to_list() == [2025]
    assert out.schema["season"] == pl.Int64


def test_synergy_filters_narrow_the_fan_out(urls):
    x.load_nba_stats_synergy([2023, 2024], season_type="playoffs", per_mode="totals", play_type="cut")
    assert urls == [BASE + f"nba_stats_synergy/playoffs_cut_offensive_totals_{y}.parquet" for y in (2024, 2025)]


@pytest.mark.parametrize(
    "kwargs",
    [{"season_type": "Playoffs"}, {"per_mode": "per_game"}, {"play_type": "pnr"}, {"type_grouping": []}],
)
def test_synergy_unknown_filter_raises_before_the_network(urls, kwargs):
    with pytest.raises(ValueError, match="expected one of"):
        x.load_nba_stats_synergy(2024, **kwargs)
    assert urls == []


def test_synergy_explicit_unpublished_pair_warns_and_reads_the_rest(urls):
    with pytest.warns(UserWarning, match="cut/defensive"):
        x.load_nba_stats_synergy(
            2024, season_type="playoffs", per_mode="totals", type_grouping="defensive", play_type=["cut", "isolation"]
        )
    assert urls == [BASE + "nba_stats_synergy/playoffs_isolation_defensive_totals_2025.parquet"]


def test_synergy_unpublished_combination_raises(urls):
    with pytest.raises(ValueError, match="no .* split is published"):
        x.load_nba_stats_synergy(2024, type_grouping="defensive", play_type="transition")


def test_existing_columns_are_never_overwritten(monkeypatch):
    def fake(url, *args, **kwargs):
        return pl.DataFrame({"season": [1999], "season_type": ["kept"], "per_mode": ["kept"]})

    monkeypatch.setattr(x, "_read_release_parquet", fake)
    out = x.load_nba_stats_hustle_player(2024, season_type="playoffs", per_mode="pergame")
    assert out.row(0) == (1999, "kept", "kept")


def test_hustle_entities_read_their_own_files(urls):
    x.load_nba_stats_hustle_player(2015, season_type="regular-season")
    x.load_nba_stats_hustle_team(2015, per_mode="totals")
    assert urls == [
        BASE + "nba_stats_hustle/leaguehustlestatsplayer_regular-season_pergame_2016.parquet",
        BASE + "nba_stats_hustle/leaguehustlestatsplayer_regular-season_totals_2016.parquet",
        BASE + "nba_stats_hustle/leaguehustlestatsteam_regular-season_totals_2016.parquet",
        BASE + "nba_stats_hustle/leaguehustlestatsteam_playoffs_totals_2016.parquet",
    ]


def test_matchups_are_regular_season_only_and_rollup_takes_both(urls):
    pairs = x.load_nba_stats_matchups(2024, per_mode="totals")
    x.load_nba_stats_matchups_rollup(2024, season_type="playoffs", per_mode="pergame")
    assert urls == [
        BASE + "nba_stats_matchups/leagueseasonmatchups_regular-season_totals_2025.parquet",
        BASE + "nba_stats_matchups/matchupsrollup_playoffs_pergame_2025.parquet",
    ]
    assert pairs.select("season_type", "per_mode", "season").row(0) == ("regular-season", "totals", 2025)


@pytest.mark.parametrize(
    ("fn", "stem"),
    [
        (x.load_nba_stats_draft_combine_stats, "draftcombinestats"),
        (x.load_nba_stats_draft_combine_anthro, "draftcombineplayeranthro"),
        (x.load_nba_stats_draft_combine_drills, "draftcombinedrillresults"),
        (x.load_nba_stats_draft_combine_spot_shooting, "draftcombinespotshooting"),
        (x.load_nba_stats_draft_combine_nonstationary_shooting, "draftcombinenonstationaryshooting"),
    ],
)
def test_draft_combine_tables(fn, stem, urls):
    out = fn(2024)
    assert urls == [BASE + f"nba_stats_draft_combine/{stem}_2025.parquet"]
    assert out.columns == ["player_id", "season"], "a combine file has no season_type / per_mode to stamp"


@pytest.mark.parametrize(
    ("fn", "floor"),
    [
        (x.load_nba_stats_synergy, 2015),
        (x.load_nba_stats_hustle_player, 2015),
        (x.load_nba_stats_matchups, 2017),
        (x.load_nba_stats_matchups_rollup, 2017),
        (x.load_nba_stats_draft_combine_stats, 2000),
    ],
)
def test_season_below_the_tag_floor_raises(fn, floor, urls):
    with pytest.raises(SeasonNotFoundError):
        fn([floor, floor - 1])
    assert urls == []


def test_season_with_no_asset_is_skipped_with_a_warning(monkeypatch):
    def fake(url, *args, **kwargs):
        return None if url.endswith("_2027.parquet") else pl.DataFrame({"team_id": [1]})

    monkeypatch.setattr(x, "_read_release_parquet", fake)
    with pytest.warns(UserWarning, match=r"load_nba_stats_hustle_team: no data for season\(s\) \[2026\]"):
        out = x.load_nba_stats_hustle_team([2025, 2026], season_type="regular-season", per_mode="totals")
    assert out.height == 1


def test_one_absent_file_inside_a_published_season_is_not_a_missing_season(monkeypatch, recwarn):
    def fake(url, *args, **kwargs):
        return None if "playoffs" in url else pl.DataFrame({"team_id": [1]})

    monkeypatch.setattr(x, "_read_release_parquet", fake)
    out = x.load_nba_stats_matchups_rollup(2025)
    assert out.height == 2 and sorted(out["season_type"].unique()) == ["regular-season"]
    assert not recwarn.list


def test_per_mode_dtype_drift_unions_to_the_supertype(monkeypatch):
    def fake(url, *args, **kwargs):
        if "totals" in url:
            return pl.DataFrame({"matchup_min": ["12:30"], "matchup_fgm": [3]})
        return pl.DataFrame({"matchup_min": [12.5], "matchup_fgm": [1.5]})

    monkeypatch.setattr(x, "_read_release_parquet", fake)
    out = x.load_nba_stats_matchups(2024)
    assert out.schema["matchup_min"] == pl.String
    assert out.schema["matchup_fgm"] == pl.Float64


def test_no_published_season_returns_an_empty_frame_and_pandas_round_trips(monkeypatch):
    monkeypatch.setattr(x, "_read_release_parquet", lambda url, *a, **k: None)
    with pytest.warns(UserWarning, match="no data"):
        out = x.load_nba_stats_draft_combine_drills(2024)
    assert isinstance(out, pl.DataFrame) and out.height == 0
    monkeypatch.setattr(x, "_read_release_parquet", lambda url, *a, **k: pl.DataFrame({"player_id": [1]}))
    pdf = x.load_nba_stats_draft_combine_drills(2024, return_as_pandas=True)
    assert not isinstance(pdf, pl.DataFrame) and list(pdf.columns) == ["player_id", "season"]


def test_loaders_are_exported_from_the_league_package():
    import sportsdataverse.nba as nba

    for name in x.__all__:
        assert getattr(nba, name) is getattr(x, name)
    assert callable(nba.load_nba_stats_draft)


@skip_if_no_live
def test_live_synergy_and_hustle_read_the_release():
    syn = x.load_nba_stats_synergy(2024, season_type="regular-season", play_type="isolation")
    assert syn.height > 500
    assert set(syn["type_grouping"].unique()) == {"Offensive", "Defensive"}
    assert syn.schema["player_id"] == pl.Int64 and syn.schema["team_id"] == pl.Int64
    hustle = x.load_nba_stats_hustle_team(2024, per_mode="totals")
    assert hustle.height >= 30 and hustle["season"].unique().to_list() == [2025]


@skip_if_no_live
def test_live_matchups_combine_and_draft_read_the_release():
    rollup = x.load_nba_stats_matchups_rollup(2024, season_type="playoffs", per_mode="totals")
    assert set(rollup["position"].unique()) == {"G", "F", "C", "TOTAL"}
    combine = x.load_nba_stats_draft_combine_stats(2024)
    assert combine.height > 50 and combine["season"].unique().to_list() == [2025]
    from sportsdataverse.nba import load_nba_stats_draft

    draft = load_nba_stats_draft(2025)
    first = draft.filter(pl.col("overall_pick") == 1)
    assert first["player_name"].to_list() == ["Cooper Flagg"], "seasons is the draft year"


def test_pick_deduplicates_repeated_filter_values():
    from sportsdataverse.nba.nba_loaders_extra import _pick

    assert _pick("per_mode", ["totals", "totals", "pergame"], ("pergame", "totals")) == ("totals", "pergame")


_MULTIFILE = [
    "load_nba_stats_synergy",
    "load_nba_stats_hustle_player",
    "load_nba_stats_hustle_team",
    "load_nba_stats_matchups",
    "load_nba_stats_matchups_rollup",
    "load_nba_stats_draft_combine_stats",
    "load_nba_stats_draft_combine_anthro",
    "load_nba_stats_draft_combine_drills",
    "load_nba_stats_draft_combine_spot_shooting",
    "load_nba_stats_draft_combine_nonstationary_shooting",
]


@pytest.mark.parametrize("fn", _MULTIFILE)
def test_empty_schema_no_published_season_is_the_documented_table(fn, monkeypatch):
    """MD-3: no published season gives the documented columns with zero rows, never a (0, 0) frame."""
    from tests.conftest import documented_schema

    monkeypatch.setattr(x, "_read_release_parquet", lambda url, *a, **k: None)
    want = documented_schema(f"autodoc/nba/{fn}")
    with pytest.warns(UserWarning, match="no data"):
        out = getattr(x, fn)(2024)
    assert out.height == 0 and dict(out.schema) == want
    with pytest.warns(UserWarning, match="no data"):
        pdf = getattr(x, fn)(2024, return_as_pandas=True)
    import pandas as pd

    # R978-3: pandas, with the Arrow-backed dtypes a non-empty multi-file load has
    assert isinstance(pdf, pd.DataFrame) and len(pdf) == 0 and list(pdf.columns) == list(want)
    assert all(str(t).endswith("[pyarrow]") for t in pdf.dtypes)
