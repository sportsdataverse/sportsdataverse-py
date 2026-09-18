"""Multi-season NFL loaders must survive per-season release schema drift.

``sportsdataverse/nfl/nfl_loaders.py`` is hand-written (NFL is deliberately
not in the codegen's ``_GENERATED_LOADER_LEAGUES``), so it missed the
ecosystem-wide move from ``pl.concat(..., how="vertical")`` to the
drift-tolerant ``diagonal_relaxed``. ``vertical`` raises ``ShapeError`` /
``SchemaError`` the moment two per-season parquets disagree on columns or
dtypes, which they routinely do — so every multi-season span that crossed a
schema change blew up.

The fixtures here are REAL slices of the nflverse ``pbp_participation``
release (see ``tests/fixtures/nfl_loaders/README.md``): 2016 has 20 columns
with ``play_id`` as ``Int32``, 2023 has 26 columns with ``play_id`` as
``Float64``. ``test_vertical_concat_on_these_fixtures_still_raises`` asserts
the fixtures genuinely reproduce the bug, so the loader test below can't
quietly pass against a same-schema pair.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.config import (
    NFL_FF_PLAYERIDS_URL,
    NFL_FF_RANKINGS_DRAFT_URL,
    NFL_FF_RANKINGS_WEEK_URL,
)
from sportsdataverse.nfl import (
    clear_cache,
    load_nfl_ff_playerids,
    load_nfl_ff_rankings,
    load_nfl_pbp_participation,
    reset_config,
    update_config,
)
from sportsdataverse.nfl import nfl_loaders as _loaders
from sportsdataverse.nfl import nfl_players as _players
from sportsdataverse.nfl import nfl_roster_builder as _roster_builder
from sportsdataverse.nfl import nfl_schedule as _schedule
from tests.conftest import skip_if_no_live

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_loaders"

_ADDED_IN_2023 = [
    "offense_names",
    "defense_names",
    "offense_positions",
    "defense_positions",
    "offense_numbers",
    "defense_numbers",
]


# Bound before any monkeypatching: ``_loaders.pl`` IS the polars module, so
# patching ``pl.read_parquet`` there patches it here too.
_read_parquet = pl.read_parquet


def _fixture(season: int) -> pl.DataFrame:
    return _read_parquet(FIXTURES / f"pbp_participation_{season}_head3.parquet")


@pytest.fixture()
def no_cache():
    """The loaders are ``@cached_loader``-wrapped; don't serve a stale frame."""
    update_config(cache_mode="off")
    clear_cache()
    yield
    reset_config()
    clear_cache()


@pytest.fixture()
def offline_participation(monkeypatch):
    """Serve the committed per-season fixtures instead of hitting the release."""

    def fake_read_parquet(source, *args, **kwargs):
        for season in (2016, 2023):
            if str(season) in str(source):
                return _fixture(season)
        raise AssertionError(f"unexpected read_parquet source: {source!r}")

    monkeypatch.setattr(_loaders.pl, "read_parquet", fake_read_parquet)


def test_fixtures_really_do_drift():
    """Guard the guard: the two captures must disagree on columns AND dtype."""
    a, b = _fixture(2016), _fixture(2023)
    assert set(b.columns) - set(a.columns) == set(_ADDED_IN_2023)
    assert a.schema["play_id"] != b.schema["play_id"]


def test_vertical_concat_on_these_fixtures_still_raises():
    """The pre-fix ``how="vertical"`` call is what blew up. Prove it still does."""
    with pytest.raises((pl.exceptions.ShapeError, pl.exceptions.SchemaError)):
        pl.concat([_fixture(2016), _fixture(2023)], how="vertical")


def test_load_participation_spans_a_schema_change(no_cache, offline_participation):
    df = load_nfl_pbp_participation(seasons=[2016, 2023])

    assert df.height == 6, "both seasons must survive the concat"
    assert set(df.columns) == set(_fixture(2023).columns), "columns must be unioned"
    # The 2016 rows null-fill the columns that only exist from 2023 on.
    assert df.head(3).select(_ADDED_IN_2023).null_count().row(0) == (3,) * len(_ADDED_IN_2023)
    assert df.tail(3).select(_ADDED_IN_2023).null_count().row(0) == (0,) * len(_ADDED_IN_2023)


@pytest.mark.parametrize("seasons", [[2016], [2023], [2016, 2023]])
def test_participation_play_id_dtype_is_span_independent(seasons, no_cache, offline_participation):
    """``play_id`` joins against ``load_nfl_pbp``, which ships it as Float64.

    Without the boundary pin the dtype would follow whichever seasons the
    caller asked for — ``Int32`` for a 2016-only load, ``Float64`` once a
    2023+ season widens the supertype.
    """
    df = load_nfl_pbp_participation(seasons=seasons)
    assert df.schema["play_id"] == pl.Float64


@pytest.mark.parametrize("module", [_loaders, _schedule])
def test_no_vertical_concat_left_in_multi_season_modules(module):
    """The other multi-season sites share this fix; don't let one regress.

    ``vertical_relaxed`` is caught too — it tolerates the dtype half of the
    drift but still raises when the column sets disagree.
    """
    offenders = [
        line.strip()
        for line in Path(module.__file__).read_text(encoding="utf-8").splitlines()
        if "pl.concat(" in line and 'how="vertical' in line
    ]
    assert offenders == []


@skip_if_no_live
def test_load_participation_live_across_the_2023_schema_change(no_cache):
    df = load_nfl_pbp_participation(seasons=[2022, 2023])
    assert df.height > 0
    assert set(_ADDED_IN_2023).issubset(df.columns)
    assert df.schema["play_id"] == pl.Float64


# DynastyProcess CSVs are read with ``pl.read_csv``, so without a pin every id
# column's dtype follows whatever the current upstream rows happen to contain.
# Every id is ``Utf8``: upstream's own ``db_playerids.rds`` stores all 20 id
# columns as character, MFL ids are zero-padded (``0156``), and the same ids are
# ``Utf8`` in the roster/players schemas they join against. The rankings
# FantasyPros id (``id`` / ``fantasypros_id``) is the same key as playerids'
# ``fantasypros_id`` and must share its dtype.
_FF_PLAYERIDS_ID_DTYPES = {
    col: pl.Utf8
    for col in (
        "mfl_id",
        "sportradar_id",
        "fantasypros_id",
        "gsis_id",
        "pff_id",
        "sleeper_id",
        "nfl_id",
        "espn_id",
        "yahoo_id",
        "fleaflicker_id",
        "cbs_id",
        "pfr_id",
        "cfbref_id",
        "rotowire_id",
        "rotoworld_id",
        "ktc_id",
        "stats_id",
        "stats_global_id",
        "fantasy_data_id",
        "swish_id",
    )
}
_FF_RANKINGS_ID_DTYPES = {
    "draft": {"id": pl.Utf8, "sportsdata_id": pl.Utf8, "yahoo_id": pl.Utf8, "cbs_id": pl.Utf8},
    "week": {"fantasypros_id": pl.Utf8, "player_opponent_id": pl.Utf8},
}
_FF_RANKINGS_URLS = {"draft": NFL_FF_RANKINGS_DRAFT_URL, "week": NFL_FF_RANKINGS_WEEK_URL}
_FF_SLICES = {
    NFL_FF_PLAYERIDS_URL: FIXTURES / "db_playerids_slice.csv",
    NFL_FF_RANKINGS_DRAFT_URL: FIXTURES / "db_fpecr_latest_slice.csv",
    NFL_FF_RANKINGS_WEEK_URL: FIXTURES / "fp_latest_weekly_slice.csv",
}

_read_csv = pl.read_csv


@pytest.fixture()
def offline_ff_csvs(monkeypatch):
    """Serve the committed real CSV slices in place of the DynastyProcess URLs."""
    seen: list = []

    def fake_read_csv(source, **kwargs):
        seen.append(source)
        return _read_csv(_FF_SLICES[source], **kwargs)

    monkeypatch.setattr(_loaders.pl, "read_csv", fake_read_csv)
    return seen


def test_ff_fixtures_really_do_flip_under_inference():
    """Guard the guard: unpinned, each slice types a string id as ``Int64``."""

    def inferred(url):
        return _read_csv(_FF_SLICES[url], null_values=["NA", "NULL", ""]).schema

    assert inferred(NFL_FF_PLAYERIDS_URL)["fantasypros_id"] == pl.Int64
    assert inferred(NFL_FF_PLAYERIDS_URL)["mfl_id"] == pl.Int64
    assert inferred(NFL_FF_RANKINGS_DRAFT_URL)["id"] == pl.Int64
    assert inferred(NFL_FF_RANKINGS_WEEK_URL)["fantasypros_id"] == pl.Int64


def test_ff_playerids_pins_id_dtypes_against_upstream_inference(no_cache, offline_ff_csvs):
    """Id dtypes must not follow the rows DynastyProcess happens to ship.

    Every formerly-string id in the slice is purely numeric, so plain inference
    types it ``Int64`` -- and ``nfl_id`` ``038666`` / ``mfl_id`` ``0156`` lose
    their leading zeros.
    """
    df = load_nfl_ff_playerids()  # ``load_ff_playerids`` is the same object

    assert offline_ff_csvs == [NFL_FF_PLAYERIDS_URL]
    assert {c: df.schema[c] for c in _FF_PLAYERIDS_ID_DTYPES} == _FF_PLAYERIDS_ID_DTYPES
    by_name = {row["name"]: row for row in df.to_dicts()}
    assert by_name["Anthony Smith"]["nfl_id"] == "038666"
    assert by_name["James Allen"]["mfl_id"] == "0156"
    assert by_name["Fernando Mendoza"]["fantasypros_id"] == "28013"


@pytest.mark.parametrize("kind", ["draft", "week"])
def test_ff_rankings_pins_id_dtypes_against_upstream_inference(kind, no_cache, offline_ff_csvs):
    df = load_nfl_ff_rankings(kind=kind)  # ``load_ff_rankings`` is the same object

    assert offline_ff_csvs == [_FF_RANKINGS_URLS[kind]]
    expected = _FF_RANKINGS_ID_DTYPES[kind]
    assert {c: df.schema[c] for c in expected} == expected


@pytest.mark.parametrize(
    ("kind", "key", "matched"),
    [("draft", "id", ["Fernando Mendoza", "Josh Allen"]), ("week", "fantasypros_id", ["Josh Allen"])],
)
def test_ff_rankings_fantasypros_id_joins_playerids(kind, key, matched, no_cache, offline_ff_csvs):
    """The rankings FantasyPros id is playerids' ``fantasypros_id``.

    92% of the live ``db_fpecr_latest.csv`` ``id`` values (and 91% of the weekly
    ``fantasypros_id`` values) appear in ``db_playerids.csv`` ``fantasypros_id``.
    Pinning one side and letting the other infer ``Int64`` breaks the join.
    """
    ids = load_nfl_ff_playerids()
    ranks = load_nfl_ff_rankings(kind=kind)

    assert ranks.schema[key] == ids.schema["fantasypros_id"]
    joined = ranks.join(ids, left_on=key, right_on="fantasypros_id", how="inner")
    assert sorted(joined["name"].to_list()) == matched


@pytest.mark.parametrize("schema_module", [_roster_builder, _players], ids=lambda m: m.__name__)
def test_ff_playerids_ids_match_roster_schema_dtypes(schema_module, no_cache, offline_ff_csvs):
    """A roster -> playerids join on a shared id must not raise ``SchemaError``.

    Compares against the module's real pinned ``_SCHEMA`` (not a copy), so the
    two surfaces can't drift apart silently.
    """
    df = load_nfl_ff_playerids()
    shared = sorted(c for c in set(df.columns) & set(schema_module._SCHEMA) if c.endswith("_id"))
    assert "espn_id" in shared
    assert {c: df.schema[c] for c in shared} == {c: schema_module._SCHEMA[c] for c in shared}


@pytest.mark.parametrize(
    ("load", "id_cols", "padded"),
    [
        (lambda: load_nfl_ff_playerids(return_as_pandas=True), list(_FF_PLAYERIDS_ID_DTYPES), ("mfl_id", "0156")),
        (
            lambda: load_nfl_ff_rankings(kind="draft", return_as_pandas=True),
            list(_FF_RANKINGS_ID_DTYPES["draft"]),
            None,
        ),
        (lambda: load_nfl_ff_rankings(kind="week", return_as_pandas=True), list(_FF_RANKINGS_ID_DTYPES["week"]), None),
    ],
    ids=["playerids", "rankings-draft", "rankings-week"],
)
def test_ff_loaders_return_string_ids_as_pandas(load, id_cols, padded, no_cache, offline_ff_csvs):
    import pandas as pd

    pdf = load()
    assert [c for c in id_cols if not pd.api.types.is_string_dtype(pdf[c])] == []
    if padded is not None:
        col, value = padded
        assert value in pdf[col].tolist()
