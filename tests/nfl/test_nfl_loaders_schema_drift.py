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

from sportsdataverse.nfl import clear_cache, load_nfl_pbp_participation, reset_config, update_config
from sportsdataverse.nfl import nfl_loaders as _loaders
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
