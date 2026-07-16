"""Contract tests for the CFB dataset loaders that read published releases.

These six tags were published + backfilled by ``cfbfastR-cfb-data`` but had no
Python loader, so nothing asserted that what sdv-py *declares* matches what the
producer *publishes*. The codegen suite covers URL shape and that loader modules
render; it does not look at the release.

Offline by default: the season-range + wiring assertions read committed metadata.
The one test that touches the network is gated behind ``SDV_PY_LIVE_TESTS=1``,
and it is the one that matters -- it reads a real published parquet and asserts
the declared returns-schema against it, which is what makes the published
returns-table honest rather than aspirational.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
import yaml

from tests.conftest import skip_if_no_live

_ROOT = Path(__file__).resolve().parents[2]
_RELEASES = _ROOT / "tools" / "codegen" / "endpoints" / "releases.yaml"
_SCHEMAS = _ROOT / "tools" / "codegen" / "schemas" / "loader_schemas.yaml"

# fn -> the season the schema was introspected from (a season each tag actually
# publishes, so the live check reads a real asset rather than a 404).
_DATASET_LOADERS = {
    "load_cfb_model_pbp": 2023,
    "load_cfb_passing": 2023,
    "load_cfb_percentiles": 2023,
    "load_cfb_receiving": 2023,
    "load_cfb_rushing": 2023,
    "load_cfb_team_summaries": 2023,
}


def _loaders() -> dict[str, dict]:
    entries = yaml.safe_load(_RELEASES.read_text(encoding="utf-8"))["loaders"]
    return {e["fn"]: e for e in entries}


def _schemas() -> dict[str, list[dict]]:
    return yaml.safe_load(_SCHEMAS.read_text(encoding="utf-8"))


@pytest.mark.parametrize("fn", sorted(_DATASET_LOADERS))
def test_loader_is_wired_to_a_release(fn: str) -> None:
    entry = _loaders()[fn]

    assert entry["league"] == "cfb"
    # these tags are GitHub releases on sportsdataverse-data, not the raw_data
    # repo that load_cfb_rosters / load_cfb_schedule / load_cfb_team_info read
    assert entry["base"] == "sdv_releases", fn
    # the url's tag segment must be the tag it claims
    assert entry["url"].startswith(entry["tag"] + "/"), fn
    assert entry["url"].endswith("_{season}.parquet"), fn


@pytest.mark.parametrize("fn", sorted(_DATASET_LOADERS))
def test_loader_declares_a_returns_schema(fn: str) -> None:
    cols = _schemas().get(fn)

    assert cols, f"{fn} has no returns-schema -- its docs table would be empty"
    assert all(c.get("name") and c.get("type") for c in cols), fn
    # duplicate column names would silently drop rows from the rendered table
    names = [c["name"] for c in cols]
    assert len(names) == len(set(names)), f"{fn} has duplicate columns"


@pytest.mark.parametrize("fn", sorted(_DATASET_LOADERS))
def test_loader_is_exported(fn: str) -> None:
    import sportsdataverse.cfb as cfb

    assert callable(getattr(cfb, fn)), fn


@skip_if_no_live
@pytest.mark.parametrize("fn,season", sorted(_DATASET_LOADERS.items()))
def test_declared_schema_matches_the_published_parquet(fn: str, season: int) -> None:
    """The declared returns-schema must match the REAL published asset.

    This is the point of the file. The schemas were introspected from the
    published parquet, so a producer-side change (a renamed column, a dtype
    flip) leaves the shipped returns-table lying until this fails.
    """
    import sportsdataverse.cfb as cfb

    df = getattr(cfb, fn)(season)
    declared = {c["name"]: c["type"] for c in _schemas()[fn]}

    assert df.height > 0, f"{fn}({season}) returned no rows -- asset missing?"
    # published columns must be exactly what we declare (order-insensitive: the
    # docs table is a reference, not a positional contract)
    assert set(df.columns) == set(declared), (
        f"{fn}: declared-vs-published column mismatch; "
        f"missing={sorted(set(declared) - set(df.columns))} "
        f"extra={sorted(set(df.columns) - set(declared))}"
    )
    drift = {c: (declared[c], str(df.schema[c])) for c in df.columns if str(df.schema[c]) != declared[c]}
    assert not drift, f"{fn}: dtype drift declared-vs-published: {drift}"


@skip_if_no_live
@pytest.mark.parametrize("fn,season", sorted(_DATASET_LOADERS.items()))
def test_loader_returns_rows_for_a_published_season(fn: str, season: int) -> None:
    """A season the tag publishes must load non-empty.

    Release loaders are 404-safe (a missing asset warns + returns empty), so an
    empty frame here means a broken url/tag/stem in releases.yaml rather than a
    raised error -- exactly the failure this catches.
    """
    import sportsdataverse.cfb as cfb

    df = getattr(cfb, fn)(season)

    assert isinstance(df, pl.DataFrame)
    assert df.height > 0, f"{fn}({season}) is empty -- check url/tag/stem"


# NOTE: deliberately no assertion that `espn_cfb_passing` carries non-zero
# sacked/sack_yds/pass_int. cfbfastR-cfb-data#18 fixed the producer, but the
# PUBLISHED release is still the pre-fix output (verified 2026-07-16: 2023 has
# sacked=0 / pass_int=0 / sack_adj_yards==yards across all 716 rows) and the
# daily cron only rebuilds the current season, so 2014-2024 will not self-heal
# without a re-backfill. Asserting the fixed values here would fail today and,
# worse, an assertion of the CURRENT (buggy) values would flip red the moment
# the data is corrected. The loaders load what is published; the re-backfill is
# tracked separately.
