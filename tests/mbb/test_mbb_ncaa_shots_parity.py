"""MBB parity: ``parse_ncaa_bb_shots`` / ``ncaa_mbb_join_pbp_shots`` vs the
bigballR R oracle.

Oracle CSVs were produced by running bigballR's ``get_shot_locations`` +
``join_pbp_shots`` (``bigballR/R/get_shot_locations.R``) on the committed
``box_{id}.html`` fixtures. Exact equality for ints/strings; ``x``/``y``/
``shot_dist`` compared with ``math.isclose`` (1e-9).

Fixture games (see ``tests/fixtures/ncaa/bigballr/README.md``):

* 6470186 — blowout / garbage-time path
* 6479639 — close regulation game
* 6479592 — 1 OT
* 1613299 — 2019-era markup (older page vintage, same addShot grammar)
"""

from __future__ import annotations

import math

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_shots import (
    SHOTS_RENAME,
    SHOTS_SCHEMA,
    ncaa_mbb_join_pbp_shots,
    ncaa_mbb_shot_locations,
    parse_ncaa_bb_shots,
)
from tests.mbb._bigballr_oracle import GAMES, HTML_DIR, PBP_RENAME, PBP_SCHEMA, load_oracle, load_oracle_pbp

#: Shot-chart columns carried into the joined output (R select, :121).
_JOIN_SHOT_COLS = ["team", "player", "x", "y", "shot_dist"]

_FLOAT_COLS = {"x", "y", "shot_dist"}


def _fixture_html(game_id: str) -> str:
    return (HTML_DIR / f"box_{game_id}.html").read_text(encoding="utf-8")


def _assert_frames_equal(got: pl.DataFrame, exp: pl.DataFrame) -> None:
    """Exact ints/strings; floats isclose(1e-9); null placement exact."""
    assert got.columns == exp.columns
    assert got.height == exp.height, f"row count {got.height} != oracle {exp.height}"
    for col in got.columns:
        g, e = got[col].to_list(), exp[col].to_list()
        if col in _FLOAT_COLS:
            for i, (gv, ev) in enumerate(zip(g, e)):
                if gv is None or ev is None:
                    assert gv is None and ev is None, f"{col}[{i}]: {gv!r} != {ev!r}"
                else:
                    assert math.isclose(gv, ev, rel_tol=1e-9, abs_tol=1e-9), f"{col}[{i}]: {gv!r} != {ev!r}"
        else:
            assert g == e, f"column {col!r} diverges"


@pytest.fixture(scope="module")
def oracle_shots() -> pl.DataFrame:
    df = load_oracle("shot_locations", "mbb").rename(SHOTS_RENAME)
    return df.with_columns([pl.col(c).cast(dt) for c, dt in SHOTS_SCHEMA.items()]).select(list(SHOTS_SCHEMA))


@pytest.fixture(scope="module")
def oracle_joined() -> pl.DataFrame:
    df = load_oracle("pbp_shots_joined", "mbb")
    df = df.rename({k: v for k, v in PBP_RENAME.items() if k in df.columns})
    df = df.rename({"Team": "team", "Player": "player", "Shot_Dist": "shot_dist"})
    casts = [pl.col(c).cast(dt, strict=False) for c, dt in PBP_SCHEMA.items() if c in df.columns]
    casts += [pl.col(c).cast(SHOTS_SCHEMA[c]) for c in _JOIN_SHOT_COLS]
    return df.with_columns(casts)


@pytest.fixture(scope="module")
def parsed_shots() -> pl.DataFrame:
    return pl.concat([parse_ncaa_bb_shots(_fixture_html(g), g) for g in GAMES["mbb"]])


@pytest.mark.parametrize("game_id", GAMES["mbb"])
def test_mbb_shot_locations_parity(game_id: str, oracle_shots: pl.DataFrame, parsed_shots: pl.DataFrame) -> None:
    got = parsed_shots.filter(pl.col("game_id") == game_id)
    exp = oracle_shots.filter(pl.col("game_id") == game_id)
    assert got.schema == pl.Schema(SHOTS_SCHEMA)
    _assert_frames_equal(got, exp)


def test_empty_html_returns_contract_schema() -> None:
    got = parse_ncaa_bb_shots("<html><body></body></html>", "0")
    assert got.height == 0
    assert got.schema == pl.Schema(SHOTS_SCHEMA)


def test_mbb_join_pbp_shots_parity(parsed_shots: pl.DataFrame, oracle_joined: pl.DataFrame) -> None:
    pbp = load_oracle_pbp("mbb")
    got = ncaa_mbb_join_pbp_shots(pbp, parsed_shots)

    assert got.columns == pbp.columns + _JOIN_SHOT_COLS
    _assert_frames_equal(got, oracle_joined.select(got.columns))


def test_join_guard_mismatched_ids(parsed_shots: pl.DataFrame) -> None:
    pbp = load_oracle_pbp("mbb")
    partial = parsed_shots.filter(pl.col("game_id") != GAMES["mbb"][0])
    with pytest.raises(ValueError, match="PBP and Shot Locations do not match."):
        ncaa_mbb_join_pbp_shots(pbp, partial)


class _FixtureFetcher:
    """Offline stand-in for ``NcaaFetcher`` reading the committed captures."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch_game_box(self, contest_id: object) -> str:
        self.calls.append(str(contest_id))
        path = HTML_DIR / f"box_{contest_id}.html"
        if not path.exists():
            return "<html></html>"
        return path.read_text(encoding="utf-8")


def test_multi_game_driver_offline(oracle_shots: pl.DataFrame) -> None:
    """NA ids dropped up front (R :5); per-game frames row-bound (R :75)."""
    fetcher = _FixtureFetcher()
    ids: list[object] = [GAMES["mbb"][0], None, GAMES["mbb"][1]]
    got = ncaa_mbb_shot_locations(ids, fetcher=fetcher)

    keep = [GAMES["mbb"][0], GAMES["mbb"][1]]
    exp = oracle_shots.filter(pl.col("game_id").is_in(keep))
    assert got.height == exp.height
    assert got["game_id"].unique().sort().to_list() == sorted(keep)
    assert "None" not in fetcher.calls
