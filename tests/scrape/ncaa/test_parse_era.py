"""The NCAA parse stage on a real halves-era WBB bundle (B-M35, B-L39).

``tests/fixtures/ncaa/wbb_raw/wbb/raw/2015/385627.json.gz`` is a producer bundle copied
byte-for-byte from ``ncaa-wbb-hoops-raw`` (Alabama A&M at Southeast Mo. St., 2014-11-14,
season 2015): women's college basketball still played two halves, and the page writes names
in the V1 ``LAST,FIRST`` grammar. The oracle is the page itself: the linescore table of the
box-score page (per-half and final scores) and the raw event text.
"""

from __future__ import annotations

import io
import json
import re
import shutil
import sys
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.scrape.ncaa import parse
from sportsdataverse.scrape.ncaa.bundle import read_bundle

RAW_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "ncaa" / "wbb_raw"
BUNDLE_PATH = RAW_ROOT / "wbb" / "raw" / "2015" / "385627.json.gz"


@pytest.fixture(scope="module")
def bundle() -> dict:
    return read_bundle(BUNDLE_PATH)


@pytest.fixture(scope="module")
def parsed(bundle) -> dict:
    return parse.parse_bundle(bundle, league="wbb")


def _linescore(bundle: dict) -> pd.DataFrame:
    """The box-score page's linescore table: one row per team, columns 1, 2, S."""
    for table in pd.read_html(io.StringIO(bundle["pages"]["box_score"])):
        if table.shape[1] == 4 and list(table.iloc[0, 1:]) == ["1", "2", "S"]:
            return table.iloc[1:3].set_index(0).astype(int)
    raise AssertionError("no linescore table on the box-score page")


def test_halves_era_bundle_parses_two_periods(bundle, parsed):
    assert bundle["season"] == "2015" and bundle["league"] == "wbb"
    pbp = pl.DataFrame(parsed["pbp"])
    assert pbp.height > 400
    assert sorted(pbp["period"].unique().to_list()) == [1, 2]
    assert set(pbp["contest_id"].unique().to_list()) == {"385627"}
    assert len(parsed["team_box"]) == 2 and len(parsed["player_box"]) > 10 and len(parsed["possessions"]) > 100


def test_halves_era_scores_match_the_linescore(bundle, parsed):
    line = _linescore(bundle)
    pbp = pl.DataFrame(parsed["pbp"]).sort("game_seconds", maintain_order=True)
    home, away = pbp["home"][0], pbp["away"][0]
    assert (home, away) == ("Southeast Mo. St.", "Alabama A&M")
    for period, col in ((1, 1), (2, 3)):  # halftime = column "1", final = column "S"
        last = pbp.filter(pl.col("period") == period).tail(1).row(0, named=True)
        assert (last["home_score"], last["away_score"]) == (line.loc[home, col], line.loc[away, col]), period


def test_v1_last_first_names_become_first_dot_last(parsed):
    # B-L39: the V1 grammar is "LAST,FIRST event"; the player token is rebuilt as FIRST.LAST
    pbp = pl.DataFrame(parsed["pbp"])
    v1 = re.compile(r"^([A-Z][A-Z'\-]*),([A-Z][A-Z'\-]*) ")
    rows = [r for r in pbp.iter_rows(named=True) if v1.match(r["event_description"] or "")]
    assert len(rows) > 200
    for r in rows:
        last, first = v1.match(r["event_description"]).groups()
        assert r["player_1"] == f"{first}.{last}".replace("'", "").replace("-", ""), r["event_description"]
    made = [r for r in rows if " made " in r["event_description"]]
    assert made and all(r["event_result"] == "made" for r in made)


def test_parse_and_write_writes_the_halves_era_game(tmp_path, bundle, parsed):
    path = parse.parse_and_write(bundle, tmp_path, league="wbb")
    assert path == tmp_path / "wbb" / "json" / "385627.json"
    written = json.loads(path.read_text(encoding="utf-8"))
    assert len(written["pbp"]) == len(parsed["pbp"]) > 0
    assert written["pbp"][-1]["home_score"] == 66 and written["pbp"][-1]["away_score"] == 38


def test_parse_stage_main_on_a_raw_tree(tmp_path, monkeypatch, capsys):
    # B-L30: `_main(league, root)` is the producers' real interface
    shutil.copytree(RAW_ROOT, tmp_path, dirs_exist_ok=True)
    monkeypatch.setattr(sys, "argv", ["parse", "--season", "2015"])
    parse._main("wbb", str(tmp_path))
    out = capsys.readouterr().out
    assert "bundles=1 pending=1" in out and "parsed=1 failed=0" in out
    written = json.loads((tmp_path / "wbb" / "json" / "385627.json").read_text(encoding="utf-8"))
    assert {r["period"] for r in written["pbp"]} == {1, 2}
