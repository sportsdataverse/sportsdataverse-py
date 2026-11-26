"""Offline tests for ``load_mlb_chadwick_register`` against verbatim register shard slices."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
import yaml

from sportsdataverse.errors import AssetFetchError, SchemaDriftWarning
from sportsdataverse.mlb import load_mlb_chadwick_register
from sportsdataverse.mlb.mlb_people_register import CHADWICK_REGISTER_PEOPLE_URL

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "mlb_chadwick_register"
ROOT = Path(__file__).resolve().parents[2]


class _Resp:
    def __init__(self, body: bytes, status_code: int = 200):
        self.content = body
        self.text = body.decode("utf-8")
        self.status_code = status_code


def _header() -> bytes:
    return (FIX / "people-f.csv").read_bytes().split(b"\n", 1)[0] + b"\n"


@pytest.fixture
def served(monkeypatch):
    """Serve the two fixture shards verbatim; every other shard is the real header with no rows."""
    calls: list[str] = []

    def fake_download(url, *a, **k):
        calls.append(url)
        shard = url.rsplit("people-", 1)[1].removesuffix(".csv")
        path = FIX / f"people-{shard}.csv"
        return _Resp(path.read_bytes() if path.exists() else _header())

    monkeypatch.setattr("sportsdataverse._codegen_runtime.download", fake_download)
    return calls


def test_reads_all_16_shards_in_order(served):
    load_mlb_chadwick_register()
    assert served == [CHADWICK_REGISTER_PEOPLE_URL.format(shard=s) for s in "0123456789abcdef"]


def test_rows_and_id_dtypes(served):
    df = load_mlb_chadwick_register()
    assert df.height == 5 and df.width == 40
    assert df["key_person"].n_unique() == 5
    for c in ("key_mlbam", "key_fangraphs", "birth_year", "mlb_played_first", "mlb_umpired_last"):
        assert df.schema[c] == pl.Int64, c
    for c in ("key_person", "key_uuid", "key_retro", "key_bbref", "key_bbref_minors", "key_npb", "key_wikidata"):
        assert df.schema[c] == pl.Utf8, c
    trout = df.filter(pl.col("key_bbref") == "troutmi01").row(0, named=True)
    assert (trout["key_mlbam"], trout["key_fangraphs"], trout["key_retro"]) == (545361, 10155, "troum001")
    assert (trout["mlb_played_first"], trout["birth_year"]) == (2011, 1991)
    ichiro = df.filter(pl.col("key_bbref") == "suzukic01").row(0, named=True)
    assert (ichiro["key_mlbam"], ichiro["key_fangraphs"], ichiro["pro_played_first"]) == (400085, 1101, 1992)
    # a register person with no MLBAM id stays a row with a null key, never dropped or zero-filled
    assert df["key_mlbam"].null_count() == 2
    # NPB ids are zero-padded upstream; an integer cast would turn "01905114" into 1905114
    shima = df.filter(pl.col("key_person") == "f0b36cbd").row(0, named=True)
    assert (shima["key_npb"], shima["key_mlbam"]) == ("01905114", 660278)


def test_key_mlbam_dtype_matches_the_pbp_loader(served):
    """The documented join: key_mlbam onto load_mlb_pbp batter_id / pitcher_id."""
    schemas = yaml.safe_load((ROOT / "tools/codegen/schemas/loader_schemas.yaml").read_text(encoding="utf-8"))
    pbp = {c["name"]: c["type"] for c in schemas["load_mlb_pbp"]}
    assert pbp["batter_id"] == pbp["pitcher_id"] == "Int64"
    df = load_mlb_chadwick_register()
    pitches = pl.DataFrame({"batter_id": [545361, 400085]}, schema={"batter_id": pl.Int64})
    assert pitches.schema["batter_id"] == df.schema["key_mlbam"]
    joined = pitches.join(df, left_on="batter_id", right_on="key_mlbam", how="left")
    assert joined["key_bbref"].to_list() == ["troutmi01", "suzukic01"]


def test_a_page_instead_of_csv_fails_the_whole_load(monkeypatch):
    def fake_download(url, *a, **k):
        if url.endswith("people-7.csv"):
            return _Resp(b"<!DOCTYPE html><html>rate limited</html>")
        return _Resp(_header())

    monkeypatch.setattr("sportsdataverse._codegen_runtime.download", fake_download)
    with pytest.raises(AssetFetchError, match="page, not data"):
        load_mlb_chadwick_register()


def test_a_non_integer_value_keeps_the_column_as_text_and_warns(monkeypatch):
    """Upstream drift (a FanGraphs minor-league ``sa`` id in key_fangraphs) must not fail the load."""
    real = (FIX / "people-f.csv").read_bytes()
    trout = next(line for line in real.split(b"\n") if b",troutmi01," in line)
    drifted = real.replace(trout, trout.replace(b",10155,", b",sa3011918,"))  # one real line, one value changed
    assert drifted != real

    def fake_download(url, *a, **k):
        return _Resp(drifted if url.endswith("people-f.csv") else _header())

    monkeypatch.setattr("sportsdataverse._codegen_runtime.download", fake_download)
    with pytest.warns(SchemaDriftWarning, match=r"\['key_fangraphs'\]"):
        df = load_mlb_chadwick_register()
    assert df.schema["key_fangraphs"] == pl.Utf8 and df.schema["key_mlbam"] == pl.Int64
    assert "sa3011918" in df["key_fangraphs"].to_list() and "10155" not in df["key_fangraphs"].to_list()


def test_return_as_pandas(served):
    pdf = load_mlb_chadwick_register(return_as_pandas=True)
    assert len(pdf) == 5 and "key_mlbam" in pdf.columns
