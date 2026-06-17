from __future__ import annotations
import polars as pl


def test_date_chunks_splits_inclusive_week():
    from sportsdataverse.mlb.mlb_statcast_extra import _date_chunks

    chunks = _date_chunks("2024-04-01", "2024-04-15", days=7)
    assert chunks[0] == ("2024-04-01", "2024-04-07")
    assert chunks[-1][1] == "2024-04-15"
    # no gaps / overlaps
    assert chunks[1][0] == "2024-04-08"


def test_search_concats_chunks_and_rechunks_on_truncation(monkeypatch):
    from sportsdataverse.mlb import mlb_statcast_extra as ex

    calls = {"n": 0}

    def fake_download(url, params=None, **kw):
        calls["n"] += 1
        # first call (7-day chunk) returns a full 25k -> truncated; sub-chunks return 1 row
        rng = params["game_date_lt"]
        rows = 25000 if params["game_date_gt"] == "2024-04-01" and rng == "2024-04-07" else 1
        body = "pitch_type,game_date\n" + "\n".join("FF,%s" % rng for _ in range(rows))

        class R:  # minimal response-like
            text = body

        return R()

    monkeypatch.setattr(ex, "download", fake_download)
    df = ex.mlb_statcast_search("2024-04-01", "2024-04-07", chunk_days=7)
    assert isinstance(df, pl.DataFrame)
    # truncated 7-day chunk was split into smaller chunks -> more than one fetch
    assert calls["n"] > 1
