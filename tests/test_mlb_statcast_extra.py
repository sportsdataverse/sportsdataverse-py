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


def test_translate_filters_maps_friendly_kwargs_to_savant():
    from sportsdataverse.mlb.mlb_statcast_extra import _translate_filters

    out = _translate_filters(
        {
            "season": [2024, 2025],  # pipe-list
            "pitch_type": "FF",  # scalar -> trailing pipe
            "at_bat_result": ["single", "home_run"],
            "batters_lookup": 592450,  # -> name[] list
            "team": "NYY",  # scalar passthrough
            "hfRO": "RISP|",  # raw Savant param -> verbatim
        }
    )
    assert out["hfSea"] == "2024|2025|"
    assert out["hfPT"] == "FF|"
    assert out["hfAB"] == "single|home_run|"
    assert out["batters_lookup[]"] == ["592450"]
    assert out["team"] == "NYY"
    assert out["hfRO"] == "RISP|"  # unknown key forwarded verbatim


def test_pipe_handles_non_string_scalar():
    """A bare int (e.g. season=2024) must not raise; it pipes as a single value."""
    from sportsdataverse.mlb.mlb_statcast_extra import _pipe, _translate_filters

    assert _pipe(2024) == "2024|"
    assert _pipe(None) == ""
    assert _pipe("FF") == "FF|"
    assert _pipe([1, 2]) == "1|2|"
    # exercised end-to-end through a scalar friendly filter
    assert _translate_filters({"season": 2024})["hfSea"] == "2024|"


def test_search_passes_friendly_filters_through_to_savant(monkeypatch):
    from sportsdataverse.mlb import mlb_statcast_extra as ex

    seen = {}

    def fake_download(url, params=None, **kw):
        seen.update(params or {})

        class R:
            text = "pitch_type,game_date\nFF,2024-07-01"

        return R()

    monkeypatch.setattr(ex, "download", fake_download)
    ex.mlb_statcast_search("2024-07-01", "2024-07-01", batters_lookup=592450, at_bat_result="home_run")
    assert seen["batters_lookup[]"] == ["592450"]
    assert seen["hfAB"] == "home_run|"


def test_player_raw_returns_html_else_frame(monkeypatch):
    from sportsdataverse.mlb import mlb_statcast_extra as ex

    html = (
        "<html><body><script>var serverVals = "
        '{"playerId":"592450","statcast":[{"player_id":592450,"xwoba":0.42}]};</script></body></html>'
    )

    class R:
        text = html

    monkeypatch.setattr(ex, "download", lambda url, params=None, **kw: R())
    # raw=True -> the page HTML string
    assert ex.mlb_statcast_player(592450, raw=True) == html
    # default -> a parsed frame of the requested section
    import polars as pl

    df = ex.mlb_statcast_player(592450)
    assert isinstance(df, pl.DataFrame) and df.height == 1 and "xwoba" in df.columns
