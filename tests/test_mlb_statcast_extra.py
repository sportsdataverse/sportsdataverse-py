from __future__ import annotations

import csv
import io
import warnings
from pathlib import Path

import polars as pl
import pytest


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


_SEARCH_HEAD = Path(__file__).resolve().parent / "fixtures" / "mlb_statcast" / "search_2024-06-15_head.csv"
_RUNNERS = ["on_1b", "on_2b", "on_3b"]
#: (on_1b, on_2b, on_3b) of the fixture's bases-loaded pitch: game 745329, AB 44, pitch 2.
_BASES_LOADED = (656305, 671218, 596103)


class _Resp:
    def __init__(self, text: str) -> None:
        self.text = text


def _fixture_rows() -> list:
    return list(csv.reader(io.StringIO(_SEARCH_HEAD.read_text(encoding="utf-8"))))


def _to_csv(rows: list) -> str:
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerows(rows)
    return buf.getvalue()


def _exact_ints(values: list) -> bool:
    # type check, not just ==: 656305.0 == 656305 is True, so a float id would slip past a value compare.
    return all(type(v) is int for v in values)


def test_search_real_capture_ids_stay_int64_in_both_outputs(monkeypatch):
    """Real Savant search CSV through the chunked search: runner ids exact Int64 in polars AND pandas output."""
    from sportsdataverse.mlb import mlb_statcast_extra as ex

    body = _SEARCH_HEAD.read_text(encoding="utf-8")
    monkeypatch.setattr(ex, "download", lambda url, params=None, **kw: _Resp(body))

    df = ex.mlb_statcast_search("2024-06-15", "2024-06-15")
    key = (pl.col("game_pk") == 745329) & (pl.col("at_bat_number") == 44) & (pl.col("pitch_number") == 2)
    row = list(df.filter(key).select(_RUNNERS).row(0))
    assert row == list(_BASES_LOADED) and _exact_ints(row)
    assert [df.schema[c] for c in (*_RUNNERS, "game_pk")] == [pl.Int64] * 4

    pdf = ex.mlb_statcast_search("2024-06-15", "2024-06-15", return_as_pandas=True)
    hit = pdf.loc[(pdf["game_pk"] == 745329) & (pdf["at_bat_number"] == 44) & (pdf["pitch_number"] == 2), _RUNNERS]
    assert len(hit) == 1
    prow = hit.iloc[0].tolist()
    assert prow == list(_BASES_LOADED) and _exact_ints(prow)
    assert [str(pdf[c].dtype) for c in _RUNNERS] == ["Int64"] * 3 and pdf["on_1b"].isna().sum() == 26


def test_search_all_blank_chunk_concat_keeps_runner_ids_int64(monkeypatch):
    """Two chunks, one with on_3b blank on every row: the stitched on_3b is Int64 with the real ids intact."""
    from sportsdataverse.mlb import mlb_statcast_extra as ex

    rows = _fixture_rows()
    idx = rows[0].index("on_3b")
    expected = [int(r[idx]) for r in rows[1:] if r[idx]]
    blanked = [rows[0]] + [r[:idx] + [""] + r[idx + 1 :] for r in rows[1:]]
    bodies = {"2024-06-15": _to_csv(blanked), "2024-06-16": _SEARCH_HEAD.read_text(encoding="utf-8")}
    monkeypatch.setattr(ex, "download", lambda url, params=None, **kw: _Resp(bodies[params["game_date_gt"]]))

    df = ex.mlb_statcast_search("2024-06-15", "2024-06-16", chunk_days=1)
    assert df.height == 92
    assert df.schema["on_3b"] == pl.Int64
    assert df["on_3b"].head(46).null_count() == 46
    got = df["on_3b"].tail(46).drop_nulls().to_list()
    assert got == expected and _exact_ints(got)


def test_search_non_integral_id_warns_once_at_the_caller(monkeypatch):
    """A non-integral id in every chunk warns ONCE per search call, attributed to the caller's line."""
    from sportsdataverse.mlb import mlb_statcast_extra as ex

    rows = _fixture_rows()
    idx = rows[0].index("on_1b")
    target = next(r for r in rows[1:] if r[idx])
    target[idx] = target[idx] + ".5"
    body = _to_csv(rows)
    monkeypatch.setattr(ex, "download", lambda url, params=None, **kw: _Resp(body))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = ex.mlb_statcast_search("2024-06-15", "2024-06-17", chunk_days=1)  # 3 chunks
    hits = [w for w in caught if issubclass(w.category, UserWarning) and "on_1b" in str(w.message)]
    assert len(hits) == 1, [str(w.message) for w in hits]
    assert hits[0].filename == __file__
    assert df.schema["on_1b"] == pl.Float64 and df.schema["on_2b"] == pl.Int64


_MINORS_HEAD = _SEARCH_HEAD.with_name("search_minors_2024-06-01_head.csv")
_WBC_HEAD = _SEARCH_HEAD.with_name("search_wbc_2023-03-11_head.csv")


def test_minors_and_wbc_routes_send_their_population_flags(monkeypatch):
    """Savant's /csv routes share one backend that defaults to MLB; the search UI selects the
    population with ``minors=<bool>&wbc=<bool>``. Without them the MiLB route returns MLB games
    and the WBC route returns spring training (reproduced live 2026-09-17)."""
    from sportsdataverse.mlb import mlb_statcast_extra as ex

    seen: dict = {}

    def fake_download(url, params=None, **kw):
        seen[url] = dict(params)
        return _Resp(_SEARCH_HEAD.read_text(encoding="utf-8"))

    monkeypatch.setattr(ex, "download", fake_download)
    ex.mlb_statcast_search("2024-06-01", "2024-06-01")
    ex.mlb_statcast_search_minors("2024-06-01", "2024-06-01", hfLevel="AAA|")
    ex.mlb_statcast_search_wbc("2023-03-11", "2023-03-11")
    assert (seen[ex._SEARCH_URL_MINORS]["minors"], seen[ex._SEARCH_URL_MINORS]["wbc"]) == ("true", "false")
    assert seen[ex._SEARCH_URL_MINORS]["hfLevel"] == "AAA|"  # user filters still ride along
    assert (seen[ex._SEARCH_URL_WBC]["minors"], seen[ex._SEARCH_URL_WBC]["wbc"]) == ("false", "true")
    assert not {"minors", "wbc"} & seen[ex._SEARCH_URL].keys()  # MLB request unchanged


def test_minors_and_wbc_real_captures_are_their_own_population(monkeypatch):
    """Captures taken WITH the flags: MiLB affiliates (ROC/STP, NOR/GWN) and WBC nations (JPN/CZE,
    game_type F = pool play); the same 119-column shape with MLBAM ids Int64."""
    from sportsdataverse.mlb import mlb_statcast_extra as ex

    bodies = {ex._SEARCH_URL_MINORS: _MINORS_HEAD, ex._SEARCH_URL_WBC: _WBC_HEAD}
    monkeypatch.setattr(ex, "download", lambda url, params=None, **kw: _Resp(bodies[url].read_text(encoding="utf-8")))

    minors = ex.mlb_statcast_search_minors("2024-06-01", "2024-06-01")
    assert minors.shape == (5, 119)
    assert set(minors.select("home_team", "away_team").rows()) == {("NOR", "GWN"), ("ROC", "STP")}
    assert minors["game_pk"].unique().sort().to_list() == [752539, 752692] and minors.schema["game_pk"] == pl.Int64

    wbc = ex.mlb_statcast_search_wbc("2023-03-11", "2023-03-11")
    assert wbc.shape == (5, 119)
    assert set(wbc.select("home_team", "away_team", "game_type").rows()) == {("JPN", "CZE", "F")}
    assert wbc["game_pk"].unique().to_list() == [719529] and wbc.schema["batter"] == pl.Int64


def test_search_header_only_every_chunk_keeps_the_schema(monkeypatch):
    """Every chunk header-only (no games in the window): 0 rows with the 119 columns, not a 0-column frame."""
    from sportsdataverse.mlb import mlb_statcast_extra as ex

    header_only = _SEARCH_HEAD.read_text(encoding="utf-8").splitlines()[0] + "\n"
    monkeypatch.setattr(ex, "download", lambda url, params=None, **kw: _Resp(header_only))

    df = ex.mlb_statcast_search("2024-06-15", "2024-06-16", chunk_days=1)
    assert df.shape == (0, 119) and df.schema["game_pk"] == pl.Int64
    pdf = ex.mlb_statcast_search("2024-06-15", "2024-06-16", chunk_days=1, return_as_pandas=True)
    assert pdf.shape == (0, 119) and str(pdf["game_pk"].dtype) == "Int64"


def test_search_header_only_chunk_does_not_poison_populated_chunk_dtypes(monkeypatch):
    """A header-only chunk (all-String schema) next to a populated one must not widen Float64 to String."""
    from sportsdataverse.mlb import mlb_statcast_extra as ex

    text = _SEARCH_HEAD.read_text(encoding="utf-8")
    bodies = {"2024-06-15": text.splitlines()[0] + "\n", "2024-06-16": text}
    monkeypatch.setattr(ex, "download", lambda url, params=None, **kw: _Resp(bodies[params["game_date_gt"]]))

    df = ex.mlb_statcast_search("2024-06-15", "2024-06-16", chunk_days=1)
    assert df.height == 46
    assert df.schema["release_speed"] == pl.Float64 and df.schema["game_pk"] == pl.Int64


def test_empty_window_result_widens_into_a_populated_one(monkeypatch):
    """A no-games window must not poison a caller's concat.

    The header-only body has no values to infer dtypes from; read as String it would
    silently widen a populated frame's Float64 columns (``diagonal_relaxed``) or make a
    strict concat raise. Null is polars' unknown dtype, so it widens the other way.
    """
    from sportsdataverse.mlb import mlb_statcast_extra as ex

    text = _SEARCH_HEAD.read_text(encoding="utf-8")
    bodies = {"2024-01-15": text.splitlines()[0] + "\n", "2024-06-15": text}
    monkeypatch.setattr(ex, "download", lambda url, params=None, **kw: _Resp(bodies[params["game_date_gt"]]))

    offseason = ex.mlb_statcast_search("2024-01-15", "2024-01-15")
    june = ex.mlb_statcast_search("2024-06-15", "2024-06-15")
    assert offseason.shape == (0, 119) and offseason.schema["release_speed"] == pl.Null
    for how in ("vertical_relaxed", "diagonal_relaxed"):
        merged = pl.concat([offseason, june], how=how)
        assert merged.height == june.height, how
        assert merged.schema["release_speed"] == pl.Float64, (how, merged.schema["release_speed"])
        assert merged.schema["game_pk"] == pl.Int64, how
        assert merged.schema["pitch_type"] == pl.String, how


def test_search_error_status_raises_instead_of_an_empty_window(monkeypatch):
    """download() returns the last response when the retry budget is exhausted on a 403/5xx,
    so the search must read the status: an outage is never reported as "no games"."""
    from sportsdataverse.errors import AssetFetchError
    from sportsdataverse.mlb import mlb_statcast_extra as ex

    class _Err:
        status_code = 503
        text = "<!DOCTYPE html><html><head><title>503 Service Unavailable</title></head></html>"

    monkeypatch.setattr(ex, "download", lambda url, params=None, **kw: _Err())
    with pytest.raises(AssetFetchError, match="503"):
        ex.mlb_statcast_search_minors("2024-06-01", "2024-06-01")


def test_search_error_body_is_not_returned_as_the_documented_schema(monkeypatch):
    """A 200 carrying an error page parses into a 1-column frame named after the error text;
    it must never stand in as the schema-carrying empty result."""
    from sportsdataverse.mlb import mlb_statcast_extra as ex

    monkeypatch.setattr(ex, "download", lambda url, params=None, **kw: _Resp("Error: invalid request\n"))
    df = ex.mlb_statcast_search("2024-06-15", "2024-06-15")
    assert df.shape == (0, 0), df.columns
