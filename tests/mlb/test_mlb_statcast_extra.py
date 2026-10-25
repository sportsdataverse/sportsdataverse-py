"""Savant search chunks that are not a Statcast CSV are failed fetches (B-M7, B-L79, task 4.4).

``_search_core`` kept any chunk with rows and dropped any header-only one, so a 200 HTML page
became a junk 1x1 chunk concatenated into the result and a 200 error body that parsed
header-only was read as "no games that week". No Savant outage body has been captured
(``audit/live-checks/network.md`` gap-B-184: Savant answered normally at run time), so the
bodies here are real Savant answers from other routes: the player page (HTML) and the
header line of the expected-stats leaderboard CSV (a header-only CSV that is not the search's).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
import requests

from sportsdataverse.errors import AssetFetchError
from sportsdataverse.mlb import mlb_statcast_extra as ex

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "mlb_statcast"
_SEARCH = (_FIX / "search_2024-06-15_head.csv").read_text(encoding="utf-8")
_HTML = (_FIX / "player_page.html").read_text(encoding="utf-8")
_OTHER_CSV_HEADER = (_FIX / "leaderboard_xstats.csv").read_text(encoding="utf-8").splitlines()[0] + "\n"


class _Resp:
    status_code = 200

    def __init__(self, text: str) -> None:
        self.text = text


def _serve_by_window(monkeypatch, bodies: dict) -> None:
    monkeypatch.setattr(ex, "download", lambda url, params=None, **kw: _Resp(bodies[params["game_date_gt"]]))


def test_html_chunk_raises_instead_of_a_junk_row(monkeypatch) -> None:
    _serve_by_window(monkeypatch, {"2024-06-15": _SEARCH, "2024-06-16": _HTML})
    with pytest.raises(AssetFetchError, match=r"2024-06-16\.\.2024-06-16\) answered HTTP 200 with a body that is not"):
        ex.mlb_statcast_search("2024-06-15", "2024-06-16", chunk_days=1)


def test_error_chunk_raises_instead_of_no_games(monkeypatch) -> None:
    assert "game_pk" not in _OTHER_CSV_HEADER
    _serve_by_window(monkeypatch, {"2024-06-15": _SEARCH, "2024-06-16": _OTHER_CSV_HEADER})
    with pytest.raises(AssetFetchError, match="not a Statcast CSV"):
        ex.mlb_statcast_search("2024-06-15", "2024-06-16", chunk_days=1)


def test_untokenizable_page_raises_instead_of_no_games(monkeypatch) -> None:
    # R4.4-1: pandas cannot tokenize this real 200 HTML page at all, so it parses to a 0x0 frame;
    # judged on the parse it read as "no games" and the window vanished from a 46-row result.
    page = (_FIX.parent / "herhoopstats" / "team_single_seasons_2025_logged_out.html").read_text(encoding="utf-8")
    assert ex._csv_to_frame(page).shape == (0, 0)
    _serve_by_window(monkeypatch, {"2024-06-15": _SEARCH, "2024-06-16": page})
    with pytest.raises(AssetFetchError, match=r"2024-06-16\.\.2024-06-16\) answered HTTP 200 with a body that is not"):
        ex.mlb_statcast_search("2024-06-15", "2024-06-16", chunk_days=1)


def test_text_under_a_game_pk_header_raises(monkeypatch) -> None:
    # CodeRabbit on #919: this body parses to one row with a `game_pk` column, so the column check
    # alone passed it and the error text came back as Statcast data.
    _serve_by_window(monkeypatch, {"2024-06-15": _SEARCH, "2024-06-16": "game_pk\nAccess denied"})
    with pytest.raises(AssetFetchError, match="not a Statcast CSV: game_pk Access denied"):
        ex.mlb_statcast_search("2024-06-15", "2024-06-16", chunk_days=1)


def test_header_only_search_answer_is_still_an_empty_window(monkeypatch) -> None:
    header_only = _SEARCH.splitlines()[0] + "\n"  # the real 119-column header, no rows
    _serve_by_window(monkeypatch, {"2024-06-15": _SEARCH, "2024-06-16": header_only})
    assert ex.mlb_statcast_search("2024-06-15", "2024-06-16", chunk_days=1).height == 46


def test_empty_body_is_left_to_the_empty_path(monkeypatch) -> None:
    # Unverified what Savant sends for a window with no data; an empty body keeps its old reading.
    _serve_by_window(monkeypatch, {"2024-06-15": _SEARCH, "2024-06-16": ""})
    df = ex.mlb_statcast_search("2024-06-15", "2024-06-16", chunk_days=1)
    assert df.height == 46


def test_dropped_connection_is_a_failed_fetch(monkeypatch) -> None:
    def refuse(url, params=None, **kw):
        raise requests.ConnectionError("connection reset")

    monkeypatch.setattr(ex, "download", refuse)
    with pytest.raises(AssetFetchError):
        ex.mlb_statcast_search("2024-06-15", "2024-06-15")


def _blank_one_cell(csv: str, column: str) -> str:
    """The real capture with ``column`` emptied on its first row: a pitch Savant ships without that value."""
    import io

    import pandas as pd

    frame = pd.read_csv(io.StringIO(csv), dtype=str, keep_default_na=False)
    frame.loc[0, column] = ""
    return frame.to_csv(index=False)


def test_documented_dtypes_do_not_depend_on_nulls(monkeypatch) -> None:
    """B-L83: a column's dtype is the returns table's whether or not a chunk has a missing value."""
    from tests.conftest import documented_schema

    gaps = _blank_one_cell(_blank_one_cell(_SEARCH, "zone"), "release_spin_rate")
    header_only = _SEARCH.splitlines()[0] + "\n"
    _serve_by_window(monkeypatch, {"2024-06-15": _SEARCH, "2024-06-16": gaps, "2024-06-17": header_only})
    full, holes, empty = (ex.mlb_statcast_search(day, day) for day in ("2024-06-15", "2024-06-16", "2024-06-17"))
    want = documented_schema("autodoc/mlb/mlb_statcast_search")
    assert full.schema == holes.schema
    assert {c: full.schema[c] for c in ("zone", "release_spin_rate", "spin_axis")} == {
        c: want[c] for c in ("zone", "release_spin_rate", "spin_axis")
    }
    assert holes["zone"].null_count() == 1 and full["zone"].to_list()[1:] == holes["zone"].to_list()[1:]
    assert empty.height == 0 and dict(empty.schema) == want


def _set_cell(csv: str, column: str, value: str) -> str:
    import io

    import pandas as pd

    frame = pd.read_csv(io.StringIO(csv), dtype=str, keep_default_na=False)
    frame.loc[0, column] = value
    return frame.to_csv(index=False)


def test_whole_numbers_read_as_float_are_pinned_back_and_fractions_are_not(monkeypatch) -> None:
    """B-L83 / R979-2: a missing value makes pandas read an integer column float64; the pin restores Int64 with
    the values unchanged. A fraction in a documented-integer column is not truncated: that column stays Float64."""
    full_text = _SEARCH
    gap = _blank_one_cell(full_text, "balls")
    frac = _set_cell(full_text, "balls", "1.5")
    _serve_by_window(monkeypatch, {"2024-06-15": full_text, "2024-06-16": gap, "2024-06-17": frac})
    full, holes, odd = (ex.mlb_statcast_search(day, day) for day in ("2024-06-15", "2024-06-16", "2024-06-17"))
    assert full.schema["balls"] == holes.schema["balls"] == pl.Int64
    assert holes["balls"][0] is None and holes["balls"].to_list()[1:] == full["balls"].to_list()[1:]
    assert odd.schema["balls"] == pl.Float64 and odd["balls"][0] == 1.5


@pytest.mark.parametrize(
    ("fn", "stem", "day", "table"),
    [
        ("mlb_statcast_search_minors", "search_minors_2024-06-01_head", "2024-06-01", "mlb_statcast_search_minors"),
        ("mlb_statcast_search_wbc", "search_wbc_2023-03-11_head", "2023-03-11", "mlb_statcast_search_wbc"),
    ],
)
def test_minors_and_wbc_chunks_take_their_own_table(monkeypatch, fn, stem, day, table) -> None:
    """R979-2: each route's chunk is pinned to its own returns table (the WBC capture's all-null fielding
    alignment reads Float64 from pandas; its table says String)."""
    from tests.conftest import documented_schema

    text = (_FIX / f"{stem}.csv").read_text(encoding="utf-8")
    _serve_by_window(monkeypatch, {day: _blank_one_cell(text, "zone")})
    df = getattr(ex, fn)(day, day)
    want = documented_schema(f"autodoc/mlb/{table}")
    assert df.schema["zone"] == want["zone"] == pl.Int64 and df["zone"][0] is None
    assert df.schema["if_fielding_alignment"] == want["if_fielding_alignment"]
