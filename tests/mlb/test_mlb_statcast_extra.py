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
