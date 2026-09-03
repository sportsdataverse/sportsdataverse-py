"""Generic HTML-table -> polars parsing, shared by every sdv-py HTML-scraping source.

Several sdv-py sources have no API at all -- they serve server-rendered HTML tables:
KenPom (:mod:`sportsdataverse.mbb.kenpom_runtime`), Her Hoop Stats
(:mod:`sportsdataverse.wbb.herhoopstats`), and the NBA reference scrapes in
:mod:`sportsdataverse.nba`. This module is the one table reader they share.

The load-bearing part is header flattening. A grouped two-row ``<thead>`` --
a ``colspan`` group row above the real column row, with each metric followed by an
*unlabelled* rank cell -- is the standard layout on these sites, and
``rvest::html_table()`` mangles it, which is why the R packages carry dozens of
hardcoded ``header_cols`` vectors. ``pandas.read_html`` builds a proper
``MultiIndex`` from that thead instead, so two rules (:func:`_flatten_header` +
:func:`_dedupe_headers`) reproduce the intended names generically -- and a new
upstream column widens the frame rather than shifting every column.

Sports-Reference sites are the documented exception: their cells carry a stable
``data-stat`` attribute that should be read instead of the rendered header. See
``sdv-internal-refs/basketball-reference/README.md``.
"""

from __future__ import annotations

import io
import re
from typing import Any, Dict

import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = ["html_tables"]



_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")
_UNNAMED = re.compile(r"^unnamed[:_]?\s*\d*", re.IGNORECASE)
#: pandas' duplicate-label suffix on a repeated header level ("NetRtg.1").
_DUP_SUFFIX = re.compile(r"_\d+$")


def _clean_name(raw: Any) -> str:
    """snake_case one column label (``"SOS.AdjEM"`` -> ``"sos_adj_em"``).

    Drops pandas' ``Unnamed: N_level_M`` placeholders to ``""`` so
    :func:`_dedupe_headers` can turn them into ``<prev>_rk``.

    Args:
        raw: A column label from ``pandas.read_html``.

    Returns:
        A snake_case name, or ``""`` for a placeholder/blank label.
    """
    text = "" if raw is None else str(raw).strip()
    if not text or _UNNAMED.match(text):
        return ""
    # Symbol-only headers are real labels, not blanks. Spelling them out BEFORE the
    # non-alnum strip is load-bearing: a header that strips to "" is treated by
    # _dedupe_headers as an unlabelled rank cell and renamed <previous>_rk, so a "#"
    # column would silently become a duplicate of the column before it. "+/-" must be
    # handled before the bare "+" rule or it degrades to "plus".
    for symbol, word in (("+/-", " plus_minus "), ("#", " number "), ("%", " pct "), ("+", " plus ")):
        text = text.replace(symbol, word)
    text = _NON_ALNUM.sub("_", text).strip("_")
    return underscore(text).strip("_") if text else ""


def _flatten_header(col: Any) -> str:
    """Flatten one ``pandas.read_html`` column label to a single name.

    A grouped two-row ``<thead>`` reaches us as a tuple per column, e.g.
    ``("AdjO", "AdjO")`` for a value cell and ``("AdjO", "Unnamed: 3_level_1")``
    for the unlabelled rank cell beside it. Two rules make that legible:

    * **A blank LAST level means the column is unlabelled** -- return ``""`` so
      :func:`_dedupe_headers` renames it ``<previous>_rk``. Dropping the blank
      and keeping the group label instead would name the rank column after the
      metric and lose the distinction entirely.
    * **Consecutive duplicate levels collapse** (``("AdjO", "AdjO")`` ->
      ``"adj_o"``, not ``"adj_o_adj_o"``) -- a colspan group label repeated on
      the row below is one name, not two.

    Args:
        col: A column label -- a ``tuple`` for a MultiIndex, else a scalar.

    Returns:
        A snake_case name, or ``""`` for an unlabelled column.

    Example:
        The two KenPom shapes::

            from sportsdataverse._subscription_http import _flatten_header

            _flatten_header(("AdjO", "AdjO"))                   # 'adj_o'
            _flatten_header(("AdjO", "Unnamed: 3_level_1"))     # ''
    """
    if not isinstance(col, tuple):
        return _clean_name(col)
    parts = [_clean_name(p) for p in col]
    if not parts:
        return ""
    # pandas disambiguates a repeated LAST level with a ".N" suffix. When the stem
    # already appears earlier in the tuple, that column is the unlabelled twin of a
    # labelled one -- i.e. a rank cell -- so report it blank and let
    # _dedupe_headers name it <previous>_rk.
    last, stem = parts[-1], _DUP_SUFFIX.sub("", parts[-1])
    if not last or (stem != last and stem in parts[:-1]):
        return ""
    # Dedupe by FIRST occurrence, not just adjacent repeats. A site that repeats its
    # header block inside <thead> (KenPom re-renders its 2-row header every ~40 rows,
    # which pandas reads as 20 header levels) yields ("Strength of Schedule", "NetRtg")
    # ten times over; collapsing only neighbours would emit that pair ten times.
    out: list[str] = []
    for part in parts:
        if part and part not in out:
            out.append(part)
    return "_".join(out)


def _dedupe_headers(names: list[str]) -> list[str]:
    """Resolve KenPom/HHS header collisions into stable column names.

    Both sites render each metric as a value cell followed by an unlabelled (or
    identically-labelled) rank cell. Flattening the two-row ``<thead>`` therefore
    yields blanks and consecutive duplicates. One rule handles both: **a blank
    name, or a repeat of the name before it, becomes** ``<previous>_rk`` -- which
    reproduces the ``AdjO`` / ``AdjO.Rk`` pairing the ~44 hardcoded
    ``header_cols`` vectors in hoopR spell out by hand. Anything still colliding
    after that gets a numeric suffix rather than silently overwriting.

    Args:
        names: Cleaned column names in table order.

    Returns:
        A same-length list of unique, non-empty names.

    Example:
        The rank rule::

            from sportsdataverse._subscription_http import _dedupe_headers

            _dedupe_headers(["team", "adj_o", "", "adj_d", "adj_d"])
            # ['team', 'adj_o', 'adj_o_rk', 'adj_d', 'adj_d_rk']
    """
    out: list[str] = []
    for i, name in enumerate(names):
        candidate = name
        if not candidate or (out and candidate == out[-1]):
            candidate = f"{out[-1]}_rk" if out else f"column_{i}"
        while candidate in out:
            suffix = 2
            while f"{candidate}_{suffix}" in out:
                suffix += 1
            candidate = f"{candidate}_{suffix}"
        out.append(candidate)
    return out


def _table_key(node: Any, index: int, used: set[str]) -> str:
    """Stable dict key for one ``<table>``: its ``id``, else caption, else position."""
    raw = node.get("id") or ""
    if not raw:
        caption = node.find("caption")
        raw = caption.get_text(strip=True) if caption else ""
    key = _clean_name(raw) or f"table_{index}"
    while key in used:
        index += 1
        key = f"{key}_{index}"
    return key


def html_tables(
    html: str,
    *,
    min_rows: int = 1,
    return_as_pandas: bool = False,
) -> Dict[str, Any]:
    """Parse every ``<table>`` on a page into cleaned DataFrames.

    The Python counterpart of wehoop's ``.hhs_tables()`` and of the per-function
    ``rvest::html_element("#ratings-table") |> html_table()`` calls in hoopR --
    but generic: ``pandas.read_html`` builds a proper ``MultiIndex`` from a
    multi-row ``<thead>``, which is flattened and deduped by
    :func:`_dedupe_headers` instead of by a hardcoded per-endpoint header vector.

    Args:
        html: Full page HTML.
        min_rows: Drop tables with fewer than this many data rows (KenPom pages
            carry small nav/legend tables alongside the real one).
        return_as_pandas: Return ``pandas.DataFrame`` values instead of polars.

    Returns:
        ``{table_key: DataFrame}`` -- keyed by the table's HTML ``id`` (e.g.
        ``"ratings_table"``), else its caption, else ``"table_<n>"``. Empty when
        the page has no qualifying table (a logged-out page, typically).

    Example:
        Parse a two-column table::

            from sportsdataverse._subscription_http import html_tables

            frames = html_tables(
                "<table id='t'><tr><th>Team</th><th>AdjO</th></tr>"
                "<tr><td>Duke</td><td>120.1</td></tr></table>"
            )
            list(frames), frames["t"].columns
            # (['t'], ['team', 'adj_o'])
    """
    import pandas as pd
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html or "", "lxml")
    # Strip elements that carry no tabular text before parsing. pandas.read_html
    # concatenates ALL descendant text of a cell, so a media element's fallback
    # copy is glued onto the value -- Her Hoop Stats embeds a name-pronunciation
    # <audio> in the player cell, which turns "Te-Hina Paopao" into
    # "Te-Hina Paopao  This HTML5 audio..." and quietly breaks every name join.
    for junk in soup.find_all(["script", "style", "audio", "video", "noscript", "svg"]):
        junk.decompose()
    out: Dict[str, Any] = {}
    for i, node in enumerate(soup.find_all("table")):
        try:
            frames = pd.read_html(io.StringIO(str(node)), flavor="lxml")
        except ValueError:  # read_html raises when a <table> has no parseable rows
            continue
        if not frames:
            continue
        frame = frames[0]
        if len(frame) < min_rows:
            continue
        flat = [_flatten_header(col) for col in frame.columns]
        frame.columns = _dedupe_headers(flat)
        key = _table_key(node, i, set(out))
        out[key] = frame if return_as_pandas else pl.from_pandas(frame)
    return out
