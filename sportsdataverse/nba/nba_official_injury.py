"""The official NBA injury report (league PDFs on ``ak-static.cms.nba.com``).

The league publishes every team's pre-game availability -- ``Out`` /
``Doubtful`` / ``Questionable`` / ``Probable`` / ``Available`` plus a reason --
as a PDF on a fixed schedule, and keeps every past report at a predictable URL.
Unlike the current-state feeds (:func:`sportsdataverse.nba.bref.bref_injuries`,
:func:`sportsdataverse.espn_snapshots.espn_injuries_snapshot`) this one has
**history**: reports exist from 2018-12-17 onward.

URL and slot spec (all times US Eastern), from ``mxufc29/nbainjuries`` (MIT,
(c) 2024 mxufc29) and re-verified against the archive on 2026-10-26:

* ``https://ak-static.cms.nba.com/referee/injury/Injury-Report_{YYYY-MM-DD}_{SLOT}.pdf``.
* 2018-12-17 .. 2021-10-14: three reports a day, slots ``01PM`` / ``05PM`` /
  ``08PM`` (each is the HH:30 snapshot; the PDF title says ``05:30 PM``).
* 2021-10-15 .. 2025-12-21: hourly, ``12AM`` .. ``11PM``.
* 2025-12-22 09:00 onward: every 15 minutes, ``05_30PM``.
* A missing slot answers **403** (S3 AccessDenied), never 404, and the folder
  cannot be listed -- slots are found by probing.

The PDF parser is word-position based (``pdfplumber``), not a table extractor:
2025-26+ reports have no vertical rules, so ``extract_table()`` returns only the
header there. Column bounds come from the header line repeated on every page;
a line carrying a player name, a status or ``NOT YET SUBMITTED`` is a row, and
every other line is a wrapped fragment. Reason cells are vertically centred, so
a row with an empty reason owns the fragment just above AND just below it; a
fragment before the first row of a page continues the previous page's last row.
Game, matchup and team carry forward across rows and pages. ``x_tolerance=1.5``
keeps the 2025-26 word spacing (the default 3 glues ``Clarke,Brandon``).

``pdfplumber`` is an optional dependency (``pip install sportsdataverse[pdf]``),
imported lazily.
"""

from __future__ import annotations

import datetime as dt
import io
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import polars as pl

from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

if TYPE_CHECKING:  # pragma: no cover -- annotation-only import
    import pandas as pd

__all__ = [
    "nba_injury_report",
    "nba_injury_report_slots",
    "nba_injury_report_url",
    "parse_nba_injury_report",
]

_BASE = "https://ak-static.cms.nba.com/referee/injury/Injury-Report_{date}_{slot}.pdf"
_FIRST_DATE = dt.date(2018, 12, 17)
_HOURLY_FROM = dt.date(2021, 10, 15)
_QUARTER_HOURLY_FROM = dt.datetime(2025, 12, 22, 9, 0)
_NYS = "NOT YET SUBMITTED"
# the only 403 that means "slot not published"; any other 403 is a block, not an answer
_S3_MISSING = b"<Code>AccessDenied</Code>"
# header label -> output column; a header cell starts at one of these words
_COLUMNS = {
    "Game Date": "game_date",
    "Game Time": "game_time",
    "Matchup": "matchup",
    "Team": "team",
    "Player Name": "player_name",
    "Category": "category",
    "Current Status": "current_status",
    "Previous Status": "previous_status",
    "Reason": "reason",
}
_LABEL_STARTS = {label.split()[0] for label in _COLUMNS}
_CARRY = ("game_date", "game_time", "matchup", "team")

_SCHEMA = {
    "report_datetime": pl.Datetime("us"),
    "game_date": pl.Date,
    "game_time": pl.Utf8,
    "matchup": pl.Utf8,
    "away_team_abbr": pl.Utf8,
    "home_team_abbr": pl.Utf8,
    "team": pl.Utf8,
    "player_name": pl.Utf8,
    "current_status": pl.Utf8,
    "reason": pl.Utf8,
    "category": pl.Utf8,
    "previous_status": pl.Utf8,
}


def nba_injury_report_url(report_time: dt.datetime) -> str:
    """The archive URL of the report published at ``report_time`` (US Eastern).

    Args:
        report_time: The slot, naive and in US Eastern time. Before
            2025-12-22 09:00 only the hour is used (``05PM``); after it the slot
            is quarter-hourly (``05_30PM``).

    Returns:
        The PDF URL. Whether that slot exists is only known by fetching it.

    Example:
        Quick start::

            import datetime as dt
            from sportsdataverse.nba.nba_official_injury import nba_injury_report_url

            nba_injury_report_url(dt.datetime(2026, 1, 15, 17, 30))
            # '.../Injury-Report_2026-01-15_05_30PM.pdf'
    """
    fmt = "%I_%M%p" if report_time >= _QUARTER_HOURLY_FROM else "%I%p"
    return _BASE.format(date=report_time.date().isoformat(), slot=report_time.strftime(fmt))


def nba_injury_report_slots(date: dt.date) -> List[dt.datetime]:
    """Every slot the league's schedule allows on ``date``, earliest first.

    Args:
        date: The report date (US Eastern).

    Returns:
        Naive US Eastern datetimes: three a day (13:00 / 17:00 / 20:00) before
        2021-10-15, hourly until 2025-12-22 09:00, every 15 minutes after.
        Empty before the archive's first report (2018-12-17). Not every slot
        is published (off days, the All-Star break), so a slot is a candidate.

    Example:
        Quick start::

            import datetime as dt
            from sportsdataverse.nba.nba_official_injury import nba_injury_report_slots

            len(nba_injury_report_slots(dt.date(2026, 1, 15)))  # 96
    """
    if date < _FIRST_DATE:
        return []
    midnight = dt.datetime.combine(date, dt.time())
    if date < _HOURLY_FROM:
        return [midnight.replace(hour=h) for h in (13, 17, 20)]
    hourly = [midnight.replace(hour=h) for h in range(24)]
    quarters = [midnight + dt.timedelta(minutes=15 * i) for i in range(96)]
    return [t for t in hourly if t < _QUARTER_HOURLY_FROM] + [t for t in quarters if t >= _QUARTER_HOURLY_FROM]


def _lines(words: List[Dict[str, Any]], tol: float = 2.0) -> List[List[Dict[str, Any]]]:
    out: List[List[Dict[str, Any]]] = []
    tops: List[float] = []
    for w in sorted(words, key=lambda w: (w["top"], w["x0"])):
        if out and abs(tops[-1] - w["top"]) <= tol:
            out[-1].append(w)
        else:
            out.append([w])
            tops.append(w["top"])
    return out


def _header(words: List[Dict[str, Any]]) -> List[tuple]:
    cols: List[list] = []
    for w in sorted(words, key=lambda w: w["x0"]):
        if w["text"] in _LABEL_STARTS or not cols:
            cols.append([w["text"], w["x0"]])
        else:
            cols[-1][0] += " " + w["text"]
    return [(_COLUMNS[name], x0 - 3) for name, x0 in cols if name in _COLUMNS]


def _cells(words: List[Dict[str, Any]], cols: List[tuple]) -> Dict[str, str]:
    cells: Dict[str, List[str]] = {}
    for w in sorted(words, key=lambda w: w["x0"]):
        name = [c for c, x in cols if w["x0"] >= x]
        cells.setdefault(name[-1] if name else cols[0][0], []).append(w["text"])
    return {k: " ".join(v) for k, v in cells.items()}


def _report_time(text: str) -> Optional[dt.datetime]:
    m = re.search(r"Injury Report:\s*(\d{2}/\d{2}/\d{2})\s+(\d{1,2}:\d{2}\s*[AP]M)", text)
    return dt.datetime.strptime(f"{m.group(1)} {m.group(2)}", "%m/%d/%y %I:%M %p") if m else None


def _parse(pdf: Any) -> tuple:
    rows: List[Dict[str, Any]] = []
    cols: List[tuple] = []
    report_time = None
    for page in pdf.pages:
        anchors: List[Dict[str, Any]] = []
        frags: List[tuple] = []
        for words in _lines(page.extract_words(x_tolerance=1.5)):
            top = words[0]["top"]
            text = " ".join(w["text"] for w in words)
            if text.startswith("Injury Report:"):
                report_time = report_time or _report_time(text)
                continue
            if re.fullmatch(r"Page \d+ of \d+", text):
                continue
            if "Matchup" in text and "Reason" in text:
                cols = _header(words)  # repeated on every page
                continue
            if not cols:
                continue
            cells = _cells(words, cols)
            if _NYS in text:
                cells = {k: v.replace(_NYS, "").strip() for k, v in cells.items()}
                cells = {k: v for k, v in cells.items() if v}
                cells["reason"] = _NYS
            if cells.get("reason") == _NYS or "player_name" in cells or "current_status" in cells:
                anchors.append({"top": top, "cells": cells, "frags": []})
            else:
                frags.append((top, cells))
        # a Team / Matchup / Game Time fragment belongs to the NEAREST row: a wrap of
        # that row's label when it has one, else the label itself (2018-20 PDFs print
        # a group's label ~4pt above its first player line)
        for top, cells in frags:
            for c in _CARRY:
                if c in cells and anchors:
                    owner = min(anchors, key=lambda a: abs(a["top"] - top))
                    text = cells.pop(c)
                    have = owner["cells"].get(c)
                    owner["cells"][c] = (
                        text if not have else (f"{have} {text}" if top > owner["top"] else f"{text} {have}")
                    )
        seq = sorted(
            [(a["top"], 0, a) for a in anchors]
            + [(t, 1, {"top": t, "text": c["reason"], "owner": None}) for t, c in frags if "reason" in c],
            key=lambda x: (x[0], x[1]),
        )
        # a reason-less row owns the fragment just above and just below it
        for i, (_, kind, a) in enumerate(seq):
            if kind == 0 and not a["cells"].get("reason"):
                for j in (i - 1, i + 1):
                    if 0 <= j < len(seq) and seq[j][1] == 1 and seq[j][2]["owner"] is None:
                        seq[j][2]["owner"] = a
        for i, (top, kind, f) in enumerate(seq):
            if kind == 1 and f["owner"] is None:
                if not any(k == 0 for _, k, _ in seq[:i]) and rows:
                    f["owner"], f["top"] = rows[-1], float("inf")  # continues the previous page
                elif anchors:
                    f["owner"] = min(anchors, key=lambda a: abs(a["top"] - top))
        for _, kind, f in seq:
            if kind == 1 and f["owner"] is not None:
                f["owner"]["frags"].append((f["top"], f["text"]))
        rows.extend(anchors)
    return rows, report_time


def _finish(rows: List[Dict[str, Any]], report_time: Optional[dt.datetime]) -> pl.DataFrame:
    out: List[Dict[str, Any]] = []
    last: Dict[str, str] = {}
    for r in rows:
        c = r["cells"]
        if "matchup" in c and "team" not in c:
            last.pop("team", None)  # a new game restarts the team
        for k in _CARRY:
            if k in c:
                last[k] = c[k]
            else:
                c[k] = last.get(k)
        parts = sorted(r["frags"] + ([(r["top"], c["reason"])] if c.get("reason") else []))
        c["reason"] = " ".join(t for _, t in parts) or None
        out.append({col: c.get(col) for col in _SCHEMA if col not in ("report_datetime",)})
    if not out:
        return pl.DataFrame(schema=_SCHEMA)
    df = pl.DataFrame(out, schema={k: pl.Utf8 for k in _SCHEMA if k != "report_datetime"})
    return (
        df.with_columns(
            pl.lit(report_time, dtype=pl.Datetime("us")).alias("report_datetime"),
            pl.col("game_date").str.strip_chars().str.to_date("%m/%d/%Y", strict=False),
            pl.col("matchup").str.extract(r"^(\w+)@", 1).alias("away_team_abbr"),
            pl.col("matchup").str.extract(r"@(\w+)$", 1).alias("home_team_abbr"),
        )
        .select(list(_SCHEMA))
        .cast(_SCHEMA)
    )


def _out(df: pl.DataFrame, return_as_pandas: bool) -> pl.DataFrame | pd.DataFrame:
    return df.to_pandas() if return_as_pandas else df


def parse_nba_injury_report(
    pdf: Union[bytes, str, Path], *, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Parse one official NBA injury report PDF into rows.

    Args:
        pdf: The PDF as bytes, or a path to it.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per listed player, plus one row per team whose report was
        ``NOT YET SUBMITTED`` (``player_name`` and ``current_status`` null,
        ``reason`` ``"NOT YET SUBMITTED"``). Columns: ``report_datetime`` (the
        PDF title's US Eastern time), ``game_date``, ``game_time`` (as printed,
        e.g. ``"07:30 (ET)"`` -- no AM/PM), ``matchup`` (``"WAS@SAC"``),
        ``away_team_abbr``, ``home_team_abbr``, ``team`` (full name),
        ``player_name`` (``"Last, First"``; no player id), ``current_status``,
        ``reason``, and the 2018-19 era's ``category`` / ``previous_status``
        (null afterwards). A report with no rows gives a zero-row frame.

    Raises:
        ImportError: ``pdfplumber`` is not installed.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_official_injury import parse_nba_injury_report

            df = parse_nba_injury_report("Injury-Report_2026-01-15_05_30PM.pdf")
            df.filter(pl.col("current_status") == "Out").head()

        See Also:
            * :func:`nba_injury_report` -- fetch a slot and parse it.
            * `Parser guide`_ -- this parser's bytes / path input contract.

        .. _Parser guide: https://py.sportsdataverse.org/docs/parsers/#parse_nba_injury_report
    """
    try:
        import pdfplumber
    except ImportError as exc:  # pragma: no cover -- exercised only without the extra
        raise ImportError("parse_nba_injury_report needs pdfplumber: pip install sportsdataverse[pdf]") from exc
    source = io.BytesIO(pdf) if isinstance(pdf, bytes) else pdf
    with pdfplumber.open(source) as doc:
        rows, report_time = _parse(doc)
    return _out(_finish(rows, report_time), return_as_pandas)


def nba_injury_report(
    date: Optional[dt.date] = None,
    report_time: Optional[dt.datetime] = None,
    *,
    return_as_pandas: bool = False,
    proxy: Any = None,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame:
    """Fetch and parse an official NBA injury report.

    Args:
        date: Report date (US Eastern). Defaults to today. With no
            ``report_time``, the LATEST published slot of that date is used
            (probing backwards through :func:`nba_injury_report_slots`; for
            today, only slots up to now).
        report_time: An exact slot (naive US Eastern), e.g.
            ``dt.datetime(2026, 1, 15, 17, 30)``. Overrides ``date``.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.
        proxy: Proxy configuration in the ``requests`` ``proxies=`` shape.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The parsed report (see :func:`parse_nba_injury_report`).

    Raises:
        NoDataError: The slot (or every slot of the date) is not published
            (S3's 403 ``AccessDenied``).
        AssetFetchError: Any other non-200 answer -- including a 403 that is not
            S3's ``AccessDenied`` (a CDN / WAF block) -- or an HTTP-200 body that
            is not a PDF (e.g. a CDN error page).
        ImportError: ``pdfplumber`` is not installed.

    Example:
        Quick start::

            import datetime as dt
            from sportsdataverse.nba.nba_official_injury import nba_injury_report

            df = nba_injury_report(report_time=dt.datetime(2026, 1, 15, 17, 30))
            print(df.shape)

        Latest report today::

            df = nba_injury_report()

        Pandas output::

            df_pd = nba_injury_report(dt.date(2024, 1, 15), return_as_pandas=True)

        See Also:
            * :func:`sportsdataverse.nba.bref.bref_injuries` -- current list, no history.
            * :func:`sportsdataverse.espn_snapshots.espn_injuries_snapshot` -- ESPN, current only.
            * `Parser guide`_ -- ``parse_nba_injury_report``, for a PDF you already have.

        .. _Parser guide: https://py.sportsdataverse.org/docs/parsers/#parse_nba_injury_report
    """
    if report_time is not None:
        slots = [report_time]
    else:
        day = date or _today_eastern()
        slots = nba_injury_report_slots(day)[::-1]
        if day == _today_eastern():
            now = _now_eastern()
            slots = [s for s in slots if s <= now]
    for slot in slots:
        url = nba_injury_report_url(slot)
        # 403 = slot not published (S3 AccessDenied); never retry it
        resp = download(url, proxy=proxy, retry_statuses=(408, 429, 500, 502, 503, 504), **kwargs)
        if resp.status_code == 200:
            if not resp.content.startswith(b"%PDF"):
                raise AssetFetchError(f"{url} answered HTTP 200 without a PDF body")
            return parse_nba_injury_report(resp.content, return_as_pandas=return_as_pandas)
        if resp.status_code != 403 or _S3_MISSING not in (resp.content or b""):
            raise AssetFetchError(f"{url} answered HTTP {resp.status_code}")
    raise NoDataError(f"no NBA injury report published for {report_time or date or 'today'}")


def _now_eastern() -> dt.datetime:
    from zoneinfo import ZoneInfo

    return dt.datetime.now(ZoneInfo("America/New_York")).replace(tzinfo=None)


def _today_eastern() -> dt.date:
    return _now_eastern().date()
