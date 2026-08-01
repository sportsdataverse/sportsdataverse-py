"""stats.ncaa.org scoreboard discovery — bigballR ``get_date_games`` port.

Faithful polars port of bigballR ``get_date_games`` (``all_functions.R:1119-1427``)
and its wbigballR sibling (``all_functions.R:1100-1328``) — the daily-scoreboard
scraper that discovers every game (and its ``/contests/{id}`` game id) played on
a given date. Per ``dev/bigballr_port/spec_wbigballr_divergence.md`` §2.2 the two
R implementations differ only in transport (gone here — one fetch layer) and the
league season-id table (a data knob), so this module is one shared core
(:func:`parse_ncaa_bb_scoreboard` + :func:`_ncaa_bb_date_games`) with the MBB
public binding; the WBB family binds the same core with
:data:`NCAA_WBB_SEASON_DIVISIONS`.

**Table readout semantics (the one substantive porting decision).** The
scoreboard table ships 5 rows per game (R: ``starting_rows <- (1:(nrow/5))*5-4``)
where the two middle rows are a NESTED line-score table (per-half / per-quarter
points). The two R packages read that layout through different HTML-table
engines and disagree:

* wbigballR (and bigballR's non-chromote path) uses ``XML::readHTMLTable``,
  whose per-row cells are the ``<tr>``'s DIRECT ``<td>`` children — the nested
  line-score table collapses into one cell, so V5/V6/V7 of the block's first
  row are the real final away score / neutral-site cell / attendance.
* bigballR's chromote path (``rvest::html_table`` on an ``xml_document``)
  expands the nested table's cells into the parent row, silently shifting
  V5/V6/V7 onto period scores (away score becomes the away FIRST-HALF score,
  attendance becomes the home first-half score, neutral-site is always truthy).

This port implements the **direct-cell readout** — the layout the V1..V7 field
positions were designed against, and the one that yields correct data. The MBB
oracle fixture was captured through the chromote path, so its ``Away_Score`` /
``Neutral_Site`` / ``Attendance`` columns are wrong-by-construction and the MBB
parity test skips exactly those three (see
``tests/mbb/test_mbb_ncaa_scoreboard_parity.py``); the WBB oracle
(``readHTMLTable`` path) matches this port exactly on all 14 columns.

Deliberate divergences from R (each documented at its site):

* Unknown season raises :class:`ValueError` instead of returning the STRING
  ``"Season Not Available"`` (R's type-unstable return, M:1210-1212).
* Conference-name normalization strips ALL non-alphanumeric characters;
  R's ``sub("[^[:alnum:]=\\.]", "", conference)`` (M:1216-1217) uses ``sub``
  not ``gsub`` and therefore strips only the FIRST such character — a bug
  (e.g. ``"Big Ten "`` normalizes to ``"bigten "`` and misses the table).
* A game-id count mismatch against the assignable-game mask raises
  :class:`ValueError` instead of R's silent vector recycling (M:1367).
* R's lookaround regexes (``(?<=/contests/)\\d+(?=/box_score)`` etc.) are
  rewritten as capture groups — Rust/polars regex has no lookaround, and the
  plain-``re`` rewrites keep the module engine-agnostic.

Example:
    Parse a saved scoreboard page offline::

        from sportsdataverse.mbb.mbb_ncaa_scoreboard import parse_ncaa_bb_scoreboard
        html = open("scoreboard.html", encoding="utf-8").read()
        df = parse_ncaa_bb_scoreboard(html, "11/11/2025")
        print(df.shape)

    Live discovery for yesterday's MBB slate (browser transport)::

        from sportsdataverse.mbb.mbb_ncaa_scoreboard import ncaa_mbb_date_games
        games = ncaa_mbb_date_games()
        game_ids = games["game_id"].drop_nulls().to_list()

See Also:
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R men's basketball companion package
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- R women's basketball companion package
"""

from __future__ import annotations

import re
import warnings
from datetime import date as date_cls
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Literal, Optional, Union, overload

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_html import jsoup_text, parse_html

if TYPE_CHECKING:
    import pandas as pd

    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

__all__ = [
    "NCAA_MBB_SEASON_DIVISIONS",
    "NCAA_WBB_SEASON_DIVISIONS",
    "NCAA_BB_CONFERENCE_IDS",
    "SCOREBOARD_SCHEMA",
    "parse_ncaa_bb_scoreboard",
    "ncaa_mbb_date_games",
]

#: MBB ``season_divisions`` ids by season label (bigballR all_functions.R:1132-1183).
#: Boundary semantics: a date belongs to season ``YYYY-(YY+1)`` iff it is
#: strictly after May 1 of YYYY and on/before May 1 of YYYY+1. bigballR's
#: commented-out 2001-02..2008-09 rows are excluded (no box scores upstream).
NCAA_MBB_SEASON_DIVISIONS: "dict[str, int]" = {
    "2009-10": 10060,
    "2010-11": 10220,
    "2011-12": 10480,
    "2012-13": 10883,
    "2013-14": 11700,
    "2014-15": 12320,
    "2015-16": 12700,
    "2016-17": 13100,
    "2017-18": 13533,
    "2018-19": 16700,
    "2019-20": 17060,
    "2020-21": 17420,
    "2021-22": 17783,
    "2022-23": 17940,
    "2023-24": 18221,
    "2024-25": 18403,
    "2025-26": 18703,
}

#: WBB ``season_divisions`` ids by season label (wbigballR all_functions.R:1106-1147).
#: NOTE: the women's table has NO 2009-10 entry (wbigballR is an older fork;
#: extend forward as new ids are confirmed). 2025-26 was likewise missing
#: upstream and was discovered + backfilled here on 2026-08-01.
NCAA_WBB_SEASON_DIVISIONS: "dict[str, int]" = {
    "2010-11": 10200,
    "2011-12": 10520,
    "2012-13": 10882,
    "2013-14": 11660,
    "2014-15": 12321,
    "2015-16": 12701,
    "2016-17": 13120,
    "2017-18": 14100,
    "2018-19": 16720,
    "2019-20": 17001,
    "2020-21": 17440,
    "2021-22": 17763,
    "2022-23": 17941,
    "2023-24": 18220,
    "2024-25": 18423,
    # Discovered 2026-08-01 by probing candidate ids around the men's 18703 and
    # confirming sport_code=WBB on a team page from the division (the ids carry
    # no MBB<->WBB offset: observed deltas run -59..+567).
    "2025-26": 18704,
}

#: Conference name -> stats.ncaa.org conference id (bigballR
#: all_functions.R:1218-1253; verified identical in wbigballR — conference ids
#: are cross-sport on stats.ncaa.org). Keys are the fully-normalized form
#: produced by :func:`_resolve_conference_id` (lowercased, all non-alphanumeric
#: stripped) — a deliberate fix of R's single-char ``sub`` normalization bug
#: (see module docstring). ``big10`` is R's explicit alias for ``bigten``.
NCAA_BB_CONFERENCE_IDS: "dict[str, int]" = {
    "aac": 823,
    "acc": 821,
    "asun": 920,
    "americaneast": 845,
    "atlantic10": 820,
    "big12": 25354,
    "bigeast": 30184,
    "bigsky": 825,
    "bigsouth": 826,
    "bigten": 827,
    "big10": 827,
    "bigwest": 904,
    "cusa": 24312,
    "caa": 837,
    "horizon": 881,
    "ivy": 865,
    "maac": 871,
    "mac": 875,
    "meac": 876,
    "mvc": 884,
    "mwc": 5486,
    "nec": 846,
    "ovc": 902,
    "pac12": 905,
    "patriot": 838,
    "sec": 911,
    "swac": 916,
    "socon": 912,
    "southland": 914,
    "summit": 819,
    "sunbelt": 818,
    "wac": 923,
    "wcc": 922,
    "all": 0,
}

#: Output contract (snake_case of R's ``Date, Start_Time, Home, Away, BoxID,
#: GameID, Home_Score, Away_Score, Attendance, Neutral_Site, Home_Wins,
#: Home_Losses, Away_Wins, Away_Losses``; M:1402-1420). Scores/ids/attendance
#: stay Utf8 — R emits them as character, and score cells legitimately hold
#: ``"Canceled"`` / ``"Ppd"`` for unplayed games.
SCOREBOARD_SCHEMA: "dict[str, pl.DataType]" = {
    "date": pl.Utf8,
    "start_time": pl.Utf8,
    "home": pl.Utf8,
    "away": pl.Utf8,
    "box_id": pl.Utf8,
    "game_id": pl.Utf8,
    "home_score": pl.Utf8,
    "away_score": pl.Utf8,
    "attendance": pl.Utf8,
    "neutral_site": pl.Boolean,
    "home_wins": pl.Int64,
    "home_losses": pl.Int64,
    "away_wins": pl.Int64,
    "away_losses": pl.Int64,
}

# R: str_extract_all(html, "(?<=/contests/)\\d+(?=/box_score)") (M:1362) —
# lookaround rewritten as a capture group.
_GAME_ID_RE = re.compile(r"/contests/(\d+)/box_score")
# R: gsub(" \\([0-9].+", "", team) (M:1385) — strip the "(W-L)" record tail.
_RECORD_STRIP_RE = re.compile(r" \([0-9].+")
# R: gsub('\\#[0-9]{1,2} ', "", team) (M:1386) — strip an "#NN " rank prefix.
_RANK_STRIP_RE = re.compile(r"#[0-9]{1,2} ")
# R: "(?<=[(])\\d+(?=-)" / "(?<=-)\\d+(?=[)])" (M:1387-1394) — capture rewrites.
_WINS_RE = re.compile(r"\((\d+)-")
_LOSSES_RE = re.compile(r"-(\d+)\)")
# Deliberate fix of R's sub() (first-char-only) normalization (M:1216-1217):
# strip EVERY character outside R's keep-class [[:alnum:]=.].
_CONF_NORM_RE = re.compile(r"[^0-9A-Za-z=.]")


def _empty_scoreboard() -> pl.DataFrame:
    """Zero-row frame carrying the documented scoreboard schema."""
    return pl.DataFrame(schema=SCOREBOARD_SCHEMA)


def _clean_team(raw: Optional[str]) -> Optional[str]:
    """Strip the record tail then the rank prefix (M:1385-1386)."""
    if raw is None:
        return None
    return _RANK_STRIP_RE.sub("", _RECORD_STRIP_RE.sub("", raw))


def _extract_int(pattern: "re.Pattern[str]", raw: Optional[str]) -> Optional[int]:
    """First capture of *pattern* in *raw* as int, else None (R: NA)."""
    if raw is None:
        return None
    m = pattern.search(raw)
    return int(m.group(1)) if m else None


def parse_ncaa_bb_scoreboard(
    html: str,
    date: str,
    *,
    season_id: Optional[int] = None,
) -> pl.DataFrame:
    """Parse a stats.ncaa.org scoreboard page into one row per game.

    Pure core shared by the MBB and WBB date-games scrapers — the transform
    half of bigballR ``get_date_games`` (all_functions.R:1336-1426), applied
    to an already-fetched ``/season_divisions/{sid}/scoreboards`` page.

    The first ``<table>`` on the page lays out each game as a 5-row block
    (R: ``starting_rows <- (1:(nrow(table)/5))*5 - 4``):
    block row 1 = date/time, away team, nested line-score table, final away
    score, neutral-site cell, attendance (direct cells V1/V3/V5/V6/V7);
    rows 2-3 = the nested line-score table's own rows; row 4 = home team +
    final home score (V2/V3); row 5 = the "Box Score" link cell (V1). Cells
    are the row's DIRECT ``<td>`` children — see the module docstring for why
    this (and not rvest's nested-cell expansion) is the correct readout.

    Game ids come from ``/contests/{id}/box_score`` hrefs in the raw HTML and
    are assigned, in document order, to the games that are neither
    ``"Canceled"`` nor ``"Ppd"`` and have a box-score link (M:1361-1367).

    Args:
        html: Raw scoreboard-page HTML.
        date: The requested date, ``"MM/DD/YYYY"`` (echoed into the output;
            R derives ``Date``/``Start_Time`` from the page's own date cell,
            and so does this port — *date* is only used for messages upstream).
        season_id: When given, drop any extracted game id equal to it
            (R: ``game_ids[which(!game_ids %in% seasonid)]``, M:1363 — a
            defensive filter against the season id leaking into the link set).

    Returns:
        One row per game with the 14-column :data:`SCOREBOARD_SCHEMA`
        contract. Zero-row frame (same schema) when the page has no game
        blocks.

    Raises:
        ValueError: No ``<table>`` on the page (R: ``stop("No Games Table
            Found")``, M:1337), or the extracted game-id count does not match
            the number of assignable games (R silently recycles — see module
            docstring).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_scoreboard import parse_ncaa_bb_scoreboard
            html = open("scoreboard_18703_11-11-2025.html", encoding="utf-8").read()
            df = parse_ncaa_bb_scoreboard(html, "11/11/2025", season_id=18703)
            print(df.shape)  # (82, 14)

        Pipeline next step (one line)::

            df.filter(pl.col("game_id").is_not_null()).select("game_id", "home", "away")
    """
    soup = parse_html(html)
    table = soup.find("table")
    if table is None:
        # R: stop("No Games Table Found") (M:1337).
        raise ValueError("No Games Table Found")

    # Rows = every descendant <tr> that has direct <td> children (the th-only
    # header row drops out; the nested line-score tables' rows stay, matching
    # both R engines' recursive row walk that makes the 5-row block math work).
    rows: "list[list[str]]" = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td", recursive=False)
        if tds:
            rows.append([jsoup_text(td) for td in tds])

    n_games = len(rows) // 5  # R: 1:(nrow(table)/5) truncates a ragged tail.
    if n_games == 0:
        return _empty_scoreboard()

    def cell(r: int, c: int) -> Optional[str]:
        """0-based cell lookup; short rows read as None (R's NA padding)."""
        row = rows[r]
        return row[c] if c < len(row) else None

    game_dates: "list[str]" = []
    away_teams: "list[Optional[str]]" = []
    home_teams: "list[Optional[str]]" = []
    away_scores: "list[Optional[str]]" = []
    home_scores: "list[Optional[str]]" = []
    attendance: "list[Optional[str]]" = []
    neutral_site: "list[Optional[bool]]" = []
    box_present: "list[bool]" = []
    for i in range(n_games):
        s = i * 5
        game_dates.append(cell(s, 0) or "")  # V1[start] (M:1348)
        away_teams.append(cell(s, 2))  # V3[start] (M:1351)
        away_scores.append(cell(s, 4))  # V5[start] (M:1355)
        neutral_cell = cell(s, 5)  # V6[start] != "" (M:1370)
        neutral_site.append(None if neutral_cell is None else neutral_cell != "")
        attendance.append(cell(s, 6))  # V7[start] (M:1349)
        home_teams.append(cell(s + 3, 1))  # V2[start+3] (M:1352)
        home_scores.append(cell(s + 3, 2))  # V3[start+3] (M:1354)
        box_present.append(cell(s + 4, 0) == "Box Score")  # V1[start+4] (M:1358)

    game_ids = _GAME_ID_RE.findall(html)
    if season_id is not None:
        game_ids = [g for g in game_ids if g != str(season_id)]

    # R: id_found[!away_score %in% c("Canceled","Ppd") & box_score_present]
    #    <- game_ids (M:1366-1367). NA away_score passes the %in% filter in R
    # (NA %in% c(...) is FALSE), so None passes here too.
    assignable = [a not in ("Canceled", "Ppd") and b for a, b in zip(away_scores, box_present)]
    n_assignable = sum(assignable)
    if len(game_ids) != n_assignable:
        # Deliberate divergence: R silently recycles on mismatch (spec §2
        # R-gotcha) — that can only mis-key games, so fail loudly instead.
        raise ValueError(
            f"scoreboard game-id count mismatch: {len(game_ids)} /contests/ links "
            f"vs {n_assignable} assignable games — page layout drift?"
        )
    id_found: "list[Optional[str]]" = [None] * n_games
    id_iter = iter(game_ids)
    for i, ok in enumerate(assignable):
        if ok:
            id_found[i] = next(id_iter)

    return pl.DataFrame(
        {
            # R: substr(game_date, 1, 10) / substr(game_date, 12, 19) (M:1403-1404).
            "date": [gd[:10] for gd in game_dates],
            "start_time": [gd[11:19] for gd in game_dates],
            "home": [_clean_team(t) for t in home_teams],
            "away": [_clean_team(t) for t in away_teams],
            "box_id": id_found,
            "game_id": id_found,  # BoxID == GameID in current bigballR (M:1407-1408).
            "home_score": home_scores,
            "away_score": away_scores,
            "attendance": attendance,
            "neutral_site": neutral_site,
            # Wins/losses parse from the RAW team string, before cleanup (M:1387-1394).
            "home_wins": [_extract_int(_WINS_RE, t) for t in home_teams],
            "home_losses": [_extract_int(_LOSSES_RE, t) for t in home_teams],
            "away_wins": [_extract_int(_WINS_RE, t) for t in away_teams],
            "away_losses": [_extract_int(_LOSSES_RE, t) for t in away_teams],
        },
        schema=SCOREBOARD_SCHEMA,
    )


def _resolve_conference_id(conference: str) -> int:
    """Fold a conference name to its stats.ncaa.org id (M:1216-1259).

    Normalization strips ALL non-``[0-9A-Za-z=.]`` characters then lowercases
    (deliberate fix of R's first-char-only ``sub`` — module docstring).
    Unknown names warn and fall back to 0 ("All"), matching R's
    ``message("Conference ID not found, using all")`` behavior.
    """
    key = _CONF_NORM_RE.sub("", conference).lower()
    cid = NCAA_BB_CONFERENCE_IDS.get(key)
    if cid is None:
        warnings.warn(
            f"Conference ID not found for {conference!r}, using all",
            stacklevel=3,
        )
        return 0
    return cid


def _season_id_for_date(d: date_cls, league: str) -> int:
    """Resolve the ``season_divisions`` id for *d* (M:1132-1212 / W:1106-1150).

    Season boundary is May 1: a date belongs to season ``Y-(Y+1)`` iff it is
    strictly after ``Y-05-01`` and on/before ``(Y+1)-05-01``.

    Raises:
        ValueError: The date falls outside the league's season-id table
            (deliberate fix of R's type-unstable string return
            ``"Season Not Available"``, M:1210-1212).
    """
    table = NCAA_MBB_SEASON_DIVISIONS if league == "mbb" else NCAA_WBB_SEASON_DIVISIONS
    start_year = d.year if (d.month, d.day) > (5, 1) else d.year - 1
    season = f"{start_year}-{(start_year + 1) % 100:02d}"
    sid = table.get(season)
    if sid is None:
        raise ValueError(
            f"Season Not Available: no {league} season_divisions id for {season} "
            f"(date {d.isoformat()}); known seasons: "
            f"{min(table)}..{max(table)}"
        )
    return sid


def _ncaa_bb_date_games(
    date: Optional[str],
    *,
    conference: str,
    conference_id: Optional[int],
    fetcher: "Optional[NcaaFetcher]",
    league: str,
) -> pl.DataFrame:
    """Shared fetch-and-parse core behind the MBB/WBB date-games scrapers.

    Args:
        date: ``"MM/DD/YYYY"``; ``None`` -> yesterday (R's default,
            ``format(Sys.Date() - 1, "%m/%d/%Y")``, M:1120).
        conference: Conference name, resolved via
            :func:`_resolve_conference_id`. Default ``"All"`` -> 0.
        conference_id: Explicit id override (R's ``conference.ID``; wins over
            *conference* when given, M:1261-1263).
        fetcher: Injectable :class:`~sportsdataverse.mbb.mbb_ncaa_fetch
            .NcaaFetcher`. ``None`` -> ``NcaaFetcher.with_browser()`` — the
            scoreboard page is JS-rendered behind Akamai bm-verify, so the
            browser transport is the working default (the R equivalent is
            ``use_chromote=TRUE``).
        league: ``"mbb"`` or ``"wbb"`` — selects the season-id table (the ONE
            league knob; spec_wbigballr_divergence.md §2.2).

    Returns:
        The :data:`SCOREBOARD_SCHEMA` frame from
        :func:`parse_ncaa_bb_scoreboard`.
    """
    if date is None:
        date = (datetime.now().date() - timedelta(days=1)).strftime("%m/%d/%Y")
    d = datetime.strptime(date, "%m/%d/%Y").date()
    sid = _season_id_for_date(d, league)
    cid = conference_id if conference_id is not None else _resolve_conference_id(conference)
    # R: gsub("[/]", "%2F", date) into the query string (M:1266-1278).
    path = f"season_divisions/{sid}/scoreboards?game_date={date.replace('/', '%2F')}&conference_id={cid}&commit=Submit"
    if fetcher is None:
        from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

        fetcher = NcaaFetcher.with_browser()
    html = fetcher.fetch_html(path)
    return parse_ncaa_bb_scoreboard(html, date, season_id=sid)


@overload
def ncaa_mbb_date_games(
    date: Optional[str] = ...,
    *,
    conference: str = ...,
    conference_id: Optional[int] = ...,
    fetcher: "Optional[NcaaFetcher]" = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def ncaa_mbb_date_games(
    date: Optional[str] = ...,
    *,
    conference: str = ...,
    conference_id: Optional[int] = ...,
    fetcher: "Optional[NcaaFetcher]" = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...


def ncaa_mbb_date_games(
    date: Optional[str] = None,
    *,
    conference: str = "All",
    conference_id: Optional[int] = None,
    fetcher: "Optional[NcaaFetcher]" = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Discover every NCAA MBB game played on a date (bigballR ``get_date_games``).

    Fetches ``stats.ncaa.org/season_divisions/{sid}/scoreboards`` for the
    date's season and returns one row per game with the ``/contests/{id}``
    game id needed by the play-by-play / box-score scrapers. Port of bigballR
    ``get_date_games`` (all_functions.R:1119-1427).

    Args:
        date: ``"MM/DD/YYYY"``. Defaults to yesterday (R default).
        conference: Conference name filter (e.g. ``"ACC"``, ``"Big Ten"``);
            case/punctuation-insensitive. Default ``"All"``. Unknown names
            warn and fall back to all conferences (R behavior).
        conference_id: Explicit stats.ncaa.org conference id; overrides
            *conference* when given (R's ``conference.ID``).
        fetcher: Injectable :class:`~sportsdataverse.mbb.mbb_ncaa_fetch
            .NcaaFetcher` (tests pass an offline fake). ``None`` uses
            ``NcaaFetcher.with_browser()`` — the page is JS-rendered behind
            Akamai bm-verify, so the browser transport is the live default.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per game with columns ``date, start_time, home, away, box_id,
        game_id, home_score, away_score, attendance, neutral_site, home_wins,
        home_losses, away_wins, away_losses`` (:data:`SCOREBOARD_SCHEMA`).
        Scores stay Utf8 — they hold ``"Canceled"`` / ``"Ppd"`` for unplayed
        games; ``game_id`` is null for games without a box score.

    Raises:
        ValueError: The date's season has no known ``season_divisions`` id
            (R returns the string ``"Season Not Available"`` — deliberate
            fix), the date is not ``MM/DD/YYYY``, the fetched page has no
            games table, or the game-id link count mismatches the schedule.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_scoreboard import ncaa_mbb_date_games
            games = ncaa_mbb_date_games("11/11/2025")
            print(games.shape)

        Useful parameter combination::

            acc_pd = ncaa_mbb_date_games("02/01/2025", conference="ACC",
                                         return_as_pandas=True)

        Pipeline next step (one line)::

            games.filter(pl.col("game_id").is_not_null())["game_id"].to_list()

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R men's basketball companion package
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- R women's basketball companion package
    """
    df = _ncaa_bb_date_games(
        date,
        conference=conference,
        conference_id=conference_id,
        fetcher=fetcher,
        league="mbb",
    )
    if return_as_pandas:
        return df.to_pandas()
    return df
