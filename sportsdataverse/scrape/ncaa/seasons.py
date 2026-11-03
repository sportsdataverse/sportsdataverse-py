"""stats.ncaa.org season and division ids for every sport, resolved from the season.

stats.ncaa.org keys each scoreboard by a ``season_division_id``: one id per sport, academic year and
division (2026-27 men's basketball D-I is ``18983``). The ids are not sequential across sports or
divisions (2025-26 MBB is D-I ``18703``, D-II ``18705``, D-III ``18710``), so they cannot be derived;
they have to be read off the site. This module does that so callers never handle one:

* :func:`ncaa_season_division_id` -- ``("mbb", 2027)`` -> ``18983``.
* :func:`ncaa_season_divisions` -- the whole table for one sport (or all 20) as a frame.
* :func:`ncaa_academic_year` -- the season -> academic-year mapping the two above use.

Resolution order: the table bundled with the package (``data/ncaa_season_divisions.csv``, every
sport, season and division the site lists back to 1996-97, built by
``tools/build_ncaa_season_divisions.py``), then a cache file of ids already resolved live, then
one live fetch of the scoreboard page (a few KB), whose answer is written to that cache. A season
the site opened after the package was built therefore costs one request, once.

**Season convention.** ``season`` follows SportsDataverse's convention for each sport: the END year
for winter and spring sports (``2027`` = 2026-27 basketball; ``2026`` = the spring 2026 baseball
season) and the START year for fall sports (``2026`` = the fall 2026 football season, which NCAA
files under academic year 2026-27). Pass ``academic_year=`` (the ending year of NCAA's academic
year) to skip the mapping.
"""

from __future__ import annotations

import html as html_lib
import json
import os
import re
import warnings
from datetime import date
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING, Callable, NamedTuple, Optional, Union

import polars as pl

from sportsdataverse.errors import SeasonNotFoundError

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "NCAA_SPORTS",
    "SEASON_DIVISIONS_SCHEMA",
    "ncaa_academic_year",
    "ncaa_season_division_id",
    "ncaa_season_divisions",
    "ncaa_sport_code",
]


class _Sport(NamedTuple):
    name: str
    fall: bool  # True: SDV season = START year; False: END year


#: Every sport stats.ncaa.org serves, by its ``sport_code``. ``fall`` sports key ``season`` by the
#: START year (football's convention); the rest by the END year.
NCAA_SPORTS: "dict[str, _Sport]" = {
    "MBA": _Sport("Baseball", False),
    "WFH": _Sport("Field Hockey", True),
    "MFB": _Sport("Football", True),
    "MBB": _Sport("Men's Basketball", False),
    "MIH": _Sport("Men's Ice Hockey", False),
    "MLA": _Sport("Men's Lacrosse", False),
    "MSO": _Sport("Men's Soccer", True),
    "MTE": _Sport("Men's Tennis", False),
    "MVB": _Sport("Men's Volleyball", False),
    "MWP": _Sport("Men's Water Polo", True),
    "WSB": _Sport("Softball", False),
    "WBB": _Sport("Women's Basketball", False),
    "WSV": _Sport("Women's Beach Volleyball", False),
    "WBW": _Sport("Women's Bowling", False),
    "WIH": _Sport("Women's Ice Hockey", False),
    "WLA": _Sport("Women's Lacrosse", False),
    "WSO": _Sport("Women's Soccer", True),
    "WTE": _Sport("Women's Tennis", False),
    "WVB": _Sport("Women's Volleyball", True),
    "WWP": _Sport("Women's Water Polo", False),
}

#: Short names SportsDataverse uses elsewhere, beside the codes and the site's own names.
_ALIASES = {
    "mbb": "MBB", "wbb": "WBB", "mfb": "MFB", "cfb": "MFB", "football": "MFB",
    "baseball": "MBA", "college_baseball": "MBA", "softball": "WSB", "college_softball": "WSB",
    "mch": "MIH", "wch": "WIH", "mens_ice_hockey": "MIH", "womens_ice_hockey": "WIH",
    "mens_lacrosse": "MLA", "womens_lacrosse": "WLA", "field_hockey": "WFH",
    "mens_soccer": "MSO", "womens_soccer": "WSO", "mens_tennis": "MTE", "womens_tennis": "WTE",
    "mens_volleyball": "MVB", "womens_volleyball": "WVB", "beach_volleyball": "WSV",
    "bowling": "WBW", "mens_water_polo": "MWP", "womens_water_polo": "WWP",
}  # fmt: skip

SEASON_DIVISIONS_SCHEMA: "dict[str, pl.DataType]" = {
    "sport_code": pl.Utf8,
    "sport": pl.Utf8,
    "season": pl.Int32,
    "academic_year": pl.Int32,
    "season_label": pl.Utf8,
    "division": pl.Int8,
    "division_label": pl.Utf8,
    "season_division_id": pl.Int64,
    "game_sport_year_ctl_id": pl.Int64,
}

_CSV_SCHEMA = {k: v for k, v in SEASON_DIVISIONS_SCHEMA.items() if k not in ("sport", "season", "division")}
_SCOREBOARD = "contests/scoreboards?utf8=%E2%9C%93"
_LABEL_RE = re.compile(r"^(\d{4})-(\d{2})$")
_DIVISION_RE = re.compile(r"^(?:D-?)?(I{1,3}|[123])$", re.I)
#: Labels that are a subdivision of D-I (football's two D-I scoreboards).
_D1_SUBDIVISIONS = ("FBS", "FCS")
_CACHE_NAME = "ncaa_season_divisions.json"


def _norm(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower().replace("'", "")).strip("_")


def ncaa_sport_code(sport: str) -> str:
    """Resolve a sport name, alias or code to the stats.ncaa.org ``sport_code``.

    Accepts the code in any case (``"mbb"``, ``"MBA"``), a SportsDataverse alias (``"football"``,
    ``"baseball"``, ``"softball"``, ``"mch"``) or the site's own name (``"Men's Ice Hockey"``).

    Args:
        sport: The sport to resolve.

    Returns:
        The three-letter ``sport_code``, for example ``"MBB"``.

    Raises:
        ValueError: The sport is not one stats.ncaa.org serves.

    Example:
        Resolve an alias::

            from sportsdataverse.scrape.ncaa.seasons import ncaa_sport_code
            ncaa_sport_code("football")   # 'MFB'
    """
    key = sport.strip()
    if key.upper() in NCAA_SPORTS:
        return key.upper()
    norm = _norm(key)
    if norm in _ALIASES:
        return _ALIASES[norm]
    for code, info in NCAA_SPORTS.items():
        if _norm(info.name) == norm:
            return code
    raise ValueError(f"unknown NCAA sport {sport!r}; expected a code ({', '.join(NCAA_SPORTS)}) or a name")


def ncaa_academic_year(sport: str, season: Optional[int] = None, *, on: Optional[date] = None) -> int:
    """Map a SportsDataverse ``season`` to NCAA's academic year (its ENDING calendar year).

    Fall sports are keyed by the START year (``season=2026`` football is academic year 2027);
    winter and spring sports by the END year, which already is the academic year. With no
    ``season``, the academic year containing ``on`` (default today) is returned: it turns over on
    July 1, as NCAA's does.

    Args:
        sport: Sport name, alias or code (see :func:`ncaa_sport_code`).
        season: The SportsDataverse season. ``None`` -> the current academic year.
        on: The date "current" is measured from (default today).

    Returns:
        The academic year, for example ``2027`` for 2026-27.

    Example:
        Football keys by the start year::

            from sportsdataverse.scrape.ncaa.seasons import ncaa_academic_year
            ncaa_academic_year("football", 2026)   # 2027
            ncaa_academic_year("mbb", 2027)        # 2027
    """
    code = ncaa_sport_code(sport)
    if season is None:
        d = on or date.today()
        return d.year + 1 if d.month >= 7 else d.year
    return int(season) + 1 if NCAA_SPORTS[code].fall else int(season)


def _division_number(label: object) -> "Optional[int]":
    """``"D-II"``/``"II"``/``2`` -> 2; ``"FBS"``/``"FCS"`` -> 1; any other label -> ``None``."""
    text = str(label).strip()
    if text.upper() in _D1_SUBDIVISIONS:
        return 1
    m = _DIVISION_RE.match(text)
    if m is None:
        return None
    token = m.group(1).upper()
    return int(token) if token.isdigit() else len(token)


def _division(division: Union[int, str]) -> int:
    n = _division_number(division)
    if n is None:
        raise ValueError(f"unknown division {division!r}; expected 1, 2, 3 or 'D-I', 'D-II', 'D-III'")
    return n


def _with_derived(df: pl.DataFrame) -> pl.DataFrame:
    fall = [c for c, s in NCAA_SPORTS.items() if s.fall]
    return df.with_columns(
        division=pl.col("division_label").map_elements(_division_number, return_dtype=pl.Int8),
        sport=pl.col("sport_code").replace_strict({c: s.name for c, s in NCAA_SPORTS.items()}, default=None),
        season=pl.when(pl.col("sport_code").is_in(fall))
        .then(pl.col("academic_year") - 1)
        .otherwise(pl.col("academic_year"))
        .cast(pl.Int32),
    ).select(list(SEASON_DIVISIONS_SCHEMA))


@lru_cache(maxsize=1)
def _bundled() -> pl.DataFrame:
    """The table shipped with the package; an empty table (and a warning) if it cannot be read.

    Read at import (the bigballR-era dicts are built from it), so it must never raise: a damaged
    file would otherwise break ``import sportsdataverse`` for every sport, including the tool that
    rebuilds it. Columns beyond the schema are ignored, so a column added later reads on old code.
    """
    path = files("sportsdataverse.scrape.ncaa") / "data" / "ncaa_season_divisions.csv"
    if not path.is_file():
        return pl.DataFrame(schema=SEASON_DIVISIONS_SCHEMA)
    try:
        raw = pl.read_csv(path.read_bytes(), schema_overrides=_CSV_SCHEMA).select(list(_CSV_SCHEMA))
        return _with_derived(raw)
    except (pl.exceptions.PolarsError, OSError, ValueError) as exc:
        warnings.warn(
            f"cannot read the bundled stats.ncaa.org season table ({exc}); season ids will be resolved live instead",
            stacklevel=2,
        )
        return pl.DataFrame(schema=SEASON_DIVISIONS_SCHEMA)


def _cache_path() -> Path:
    from sportsdataverse.cache import _cache_dir

    return _cache_dir() / _CACHE_NAME


def _cached_rows() -> "list[dict]":
    path = _cache_path()
    if not path.is_file():
        return []
    try:
        rows = json.loads(path.read_text())
    except (OSError, ValueError):
        return []
    return rows if isinstance(rows, list) else []


def _table() -> pl.DataFrame:
    rows = _cached_rows()
    if not rows:
        return _bundled()
    extra = _with_derived(pl.DataFrame(rows, schema=_CSV_SCHEMA))
    return pl.concat([_bundled(), extra]).unique(["sport_code", "academic_year", "division_label"], keep="first")


def _d1_ids_by_label(sport_code: str) -> "dict[str, int]":
    """``{"2026-27": 18983, ...}`` for one sport's D-I seasons: the bigballR-era dict shape."""
    df = (
        _bundled()
        .filter((pl.col("sport_code") == sport_code) & (pl.col("division_label") == "D-I"))
        .sort("academic_year")
    )
    return dict(df.select("season_label", "season_division_id").iter_rows())


def _select_options(page: str, name: str) -> "list[tuple[str, str, bool]]":
    """``(value, label, selected)`` for each non-empty option of the page's ``<select name=name>``."""
    m = re.search(rf'<select name="{name}".*?</select>', page, re.S)
    if m is None:
        return []
    return [
        (value, html_lib.unescape(label).strip(), "selected" in attrs)
        for attrs, value, label in re.findall(r"<option([^>]*)value=\"([^\"]*)\"[^>]*>([^<]*)", m.group(0))
        if value
    ]


def _parse_scoreboard_page(page: str, sport_code: str) -> "list[dict]":
    """Rows for the season a scoreboard page shows: one per division in its division dropdown."""
    picked = [(v, label) for v, label, sel in _select_options(page, "game_sport_year_ctl_id_select") if sel]
    if not picked:
        return []
    ctl_id, label = picked[0]
    m = _LABEL_RE.match(label)
    rows = []
    for sid, div_label, _ in _select_options(page, "season_division_id_select"):
        rows.append(
            {
                "sport_code": sport_code,
                "academic_year": int(m.group(1)) + 1 if m else None,
                "season_label": label,
                "division_label": div_label,
                "season_division_id": int(sid),
                "game_sport_year_ctl_id": int(ctl_id),
            }
        )
    return rows


def _resolve_live(code: str, academic_year: int, fetch_html: "Optional[Callable[[str], str]]") -> "list[dict]":
    path = f"{_SCOREBOARD}&sport_code={code}&academic_year={academic_year}&division=1&commit=Submit"
    if fetch_html is None:
        from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

        with NcaaFetcher.with_browser() as fetcher:  # force: a cached page could predate the season
            page = fetcher.fetch_html(path, force=True)
    else:
        page = fetch_html(path)
    rows = [r for r in _parse_scoreboard_page(page, code) if r["academic_year"] == academic_year]
    if rows:
        cache = _cache_path()
        cache.parent.mkdir(parents=True, exist_ok=True)
        # Through a per-process temp file and os.replace, so a reader never sees a half-written file.
        # ponytail: no lock -- two processes resolving at once can drop one's rows, which costs that
        # season one more fetch; add a file lock if the cache ever holds something not re-fetchable.
        tmp = cache.with_name(f"{cache.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(_cached_rows() + rows))
        os.replace(tmp, cache)
    return rows


def ncaa_season_divisions(
    sport: Optional[str] = None, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Every known stats.ncaa.org season and division id, for one sport or all of them.

    Reads the table bundled with the package plus any ids resolved live since (see
    :func:`ncaa_season_division_id`). It never fetches.

    Args:
        sport: Sport name, alias or code; ``None`` returns every sport.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per sport, academic year and division:

        | col_name | type | description |
        |---|---|---|
        | sport_code | String | stats.ncaa.org sport code, e.g. ``MBB`` |
        | sport | String | The site's sport name |
        | season | Int32 | SportsDataverse season (START year for fall sports, else END year) |
        | academic_year | Int32 | Ending year of NCAA's academic year (2027 = 2026-27) |
        | season_label | String | The site's label, e.g. ``2026-27`` |
        | division | Int8 | 1, 2 or 3; null for a division the site labels another way |
        | division_label | String | The site's label, e.g. ``D-I`` |
        | season_division_id | Int64 | The scoreboard id for this sport, season and division |
        | game_sport_year_ctl_id | Int64 | The site's id for the sport-season (all divisions) |

    Example:
        All men's basketball D-I seasons::

            import polars as pl
            from sportsdataverse.scrape.ncaa.seasons import ncaa_season_divisions
            ncaa_season_divisions("mbb").filter(pl.col("division") == 1)
    """
    df = _table()
    if sport is not None:
        df = df.filter(pl.col("sport_code") == ncaa_sport_code(sport))
    df = df.sort("sport_code", "academic_year", "division", descending=[False, True, False])
    return df.to_pandas() if return_as_pandas else df


def _pick(rows: pl.DataFrame, division: "Union[int, str, None]") -> "Optional[int]":
    """One season's rows -> the id for *division*; raise when a number names two scoreboards."""
    if rows.height == 0:
        return None
    if division is None:  # the sport's top division: D-I, else FBS (football)
        for label in ("D-I", "FBS"):
            hit = rows.filter(pl.col("division_label") == label)
            if hit.height:
                return int(hit["season_division_id"][0])
        top = rows.filter(pl.col("division") == 1)
        return int(top["season_division_id"][0]) if top.height == 1 else None
    by_label = rows.filter(pl.col("division_label").str.to_uppercase() == str(division).strip().upper())
    if by_label.height:
        return int(by_label["season_division_id"][0])
    hit = rows.filter(pl.col("division") == _division(division))
    if hit.height > 1:
        options = ", ".join(
            f"{lbl!r} ({sid})" for lbl, sid in hit.select("division_label", "season_division_id").rows()
        )
        raise ValueError(f"division {division!r} names {hit.height} scoreboards: {options}; pass one of the labels")
    return int(hit["season_division_id"][0]) if hit.height else None


def ncaa_season_division_id(
    sport: str,
    season: Optional[int] = None,
    division: "Union[int, str, None]" = None,
    *,
    academic_year: Optional[int] = None,
    fetch_html: "Optional[Callable[[str], str]]" = None,
    allow_fetch: bool = True,
) -> int:
    """The stats.ncaa.org ``season_division_id`` for a sport, season and division.

    Looks the id up in the bundled table, then in the cache of ids resolved live. If neither has
    it (a season the site opened after this package was built), one scoreboard page is fetched
    and the answer cached, so each new season costs one request, once.

    Args:
        sport: Sport name, alias or code (``"mbb"``, ``"football"``, ``"MIH"``).
        season: SportsDataverse season: the END year for winter and spring sports, the START year
            for fall sports. ``None`` -> the current academic year.
        division: ``1``/``2``/``3``, ``"D-I"``/``"D-II"``/``"D-III"``, or any label the site uses
            (football's ``"FBS"`` / ``"FCS"``). ``None`` (default) -> the sport's top division:
            D-I, or FBS for football. Football's ``1`` names two scoreboards and raises.
        academic_year: The ending year of NCAA's academic year. Overrides ``season``.
        fetch_html: ``path -> html`` used for the live lookup (inject a vendor transport or an
            offline fake). Default: a fresh :class:`~sportsdataverse.mbb.mbb_ncaa_fetch.NcaaFetcher`.
        allow_fetch: Set ``False`` to never touch the network (raise instead).

    Returns:
        The ``season_division_id``.

    Raises:
        ValueError: Unknown sport or division, or a division number that names two scoreboards.
        SeasonNotFoundError: The site lists no such sport, season and division.

    Example:
        Resolve scoreboard ids without knowing any::

            from sportsdataverse.scrape.ncaa.seasons import ncaa_season_division_id
            ncaa_season_division_id("mbb", 2027)               # 18983 (D-I)
            ncaa_season_division_id("baseball", 2026, "D-II")  # 18784
            ncaa_season_division_id("football", 2026, "FCS")   # fall 2026, FCS
    """
    code = ncaa_sport_code(sport)
    ay = int(academic_year) if academic_year is not None else ncaa_academic_year(code, season)

    def _season_rows(df: pl.DataFrame) -> pl.DataFrame:
        return df.filter((pl.col("sport_code") == code) & (pl.col("academic_year") == ay))

    found = _pick(_season_rows(_table()), division)
    if found is None and allow_fetch and _season_rows(_table()).height == 0:
        rows = _resolve_live(code, ay, fetch_html)
        if rows:
            found = _pick(_with_derived(pl.DataFrame(rows, schema=_CSV_SCHEMA)), division)
    if found is None:
        label = "top division" if division is None else f"division {division}"
        raise SeasonNotFoundError(
            f"no stats.ncaa.org season_division_id for {NCAA_SPORTS[code].name} academic year "
            f"{ay - 1}-{ay % 100:02d} {label}"
        )
    return found
