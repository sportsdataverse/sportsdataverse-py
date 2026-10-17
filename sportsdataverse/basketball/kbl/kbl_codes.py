"""Code tables and the season crosswalk of the KBL (Korean Basketball League) wrappers.

Everything here is data from ``sdv-internal-refs/kbl`` (``README.md`` and ``kbl-returns.md``, probed 2026-10-12). The
Korean labels are the site's own; the English meanings are the refs' translations. A code that is missing from a
table is passed through undecoded by the parsers (never guessed).

**Seasons.** KBL numbers every season **and** every D-League (reserve league) edition with one sequential
``seasonCode``. The sdv season is the END year: the second year of ``YYYY-YYYY`` (1997 for the inaugural ``1997``
season; a D-League edition is the label year + 1, except ``2009KBLSUMMER`` = 2009). :data:`SEASONS` is the crosswalk
(``tools/codegen/gen_kbl.py`` checks it against the refs README on every run and the tests against a committed
copy). ``seasonGrade`` 1 is the KBL itself, 2 the D-League.
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple

__all__ = [
    "ACTIONS",
    "DIVISIONS",
    "FOULS",
    "GAME_CODES",
    "LEADER_PARTS",
    "PLAYER_FLAGS",
    "POSITIONS",
    "SEASONS",
    "SUBSTITUTIONS",
    "game_key_parts",
    "period_of",
    "season_code",
    "season_dates",
    "season_end",
]

#: ``(seasonCode, seasonName1, seasonGrade, first game YYYYMMDD, last game YYYYMMDD, sdv END year)``.
SEASONS: Tuple[Tuple[int, str, int, str, str, int], ...] = (
    (1, "1997", 1, "19970201", "19970501", 1997),
    (2, "1997-1998", 1, "19971108", "19980411", 1998),
    (3, "1998-1999", 1, "19981108", "19990416", 1999),
    (4, "1999-2000", 1, "19991107", "20000402", 2000),
    (5, "2000-2001", 1, "20001104", "20010406", 2001),
    (6, "2001-2002", 1, "20011103", "20020419", 2002),
    (7, "2002-2003", 1, "20021026", "20030413", 2003),
    (8, "2003-2004", 1, "20031025", "20040410", 2004),
    (9, "2004-2005", 1, "20041029", "20050417", 2005),
    (10, "2005-2006", 1, "20051021", "20060425", 2006),
    (11, "2006-2007", 1, "20060927", "20070501", 2007),
    (12, "2007-2008", 1, "20070930", "20080425", 2008),
    (13, "2008-2009", 1, "20080928", "20090501", 2009),
    (14, "2009KBLSUMMER", 2, "20090713", "20090812", 2009),
    (15, "2009-2010", 1, "20090905", "20100411", 2010),
    (16, "2009KBLWINTER", 2, "20091201", "20100305", 2010),
    (17, "2010-2011", 1, "20101015", "20110426", 2011),
    (18, "2010KBLWINTER", 2, "20101025", "20110315", 2011),
    (19, "2011-2012", 1, "20111013", "20120406", 2012),
    (20, "2011KBLWINTER", 2, "20111017", "20120229", 2012),
    (21, "2012-2013", 1, "20121013", "20130417", 2013),
    (22, "2012KBLWINTER", 2, "20121022", "20130123", 2013),
    (23, "2013-2014", 1, "20131012", "20140410", 2014),
    (24, "2013KBLWINTER", 2, "20131028", "20140224", 2014),
    (25, "2014-2015", 1, "20141011", "20150404", 2015),
    (26, "2014 KBL D리그", 2, "20141110", "20150217", 2015),
    (27, "2015-2016", 1, "20150902", "20160329", 2016),
    (28, "2015 KBL D리그", 2, "20151102", "20160215", 2016),
    (29, "2016-2017", 1, "20161001", "20170502", 2017),
    (30, "2016 KBL D리그", 2, "20161107", "20170315", 2017),
    (31, "2017-2018", 1, "20170804", "20180418", 2018),
    (32, "2017 KBL D리그", 2, "20171106", "20180226", 2018),
    (33, "2018-2019", 1, "20181003", "20190421", 2019),
    (34, "2018 KBL D리그", 2, "20181029", "20190304", 2019),
    (35, "2019-2020", 1, "20191005", "20200229", 2020),
    (36, "2019 KBL D리그", 2, "20191111", "20200224", 2020),
    (37, "2020-2021", 1, "20200920", "20210509", 2021),
    (38, "2020 KBL D리그", 2, "20201104", "20210316", 2021),
    (39, "2021-2022", 1, "20210911", "20220510", 2022),
    (40, "2021 KBL D리그", 2, "20211027", "20220126", 2022),
    (41, "2022-2023", 1, "20221001", "20230507", 2023),
    (42, "2022 KBL D리그", 2, "20221108", "20230227", 2023),
    (43, "2023-2024", 1, "20231008", "20240505", 2024),
    (44, "2023 KBL D리그", 2, "20231031", "20240313", 2024),
    (45, "2024-2025", 1, "20241005", "20250517", 2025),
    (46, "2024 KBL D리그", 2, "20241121", "20250407", 2025),
    (47, "2025-2026", 1, "20250920", "20260513", 2026),
    (48, "2025 KBL D리그", 2, "20251121", "20260401", 2026),
    (49, "2026-2027", 1, "20260924", "20270411", 2027),
    (50, "2026 KBL D리그", 2, "20261125", "20270310", 2027),
)

_END_OF_CODE: Dict[int, int] = {row[0]: row[5] for row in SEASONS}
_CODE_OF: Dict[Tuple[int, int], int] = {(row[5], row[2]): row[0] for row in SEASONS}

#: Public ``division`` argument -> ``seasonGrade``.
DIVISIONS: Dict[str, int] = {"kbl": 1, "d_league": 2}

#: Text-cast action ``a`` -> (Korean label, English meaning).
ACTIONS: Dict[str, Tuple[str, str]] = {
    "001": ("게임시작", "Game start (a period starts)"),
    "002": ("게임중단", "Game suspended (the clock stops)"),
    "003": ("작전시간", "Timeout"),
    "009": ("게임종료", "Game end (a period ends)"),
    "010": ("시간설정", "Time setting (a clock correction)"),
    "101": ("교체(IN)", "Substitution in"),
    "102": ("교체(OUT)", "Substitution out"),
    "201": ("2점슛성공", "Two-point field goal made (dunks excepted)"),
    "202": ("2점슛시도", "Two-point field goal missed"),
    "203": ("자유투성공", "Free throw made"),
    "204": ("자유투시도", "Free throw missed"),
    "205": ("3점슛성공", "Three-point field goal made"),
    "206": ("3점슛시도", "Three-point field goal missed"),
    "207": ("덩크슛성공", "Dunk made"),
    "208": ("덩크슛시도", "Dunk missed"),
    "209": ("공격리바운드", "Offensive rebound"),
    "210": ("수비리바운드", "Defensive rebound"),
    "211": ("어시스트", "Assist"),
    "212": ("스틸", "Steal"),
    "213": ("블록", "Blocked shot"),
    "214": ("턴오버", "Turnover"),
    "215": ("파울자유투", "Foul that awards free throws"),
    "216": ("파울", "Foul without free throws"),
    "217": ("팀속공", "Fast break"),
    "218": ("팀리바운드", "Team rebound"),
    "219": ("Illegal", "Illegal defense"),
    "221": ("굿디펜스", "Good defense"),
    "223": ("팀턴오버", "Team turnover"),
    "224": ("기타파울", "Other foul"),
    "225": ("팀파울", "Team foul"),
    "226": ("스크린 어시스트", "Screen assist"),
    "227": ("디플렉션", "Deflection"),
    "228": ("비디오판독", "Video review"),
    "229": ("코치챌린지", "Coach's challenge"),
}

#: Points a made-shot action scores.
ACTION_POINTS: Dict[str, int] = {"201": 2, "203": 1, "205": 3, "207": 2}

#: Substitution sub-type ``c`` (on 102 rows) -> (Korean label, English meaning).
SUBSTITUTIONS: Dict[str, Tuple[str, str]] = {
    "00_0": ("", "Ordinary substitution"),
    "101_0": ("", "Ordinary substitution out"),
    "104_0": ("", "Substitution out, sub-type 104 (meaning unverified)"),
    "106_0": ("", "Ejection-type exit, no reason (unverified)"),
    "106_1": ("5반칙", "Fouled out (five fouls)"),
    "106_2": ("디스퀄리파잉", "Disqualifying foul"),
    "106_3": ("기타 퇴장", "Other ejection"),
}

#: Foul type ``f`` (the site reads it; no sampled row carries it) -> (Korean label, English meaning).
FOULS: Dict[str, Tuple[str, str]] = {
    "EBF": ("엘보우 파울", "Elbow foul"),
    "FRF": ("U파울", "Unsportsmanlike foul"),
    "FTF": ("파이팅 파울", "Fighting foul"),
    "PCF": ("펀칭 파울", "Punching foul"),
    "PNF": ("퍼스널 파울", "Personal foul"),
    "TCF": ("테크니컬 파울", "Technical foul"),
    "XXX": ("", "No foul type"),
}

#: ``gameCode`` -> (seasonCategory, Korean category name, English meaning).
GAME_CODES: Dict[str, Tuple[str, str, str]] = {
    "01": ("R", "정규시즌", "Regular season (D-League: stage 1, category D1)"),
    "03": ("PO", "플레이오프", "Playoffs (quarterfinals and semifinals)"),
    "04": ("CP", "챔피언결정전", "Championship final"),
    "07": ("EA", "EASL", "East Asia Super League games of KBL clubs"),
    "08": ("D2", "D리그 2차", "D-League second stage"),
    "10": ("AS", "올스타게임", "All-Star Game"),
    "11": ("EG", "컵대회", "Cup / exhibition tournament, 2006-07 to 2018-19"),
    "13": ("CU", "KBL컵대회", "KBL Cup, from 2020-21"),
    "14": ("OM", "OPEN MATCH DAY", "Open Match Day (preseason showcase), from 2025-26"),
}

#: ``playerFlag``: player type.
PLAYER_FLAGS: Dict[str, str] = {
    "0": "Domestic (Korean) player",
    "1": "Foreign (import) player",
    "2": "Asian-quota player",
}

#: ``pos``: position code (AA, AB, AC, AD, XX appear only on former players; meaning unverified).
POSITIONS: Dict[str, str] = {
    "C": "Center",
    "FD": "Forward",
    "GD": "Guard",
    "FC": "Forward-center",
    "GF": "Guard-forward",
}

#: ``/league/season-leader`` and match-chart ``bestPlayer`` ``part``: the statistic.
LEADER_PARTS: Dict[str, str] = {
    "score": "Points per game",
    "rb": "Rebounds per game",
    "ast": "Assists per game",
    "bs": "Blocks per game",
    "stl": "Steals per game",
    "fgt": "Field goals made per game",
    "threep": "Three-pointers made per game",
    "ftR": "Free-throw percentage",
    "fgtR": "Field-goal percentage",
    "threepR": "Three-point percentage",
    "dk": "Dunks per game",
}


def season_end(code: int) -> Optional[int]:
    """The sdv END year of a KBL ``seasonCode`` (``None`` for a code the table does not know).

    Args:
        code: the KBL season code (47 = 2025-26).

    Returns:
        The END year (2026 for 47), or ``None``.
    """
    return _END_OF_CODE.get(code)


def season_code(season: int, division: str = "kbl") -> int:
    """The KBL ``seasonCode`` of an sdv END year.

    Args:
        season: the sdv season, the END year (2026 = 2025-26).
        division: ``"kbl"`` (the KBL itself, ``seasonGrade`` 1) or ``"d_league"`` (the reserve league, grade 2).

    Returns:
        The KBL season code (47 for ``2026``, ``"kbl"``).

    Raises:
        ValueError: ``season`` is not an END year between 1997 and 2027, ``division`` is unknown, or that division has
            no edition in that season (the D-League starts with the 2009 summer edition).
    """
    if isinstance(season, bool) or not isinstance(season, int):
        try:
            season = int(str(season).strip())
        except ValueError:
            raise ValueError(f"season must be an END year such as 2026 (2025-26); got {season!r}") from None
    if division not in DIVISIONS:
        raise ValueError(f"division must be one of {sorted(DIVISIONS)}; got {division!r}")
    code = _CODE_OF.get((season, DIVISIONS[division]))
    if code is None:
        raise ValueError(
            f"the KBL has no {division} season {season}: seasons run 1997 to 2027 (the D-League from 2009)"
        )
    return code


def season_dates(season: int) -> Tuple[str, str]:
    """First and last game date (``YYYYMMDD``) over every competition of an sdv END year.

    Args:
        season: the sdv season, the END year.

    Returns:
        ``(first, last)``, the KBL and D-League editions of that year taken together.

    Raises:
        ValueError: ``season`` is not an END year between 1997 and 2027.
    """
    rows = [r for r in SEASONS if r[5] == season]
    if not rows:
        raise ValueError(f"season must be an END year between 1997 and 2027; got {season!r}")
    return min(r[3] for r in rows), max(r[4] for r in rows)


def game_key_parts(gmkey: str) -> Optional[Tuple[int, str, int]]:
    """``(seasonCode, gameCode, gameNo)`` of a ``S{seasonCode}G{gameCode}N{gameNo}`` game key, else ``None``."""
    import re

    m = re.fullmatch(r"S(\d+)G(\d{2})N(\d+)", gmkey) if isinstance(gmkey, str) else None
    return (int(m.group(1)), m.group(2), int(m.group(3))) if m else None


def period_of(label: object) -> Tuple[Optional[int], Optional[str]]:
    """``(period, period_label)`` of a period label: ``Q1``-``Q4`` -> 1-4, overtime ``X1``/``EQ1``/``eq1`` -> 5, 6 ...

    Args:
        label: the wire label (text-cast ``X1``, match-chart ``EQ1`` or ``eq1``, quarter ``Q1`` or ``q1``).

    Returns:
        ``(period, label)`` with the label normalised to ``Q1``-``Q4`` / ``OT1``, ``OT2`` ...; ``(None, None)`` for
        anything else.
    """
    import re

    m = re.fullmatch(r"(?i)(Q|X|EQ)(\d)", label) if isinstance(label, str) else None
    if not m:
        return None, None
    n = int(m.group(2))
    if m.group(1).upper() == "Q":
        return (n, f"Q{n}") if 1 <= n <= 4 else (None, None)
    return 4 + n, f"OT{n}"
