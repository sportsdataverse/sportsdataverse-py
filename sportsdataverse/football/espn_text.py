"""ESPN football play-text grammar shared by the CFB and NFL processors.

ESPN's newer play text names a player by an abbreviated form ("M.Chiumento",
"A.St. Brown", "D.Jones Jr.") rather than spelling the name out. The NFL feed
has always read that way; the college feed adopted the same vendor template in
2025, with the jersey number inline ("#43 M.Chiumento punt 43 yards to the
OSU36 #0 B.Inniss return 16 yards to the TEX48 (#81 N.Townsend), out of
bounds"). Rust regex has no lookaround, so every pattern anchors on the verb
that follows the name instead.

The constants here are the one definition of that name shape; the NFL grammar
in :mod:`sportsdataverse.nfl.nfl_pbp` composes its verb-anchored patterns from
:data:`ABBREVIATED_NAME`, and the CFB processor reads the jersey-style
special-teams clauses through the ``jersey_*`` expressions below.
"""

from __future__ import annotations

import polars as pl

#: An abbreviated player name: "X.Surname", optionally "Ja.Surname", with a
#: particle ("A.St. Brown", "A.Van Ginkel") and a generational suffix.
ABBREVIATED_NAME = (
    r"[A-Z][a-z]{0,2}\.(?:St\. |Ste\. |Van |Von |De |Da |Del |Di |Du |La |Le )?[A-Za-z'\-]+"
    r"(?: (?:Jr|Sr|II|III|IV)\.?)?"
)

#: The jersey-prefixed form the 2025 college feed uses: "#43 M.Chiumento".
#: Three digits: the vendor feed uses #99 and higher. Group 1 is the name.
JERSEY_NAME = r"#\d{1,3} (" + ABBREVIATED_NAME + r")"
_JERSEY_NAME_NOCAP = r"#\d{1,3} " + ABBREVIATED_NAME

# Jersey-style special-teams clauses. The verbs are matched case-insensitively
# (ESPN writes "Touchback" and "GOOD" / "NO GOOD" / "BLOCKED" in mixed case);
# the name itself is matched case-sensitively so its capital initial keeps
# meaning -- hence the ``(?i)`` prefix with a ``(?-i:...)`` island around the
# name, the inline case toggle Rust regex offers in place of lookaround.
JERSEY_PUNT_YDS_RE = r"(?i)\bpunt (\d+) yards"
JERSEY_KICKOFF_YDS_RE = r"(?i)\bkickoff (\d+) yards"
JERSEY_FG_YDS_RE = r"(?i)field goal attempt from (\d+) yards"
JERSEY_FG_RESULT_RE = r"(?i)field goal attempt from \d+ yards (GOOD|NO GOOD|BLOCKED)"
JERSEY_RETURN_YDS_RE = r"(?i)(?-i:" + _JERSEY_NAME_NOCAP + r") return (-?\d+) yards"
JERSEY_RETURNER_RE = r"(?i)(?-i:" + JERSEY_NAME + r") return "
JERSEY_FAIR_CATCH_RE = r"(?i)fair catch by (?-i:" + JERSEY_NAME + r")"
JERSEY_PUNTER_RE = r"(?i)(?-i:" + JERSEY_NAME + r") punt "
JERSEY_KICKER_RE = r"(?i)(?-i:" + JERSEY_NAME + r") kickoff "
JERSEY_FG_KICKER_RE = r"(?i)(?-i:" + JERSEY_NAME + r") field goal attempt"


def _text(col: str) -> pl.Expr:
    return pl.col(col).cast(pl.Utf8, strict=False)


def jersey_yards(pattern: str, col: str = "text") -> pl.Expr:
    """Capture group 1 of *pattern* on the play text as an ``Int32`` yardage.

    Args:
        pattern: A regex whose group 1 is the yardage (one of the ``JERSEY_*_RE``
            constants, or any pattern of that shape).
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: ``Int32`` yardage, null where the pattern does not match.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import JERSEY_PUNT_YDS_RE, jersey_yards
            df = pl.DataFrame({"text": ["#43 M.Chiumento punt 43 yards to the OSU36"]})
            df.with_columns(yds=jersey_yards(JERSEY_PUNT_YDS_RE))["yds"].to_list()  # [43]
    """
    return _text(col).str.extract(pattern, 1).cast(pl.Int32, strict=False)


def jersey_name(pattern: str, col: str = "text") -> pl.Expr:
    """Capture group 1 of *pattern* on the play text: an abbreviated name without its jersey.

    Args:
        pattern: A regex whose group 1 is the name (one of the ``JERSEY_*_RE``
            name constants, or any pattern built on :data:`JERSEY_NAME`).
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: The ``"X.Surname"`` name, null where the pattern does not match.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import JERSEY_PUNTER_RE, jersey_name
            df = pl.DataFrame({"text": ["#43 M.Chiumento punt 43 yards to the OSU36"]})
            df.with_columns(who=jersey_name(JERSEY_PUNTER_RE))["who"].to_list()  # ["M.Chiumento"]
    """
    return _text(col).str.extract(pattern, 1)


def jersey_punt_yards(col: str = "text") -> pl.Expr:
    """Punt distance from a jersey-style punt clause.

    Args:
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: Int32 punt distance, null on any other text.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import jersey_punt_yards
            df = pl.DataFrame({"text": ["#43 M.Chiumento punt 43 yards to the OSU36"]})
            df.with_columns(v=jersey_punt_yards())["v"].to_list()  # [43]
    """
    return jersey_yards(JERSEY_PUNT_YDS_RE, col)


def jersey_kickoff_yards(col: str = "text") -> pl.Expr:
    """Kickoff distance from a jersey-style kickoff clause.

    Args:
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: Int32 kickoff distance, null on any other text.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import jersey_kickoff_yards
            df = pl.DataFrame({"text": ["#49 M.Diomede kickoff 65 yards to the TEX00, Touchback"]})
            df.with_columns(v=jersey_kickoff_yards())["v"].to_list()  # [65]
    """
    return jersey_yards(JERSEY_KICKOFF_YDS_RE, col)


def jersey_fg_yards(col: str = "text") -> pl.Expr:
    """Field-goal distance from ``field goal attempt from N yards``, whatever the result token (``GOOD`` / ``NO GOOD`` / ``BLOCKED``).

    Args:
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: Int32 attempt distance, null on any other text.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import jersey_fg_yards
            df = pl.DataFrame({"text": ["#96 C.Hawkins field goal attempt from 45 yards NO GOOD"]})
            df.with_columns(v=jersey_fg_yards())["v"].to_list()  # [45]
    """
    return jersey_yards(JERSEY_FG_YDS_RE, col)


def jersey_fg_result(col: str = "text") -> pl.Expr:
    """The result token after ``field goal attempt from N yards``, upper-cased.

    Args:
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: ``"GOOD"`` / ``"NO GOOD"`` / ``"BLOCKED"``, null on any other text.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import jersey_fg_result
            df = pl.DataFrame({"text": ["#96 C.Hawkins field goal attempt from 45 yards NO GOOD"]})
            df.with_columns(v=jersey_fg_result())["v"].to_list()  # ["NO GOOD"]
    """
    return _text(col).str.extract(JERSEY_FG_RESULT_RE, 1).str.to_uppercase()


def jersey_return_yards(col: str = "text") -> pl.Expr:
    """Return yardage from ``#N X.Surname return N yards`` (the first return clause; a penalty appended after it is not read).

    Args:
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: Int32 return yardage (may be negative), null on any other text.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import jersey_return_yards
            df = pl.DataFrame({"text": ["#43 M.Chiumento punt 43 yards to the OSU36 #0 B.Inniss return 16 yards to the TEX48"]})
            df.with_columns(v=jersey_return_yards())["v"].to_list()  # [16]
    """
    return jersey_yards(JERSEY_RETURN_YDS_RE, col)


def jersey_returner(col: str = "text") -> pl.Expr:
    """The returner: the jersey-prefixed name before ``return``, else the fair catcher.

    A muffed kick (``"muffed by #21 R.Niblett"``) names nobody -- a muff is not a
    return.

    Args:
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: The abbreviated returner / fair-catcher name, null on any other text.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import jersey_returner
            df = pl.DataFrame({"text": ["#42 J.McGuire punt 26 yards to the TEX13 fair catch by #21 R.Niblett at TEX13"]})
            df.with_columns(v=jersey_returner())["v"].to_list()  # ["R.Niblett"]
    """
    return pl.coalesce(jersey_name(JERSEY_RETURNER_RE, col), jersey_name(JERSEY_FAIR_CATCH_RE, col))


def jersey_punter(col: str = "text") -> pl.Expr:
    """The punter: the jersey-prefixed name before ``punt``.

    Args:
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: The abbreviated punter name, null on any other text.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import jersey_punter
            df = pl.DataFrame({"text": ["#43 M.Chiumento punt 43 yards to the OSU36"]})
            df.with_columns(v=jersey_punter())["v"].to_list()  # ["M.Chiumento"]
    """
    return jersey_name(JERSEY_PUNTER_RE, col)


def jersey_kicker(col: str = "text") -> pl.Expr:
    """The kickoff kicker: the jersey-prefixed name before ``kickoff``.

    Args:
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: The abbreviated kicker name, null on any other text.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import jersey_kicker
            df = pl.DataFrame({"text": ["#49 M.Diomede kickoff 65 yards to the TEX00, Touchback"]})
            df.with_columns(v=jersey_kicker())["v"].to_list()  # ["M.Diomede"]
    """
    return jersey_name(JERSEY_KICKER_RE, col)


def jersey_fg_kicker(col: str = "text") -> pl.Expr:
    """The field-goal kicker: the jersey-prefixed name before ``field goal attempt``.

    Args:
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: The abbreviated kicker name, null on any other text.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import jersey_fg_kicker
            df = pl.DataFrame({"text": ["#96 C.Hawkins field goal attempt from 26 yards GOOD"]})
            df.with_columns(v=jersey_fg_kicker())["v"].to_list()  # ["C.Hawkins"]
    """
    return jersey_name(JERSEY_FG_KICKER_RE, col)


def has_jersey_return(col: str = "text") -> pl.Expr:
    """Whether the text carries a jersey-style return clause.

    A trailing ``", out of bounds"`` after that clause describes the returner
    stepping out, not the kick, so the kick's out-of-bounds flags gate on this.

    Args:
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: Boolean, ``False`` (never null) where there is no return clause.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import has_jersey_return
            df = pl.DataFrame({"text": ["#0 B.Inniss return 16 yards to the TEX48 (#81 N.Townsend), out of bounds"]})
            df.with_columns(v=has_jersey_return())["v"].to_list()  # [True]
    """
    return _text(col).str.contains(JERSEY_RETURN_YDS_RE).fill_null(False)
