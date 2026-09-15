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
    """Group 1 of *pattern* on the play text as ``Int32`` (null where it does not match)."""
    return _text(col).str.extract(pattern, 1).cast(pl.Int32, strict=False)


def jersey_name(pattern: str, col: str = "text") -> pl.Expr:
    """Group 1 of *pattern* on the play text: the abbreviated name without its jersey."""
    return _text(col).str.extract(pattern, 1)


def jersey_punt_yards(col: str = "text") -> pl.Expr:
    """``"#43 M.Chiumento punt 43 yards"`` -> 43."""
    return jersey_yards(JERSEY_PUNT_YDS_RE, col)


def jersey_kickoff_yards(col: str = "text") -> pl.Expr:
    """``"#49 M.Diomede kickoff 65 yards"`` -> 65."""
    return jersey_yards(JERSEY_KICKOFF_YDS_RE, col)


def jersey_fg_yards(col: str = "text") -> pl.Expr:
    """``"field goal attempt from 26 yards GOOD"`` -> 26, whatever the result token."""
    return jersey_yards(JERSEY_FG_YDS_RE, col)


def jersey_fg_result(col: str = "text") -> pl.Expr:
    """``"GOOD"`` / ``"NO GOOD"`` / ``"BLOCKED"`` after the attempt clause, upper-cased."""
    return _text(col).str.extract(JERSEY_FG_RESULT_RE, 1).str.to_uppercase()


def jersey_return_yards(col: str = "text") -> pl.Expr:
    """``"#0 B.Inniss return 16 yards"`` -> 16 (the first return clause; a penalty
    appended after the return is not read)."""
    return jersey_yards(JERSEY_RETURN_YDS_RE, col)


def jersey_returner(col: str = "text") -> pl.Expr:
    """The returner: the name before ``return``, else the fair catcher. A muffed
    kick ("muffed by #21 R.Niblett") names nobody -- a muff is not a return."""
    return pl.coalesce(jersey_name(JERSEY_RETURNER_RE, col), jersey_name(JERSEY_FAIR_CATCH_RE, col))


def jersey_punter(col: str = "text") -> pl.Expr:
    """``"#43 M.Chiumento punt 43 yards"`` -> ``"M.Chiumento"``."""
    return jersey_name(JERSEY_PUNTER_RE, col)


def jersey_kicker(col: str = "text") -> pl.Expr:
    """``"#49 M.Diomede kickoff 65 yards"`` -> ``"M.Diomede"``."""
    return jersey_name(JERSEY_KICKER_RE, col)


def jersey_fg_kicker(col: str = "text") -> pl.Expr:
    """``"#96 C.Hawkins field goal attempt from 26 yards"`` -> ``"C.Hawkins"``."""
    return jersey_name(JERSEY_FG_KICKER_RE, col)


def has_jersey_return(col: str = "text") -> pl.Expr:
    """True where the text carries a jersey-style return clause. A trailing
    ``", out of bounds"`` after that clause describes the returner, not the kick."""
    return _text(col).str.contains(JERSEY_RETURN_YDS_RE).fill_null(False)
