"""ESPN football play-text grammar shared by the CFB and NFL processors.

ESPN's newer play text names a player by an abbreviated form ("M.Chiumento",
"A.St. Brown", "D.Jones Jr.", "L.Vander Esch", "DK.Metcalf") rather than
spelling the name out. The NFL feed has always read that way; the college feed
adopted the same vendor template in 2025, with the jersey number inline ("#43
M.Chiumento punt 43 yards to the OSU36 #0 B.Inniss return 16 yards to the TEX48
(#81 N.Townsend), out of bounds"). Rust regex has no lookaround, so every
pattern anchors on the verb that follows the name instead.

The constants here are the one definition of that name shape; the NFL grammar
in :mod:`sportsdataverse.nfl.nfl_pbp` composes its verb-anchored patterns from
:data:`ABBREVIATED_NAME`, and the CFB processor reads the jersey-style
special-teams clauses through the ``jersey_*`` expressions below.

Beyond the names, the module holds the one definition of the turnover clause --
:data:`TURNOVER_TAIL_RE`, read through :func:`before_turnover` and
:func:`returned_for_touchdown` -- because both processors have to know where the
offense's play ended before they credit a gain or a touchdown to it.

The same clauses also carry names in shapes that grammar does not cover -- the
surname-first "Arreola,Carlos" / "Wesco Jr.,Bryant" stats.ncaa.org writes, and
2005-2014's spelled-out "Bryan Hahnfeldt". Those are read through
:data:`CLAUSE_NAME`, one loose name expression bounded by the kick and
field-goal anchors built from it (:data:`CLAUSE_RETURNER_RE`,
:data:`CLAUSE_FG_KICKER_RE`), so the CFB processor keeps no name regex of its
own for them.
"""

from __future__ import annotations

import polars as pl

# One surname word: accented letters ("K.Colón"), straight and curly apostrophes
# ("J.Malau’ulu"), hyphens, and the escaped apostrophe of the 2008-10 NFL feed
# ("D.O&apos;Neal").
_SURNAME_WORD = r"(?:[A-Za-zÀ-ÖØ-öø-ÿ'’\-]|&apos;)+"
# A further surname word ("L.Vander Esch", "A.Randle El", "J.Echeverria Lozano").
# Its second letter is lower-case, so an upper-case verb or team code ("R.Cobb
# MUFFS", "to JAX 28") never joins the name, and it is never "Jr" / "Sr", so the
# suffix keeps its period ("C.Harris Jr."). Where no verb follows to end the name,
# a title-case word after it is read in: 2 of 1.09M NFL plays 2002-2026
# ("Backward pass to T.Cohen The Replay Official ...", "to J.Lane For Two-Point").
_NEXT_SURNAME_WORD = r"(?:[A-IK-RT-Z][a-z]|[JS][a-qs-z]|[JS]r[a-z])[A-Za-zÀ-ÖØ-öø-ÿ'’\-]*"

#: An abbreviated player name as ESPN's NFL (2002-) and college vendor (2025-)
#: text writes it: the initials ("T.", "Ja.", "Josh.", "Dari.", "A.J. ", and only at
#: a word start "D.J." / "DK." / "SamL.", so "YACJ.Elliott" reads "J.Elliott" and
#: "TOUCHDOWN.B.Maher" reads "B.Maher"), an
#: optional "St." particle, one to three surname words (a lower-case particle
#: allowed before the later ones: "J.van den Berg"), and a suffix -- "Jr." / "Sr"
#: or a whole numeral ("III", never the "II" inside it), either after a comma
#: ("J.Ruffin, Jr.", "R.Royal, III"; not "V", which after a comma is the next
#: tackler: "(T.Bruschi, V.Wilfork)"), a numeral without a period ("W.Snead IV."
#: ends a sentence), including the vendor feed's lower-case-L "lll".
ABBREVIATED_NAME = (
    r"(?:[A-Z]\.[A-Z]\. |\b[A-Z]\.[A-Z]\.|\b[A-Z]{2}\.|\b[A-Z][a-z]{1,3}[A-Z]\.|[A-Z][a-z]{0,4}\.)"
    r"(?:St\. |Ste\. )?"
    + _SURNAME_WORD
    + r"(?: (?:(?:van|von|de|den|der|da|del|di|du|la|le) )?"
    + _NEXT_SURNAME_WORD
    + r"){0,2}"
    + r"(?:,? (?:Jr|Sr)\.?|,? (?:III|II|IV)\b| (?:V|lll|ll)\b)?"
)

#: A name inside a vendor special-teams clause, WHATEVER its shape -- the abbreviated
#: "#43 M.Chiumento", stats.ncaa.org's surname-first "Arreola,Carlos" / "Wesco Jr.,Bryant",
#: and 2005-2014's spelled-out "Bryan Hahnfeldt". It is deliberately loose -- the clause
#: anchors below are what bound it -- and the capture excludes "#", parentheses and digits,
#: so it can neither run back across a jersey or a yardline nor take the spot token ("to the
#: Bryant14  return 14 yards" names no returner and stays null). Group 1 is the name, never
#: the jersey. This is the one definition of that shape; the CFB processor holds none of its
#: own.
CLAUSE_NAME = r"(?:#\d{1,3} )?([^#()\d]+?)"

#: The jersey-prefixed form the 2025 college feed uses: "#43 M.Chiumento".
#: Three digits: the vendor feed uses #99 and higher. Group 1 is the name.
JERSEY_NAME = r"#\d{1,3} (" + ABBREVIATED_NAME + r")"
_JERSEY_NAME_NOCAP = r"#\d{1,3} " + ABBREVIATED_NAME

# Jersey-style special-teams clauses. The verbs are matched case-insensitively
# (ESPN writes "Touchback" and "GOOD" / "NO GOOD" / "BLOCKED" in mixed case);
# the name itself is matched case-sensitively so its capital initial keeps
# meaning -- hence the ``(?i)`` prefix with a ``(?-i:...)`` island around the
# name, the inline case toggle Rust regex offers in place of lookaround. A
# distance of one is singular ("punt 1 yard", "return 1 yard").
JERSEY_PUNT_YDS_RE = r"(?i)\bpunt (\d+) yards?\b"
JERSEY_KICKOFF_YDS_RE = r"(?i)\bkickoff (\d+) yards?\b"
JERSEY_FG_YDS_RE = r"(?i)field goal attempt from (\d+) yards?\b"
JERSEY_FG_RESULT_RE = r"(?i)field goal attempt from \d+ yards? (GOOD|NO GOOD|BLOCKED)"
#: Group 1 is a return's signed yardage ("return 16 yards", "return 1 yard"); group 2
#: is the loss in "return for loss of 2 yards" -- read both through jersey_return_yards().
JERSEY_RETURN_YDS_RE = r"(?i)(?-i:" + _JERSEY_NAME_NOCAP + r") return (?:(-?\d+)|for loss of (\d+)) yards?\b"
JERSEY_RETURNER_RE = r"(?i)(?-i:" + JERSEY_NAME + r") return "
JERSEY_FAIR_CATCH_RE = r"(?i)fair catch by (?-i:" + JERSEY_NAME + r")"
JERSEY_PUNTER_RE = r"(?i)(?-i:" + JERSEY_NAME + r") punt "
JERSEY_KICKER_RE = r"(?i)(?-i:" + JERSEY_NAME + r") kickoff "
JERSEY_FG_KICKER_RE = r"(?i)(?-i:" + JERSEY_NAME + r") field goal attempt"

#: The kick clause's returner in any name shape -- one expression for punts and kickoffs,
#: since the call site already knows which kind of kick the row is. Anchored on the kick
#: and its landing spot, which is what makes the loose name safe.
CLAUSE_RETURNER_RE = (
    r"(?:punt|kickoff) -?\d+ yards? to the [A-Za-z]*\s?\d{0,2},? " + CLAUSE_NAME + r" return -?\d+ yards?"
)
#: The field-goal kicker in any name shape: the name before "field goal attempt", at the
#: text start, after the clock parenthetical, or after a jersey.
CLAUSE_FG_KICKER_RE = r"(?:^|\)\s|#\d{1,3}\s)" + CLAUSE_NAME + r" field goal attempt"


def _text(col: str) -> pl.Expr:
    return pl.col(col).cast(pl.Utf8, strict=False)


#: The clause that hands the ball to the other team -- an interception, or a fumble and
#: the recovery and return that follow it. Everything after it describes the defence's
#: play, so a scrimmage-yardage extractor that reads past it credits the offense with the
#: defender's return yards ("pass complete to R.Deehan, fumbled, recovered by D.Hagan at
#: the Cal 18, D.Hagan for 82 yards ... for a TOUCHDOWN" booked 82 receiving yards).
#: Group 1 is the tail -- the turnover and everything after it.
TURNOVER_TAIL_RE = r"(?i)\b(?:intercept|fumbl)\w*\b(.*)$"


def before_turnover(col: str = "text") -> pl.Expr:
    """*col* truncated at the first interception / fumble clause.

    Args:
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: The text up to the turnover, unchanged where there is none.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import before_turnover
            df = pl.DataFrame({"text": ["J.Hurts up the middle for -4 yards. FUMBLES, N.Bolton for 36 yards, TOUCHDOWN."]})
            df.with_columns(v=before_turnover())["v"].to_list()  # ["J.Hurts up the middle for -4 yards. "]
    """
    return _text(col).str.replace(TURNOVER_TAIL_RE, "")


def returned_for_touchdown(col: str = "text") -> pl.Expr:
    """Whether the text says the ball was intercepted or lost on a fumble and *then* returned for a score.

    ESPN's ``type.text`` does not always say so -- 2005-2007 college pick-sixes are
    labelled ``"Passing Touchdown"`` and some NFL ones ``"Interception Return"`` -- so the
    offensive touchdown flags gate on this as well as on the play type. A try intercepted
    after a touchdown ("... TOUCHDOWN. TWO-POINT CONVERSION ATTEMPT ... is intercepted.")
    scores nothing after the turnover and reads ``False``.

    Args:
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: Boolean, ``False`` (never null) where there is no turnover.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import returned_for_touchdown
            df = pl.DataFrame({"text": ["B.Quinn pass intercepted by D.Adams at the NDame 31, returned for 31 yards for a TOUCHDOWN."]})
            df.with_columns(v=returned_for_touchdown())["v"].to_list()  # [True]
    """
    tail = _text(col).str.extract(TURNOVER_TAIL_RE, 1)
    return tail.str.contains(r"(?i)touchdown|(?-i:\bTD\b)").fill_null(False)


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
        pattern: A regex whose group 1 is the name (one of the ``JERSEY_*_RE`` /
            ``CLAUSE_*_RE`` name constants, or any pattern built on
            :data:`JERSEY_NAME` or :data:`CLAUSE_NAME`).
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
    """Return yardage from ``#N X.Surname return N yards`` / ``return for loss of N yards`` (the first return clause; a penalty appended after it is not read).

    Args:
        col: The play-text column. Defaults to ``"text"``.

    Returns:
        pl.Expr: Int32 return yardage, negative for a loss, null on any other text.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.football.espn_text import jersey_return_yards
            df = pl.DataFrame({"text": ["#43 M.Chiumento punt 43 yards to the OSU36 #0 B.Inniss return 16 yards to the TEX48"]})
            df.with_columns(v=jersey_return_yards())["v"].to_list()  # [16]
    """
    loss = _text(col).str.extract(JERSEY_RETURN_YDS_RE, 2).cast(pl.Int32, strict=False)
    return pl.coalesce(jersey_yards(JERSEY_RETURN_YDS_RE, col), -loss)


def jersey_returner(col: str = "text") -> pl.Expr:
    """The returner: the jersey-prefixed name before ``return``, else the fair catcher.

    A muffed kick (``"muffed by #21 R.Niblett"``) names nobody -- a muff is not a
    return. A name written in another shape (surname-first, or spelled out) is read
    through :data:`CLAUSE_RETURNER_RE` instead.

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

    A name written in another shape is read through :data:`CLAUSE_FG_KICKER_RE`.

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
