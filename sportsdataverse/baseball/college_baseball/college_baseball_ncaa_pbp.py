"""Parse stats.ncaa.org college-baseball (NCAA sport code ``MBA``) play-by-play
HTML into a tidy polars frame.

Structured, cfbfastR/nflfastR-style: one row per NCAA pbp row with inning context
+ a classified ``play_type`` + the batter, fielded position, hit trajectory, count
+ pitch sequence, RBI, sacrifice/double-play/error flags, and the runner movements
(runs scored, scoring runners, advancements, outs) lifted out of the play text --
keeping the raw ``description`` for anything not yet decomposed.

**Provenance.** Original sdv-py code. The HTML is the Akamai-``bm-verify``-gated
``/contests/{id}/play_by_play`` page, fetched by the shared browser transport in
:mod:`sportsdataverse.mbb.mbb_ncaa_fetch` (patchright + ``--headless=new`` +
residential IP). This module is the *parser* half; capture/discovery is a
producer concern. The ``stats.ncaa.org`` softball surface is the same table
layout, so :mod:`sportsdataverse.baseball.college_softball.college_softball_ncaa_pbp`
re-exports this parser unchanged.

Markup (fixture-verified, contests 6357953/6356679/6356680): one
``<table class="table">`` **per inning** (table order = inning number); header
row ``[away, "Score", home]``; each play row has the batting team's cell populated
(away col = top of inning, home col = bottom) with the running ``away-home`` score
in the middle. A description's clauses are joined by the literal token ``3a`` (a
source quirk, not an entity) -- the batter clause first, then runner-movement
clauses.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Union

import polars as pl
from bs4 import BeautifulSoup

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "PBP_SCHEMA",
    "decompose_college_baseball_plays",
    "parse_college_baseball_ncaa_pbp",
]

# NCAA "Last, F" / "Last, First" incl. hyphens and apostrophes ("S-Johnson, C").
# NCAA batter/runner name. Baseball writes "Last, First" (Brooks, M.); softball
# writes last-name only (Hasapis). The ", First" part is therefore OPTIONAL so one
# parser handles both -- verified e2e against a real WSB game (contest 6548848).
_NAME = r"[A-Z][A-Za-z'.\-]+(?:,\s*[A-Z][A-Za-z'.\-]*\.?)?"

# Clause separator inside a description (literal "3a", always followed by a
# capital-led clause). Python re -> lookahead is fine here (not a polars expr).
# Clause separator inside a description. Baseball uses the literal token "3a";
# softball uses "; " (semicolon) -- accept either (verified e2e on WSB 6548848).
_SEP_RE = re.compile(r"(?:3a|;)\s+(?=[A-Z])")
# Count + pitch sequence, e.g. "(3-2 FBFBBF)" or "(0-0)".
_COUNT_RE = re.compile(r"\((\d+)-(\d+)(?:\s+([A-Z]+))?\)")
_RBI_RE = re.compile(r",\s*(\d+)?\s*RBI\b")
_ERROR_POS_RE = re.compile(r"error by (\w+)")
# Fielded position: short code right after "to " (rf/lf/cf/1b/2b/3b/ss/p/c/dh).
_POS_RE = re.compile(r"\bto\s+(rf|lf|cf|1b|2b|3b|ss|p|c|dh)\b")
# Fuller "to <phrase>" location when no short code (e.g. "right field", "second base").
_LOC_RE = re.compile(r"\bto\s+([a-z][a-z ]+?)(?=,|\s*\(|\s*$|\s+for\b)")
_LEADNAME_RE = re.compile(rf"^({_NAME})\s+(.*)$", re.DOTALL)
_SCORED_RE = re.compile(rf"^({_NAME})\s+scored")
_ADVANCED_RE = re.compile(rf"^({_NAME})\s+advanced to (\w+)")
_OUT_RE = re.compile(r"\bout (?:at|on)\b")
_POS_CODES = r"(?:p|1b|2b|3b|ss|lf|cf|rf|c|dh|ph|pr|dp|flex)"
# A single "Last" or "Last, F" name (no intervening spaces/verbs).
_SUB_NAME = r"[A-Z][A-Za-z'.\-]+(?:,\s*[A-Z][A-Za-z'.\-]*\.?)?"
# A substitution clause = "<player> to <pos>" (optionally "for <player>") and the
# WHOLE clause is just that. The player name sits DIRECTLY before "to <pos>" -- no
# action verb between -- which separates it both from a batted ball ("singled to
# rf ...") and from a runner out ("out at second ss to 2b").
_SUB_FULL_RE = re.compile(rf"^{_SUB_NAME}\s+to\s+{_POS_CODES}\b(?:\s+for\s+{_SUB_NAME})?\.*$")


def _clauses(desc: str) -> "list[str]":
    return [c.strip() for c in _SEP_RE.split(desc) if c.strip()]


def _classify(rest: str) -> "tuple[str, dict]":
    """Map a batter clause's verb phrase to (play_type, extra fields)."""
    low = rest.lower()
    x: dict = {}
    # order: specific before general (substitution is detected upstream in _decompose)
    if "double play" in low:
        return "double_play", {"is_double_play": True, "is_out": True}
    if low.startswith("singled"):
        return "single", {"is_hit": True}
    if low.startswith("doubled"):
        return "double", {"is_hit": True}
    if low.startswith("tripled"):
        return "triple", {"is_hit": True}
    if low.startswith("homered"):
        return "home_run", {"is_hit": True}
    if low.startswith("walked") or "intentionally walked" in low:
        return "walk", x
    if low.startswith("hit by pitch"):
        return "hit_by_pitch", x
    if low.startswith("struck out"):
        t = "swinging" if "swinging" in low else ("looking" if "looking" in low else None)
        return "strikeout", {"is_out": True, "strikeout_type": t}
    if low.startswith("grounded out"):
        return "groundout", {"is_out": True, "hit_trajectory": "ground"}
    if low.startswith("flied out") or low.startswith("flied into"):
        return "flyout", {"is_out": True, "hit_trajectory": "fly"}
    if low.startswith("lined out") or low.startswith("lined into"):
        return "lineout", {"is_out": True, "hit_trajectory": "line"}
    if low.startswith("popped up"):
        return "popup", {"is_out": True, "hit_trajectory": "pop"}
    if low.startswith("fouled out"):
        return "foulout", {"is_out": True, "hit_trajectory": "foul"}
    if "reached on a fielder's choice" in low or "fielder's choice" in low:
        return "fielders_choice", x
    if re.search(r"reached on (?:an? )?(?:throwing |fielding )?error", low):
        return "reached_on_error", x
    if "stole " in low:
        return "stolen_base", x
    if "wild pitch" in low:
        return "wild_pitch", x
    if "passed ball" in low:
        return "passed_ball", x
    if low.startswith("advanced to") or "advanced to" in low[:20]:
        return "runner_advance", x
    if _OUT_RE.search(low):
        return "out", {"is_out": True}
    if re.search(r"\bfor\s+[A-Z/]", rest):  # softball courtesy-runner / sub notation ("... for Name")
        return "substitution", x
    return "unknown", x


def _decompose(desc: str) -> "dict":
    """Turn one raw pbp ``description`` into the structured field dict."""
    out: dict = {
        "batter": None,
        "play_type": "unknown",
        "hit_trajectory": None,
        "fielded_position": None,
        "is_hit": False,
        "is_out": False,
        "strikeout_type": None,
        "is_sacrifice": False,
        "sac_type": None,
        "is_double_play": False,
        "rbi": 0,
        "count_balls": None,
        "count_strikes": None,
        "pitch_sequence": None,
        "error_position": None,
        "unearned": False,
        "runs_scored": 0,
        "scoring_runners": [],
        "runners_advanced": [],
        "outs_on_play": 0,
        "is_scoring_play": False,
    }
    clauses = _clauses(desc)
    if not clauses:
        return out
    primary = clauses[0]
    plow = primary.lower()
    if _SUB_FULL_RE.match(primary) or "pinch hit for" in plow or "pinch ran for" in plow:
        # roster move (defensive change / pitching change / pinch hitter-runner);
        # no batter/count/RBI to lift -- keep the raw description.
        out["play_type"] = "substitution"
    else:
        m = _LEADNAME_RE.match(primary)
        batter, rest = (m.group(1), m.group(2)) if m else (None, primary)
        out["batter"] = batter
        ptype, extra = _classify(rest)
        out["play_type"] = ptype
        out.update(extra)

        # count + pitch sequence (anywhere in the row)
        cm = _COUNT_RE.search(desc)
        if cm:
            out["count_balls"] = int(cm.group(1))
            out["count_strikes"] = int(cm.group(2))
            out["pitch_sequence"] = cm.group(3)
        # RBI
        rm = _RBI_RE.search(primary)
        if rm:
            out["rbi"] = int(rm.group(1)) if rm.group(1) else 1
        # sacrifice
        if re.search(r"\bsac\b|sac,|sacrifice", plow):
            out["is_sacrifice"] = True
            out["sac_type"] = "bunt" if "bunt" in plow else ("fly" if ("fly" in plow or " sf" in plow) else None)
        # fielded position (short code preferred, else the "to <phrase>")
        pm = _POS_RE.search(rest)
        if pm:
            out["fielded_position"] = pm.group(1)
        else:
            lm = _LOC_RE.search(rest)
            if lm:
                out["fielded_position"] = lm.group(1).strip()
        # error
        em = _ERROR_POS_RE.search(desc)
        if em:
            out["error_position"] = em.group(1)
        if "unearned" in desc.lower():
            out["unearned"] = True

        # the batter's out counts as 1; on a double play the SECOND out arrives as
        # a separate "<runner> out on the play" clause counted in the loop below.
        if out["is_out"]:
            out["outs_on_play"] += 1

    # runner clauses
    for c in clauses[1:]:
        sc = _SCORED_RE.match(c)
        if sc:
            out["runs_scored"] += 1
            out["scoring_runners"].append(sc.group(1))
            continue
        ac = _ADVANCED_RE.match(c)
        if ac:
            out["runners_advanced"].append(f"{ac.group(1)}->{ac.group(2)}")
            continue
        sh = re.match(rf"^({_NAME})\s+stole home", c)  # a steal of home is a run
        if sh:
            out["runs_scored"] += 1
            out["scoring_runners"].append(sh.group(1))
            continue
        if _OUT_RE.search(c.lower()):
            out["outs_on_play"] += 1
    # on a home run the batter also scores, but NCAA text only lists the runners.
    if out["play_type"] == "home_run" and out["batter"]:
        out["runs_scored"] += 1
        out["scoring_runners"].append(out["batter"])
    # a standalone "<batter> stole home" primary is also a run.
    if out["play_type"] == "stolen_base" and out["batter"] and "stole home" in primary.lower():
        out["runs_scored"] += 1
        out["scoring_runners"].append(out["batter"])
    out["is_scoring_play"] = out["runs_scored"] > 0
    return out


PBP_SCHEMA: "dict[str, pl.DataType]" = {
    "contest_id": pl.Utf8,
    "inning": pl.Int64,
    "inning_top_bot": pl.Utf8,
    "batting": pl.Utf8,
    "fielding": pl.Utf8,
    "play_number": pl.Int64,
    "score_away": pl.Int64,
    "score_home": pl.Int64,
    "batter": pl.Utf8,
    "play_type": pl.Utf8,
    "hit_trajectory": pl.Utf8,
    "fielded_position": pl.Utf8,
    "is_hit": pl.Boolean,
    "is_out": pl.Boolean,
    "strikeout_type": pl.Utf8,
    "is_sacrifice": pl.Boolean,
    "sac_type": pl.Utf8,
    "is_double_play": pl.Boolean,
    "rbi": pl.Int64,
    "count_balls": pl.Int64,
    "count_strikes": pl.Int64,
    "pitch_sequence": pl.Utf8,
    "error_position": pl.Utf8,
    "unearned": pl.Boolean,
    "runs_scored": pl.Int64,
    "scoring_runners": pl.List(pl.Utf8),
    "runners_advanced": pl.List(pl.Utf8),
    "outs_on_play": pl.Int64,
    "is_scoring_play": pl.Boolean,
    "description": pl.Utf8,
}


def decompose_college_baseball_plays(
    rows: "list[dict]",
    *,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Decompose pre-extracted play rows into the full :data:`PBP_SCHEMA` frame.

    The row-level half of :func:`parse_college_baseball_ncaa_pbp` -- the play-text
    decomposition engine without the HTML extraction. This is the entry point
    for sources that already hold the base play fields, e.g. the legacy R-era
    ``baseballr-data`` trees (2012-2023: ``description``/``inning``/
    ``inning_top_bot``/``batting``/``fielding``/``score``), so legacy and
    freshly captured games resolve into IDENTICAL pbp columns.

    Args:
        rows: One dict per play. Recognized keys (all optional except
            ``description``): ``contest_id``, ``inning`` (int),
            ``inning_top_bot`` (``"top"``/``"bot"``), ``batting``, ``fielding``,
            ``play_number``, ``score_away``/``score_home`` (ints) or a combined
            ``score`` string (``"3-2"``, away-home), and ``description``.
            Unrecognized keys are ignored; ``play_number`` defaults to the
            1-based position in *rows*.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of ``polars``.

    Returns:
        One row per input play with every text-derivable :data:`PBP_SCHEMA`
        column populated (``play_type``, hit/out flags, ``rbi``,
        ``pitch_sequence``, runner movement, ...). Empty input returns a
        zero-row frame with the documented schema.

    Example:
        Quick start::

            from sportsdataverse.baseball.college_baseball import decompose_college_baseball_plays
            df = decompose_college_baseball_plays(
                [{"inning": 1, "inning_top_bot": "top", "score": "0-0",
                  "description": "Jack Moss singled to left field (1-2 KBFX)."}]
            )
            print(df.select("play_type", "is_hit", "pitch_sequence").row(0))

        See Also:
            * `baseballr`_ -- NCAA baseball via R (table-scrape pbp)

        .. _baseballr: https://billpetti.github.io/baseballr/
    """
    out: "list[dict]" = []
    for i, r in enumerate(rows, start=1):
        sa, sh = r.get("score_away"), r.get("score_home")
        if sa is None and sh is None:
            sm = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", str(r.get("score") or ""))
            if sm:
                sa, sh = int(sm.group(1)), int(sm.group(2))
        desc = r.get("description") or ""
        row = {
            "contest_id": str(r["contest_id"]) if r.get("contest_id") is not None else None,
            "inning": int(r["inning"]) if r.get("inning") is not None else None,
            "inning_top_bot": r.get("inning_top_bot"),
            "batting": r.get("batting"),
            "fielding": r.get("fielding"),
            "play_number": int(r.get("play_number") or i),
            "score_away": sa,
            "score_home": sh,
            "description": desc,
        }
        row.update(_decompose(desc))
        out.append(row)
    df = pl.DataFrame(out, schema=PBP_SCHEMA) if out else pl.DataFrame(schema=PBP_SCHEMA)
    return df.to_pandas() if return_as_pandas else df


def parse_college_baseball_ncaa_pbp(
    html: str,
    contest_id: "str | int | None" = None,
    *,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse a stats.ncaa.org college-baseball ``play_by_play`` page into a tidy frame.

    One row per NCAA pbp row: inning context (``inning``/``inning_top_bot``/
    ``batting``/``fielding``/``score_away``/``score_home``), a classified
    ``play_type``, the ``batter``, fielded position, hit trajectory, count +
    ``pitch_sequence``, ``rbi``, sacrifice/double-play/error flags, and the runner
    movements (``runs_scored``/``scoring_runners``/``runners_advanced``/
    ``outs_on_play``). The raw ``description`` is retained.

    Args:
        html: Raw HTML of the ``/contests/{id}/play_by_play`` page.
        contest_id: Optional stats.ncaa.org contest id, stamped on every row.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of ``polars``.

    Returns:
        A ``polars.DataFrame`` (or ``pandas.DataFrame``) with one row per play.
        Empty/unparseable input returns a zero-row frame with the documented schema.

    Example:
        Quick start::

            from sportsdataverse.baseball.college_baseball import parse_college_baseball_ncaa_pbp
            df = parse_college_baseball_ncaa_pbp(open("contest_6357953.html").read(), contest_id=6357953)
            print(df.shape)

        See Also:
            * `baseballr`_ -- NCAA baseball via R (table-scrape pbp)

        .. _baseballr: https://billpetti.github.io/baseballr/
    """
    soup = BeautifulSoup(html or "", "html.parser")
    cid = str(contest_id) if contest_id is not None else None
    rows: "list[dict]" = []
    play_number = 0

    for inning_idx, table in enumerate(soup.select("table.table"), start=1):
        trs = table.find_all("tr")
        if not trs:
            continue
        header = [c.get_text(" ", strip=True) for c in trs[0].find_all(["th", "td"])]
        if len(header) < 3:
            continue
        away, home = header[0], header[2]
        for tr in trs[1:]:
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            if len(cells) < 3:
                continue
            away_txt, score, home_txt = cells[0], cells[1], cells[2]
            if away_txt.startswith("R:") or home_txt.startswith("R:"):
                continue  # inning run-summary row
            if away_txt:
                desc, batting, fielding, half = away_txt, away, home, "top"
            elif home_txt:
                desc, batting, fielding, half = home_txt, home, away, "bot"
            else:
                continue
            play_number += 1
            rows.append(
                {
                    "contest_id": cid,
                    "inning": inning_idx,
                    "inning_top_bot": half,
                    "batting": batting,
                    "fielding": fielding,
                    "play_number": play_number,
                    "score": score,
                    "description": desc,
                }
            )

    # extraction above, decomposition below -- one engine for HTML captures and
    # pre-extracted rows (the legacy R-era trees) alike
    return decompose_college_baseball_plays(rows, return_as_pandas=return_as_pandas)
