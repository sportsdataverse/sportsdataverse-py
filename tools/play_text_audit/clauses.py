"""Stage 2b of the ESPN play-text audit: split each form into clauses and tag what each clause says.

A play text is a chain of clauses -- an action, then optional tackle, fumble,
recovery, return, penalty and enforcement clauses -- so the long tail of forms is
mostly combinations. Splitting turns ~48k forms into a few thousand clause forms,
and the clause tags are what the parser diff (stage 3) checks the parser against.

Boundaries: top-level ``,`` ``.`` ``;`` (never inside parentheses), plus the
places a feed glues clauses with no punctuation -- a penalty or fumble appended to
an action, vendor ``fumbled by`` / ``recovered by`` / ``{J} {P} return``, a
``for a TD`` / ``for a {ORD} down`` / ``for a touchback`` outcome, the
``({P} kick)`` try, a trailing tackler list. Merges undo splits that cut one fact:
``{T} penalty, false start (...)``, a vendor completion's ``, for {N} yds``,
``two-point conversion attempt, {P} pass ... good``, a 2004 ``{P}, fumbled at``.

Each clause gets a ``kind`` (its main action, first match in KINDS order) and the
full set of ``tags`` it carries ("kickoff for 60 yds returned by X" is a kickoff
AND a return). A form-level pass adds what a clause implies but never says: the
2014+ scoring summary ``{P} {N} yds pass from {P} ({P} kick)`` is a completed
touchdown pass with a made kick; a ``{P} for {N} yds`` after a recovery is a return.

    python tools/play_text_audit/clauses.py --data <data dir>   # reads plays.parquet
    python tools/play_text_audit/clauses.py --self-test
"""

from __future__ import annotations

import argparse
import os
import re

import polars as pl

GLUE_RE = re.compile(
    r"\s(?="
    r"(?:\{[PT]\}|team) (?:fumbled|fumbles|penalty)\b"  # "...to the {SPOT} {P} fumbled" / "{T} penalty"
    r"|\{T\} fumble by\b"
    r"|(?:fumbled|muffed) by\b|recovered by\b|\{J\} \{P\} return\b"  # vendor chains
    r"|for a (?:td|touchdown|\{ORD\} down|touchback|safety)\b"
    r"|\((?:\{P\} )?(?:kick|pat|run|pass)[^)]*\)$"  # "({P} kick)" try after a TD
    r"|\{J\} \{P\} kick attempt\b"
    r")"
    r"|(?<!\{[PT]\})(?<!team)\s(?=penalty (?:\{[PT]\}|before\b|after\b))"  # vendor "... penalty {T} holding"
    r"|(?<!\sa)\s(?=touchdown\b)"  # vendor "to the {SPOT} touchdown" (not "for a touchdown")
)
GAIN_TAIL_RE = re.compile(r"^for (?:\{N\} yds|no gain|a loss of|loss of)")  # vendor ", for {N} yds to the {SPOT}"
SCORING_SUMMARY_RE = re.compile(
    r"^\{p\} \{n\} yds (?:(pass) from|(run)|(?:(fumble|interception|kickoff|punt|blocked punt|blocked (?:fg|field goal)"
    r"|missed (?:fg|field goal)) return))\b"
)

# Tag regexes, matched on the lowercased clause; a clause carries every tag that matches.
TAGS = {
    "timeout": r"\btimeout\b",
    "end_period": r"\bend of (?:the )?(?:\{ord\} |\{n\} )?(?:quarter|half|game|regulation|period|overtime)\b|^end (?:quarter|half|game)\b",
    "penalty": r"\bpenalt(?:y|ies)\b",
    "declined": r"\bdeclined\b",
    "offsetting": r"\boff-?setting\b",
    "no_play": r"\bno play\b",
    "kickoff": r"\bkick-?off\b|\bon-?side\b|\bkicks? off\b",
    "onside": r"\bon-?side\b",
    "punt": r"\bpunt(?:s|ed)?\b",
    "field_goal": r"\bfg\b|\bfield goal\b",
    "extra_point": r"\bextra point\b|\bpat\b|\bkick attempt\b|^\((?:\{p\} )?kick\b|\(\{p\} kick\b",
    "two_point": r"\btwo[- ]point\b|\{n\}[- ]?(?:pt|point)\b|\bconversion\b|^\((?:\{p\} )?(?:run|pass)\b|\b(?:rush|run|pass) attempt (?:failed|good)\b",  # 2007-13 try rows
    "defensive_2pt": r"\bdefensive (?:\{p\}|two[- ]point|\{n\}[- ]?point|conversion)",
    "sack": r"\bsack(?:ed|s)?\b",
    "interception": r"\bintercept(?:ed|ion|s)?\b",
    "pass_incomplete": r"\bincomplete\b",
    "pass_complete": r"\bcomplete\b|\bcaught\b",
    "pass": r"\bpass(?:ed|es)?\b(?! interference)",
    "rush": r"\b(?:run|runs|rush|rushed|rushes|rushing|scrambles?|scrambled|keeper)\b",
    "kneel": r"\bkneel|\btakes a knee\b",
    "spike": r"\bspike",
    "fumble": r"\bfumble[ds]?\b",
    "muff": r"\bmuff(?:ed|s)?\b",
    "forced": r"\bforced by\b",
    "recovered": r"\brecover(?:ed|s|y)\b",
    "return": r"\breturn(?:s|ed)?\b",
    "lateral": r"\blateral|\blat\b",
    "tackle": r"\btackled by\b|^\(\{players\}\)$",
    "breakup": r"\bbroken up\b|\bdefended\b|\bdeflected\b|\bbroke up\b",
    "hurry": r"\bhurr(?:ied|y)\b",
    "touchdown": r"\btouchdown\b|\btd\b",
    "safety": r"\bsafety\b",
    "touchback": r"\btouchback\b",
    "fair_catch": r"\bfair catch\b",
    "out_of_bounds": r"\bout[- ]of[- ]bounds\b|\bout of bounds\b",
    "downed": r"\bdowned\b",
    "first_down": r"\{ord\} down\b|\bfirst down\b",
    "blocked": r"(?<!illegal )(?<!chop )\bblock(?:ed|s)?\b(?! in (?:the )?back)",
    "good": r"(?<!no )\bgood\b",
    "missed": r"\bno good\b|\bmiss(?:ed|es)?\b|\bfailed\b|\bwide (?:left|right)\b",
    "loss": r"\bloss of\b|\bfor a loss\b",
    "no_gain": r"\bno gain\b",
    "yards": r"\{n\} yds\b|\byds \{n\}\b",
    "review": r"\breview|\bupheld\b|\boverturned\b|\bconfirmed\b|\bruling\b|\bchallenge",
    "injury": r"\binjur",
    "clock": r"^clock \{clock\}$|\{clock\} remaining\b",
    "gain": r"\bfor \{n\} yds\b|\bfor no gain\b|\bfor (?:a )?loss\b|\{n\} yds loss\b|^\{n\} yds$",
    "spot": r"^(?:to|at) (?:the )?\{spot\}$",
    "end_of_play": r"^end of play$",
    "turnover_on_downs": r"\bturnover on downs\b",
    "advance": r"\badvanc(?:ed|es|e)\b",
    "accepted": r"\baccepted\b",
}
TAG_RE = {k: re.compile(v) for k, v in TAGS.items()}
# A clause's kind: the first of these tags it carries. Actions before outcomes.
KINDS = (
    "penalty", "timeout", "end_period", "kickoff", "punt", "field_goal", "extra_point", "two_point", "sack",
    "interception", "pass_incomplete", "pass_complete", "pass", "rush", "kneel", "spike", "fumble", "muff",
    "forced", "recovered", "return", "lateral", "tackle", "breakup", "hurry", "touchdown", "safety", "touchback",
    "fair_catch", "out_of_bounds", "downed", "first_down", "no_play", "declined", "offsetting", "review",
    "injury", "clock", "turnover_on_downs", "advance", "gain", "spot", "end_of_play", "accepted",
)  # fmt: skip


def split_clauses(form: str) -> list[str]:
    parts, cur, depth = [], [], 0
    for i, ch in enumerate(form):
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth = max(0, depth - 1)
        if depth == 0 and ch in ",.;" and (i + 1 == len(form) or form[i + 1] == " "):
            parts.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    parts.append("".join(cur))
    segs: list[str] = []
    for p in parts:
        for s in GLUE_RE.split(p):
            s = s.strip()
            if not s:
                continue
            # a trailing tackler list on an action is its own clause: "... to the {SPOT} ({PLAYERS})"
            m = re.match(r"^(.*\S)\s(\(\{PLAYERS\}\))$", s)
            if m and "penalty" not in s:
                segs.extend([m.group(1), m.group(2)])
            else:
                segs.append(s)
    merged: list[str] = []
    for s in segs:
        prev = merged[-1] if merged else ""
        if (
            re.fullmatch(r"(?:(?:\{[TP]\}|team) )?penalty", prev)
            or prev
            and GAIN_TAIL_RE.match(s)
            and not TAG_RE["penalty"].search(prev)
            or re.search(r"conversion attempt$", prev)
        ):  # "{T} penalty" + ", false start (...)"
            merged[-1] = f"{prev}, {s}"
        elif prev == "{P}" and s.startswith(("fumbled", "fumbles")):  # 2004 "{P}, fumbled at the {SPOT}"
            merged[-1] = f"{prev} {s}"
        else:
            merged.append(s)
    return merged


def _kind(tags: list[str]) -> str:
    return next((k for k in KINDS if k in tags), "other")


def tag_clause(seg: str) -> tuple[str, list[str]]:
    low = seg.lower()
    tags = [k for k, rx in TAG_RE.items() if rx.search(low)]
    if re.search(r"\bshort\b", low) and ("field_goal" in tags or "extra_point" in tags):
        tags.append("missed")  # a kick that falls short; "pass complete short right" is a direction
    return _kind(tags), tags


def tag_form(segs: list[str]) -> list[tuple[str, list[str]]]:
    """Tag each clause, then add what the clause chain implies but never says."""
    out = [list(tag_clause(s)) for s in segs]
    for i, s in enumerate(segs):
        low, tags = s.lower(), out[i][1]
        m = SCORING_SUMMARY_RE.match(low)
        if m and i + 1 < len(segs) and segs[i + 1].startswith("("):  # 2014+ scoring summary + try trailer
            extra = ["touchdown"]
            if m.group(1):
                extra += ["pass_complete", "pass"]
            elif m.group(2):
                extra += ["rush"]
            else:
                extra += (
                    ["return"]
                    + (["fumble"] if m.group(3) == "fumble" else [])
                    + (["interception"] if m.group(3) == "interception" else [])
                )
            tags.extend(t for t in extra if t not in tags)
        is_try = low.startswith("(") and ("extra_point" in tags or "two_point" in tags)
        if is_try and not {"missed", "good"} & set(tags) and i > 0 and "touchdown" in out[i - 1][1]:
            tags.append("good")  # "({P} kick)" after a TD is a made try; failures say so
        prev_tags = out[i - 1][1] if i else []
        if (
            ("recovered" in prev_tags or "blocked" in prev_tags or "interception" in prev_tags)
            and re.match(r"^(?:\{p\}|team)(?: \{t\})? for\b|^advanced for\b", low)
            and "return" not in tags
        ):
            tags.append("return")  # "..., recovered by {P}, {P} for {N} yds" / "advanced for no gain"
        out[i][0] = _kind(tags)
    return [(k, t) for k, t in out]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--data")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args(argv)
    if a.self_test:
        return _selftest()

    plays = pl.read_parquet(os.path.join(a.data, "plays.parquet"), columns=["form"])
    forms = plays.group_by("form").len(name="n")
    recs = []
    for (form,) in forms.select("form").iter_rows():
        segs = split_clauses(form)
        for i, (seg, (kind, tags)) in enumerate(zip(segs, tag_form(segs), strict=True)):
            recs.append((form, i, len(segs), seg, kind, tags))
    cl = pl.DataFrame(
        recs,
        schema={"form": pl.String, "idx": pl.Int32, "n_clauses": pl.Int32, "clause": pl.String,
                "kind": pl.String, "tags": pl.List(pl.String)},
        orient="row",
    )  # fmt: skip
    cl.write_parquet(os.path.join(a.data, "clauses.parquet"))

    w = cl.join(forms, on="form")
    by_clause = w.group_by("clause").agg(pl.col("n").sum()).sort("n", descending=True)
    by_clause = by_clause.with_columns((pl.col("n").cum_sum() / pl.col("n").sum()).alias("cum"))
    other = w.filter(pl.col("kind") == "other")
    print(
        f"{forms.height:,} forms -> {by_clause.height:,} clause forms; "
        + ", ".join(
            f"{int(q * 100)}% of clauses in {by_clause.filter(pl.col('cum') <= q).height + 1:,}"
            for q in (0.5, 0.9, 0.95, 0.99)
        )
        + f"; untagged ('other') {other['n'].sum() / w['n'].sum():.2%} of clause occurrences"
    )
    return 0


def _selftest() -> int:
    def kinds(form: str) -> list[tuple[str, str]]:
        segs = split_clauses(form)
        return [(s, k) for s, (k, _) in zip(segs, tag_form(segs), strict=True)]

    def tags(form: str, i: int) -> list[str]:
        segs = split_clauses(form)
        return tag_form(segs)[i][1]

    cases = {
        "{P} rush for {N} yds to the {SPOT}, tackled by {P}": [
            ("{P} rush for {N} yds to the {SPOT}", "rush"),
            ("tackled by {P}", "tackle"),
        ],
        "{P} run for {N} yds to the {SPOT} for a {ORD} down {T} penalty, personal foul ({PLAYERS}) to the {SPOT} for a {ORD} down": [
            ("{P} run for {N} yds to the {SPOT}", "rush"),
            ("for a {ORD} down", "first_down"),
            ("{T} penalty, personal foul ({PLAYERS}) to the {SPOT}", "penalty"),
            ("for a {ORD} down", "first_down"),
        ],
        "{P} run for a loss of {N} yds to the {SPOT} {P} fumbled, recovered by {T} {P}": [
            ("{P} run for a loss of {N} yds to the {SPOT}", "rush"),
            ("{P} fumbled", "fumble"),
            ("recovered by {T} {P}", "recovered"),
        ],
        "{P} pass complete to {P} for {N} yds for a td, ({P} kick)": [
            ("{P} pass complete to {P} for {N} yds", "pass_complete"),
            ("for a td", "touchdown"),
            ("({P} kick)", "extra_point"),
        ],
        "penalty {T} false start ({PLAYERS}) {N} yds from {SPOT} to {SPOT}. no play": [
            ("penalty {T} false start ({PLAYERS}) {N} yds from {SPOT} to {SPOT}", "penalty"),
            ("no play", "no_play"),
        ],
        "{CLOCK} {FORMATION} {J} {P} rush middle for {N} yds gain to the {SPOT} ({PLAYERS})": [
            ("{CLOCK} {FORMATION} {J} {P} rush middle for {N} yds gain to the {SPOT}", "rush"),
            ("({PLAYERS})", "tackle"),
        ],
        "{P} kickoff for {N} yds returned by {P} for {N} yds to the {SPOT}": [
            ("{P} kickoff for {N} yds returned by {P} for {N} yds to the {SPOT}", "kickoff")
        ],
        "timeout {T}, clock {CLOCK}": [("timeout {T}", "timeout"), ("clock {CLOCK}", "clock")],
        "{T} penalty {N} yds holding accepted, no play": [
            ("{T} penalty {N} yds holding accepted", "penalty"),
            ("no play", "no_play"),
        ],
        # review S1: a vendor penalty mid-text splits off the action
        "{CLOCK} {J} {P} rush middle for {N} yds gain to the {SPOT} penalty {T} holding ({PLAYERS}) {N} yds from {SPOT} to {SPOT}": [
            ("{CLOCK} {J} {P} rush middle for {N} yds gain to the {SPOT}", "rush"),
            ("penalty {T} holding ({PLAYERS}) {N} yds from {SPOT} to {SPOT}", "penalty"),
        ],
        # review S2: "{N} yds penalty on {T}" stays one clause
        "{N} yds penalty on {T}": [("{N} yds penalty on {T}", "penalty")],
        # review S3: a vendor completion keeps its yardage
        "{J} {P} pass complete short right to {J} {P} caught at {SPOT}, for {N} yds to the {SPOT}": [
            (
                "{J} {P} pass complete short right to {J} {P} caught at {SPOT}, for {N} yds to the {SPOT}",
                "pass_complete",
            )
        ],
        # review S4: vendor fumble chain
        "{J} {P} rush right for {N} yds gain to the {SPOT} fumbled by {J} {P} at {SPOT} recovered by {T} {J} {P} at {SPOT}": [
            ("{J} {P} rush right for {N} yds gain to the {SPOT}", "rush"),
            ("fumbled by {J} {P} at {SPOT}", "fumble"),
            ("recovered by {T} {J} {P} at {SPOT}", "recovered"),
        ],
        # review S5: the two-point outcome stays with the attempt
        "two-point conversion attempt, {P} pass to {P} good": [
            ("two-point conversion attempt, {P} pass to {P} good", "two_point")
        ],
        # review S7: "nullified by penalty" does not swallow the next clause
        "touchdown nullified by penalty, clock {CLOCK}": [
            ("touchdown nullified by penalty", "penalty"),
            ("clock {CLOCK}", "clock"),
        ],
        # review S8
        "{P}, fumbled at the {SPOT}, recovered by {P} at the {SPOT}": [
            ("{P} fumbled at the {SPOT}", "fumble"),
            ("recovered by {P} at the {SPOT}", "recovered"),
        ],
    }
    for form, want in cases.items():
        got = kinds(form)
        assert got == want, (form, got)
    # tag fixes
    assert "missed" not in tags(
        "{J} {P} pass complete short right to {J} {P} caught at {SPOT}, for {N} yds to the {SPOT}", 0
    )
    assert "missed" in tags("{P} {N} yds field goal attempt short", 0)
    assert "good" not in tags("{P} field goal attempt from {N} yds no good", 0)
    assert "pass" not in tags("{T} penalty {N} yds pass interference on {P} accepted", 0)
    assert "blocked" not in tags("{T} penalty, illegal block ({PLAYERS}) to the {SPOT}", 0)
    assert "onside" in tags("{P} on-side kick recovered by {T} at the {SPOT}", 0)
    # implied facts in 2014+ scoring summaries
    t = tag_form(split_clauses("{P} {N} yds pass from {P} ({P} kick)"))
    assert t[0][0] == "pass_complete" and "touchdown" in t[0][1] and "good" in t[1][1], t
    t = tag_form(split_clauses("{P} {N} yds fumble return ({P} pat failed)"))
    assert {"touchdown", "return", "fumble"} <= set(t[0][1]) and "good" not in t[1][1] and "missed" in t[1][1], t
    t = tag_form(split_clauses("{P} fumbled, recovered by {P} at the {SPOT}, {P} for {N} yds, to the {SPOT}"))
    assert "return" in t[2][1], t
    print("self-test ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
