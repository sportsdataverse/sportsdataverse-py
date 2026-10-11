"""Stage 2 of the ESPN play-text audit: generalize play texts into templates and semantic forms.

Two levels, both deterministic:

* ``template`` keeps the exact phrasing and slots the entities: ``{P}`` player,
  ``{T}`` team, ``{N}`` number, ``{SPOT}`` yardline, ``{CLOCK}`` game clock,
  ``{J}`` jersey, ``{ORD}`` ordinal.
* ``form`` generalizes further so the *kinds* of text show: lowercase, one
  spelling per unit (``yds``), the 2025+ formation prefix as ``{FORMATION}``,
  a parenthesized player list as ``({PLAYERS})``, and no yardage sign.

Which words are names is decided from the corpus, not a word list:

* lowercase words are vocabulary when common, and kept literally when rare;
* a capitalized word is vocabulary when its lowercase form is (``Penalty``);
* single letters are initials and Jr/III are suffixes -> ``{P}``;
* a capitalized word seen in fewer than PHRASE_MIN_GAMES games is a name: no
  player's text spans that many games;
* one that frequent but concentrated on <= 5 teams is a team (``WESTRN``, ``KENTUCKY`` -> ``{T}``)
  when it spans many seasons, and one player's name when it spans few;
* one that frequent and always beside the same word on one side is a fixed
  phrase (``No Huddle-Shotgun``, ``Offensive holding``) -> vocabulary; common
  surnames and first names have hundreds of different neighbours -> ``{P}``;
* an ALL-CAPS token in most games of its seasons is vocabulary (``TD``).

    python tools/play_text_audit/templates.py --data <data dir>     # reads plays_raw.parquet
    python tools/play_text_audit/templates.py --self-test
"""

from __future__ import annotations

import argparse
import heapq
import os
import re
from collections import Counter, defaultdict

import polars as pl

ERAS = ((2004, 2006, "2004-06"), (2007, 2013, "2007-13"), (2014, 2019, "2014-19"), (2020, 2100, "2020-26"))
LOWER_VOCAB_FRAC = 0.003  # lowercase word in >= 0.3% of games is vocabulary
PHRASE_MIN_GAMES = 150  # more games than any one player's career text spans
TEAM_SHARE = 0.8  # one team (or, ALL-CAPS only, 5 teams: "KENTUCKY") in >= 80% of its games -> a team
FIXED_NEIGHBOURS = 8  # <= this many distinct neighbouring words on one side -> fixed phrase
CAPS_COVERAGE = 0.5  # ALL-CAPS token in >= half the games of the seasons it appears in -> vocabulary ("TD")
PLAYER_MAX_SEASONS = 6  # few teams AND few seasons -> one player's name, not a team
NEIGHBOUR_CAP = 50  # stop collecting neighbours past this; only "<= 8" matters

TOKEN = r"[^\W\d_](?:[^\W\d_]|['\-])*"  # Unicode letters ("José"); no dots: "M.Chiumento" -> M, Chiumento
WORD_OR_SLOT_RE = re.compile(r"\{[A-Z]+\}|" + TOKEN)  # slots first so {T}'s "T" is never a word
NAME_SUFFIXES = frozenset({"Jr", "JR", "Sr", "SR", "II", "III", "IV"})  # always part of a name
# Football words too rare (or too often capitalized) for the corpus rules to keep: always vocabulary.
FORCE_VOCAB = frozenset(
    [
        "SAFETY",
        "Safety",
        "safety",
        "PAT",
        "Spike",
        "SPIKE",
        "spike",
        "TWO-POINT",
        "Two-Point",
        "Two-point",
        "two-point",
        "onside",
        "Onside",
        "ONSIDE",
        "on-side",
        "On-side",
        "muffed",
        "Muffed",
        "MUFFED",
        "muff",
        "lat",
        "LAT",
    ]
)
# Vocabulary words that are also surnames: right after a first name they are the name ("Alondre Rush").
SURNAME_WORDS = frozenset({"Rush", "Short", "Good", "Long", "Ball", "Field", "Block"})  # punter Zachary Block
# A team name written as several words ("MIDDLE TENN", "EASTRN MICHIGAN") before "penalty" is one {T}.
QUALIFIERS = r"(?i:middle|central|centrl|eastern|eastrn|western|westrn|southern|northern|north|south|east|west|coastal)"

# Slot masks, applied in order with polars (Rust regex: no lookaround).
SLOTS = (
    (r"\(\s*\d{1,2}:\d{2}\s*\)|\b\d{1,2}:\d{2}\b", "{CLOCK}"),
    (r"#\d{1,2}\b", "{J}"),
    # formation markers (2025+ vendor "No Huddle-Shotgun", bracket codes "[NH]" / "[SG]") before names see them
    (r"\b(?:No Huddle-Shotgun|No Huddle|NO HUDDLE|Shotgun|SHOTGUN|SHOT|Pistol|PISTOL)\b|\[[A-Z]{1,6}\]", "{FORMATION}"),
    (r"\{T\}\s?\d{1,2}\b|\b[A-Z][A-Za-z&]{1,12}\d{1,2}\b", "{SPOT}"),  # "{T} 27", glued "USM45" / "VANDERBILT45"
    (r"\b(to|at|from|on|of|inside|past|near) the [A-Z][A-Za-z.&'\-]* \d{1,2}\b", "$1 the {SPOT}"),  # "the Sthrn 45"
    (r"\b(to|at|from|on|inside|past) the \d{1,2}( yard line|-yard line)?\b", "$1 the {SPOT}"),
    (r"\b(own|opponent's|opponents) \d{1,2}\b", "$1 {SPOT}"),
    (r"\b\d+(?:st|nd|rd|th|ST|ND|RD|TH)\b", "{ORD}"),
    (r"\d+", "{N}"),
)


def era_expr() -> pl.Expr:
    expr = pl.lit("pre-2004")
    for lo, hi, name in reversed(ERAS):
        expr = pl.when(pl.col("season").is_between(lo, hi)).then(pl.lit(name)).otherwise(expr)
    return expr


def slot_mask(col: str = "masked") -> pl.Expr:
    e = pl.col(col).str.replace_all(r"\s+", " ").str.strip_chars()
    for pat, rep in SLOTS:
        e = e.str.replace_all(pat, rep)
    return e


def _top(teams: Counter, k: int) -> int:
    """Games covered by the k most frequent teams (null team ignored)."""
    return sum(heapq.nlargest(k, (c for t, c in teams.items() if t)))


def build_vocab(rows, n_games: int) -> tuple[frozenset[str], frozenset[str]]:
    """rows: (game_id, season, home, away, slotted) sorted by game_id -> (vocabulary, team tokens)."""
    ngames: Counter[str] = Counter()
    last: dict[str, int] = {}
    teams: dict[str, Counter] = defaultdict(Counter)
    prev: dict[str, set] = defaultdict(set)
    nxt: dict[str, set] = defaultdict(set)
    seasons: dict[str, set] = defaultdict(set)
    season_games: Counter[int] = Counter()
    last_gid = None
    for gid, season, home, away, s in rows:
        if gid != last_gid:
            season_games[season] += 1
            last_gid = gid
        toks = WORD_OR_SLOT_RE.findall(s)
        words = [t for t in toks if t[0] != "{"]  # neighbours are words: skip slots and punctuation
        for i, w in enumerate(words):
            if not w.islower():
                if len(prev[w]) <= NEIGHBOUR_CAP:
                    prev[w].add(words[i - 1] if i else "^")
                if len(nxt[w]) <= NEIGHBOUR_CAP:
                    nxt[w].add(words[i + 1] if i + 1 < len(words) else "$")
        for w in toks:
            if not w[0].isalpha():
                continue
            if last.get(w) != gid:
                last[w] = gid
                ngames[w] += 1
                if not w.islower():
                    teams[w][home] += 1
                    teams[w][away] += 1
                    seasons[w].add(season)

    lower_min = max(3, round(LOWER_VOCAB_FRAC * n_games))
    lower_vocab = {w for w, n in ngames.items() if w.islower() and n >= lower_min}
    vocab, team_tok = set(lower_vocab), set()
    for w, n in ngames.items():
        if w.islower() or len(w) == 1 or w in NAME_SUFFIXES:
            continue
        if w.lower() in lower_vocab:  # Penalty, PENALTY, GOOD
            vocab.add(w)
        elif n < PHRASE_MIN_GAMES:
            continue
        elif _top(teams[w], 1) / n >= TEAM_SHARE or (w.isupper() and _top(teams[w], 5) / n >= TEAM_SHARE):
            if len(seasons[w]) > PLAYER_MAX_SEASONS:  # a player spans few seasons; a team abbreviation all of them
                team_tok.add(w)
        elif min(len(prev[w]), len(nxt[w])) <= FIXED_NEIGHBOURS:
            vocab.add(w)
        elif w.isupper() and n / sum(season_games[x] for x in seasons[w]) >= CAPS_COVERAGE:
            vocab.add(w)  # "for a TD": too many contexts for the neighbour test, but in most games of its era
    vocab |= FORCE_VOCAB
    return frozenset(vocab), frozenset(team_tok - FORCE_VOCAB)


def name_mask(s: str, vocab: frozenset[str], team_tok: frozenset[str] = frozenset()) -> str:
    """Words -> {P}/{T} unless vocabulary; runs collapse ("J.J. Smith Jr." -> {P}).

    A surname-word right after a name is part of the name ("Alondre Rush"); "Penalty", "Kick" are not."""
    s = re.sub(r"\b(Jr|JR|Sr|SR)\.", r"\1", s)  # drop the suffix's period only; a sentence-final one stays
    parts: list[str] = []
    pos, prev_name = 0, False
    for m in WORD_OR_SLOT_RE.finditer(s):
        w, gap = m.group(0), s[pos : m.start()]
        if w[0] == "{" or w.islower() or w in vocab and not (prev_name and gap == " " and w in SURNAME_WORDS):
            out = w
        else:
            out = "{T}" if w in team_tok else "{P}"
        parts += [gap, out]
        prev_name, pos = out == "{P}", m.end()
    s = "".join(parts) + s[pos:]
    s = re.sub(r"\{P\}(?:[\s.,']*\{P\})+", "{P}", s)
    s = re.sub(r"\{T\}(?:\s\{T\})+", "{T}", s)
    return re.sub(r"(?:\b" + QUALIFIERS + r"\b|\{P\}) \{T\}(?= (?i:penalty)\b)", "{T}", s)


FORM_RULES = (
    (r"\b(?:yds?|yards?)\b\.?", "yds"),
    (r"\b(?:no huddle-shotgun|no huddle|shotgun)\b", "{formation}"),
    (r"\((?:\{j\} )?\{p\}(?:\s*[;,]\s*(?:\{j\} )?\{p\})*\)", "({players})"),
    (r"-\{n\}", "{n}"),
    (r"\s+([.,;])", r"\1"),
    (r"[.\s]+$", ""),
)


def to_form(template: str) -> str:
    f = template.lower()
    for pat, rep in FORM_RULES:
        f = re.sub(pat, rep, f)
    f = re.sub(r"\s+", " ", f).strip()
    return re.sub(r"\{([a-z]+)\}", lambda m: "{" + m.group(1).upper() + "}", f)  # slots read as slots


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--data", help="dir holding plays_raw.parquet; outputs land beside it")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args(argv)
    if a.self_test:
        return _selftest()

    raw = pl.read_parquet(os.path.join(a.data, "plays_raw.parquet"))
    n_games = raw.select(pl.col("game_id").n_unique()).item()
    raw = raw.with_columns(slot_mask().alias("slotted"))
    per_game = raw.select("game_id", "season", "home_team", "away_team", "slotted").unique().sort("game_id")
    vocab, team_tok = build_vocab(per_game.iter_rows(), n_games)

    # name_mask depends only on the slotted text, so run it once per distinct slotted string
    uniq = raw.select("slotted").unique()
    uniq = uniq.with_columns(
        pl.col("slotted")
        .map_elements(lambda s: name_mask(s, vocab, team_tok), return_dtype=pl.String)
        .alias("template")
    )
    tmpl = uniq.select(pl.col("template").unique())
    tmpl = tmpl.with_columns(pl.col("template").map_elements(to_form, return_dtype=pl.String).alias("form"))
    plays = (
        raw.join(uniq, on="slotted", how="left")
        .join(tmpl, on="template", how="left")
        .with_columns(era_expr().alias("era"))
        .drop("masked", "slotted")
    )
    plays.write_parquet(os.path.join(a.data, "plays.parquet"))
    with open(os.path.join(a.data, "vocab.txt"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(sorted(vocab)) + "\n# team tokens\n" + "\n".join(sorted(team_tok)))
    print(
        f"{n_games:,} games, {plays.height:,} plays -> {plays['template'].n_unique():,} templates -> "
        f"{plays['form'].n_unique():,} forms (vocab {len(vocab):,}, team tokens {len(team_tok):,})"
    )
    return 0


def _selftest() -> int:
    vocab = frozenset(
        [
            "run",
            "for",
            "yds",
            "to",
            "the",
            "a",
            "down",
            "rushed",
            "up",
            "middle",
            "yards",
            "rush",
            "left",
            "gain",
            "End",
            "Of",
            "Play",
            "Penalty",
            "false",
            "start",
            "pass",
            "complete",
            "incomplete",
            "Extra",
            "point",
            "by",
            "is",
            "good",
            "penalty",
            "on",
            "yard",
            "No",
            "Huddle-Shotgun",
        ]
    )
    cases = {
        "Reggie Bush ({T}) rushed up the middle for 12 yards.": (
            "{P} ({T}) rushed up the middle for {N} yards.",
            "{P} ({T}) rushed up the middle for {N} yds",
        ),
        "Lenard Tillery run for 26 yds to the Sthrn 45 for a 1ST down": (
            "{P} run for {N} yds to the {SPOT} for a {ORD} down",
            "{P} run for {N} yds to the {SPOT} for a {ORD} down",
        ),
        "(12:37) No Huddle-Shotgun #9 G.Crowder rush left for 14 yards gain to the USM45, End Of Play.": (
            "{CLOCK} {FORMATION} {J} {P} rush left for {N} yards gain to the {SPOT}, End Of Play.",
            "{CLOCK} {FORMATION} {J} {P} rush left for {N} yds gain to the {SPOT}, end of play",
        ),
        "{T} Penalty, false start (Dillon Beard) to the Sthrn 40": (
            "{T} Penalty, false start ({P}) to the {SPOT}",
            "{T} penalty, false start ({PLAYERS}) to the {SPOT}",
        ),
        "15 yard penalty on {T}.": ("{N} yard penalty on {T}.", "{N} yds penalty on {T}"),
    }
    df = pl.DataFrame({"masked": list(cases)}).with_columns(slot_mask().alias("slotted"))
    for raw_text, slotted in df.select("masked", "slotted").iter_rows():
        want_t, want_f = cases[raw_text]
        got_t = name_mask(slotted, vocab)
        assert got_t == want_t, (raw_text, got_t)
        assert to_form(got_t) == want_f, (raw_text, to_form(got_t))
    assert name_mask("pass by J.J. Smith Jr. to the", vocab) == "pass by {P} to the"
    v2 = vocab | {"Rush", "PENALTY", "MIDDLE", "returned", "holding", "Penalty"}
    assert name_mask("returned by Alondre Rush for {N} yds", v2) == "returned by {P} for {N} yds"
    v3 = v2 | {"Kick", "Run", "Yd", "pass", "from"}
    assert name_mask("({P} {N} Yd Run (Matthew Quinn Kick)", v3).endswith("({P} Kick)")
    assert name_mask("TEXASSAN Penalty, False Start", v2 | {"False", "Start"}) == "{P} Penalty, False Start"
    assert name_mask("{T} {T} PENALTY, holding", v2) == "{T} PENALTY, holding"
    assert name_mask("MIDDLE {T} PENALTY, holding", v2) == "{T} PENALTY, holding"
    assert (
        name_mask("pass incomplete to Travin Dural. Penalty {T} holding", v2)
        == "pass incomplete to {P}. Penalty {T} holding"
    )
    assert _top(Counter({"A": 9, "B": 8, None: 50, "C": 1}), 2) == 17
    assert name_mask("WESTRN Penalty, muffed", vocab, frozenset({"WESTRN"})) == "{T} Penalty, muffed"

    VERBS = ["run", "pass", "kick", "punt", "rush", "catch", "sack", "fumble", "return", "block"]
    # 400 games, team W in 160 of them: "Offensive holding" is a fixed phrase everywhere; "WESTRN"
    # shows only in W's games -> a team; "Smith" follows 400 different first names -> a name;
    # "Bob" is in 3 games -> a name.
    rows = []
    for g in range(400):
        first = f"Q{chr(97 + g % 26)}{chr(97 + g // 26)}"
        text = f"Offensive holding on {first} Smith {VERBS[g % 10]}" + (" WESTRN" if g < 160 else "")
        rows.append((g, 2004 + g % 23, f"H{g}", "W" if g < 160 else f"A{g}", text))
    rows += [(400 + g, 2010, f"H{g}", f"A{g}", "Offensive holding on Bob") for g in range(3)]
    v, t = build_vocab(sorted(rows), 403)
    assert "Offensive" in v and "Bob" not in v and "Smith" not in v, v
    assert t == {"WESTRN"}, t
    print("self-test ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
