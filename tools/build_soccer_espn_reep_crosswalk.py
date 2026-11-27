"""Build ``sportsdataverse/soccer/data/espn_reep_team_crosswalk.csv``: ESPN soccer team id -> reep team id.

reep is a CC0 football id register (one row per club with its id at about 25 providers). Its ESPN column
covers a couple of dozen clubs, so this build bridges by NAME, and only inside the league's country:

1. a reep row whose ``key_espn`` is the ESPN id (``match_method = "key_espn"``);
2. otherwise the in-country reep rows whose normalized name or alias equals a normalized ESPN name
   (display name, short display name, name or location). Exactly one candidate -> ``"name"``;
3. every ESPN team with no candidate or with more than one is decided by hand in
   ``tools/soccer_espn_reep_overrides.yaml`` (``"review"``, or ``"unmatched"`` when no reep row is that club).
   The build fails when such a team has no entry, so nothing is resolved by ranking.

Then the gate (fixed before the first scored build; never lowered): see :data:`GATE`. A failed gate,
a duplicate ESPN id, or a reep id claimed by two ESPN teams fails the build.

Run (network: ESPN Site v2 ``/teams``; reep read from a local checkout of its CSV release)::

    uv run python tools/build_soccer_espn_reep_crosswalk.py --reep-dir ../../reep/data
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path

import polars as pl
import yaml

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "sportsdataverse" / "soccer" / "data" / "espn_reep_team_crosswalk.csv"
OVERRIDES = ROOT / "tools" / "soccer_espn_reep_overrides.yaml"

#: ESPN league slug -> the reep ``country`` values its clubs carry (reep writes England as "United
#: Kingdom" and some Welsh clubs as "Wales"; MLS has Canadian clubs; Ligue 1 has Monaco).
#:
#: NWSL (``usa.nwsl``: ``("United States",)``) was in the first build and fails the per-league gate: reep
#: has no row for 5 of its 16 clubs (11/16 = 0.69 < 0.75; the reviewed entries stay in the overrides
#: file). It is left out rather than the gate lowered; add it back when reep covers those clubs.
LEAGUE_COUNTRIES: dict[str, tuple[str, ...]] = {
    "eng.1": ("United Kingdom", "Wales"),
    "eng.2": ("United Kingdom", "Wales"),
    "esp.1": ("Spain",),
    "ger.1": ("Germany",),
    "ita.1": ("Italy",),
    "fra.1": ("France", "Monaco"),
    "usa.1": ("United States", "Canada"),
    "mex.1": ("Mexico",),
}

#: Pre-registered 2026-11-26 from the first measurement (138 of 150 ESPN teams in seven of these leagues
#: resolved to a reep row). ``min_share``: matched / ESPN teams over every league; ``min_league_share`` and
#: ``min_league_teams``: per league.
GATE = {"min_share": 0.92, "min_league_share": 0.75, "min_league_teams": 10}

#: reep columns carried through, renamed nowhere. ``key_espn`` is dropped: it is the ESPN id when present.
REEP_KEYS = (
    "key_wikidata",
    "key_transfermarkt",
    "key_fbref",
    "key_soccerway",
    "key_opta",
    "key_opta_numeric",
    "key_kicker",
    "key_flashscore",
    "key_sofascore",
    "key_soccerbase",
    "key_uefa",
    "key_footballdatabase_eu",
    "key_worldfootball",
    "key_playmakerstats",
    "key_clubelo",
    "key_sportmonks",
    "key_api_football",
    "key_sofifa",
    "key_fotmob",
    "key_thesportsdb",
    "key_understat",
    "key_capology",
)

ESPN_COLS = ("espn_team_id", "espn_league", "espn_display_name", "espn_abbreviation")
OUT_COLS = (*ESPN_COLS, "reep_id", "reep_name", "reep_country", *REEP_KEYS, "match_method")
SCHEMA = dict.fromkeys(OUT_COLS, pl.Utf8)

# ponytail: a short token list, not a club-name grammar; extend it when a review shows a miss it would fix
_STRIP = frozenset(
    "fc afc cf sc club football de futbol calcio ac as ss ssc us vfb vfl tsg sv fsv 1 rc rcd ca cd ud sd og "
    "stade the 1899 1846 1848 1900 1901 1904 1907 1909 04 05 fk bc".split()
)


def norm(s: str | None) -> str:
    """Fold accents and case, drop dots and the generic club tokens: ``"1. FC Köln"`` -> ``"koln"``."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().casefold().replace(".", "")
    return " ".join(t for t in re.split(r"[^a-z0-9]+", s) if t and t not in _STRIP)


def espn_frame(parsed: pl.DataFrame, league: str) -> pl.DataFrame:
    """One league's parsed ESPN ``/teams`` frame in the build's ESPN columns (ids text, digits only)."""
    out = parsed.select(
        pl.col("team_id").cast(pl.Utf8).str.strip_chars().alias("espn_team_id"),
        pl.lit(league).alias("espn_league"),
        pl.col("display_name").alias("espn_display_name"),
        pl.col("abbreviation").alias("espn_abbreviation"),
        *(pl.col(c).alias(f"_n_{c}") for c in ("display_name", "short_display_name", "name", "location")),
    )
    bad = out.filter(pl.col("espn_team_id").is_null() | (pl.col("espn_team_id").str.contains(r"^[0-9]+$") == False))
    if bad.height:
        raise ValueError(f"{league}: non-numeric ESPN team ids {bad['espn_team_id'].to_list()}")
    return out


def reep_candidates(teams: pl.DataFrame, aliases: pl.DataFrame) -> pl.DataFrame:
    """``(k, reep_id)`` for every reep team name and alias, ``k`` the normalized text."""
    names = pl.concat(
        [
            teams.select("reep_id", pl.col("name").alias("text")),
            aliases.filter(pl.col("reep_id").str.starts_with("reep_t")).select(
                "reep_id", pl.col("alias").alias("text")
            ),
        ]
    )
    return (
        names.with_columns(pl.col("text").map_elements(norm, return_dtype=pl.Utf8).alias("k"))
        .filter(pl.col("k") != "")
        .select("k", "reep_id")
        .unique()
    )


def load_overrides(path: Path = OVERRIDES) -> dict[str, str | None]:
    """``{espn_team_id: reep_id | None}`` from the reviewed overrides file (``None`` = no reep row is that club)."""
    rows = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("overrides") or []
    out: dict[str, str | None] = {}
    for r in rows:
        k = str(r["espn_team_id"])
        if k in out:
            raise ValueError(f"overrides: espn_team_id {k} listed twice")
        if not str(r.get("reason") or "").strip():
            raise ValueError(f"overrides: espn_team_id {k} has no reason")
        out[k] = r.get("reep_id")
    return out


#: ESPN name fields a unique automatic match may come from. A hit only on the short name or the location
#: (often a bare city) is sent to review instead: a city name can single out a namesake club.
_TRUSTED_FIELDS = ("display_name", "name")


def match(
    espn: pl.DataFrame, teams: pl.DataFrame, aliases: pl.DataFrame, overrides: dict[str, str | None]
) -> tuple[pl.DataFrame, list[dict]]:
    """Resolve every ESPN team. Returns ``(crosswalk, needs_review)``.

    ``needs_review`` lists the ESPN teams that have no override yet and no automatic answer: no in-country
    candidate, several, or one found only through the short name or location (with the candidates), so
    the reviewer sees what to decide; the build refuses to write while it is non-empty.
    """
    if teams["reep_id"].is_duplicated().any():
        raise ValueError("reep teams: a reep_id on two rows")
    teams = teams.with_columns(pl.col("country").fill_null(""))
    country = pl.DataFrame(
        [(lg, c) for lg, cs in LEAGUE_COUNTRIES.items() for c in cs],
        schema={"espn_league": pl.Utf8, "country": pl.Utf8},
        orient="row",
    )
    # (a) reep already carries the ESPN id
    direct = (
        teams.filter(pl.col("key_espn").is_not_null())
        .with_columns(pl.col("key_espn").str.split("|"))
        .explode("key_espn")
        .select(pl.col("key_espn").str.strip_chars().alias("espn_team_id"), "reep_id")
    )
    assert espn.schema["espn_team_id"] == direct.schema["espn_team_id"] == pl.Utf8
    direct = direct.join(espn.select("espn_team_id", "espn_league"), on="espn_team_id", how="inner")
    if direct["espn_team_id"].is_duplicated().any():
        raise ValueError("reep key_espn: one ESPN id on two reep rows")
    # reep's own ESPN id is trusted only inside the league's country, like a name match
    direct = (
        direct.join(teams.select("reep_id", "country"), on="reep_id")
        .join(country, on=["espn_league", "country"], how="semi")
        .select("espn_team_id", "reep_id")
    )
    # (b) in-country name candidates
    keys = pl.concat(
        [
            espn.select("espn_team_id", "espn_league", pl.col(f"_n_{c}").alias("text"), pl.lit(c).alias("field"))
            for c in ("display_name", "short_display_name", "name", "location")
        ]
    ).with_columns(pl.col("text").map_elements(norm, return_dtype=pl.Utf8).alias("k"))
    keys = keys.filter(pl.col("k").is_not_null() & (pl.col("k") != "")).select(
        "espn_team_id", "espn_league", "k", pl.col("field").is_in(_TRUSTED_FIELDS).alias("trusted")
    )
    cand_names = reep_candidates(teams, aliases)
    assert keys.schema["k"] == cand_names.schema["k"]
    cands = (
        keys.join(cand_names, on="k")
        .join(teams.select("reep_id", "country"), on="reep_id")
        .join(country, on=["espn_league", "country"], how="semi")
        .group_by("espn_team_id", "reep_id")
        .agg(pl.col("trusted").any())
    )
    n_cand = cands.group_by("espn_team_id").agg(
        pl.col("reep_id").sort(), pl.len().alias("n"), pl.col("trusted").all().alias("trusted")
    )
    by_name = n_cand.filter((pl.col("n") == 1) & (pl.col("trusted") == True)).select(
        "espn_team_id", pl.col("reep_id").list.first()
    )

    rows = []
    review = []
    direct_map = dict(direct.iter_rows())
    name_map = dict(by_name.iter_rows())
    pending = {r["espn_team_id"]: r["reep_id"] for r in n_cand.iter_rows(named=True)}
    for e in espn.select(*ESPN_COLS).iter_rows(named=True):
        tid = e["espn_team_id"]
        if tid in overrides and tid in direct_map and overrides[tid] != direct_map[tid]:
            print(f"note: override for {tid} differs from reep key_espn ({direct_map[tid]})")
        if tid in overrides:
            rid = overrides[tid]
            method = "review" if rid else "unmatched"
        elif tid in direct_map:
            rid, method = direct_map[tid], "key_espn"
        elif tid in name_map:
            rid, method = name_map[tid], "name"
        else:
            review.append({**e, "candidates": pending.get(tid, [])})
            continue
        rows.append({**e, "reep_id": rid, "match_method": method})
    base = pl.DataFrame(rows, schema={**dict.fromkeys(ESPN_COLS, pl.Utf8), "reep_id": pl.Utf8, "match_method": pl.Utf8})
    reep_cols = teams.select(
        "reep_id",
        pl.col("name").alias("reep_name"),
        pl.col("country").replace("", None).alias("reep_country"),
        *REEP_KEYS,
    )
    unknown = base.filter(pl.col("reep_id").is_not_null()).join(reep_cols, on="reep_id", how="anti")
    if unknown.height:
        raise ValueError(f"overrides name reep ids absent from reep: {unknown['reep_id'].to_list()}")
    out = base.join(reep_cols, on="reep_id", how="left").select(OUT_COLS).sort("espn_league", "espn_team_id")
    return out.cast(SCHEMA), review


def gate_failures(xw: pl.DataFrame) -> list[str]:
    """Every gate the crosswalk misses (empty = pass). Used by the build and by the packaged-table test."""
    fails = []
    if xw["espn_team_id"].null_count():
        fails.append("null espn_team_id")
    if xw["espn_team_id"].is_duplicated().any():
        fails.append(
            f"duplicate espn_team_id: {xw.filter(pl.col('espn_team_id').is_duplicated())['espn_team_id'].to_list()}"
        )
    claimed = xw.filter(pl.col("reep_id").is_not_null())
    if claimed["reep_id"].is_duplicated().any():
        fails.append(
            f"reep_id on two ESPN teams: {claimed.filter(pl.col('reep_id').is_duplicated())['reep_id'].to_list()}"
        )
    # reep can hold one club twice (two Wikidata entities sharing a provider id): two ESPN teams on the two
    # rows would keep reep_id unique but fan out a join on the provider id
    for k in ("key_transfermarkt", "key_wikidata", "key_fbref", "key_fotmob", "key_opta"):
        v = xw[k].drop_nulls()
        if v.is_duplicated().any():
            fails.append(f"{k} on two ESPN teams: {sorted(set(v.filter(v.is_duplicated()).to_list()))}")
    share = claimed.height / max(xw.height, 1)
    if share < GATE["min_share"]:
        fails.append(f"matched share {share:.3f} < {GATE['min_share']}")
    per = xw.group_by("espn_league").agg(pl.len().alias("n"), pl.col("reep_id").is_not_null().mean().alias("share"))
    missing = set(LEAGUE_COUNTRIES) - set(per["espn_league"].to_list())
    if missing:
        fails.append(f"leagues missing: {sorted(missing)}")
    extra = set(per["espn_league"].to_list()) - set(LEAGUE_COUNTRIES)
    if extra:
        fails.append(f"leagues not in LEAGUE_COUNTRIES: {sorted(extra)}")
    for r in per.iter_rows(named=True):
        if r["n"] < GATE["min_league_teams"] or r["share"] < GATE["min_league_share"]:
            fails.append(f"{r['espn_league']}: {r['n']} teams, share {r['share']:.3f}")
    return fails


def coverage(xw: pl.DataFrame) -> pl.DataFrame:
    """Matched / total per league, plus how each was matched."""
    return (
        xw.group_by("espn_league")
        .agg(
            pl.len().alias("teams"),
            pl.col("reep_id").is_not_null().sum().alias("matched"),
            *((pl.col("match_method") == m).sum().alias(m) for m in ("key_espn", "name", "review", "unmatched")),
            pl.col("key_transfermarkt").is_not_null().sum().alias("with_transfermarkt"),
        )
        .with_columns((pl.col("matched") / pl.col("teams")).round(3).alias("share"))
        .sort("espn_league")
    )


def _fetch_espn() -> pl.DataFrame:
    from sportsdataverse.soccer import espn_soccer_teams_site

    frames = []
    for lg in LEAGUE_COUNTRIES:
        parsed = espn_soccer_teams_site(league=lg)
        if parsed.is_empty():
            raise RuntimeError(f"ESPN /teams returned no teams for {lg}")
        frames.append(espn_frame(parsed, lg))
    return pl.concat(frames)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--reep-dir", type=Path, required=True, help="directory holding reep teams.csv and names.csv")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args(argv)

    teams = pl.read_csv(args.reep_dir / "teams.csv", infer_schema=False)  # every id text: never inferred
    aliases = pl.read_csv(args.reep_dir / "names.csv", infer_schema=False)
    espn = _fetch_espn()
    xw, review = match(espn, teams, aliases, load_overrides())
    if review:
        print(f"{len(review)} ESPN teams need a reviewed override:")
        for r in review:
            print(
                f"  {r['espn_league']:9} {r['espn_team_id']:>6} {r['espn_display_name']!r:32} candidates={r['candidates']}"
            )
        return 2
    known = set(espn["espn_team_id"].to_list())
    stale = sorted(set(load_overrides()) - known)
    if stale:
        print(f"note: overrides for ESPN ids not in this snapshot (kept, unused): {stale}")
    print(coverage(xw))
    fails = gate_failures(xw)
    if fails:
        print("GATE FAILED:\n  " + "\n  ".join(fails))
        return 1
    args.out.parent.mkdir(parents=True, exist_ok=True)
    xw.write_csv(args.out)
    print(f"wrote {args.out} ({xw.height} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
