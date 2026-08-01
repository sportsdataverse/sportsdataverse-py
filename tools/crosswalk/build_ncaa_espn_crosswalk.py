"""Build the stats.ncaa.org <-> ESPN basketball team-id crosswalk.

Two modes:

``--capture``
    Hit ESPN once and refresh the committed reference tables
    (``espn_mbb_teams.csv`` / ``espn_wbb_teams.csv`` in this directory).
    Requires network; run rarely (new D-I members, conference realignment).

default
    Fully offline. Reads the committed reference tables, the hoopR
    cross-provider name dictionary, and the hand-curated alias tables, then
    writes ``sportsdataverse/{mbb,wbb}/data/ncaa_espn_team_crosswalk_{mbb,wbb}.csv``
    and prints the per-season match-rate report.

Match order (first hit wins, and a candidate is only accepted when it resolves
to exactly ONE ESPN team):

1. ``exact``  -- normalized NCAA name against the ESPN candidate keys.
2. ``dict``   -- normalized NCAA name -> hoopR ``dict_hoopR`` ``NCAA`` row ->
   its ``ESPN`` / ``ESPN_PBP`` spellings -> ESPN candidate keys.
3. ``alias``  -- the hand-curated ``alias_{league}.csv`` (ncaa_team ->
   espn_team_id), for AP abbreviations and post-2023 renames/promotions the
   dictionary predates.

Anything left over is emitted with a null ``espn_team_id`` and
``match_method="unmatched"`` -- rows are never dropped. There is deliberately
NO fuzzy matching: a fuzzy candidate must be verified by hand and written into
the alias table, so the build stays deterministic and reviewable.

Usage::

    uv run python tools/crosswalk/build_ncaa_espn_crosswalk.py
    uv run python tools/crosswalk/build_ncaa_espn_crosswalk.py --capture
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional, Set

import polars as pl

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
HOOPR_DICT = HERE / "dict_hoopR_ncaa_espn.csv"
ALIAS = HERE / "alias_ncaa_espn.csv"
_SITE_TEAM = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams"

#: AP-style abbreviations stats.ncaa.org uses that ESPN spells out. Derived
#: empirically from the unmatched residue, not guessed -- every entry below
#: retired at least one real miss.
_ABBREV: Dict[str, str] = {
    "st.": "state",
    "u.": "university",
    "univ.": "university",
    "ala.": "alabama",
    "ark.": "arkansas",
    "ariz.": "arizona",
    "caro.": "carolina",
    "colo.": "colorado",
    "conn.": "connecticut",
    "fla.": "florida",
    "ga.": "georgia",
    "ill.": "illinois",
    "ind.": "indiana",
    "ky.": "kentucky",
    "la.": "louisiana",
    "md.": "maryland",
    "mass.": "massachusetts",
    "mich.": "michigan",
    "minn.": "minnesota",
    "miss.": "mississippi",
    "mo.": "missouri",
    "n.c.": "north carolina",
    "n.d.": "north dakota",
    "n.j.": "new jersey",
    "n.m.": "new mexico",
    "n.y.": "new york",
    "neb.": "nebraska",
    "okla.": "oklahoma",
    "ore.": "oregon",
    "pa.": "pennsylvania",
    "s.c.": "south carolina",
    "s.d.": "south dakota",
    "tenn.": "tennessee",
    "tex.": "texas",
    "va.": "virginia",
    "vt.": "vermont",
    "w.va.": "west virginia",
    "wash.": "washington",
    "wis.": "wisconsin",
}

#: A LEADING "St. " is Saint (St. John's, St. Mary's, St. Bonaventure); a
#: trailing/medial "St." is State (Ohio St., Central Conn. St.). Handled before
#: the token pass so the ``st. -> state`` entry above can't clobber it.
_LEADING_SAINT = re.compile(r"^st\.\s+")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def normalize(name: Optional[str]) -> str:
    """Contract an NCAA/ESPN school name to a comparable key.

    Expands AP abbreviations, folds case, drops punctuation. ``""`` for null.
    """
    if not name:
        return ""
    text = _LEADING_SAINT.sub("saint ", name.lower().strip())
    text = " ".join(_ABBREV.get(tok, tok) for tok in text.split())
    text = text.replace("&", " and ")
    return _NON_ALNUM.sub(" ", text).strip()


# --------------------------------------------------------------------------
# capture
# --------------------------------------------------------------------------

_ESPN_OUT_COLS = [
    "team_id",
    "location",
    "display_name",
    "short_name",
    "mascot",
    "nickname",
    "abbreviation",
    "conference_id",
    "conference_name",
]

_HOOPR_TEAMS = Path("C:/Users/saiem/Documents/GitHub-Data/sdv-dev/hoopR-dev/hoopR/data-raw/espn_mbb_teams.csv")
_HOOPR_DICT_SRC = Path("C:/Users/saiem/Documents/GitHub-Data/sdv-dev/hoopR-dev/hoopR/data-raw/dict_hoopR.csv")


def _site_teams(league: str) -> pl.DataFrame:
    """Live ESPN Site-API team table, renamed to the reference-table shape."""
    if league == "mbb":
        from sportsdataverse.mbb import espn_mbb_teams as fetch
    else:
        from sportsdataverse.wbb import espn_wbb_teams as fetch

    df = fetch()
    return df.select(
        pl.col("team_id").cast(pl.Utf8).alias("team_id"),
        pl.col("team_location").alias("location"),
        pl.col("team_display_name").alias("display_name"),
        pl.col("team_short_display_name").alias("short_name"),
        pl.col("team_name").alias("mascot"),
        pl.col("team_nickname").alias("nickname"),
        pl.col("team_abbreviation").alias("abbreviation"),
    )


def _hoopr_teams() -> pl.DataFrame:
    """hoopR's committed ESPN MBB table -- the only source of conference cols."""
    df = pl.read_csv(_HOOPR_TEAMS, null_values=["NA"], infer_schema_length=10000)
    return df.select(
        pl.col("team_id").cast(pl.Utf8).alias("team_id"),
        pl.col("team").alias("location"),
        pl.col("display_name"),
        pl.col("short_name"),
        pl.col("mascot"),
        pl.col("nickname"),
        pl.col("abbreviation"),
        pl.col("conference_id").cast(pl.Utf8).alias("conference_id"),
        pl.col("conference_name"),
    )


def _wbb_conferences() -> pl.DataFrame:
    """Women's conference membership from the ESPN Core v2 groups tree.

    The Site-API ``groups=`` filter is a no-op for women's basketball (it
    returns all 362 teams whatever you pass), so conference affiliation has to
    come from Core v2 ``seasons/{yr}/types/2/groups/{id}/teams``.
    """
    from sportsdataverse.dl_utils import download
    from sportsdataverse.wbb import espn_wbb_season_group, espn_wbb_season_group_teams

    season, season_type, division_i = 2025, 2, 50
    core = (
        f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball"
        f"/seasons/{season}/types/{season_type}/groups/{division_i}/children"
    )

    def _ref_ids(refs: List[str]) -> List[str]:
        out = [str(r).split("?")[0].rstrip("/").rsplit("/", 1)[-1] for r in refs]
        return [i for i in out if i.isdigit()]

    def _ids(df: pl.DataFrame) -> List[str]:
        return _ref_ids(df.get_column("$ref").to_list() if "$ref" in df.columns else [])

    # NOTE: espn_wbb_season_group_children() hardcodes params={} so it returns
    # only the first page (25 of 31 conferences). Go through the shared HTTP
    # chokepoint directly until that wrapper grows a ``limit``.
    children = download(url=core, params={"limit": 100}).json().get("items", [])

    rows: List[Dict[str, str]] = []
    for gid in _ref_ids([c.get("$ref", "") for c in children]):
        meta = espn_wbb_season_group(season=season, season_type=season_type, group_id=gid)
        cname = meta.get_column("name")[0] if meta.height else ""
        teams = espn_wbb_season_group_teams(season=season, season_type=season_type, group_id=gid)
        for tid in _ids(teams):
            rows.append({"team_id": tid, "conference_id": gid, "conference_name": cname or ""})
    return pl.DataFrame(rows, schema={"team_id": pl.Utf8, "conference_id": pl.Utf8, "conference_name": pl.Utf8}).unique(
        subset=["team_id"], keep="first"
    )


def _alias_only_teams(columns: List[str]) -> pl.DataFrame:
    """ESPN rows for alias targets neither live list nor hoopR carries.

    Defunct / reclassified programs (Savannah State, Centenary, Winston-Salem
    State) are gone from every bulk team list but their per-team endpoint still
    resolves, so the alias table's ids are enough to rebuild the row.
    """
    import requests

    alias = pl.read_csv(ALIAS, schema_overrides={"espn_team_id": pl.Utf8})
    rows: List[Dict[str, Optional[str]]] = []
    for team_id in alias.get_column("espn_team_id").unique().to_list():
        resp = requests.get(f"{_SITE_TEAM}/{team_id}", timeout=30)
        resp.raise_for_status()
        team = resp.json().get("team", {})
        rows.append(
            {
                "team_id": str(team.get("id")),
                "location": team.get("location"),
                "display_name": team.get("displayName"),
                "short_name": team.get("shortDisplayName"),
                "mascot": team.get("name"),
                "nickname": team.get("nickname"),
                "abbreviation": team.get("abbreviation"),
            }
        )
    return pl.DataFrame(rows, schema={c: pl.Utf8 for c in columns}).select(columns)


def capture() -> None:
    """Refresh the committed ESPN reference tables + the trimmed hoopR dict.

    ESPN team ids are school-level -- one id per school, shared by its men's
    and women's programs -- so the reference pool is the UNION of three
    sources, first one wins per id:

    * live men's Site-API team list,
    * live women's Site-API team list (the two D-I lists disagree: the men's
      one is missing Lindenwood / Queens / Southern Indiana, the women's one
      has them),
    * hoopR's committed 2023 snapshot, which still carries the programs ESPN
      has since dropped from D-I (Hartford, both St. Francises).

    Only the conference columns are league-specific.
    """
    hoopr = _hoopr_teams()
    site = _site_teams("mbb")
    pool = (
        site.vstack(_site_teams("wbb"))
        .vstack(hoopr.select(site.columns))
        .vstack(_alias_only_teams(list(site.columns)))
        .unique(subset=["team_id"], keep="first", maintain_order=True)
    )

    for league, conference in (
        ("mbb", hoopr.select("team_id", "conference_id", "conference_name")),
        ("wbb", _wbb_conferences()),
    ):
        table = pool.join(conference, on="team_id", how="left").select(_ESPN_OUT_COLS).sort("team_id")
        table.write_csv(HERE / f"espn_{league}_teams.csv")
        print(f"espn_{league}_teams.csv: {table.height} rows")

    dct = pl.read_csv(_HOOPR_DICT_SRC, null_values=["NA"], infer_schema_length=10000)
    dct.select("NCAA", "ESPN", "ESPN_PBP").drop_nulls("NCAA").unique(maintain_order=True).write_csv(HOOPR_DICT)
    print(f"dict_hoopR_ncaa_espn.csv: {dct.height} rows")


# --------------------------------------------------------------------------
# build
# --------------------------------------------------------------------------


def _espn_index(espn: pl.DataFrame) -> Dict[str, Set[str]]:
    """normalized-name -> {espn team_id}. Ambiguous keys keep every id."""
    index: Dict[str, Set[str]] = {}
    for row in espn.iter_rows(named=True):
        keys = {normalize(row[c]) for c in ("location", "display_name", "short_name", "nickname", "abbreviation")}
        display, mascot = row["display_name"], row["mascot"]
        if display and mascot and display.endswith(mascot):
            keys.add(normalize(display[: -len(mascot)]))
        for key in keys - {""}:
            index.setdefault(key, set()).add(row["team_id"])
    return index


def _dict_bridge() -> Dict[str, Set[str]]:
    """normalized NCAA name -> {ESPN spellings} from hoopR's name dictionary."""
    dct = pl.read_csv(HOOPR_DICT, null_values=["NA"], infer_schema_length=10000)
    bridge: Dict[str, Set[str]] = {}
    for row in dct.iter_rows(named=True):
        spellings = {v for v in (row["ESPN"], row["ESPN_PBP"]) if v}
        if row["NCAA"] and spellings:
            bridge.setdefault(normalize(row["NCAA"]), set()).update(spellings)
    return bridge


def build(league: str) -> pl.DataFrame:
    """Season-keyed crosswalk for *league*; unmatched rows kept with nulls."""
    from sportsdataverse.mbb.mbb_ncaa_team_ids import _ncaa_bb_team_ids

    ncaa = _ncaa_bb_team_ids(league)
    espn = pl.read_csv(
        HERE / f"espn_{league}_teams.csv", schema_overrides={"team_id": pl.Utf8, "conference_id": pl.Utf8}
    )
    alias_df = pl.read_csv(ALIAS, schema_overrides={"espn_team_id": pl.Utf8})
    alias = dict(zip(alias_df.get_column("ncaa_team").to_list(), alias_df.get_column("espn_team_id").to_list()))

    index = _espn_index(espn)
    bridge = _dict_bridge()
    espn_by_id = {r["team_id"]: r for r in espn.iter_rows(named=True)}

    def resolve(team: str) -> "tuple[Optional[str], str]":
        if team in alias:
            return alias[team], "alias"
        key = normalize(team)
        hit = index.get(key, set())
        if len(hit) == 1:
            return next(iter(hit)), "exact"
        ids: Set[str] = set()
        for spelling in bridge.get(key, set()):
            ids |= index.get(normalize(spelling), set())
        if len(ids) == 1:
            return next(iter(ids)), "dict"
        return None, "ambiguous" if (hit or ids) else "unmatched"

    resolved = {team: resolve(team) for team in ncaa.get_column("team").unique().to_list()}
    espn_ids = [resolved[t][0] for t in ncaa.get_column("team")]
    methods = [resolved[t][1] for t in ncaa.get_column("team")]

    def col(field: str) -> List[Optional[str]]:
        return [espn_by_id[i][field] if i is not None else None for i in espn_ids]

    return ncaa.select(
        pl.col("season"),
        pl.col("id").cast(pl.Int64).alias("ncaa_team_id"),
        pl.col("team").alias("ncaa_team"),
        pl.col("conference").alias("ncaa_conference"),
        pl.Series("espn_team_id", espn_ids, dtype=pl.Utf8),
        pl.Series("espn_display_name", col("display_name"), dtype=pl.Utf8),
        pl.Series("espn_location", col("location"), dtype=pl.Utf8),
        pl.Series("espn_mascot", col("mascot"), dtype=pl.Utf8),
        pl.Series("espn_abbreviation", col("abbreviation"), dtype=pl.Utf8),
        pl.Series("espn_conference_name", col("conference_name"), dtype=pl.Utf8),
        pl.Series("espn_conference_id", col("conference_id"), dtype=pl.Utf8),
        pl.Series("match_method", methods, dtype=pl.Utf8),
    ).sort("season", "ncaa_team")


def report(league: str, df: pl.DataFrame) -> None:
    """Per-season match rates + the unmatched residue."""
    per_season = (
        df.group_by("season")
        .agg(
            pl.len().alias("n"),
            (pl.col("espn_team_id").is_not_null()).sum().alias("matched"),
            (pl.col("match_method") == "exact").sum().alias("exact"),
            (pl.col("match_method") == "dict").sum().alias("dict"),
            (pl.col("match_method") == "alias").sum().alias("alias"),
        )
        .with_columns((pl.col("matched") / pl.col("n") * 100).round(2).alias("pct"))
        .sort("season")
    )
    print(f"\n=== {league.upper()} ===")
    for row in per_season.iter_rows(named=True):
        print(
            f"  {row['season']}  n={row['n']:3d}  matched={row['matched']:3d} ({row['pct']:6.2f}%)"
            f"  exact={row['exact']:3d} dict={row['dict']:2d} alias={row['alias']:2d}"
        )
    residue = (
        df.filter(pl.col("espn_team_id").is_null())
        .group_by("ncaa_team")
        .agg(pl.col("season").min().alias("first"), pl.col("season").max().alias("last"), pl.len().alias("seasons"))
        .sort("seasons", descending=True)
    )
    print(f"  unmatched distinct teams: {residue.height}")
    for row in residue.iter_rows(named=True):
        print(f"    {row['ncaa_team']:<28} {row['seasons']:2d} seasons  {row['first']}..{row['last']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", action="store_true", help="refresh the ESPN reference tables from the network")
    args = parser.parse_args()

    if args.capture:
        capture()
        return

    for league in ("mbb", "wbb"):
        df = build(league)
        out = ROOT / "sportsdataverse" / league / "data" / f"ncaa_espn_team_crosswalk_{league}.csv"
        df.write_csv(out)
        report(league, df)
        print(f"  -> {out.relative_to(ROOT)} ({df.height} rows)")


if __name__ == "__main__":
    main()
