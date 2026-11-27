"""Build ``sportsdataverse/soccer/data/espn_transfermarkt_player_crosswalk.csv``: ESPN athlete -> Transfermarkt player.

The rule and gate were fixed before scoring, by a pre-registration in internal notes dated 2026-11-26 (its
Amendment 2).
Population: the athletes on the ESPN rosters of the first-tier ESPN teams whose Transfermarkt club is in the clubs
table. Candidates: every Transfermarkt player. A candidate matches on the same birth date and at least one
shared name token (ESPN last name, or display name when it is empty, against the Transfermarkt name):

- one candidate -> resolved (``name_dob``); several -> the one whose citizenship equals the ESPN citizenship, if exactly
  one (``name_dob_nationality``);
- a 1 January birth date is weak (a common stand-in for an unknown day): only candidates also corroborated by club
  (the team's Transfermarkt club is their current club or a 2025-07-01+ transfer target) or nationality count, and
  exactly one must remain (``name_jan1_club`` / ``name_jan1_nationality``).

Everything else stays ``unmatched``. Gate (``GATE``): >= 80% resolved overall and >= 70% per league, one-to-one both
ways, and >= 98% agreement with reep where a reep person carries both ids. Any failure stops the build.

Run (network: ESPN Site v2 rosters and the Transfermarkt tables)::

    uv run python tools/build_soccer_espn_transfermarkt_player_crosswalk.py --reep-dir ../../reep/data
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "sportsdataverse" / "soccer" / "data" / "espn_transfermarkt_player_crosswalk.csv"

FIRST_TIER = ("eng.1", "esp.1", "ger.1", "ita.1", "fra.1", "usa.1", "mex.1")
#: Pre-registered before scoring; never lowered.
GATE = {"min_share": 0.80, "min_league_share": 0.70, "min_reep_agreement": 0.98}
MOVES_FROM = (2025, 7, 1)
KEYS = ["espn_team_id", "espn_athlete_id"]
OUT_SCHEMA = {
    "espn_athlete_id": pl.Utf8,
    "espn_team_id": pl.Utf8,
    "espn_league": pl.Utf8,
    "espn_date_of_birth": pl.Date,
    "player_id": pl.Int64,
    "tm_name": pl.Utf8,
    "match_method": pl.Utf8,
}


def fold(s: str | None) -> str:
    """Accents and case folded: ``"Côte d'Ivoire"`` -> ``"cote d'ivoire"``.

    A letter with no ASCII decomposition (``ø``, ``ł``, ``ß``) is dropped, not transliterated: that is the
    pre-registered tokenisation, so it stays even though "Großkreutz" then shares no token with "Grosskreutz".
    """
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().casefold().strip()


def tokens(s: str | None) -> set[str]:
    """Folded name tokens: ``"Mbappé Lottin"`` -> ``{"mbappe", "lottin"}``."""
    return {t for t in re.split(r"[^a-z0-9]+", fold(s)) if t}


def espn_frame(roster: pl.DataFrame, espn_team_id: str, league: str) -> pl.DataFrame:
    """One parsed ESPN roster in the build's columns (athlete id text, birth date a Date)."""
    return roster.select(
        pl.col("athlete_id").cast(pl.Utf8).alias("espn_athlete_id"),
        pl.lit(espn_team_id).alias("espn_team_id"),
        pl.lit(league).alias("espn_league"),
        pl.coalesce(
            pl.when(pl.col("last_name").str.strip_chars() != "").then(pl.col("last_name")), pl.col("display_name")
        ).alias("espn_last"),
        pl.col("date_of_birth").str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False).alias("dob"),
        pl.col("citizenship").cast(pl.Utf8).alias("citizenship"),
    ).unique("espn_athlete_id", keep="first")


def resolve(
    espn: pl.DataFrame, players: pl.DataFrame, team_club: pl.DataFrame, transfers: pl.DataFrame
) -> pl.DataFrame:
    """One row per ESPN athlete in ``OUT_SCHEMA``: the resolved Transfermarkt player, or null and ``unmatched``."""
    tm = players.select(
        "player_id", pl.col("name").alias("tm_name"), pl.col("date_of_birth").alias("dob"), "country_of_citizenship",
        "current_club_id",
    )  # fmt: skip
    moves = transfers.filter(pl.col("transfer_date") >= pl.date(*MOVES_FROM)).select(
        "player_id", pl.col("to_club_id").alias("club_id")
    )
    assert espn.schema["dob"] == tm.schema["dob"] == pl.Date
    assert espn.schema["espn_team_id"] == team_club.schema["espn_team_id"] == pl.Utf8
    assert tm.schema["current_club_id"] == team_club.schema["club_id"] == moves.schema["club_id"] == pl.Int64
    assert tm.schema["player_id"] == moves.schema["player_id"] == pl.Int64
    pairs = espn.filter(pl.col("dob").is_not_null()).join(tm, on="dob")
    name_ok = [bool(tokens(a) & tokens(b)) for a, b in pairs.select("espn_last", "tm_name").rows()]
    hits = pairs.filter(pl.Series(name_ok, dtype=pl.Boolean))
    nat_ok = [
        fold(a) != "" and fold(a) == fold(b) for a, b in hits.select("citizenship", "country_of_citizenship").rows()
    ]
    moved = moves.unique().with_columns(pl.lit(True).alias("_moved"))
    hits = (
        hits.with_columns(pl.Series("nat_ok", nat_ok, dtype=pl.Boolean))
        .join(team_club, on="espn_team_id", how="left")
        .join(moved, on=["player_id", "club_id"], how="left")
        .with_columns(
            (
                (pl.col("current_club_id") == pl.col("club_id")).fill_null(False) | pl.col("_moved").fill_null(False)
            ).alias("club_ok"),
            ((pl.col("dob").dt.month() == 1) & (pl.col("dob").dt.day() == 1)).alias("weak"),
        )
    )
    hits = hits.join(hits.group_by(KEYS).agg(pl.len().alias("n")), on=KEYS)

    def unique(df: pl.DataFrame) -> pl.DataFrame:
        return df.join(df.group_by(KEYS).agg(pl.len().alias("m")), on=KEYS).filter(pl.col("m") == 1)

    strong = hits.filter(pl.col("weak") == False)
    one = strong.filter(pl.col("n") == 1).with_columns(pl.lit("name_dob").alias("match_method"))
    by_nat = unique(strong.filter((pl.col("n") > 1) & (pl.col("nat_ok") == True))).with_columns(
        pl.lit("name_dob_nationality").alias("match_method")
    )
    weak = unique(hits.filter((pl.col("weak") == True) & ((pl.col("club_ok") == True) | (pl.col("nat_ok") == True))))
    weak = weak.with_columns(
        pl.when(pl.col("club_ok") == True)
        .then(pl.lit("name_jan1_club"))
        .otherwise(pl.lit("name_jan1_nationality"))
        .alias("match_method")
    )
    cols = [*KEYS, "player_id", "tm_name", "match_method"]
    resolved = pl.concat([one.select(cols), by_nat.select(cols), weak.select(cols)])
    if resolved.select(KEYS).is_duplicated().any():
        raise AssertionError("an ESPN athlete resolved twice")  # the three paths are disjoint by construction
    out = espn.join(resolved, on=KEYS, how="left").with_columns(
        pl.col("match_method").fill_null("unmatched"), pl.col("dob").alias("espn_date_of_birth")
    )
    return out.select(list(OUT_SCHEMA)).cast(OUT_SCHEMA).sort("espn_league", "espn_team_id", "espn_athlete_id")


def reep_pairs(people: pl.DataFrame) -> pl.DataFrame:
    """``(espn_athlete_id, ref_player_id)`` for every reep person row carrying both ids (an ESPN id on two reep rows
    gives two pairs, each scored)."""
    ref = people.filter(pl.col("key_espn").is_not_null() & pl.col("key_transfermarkt").is_not_null())
    return ref.select(
        pl.col("key_espn").alias("espn_athlete_id"),
        pl.col("key_transfermarkt").cast(pl.Int64, strict=False).alias("ref_player_id"),
    )


def gate_failures(xw: pl.DataFrame, ref: pl.DataFrame) -> list[str]:
    """Every pre-registered check the table misses (empty = pass)."""
    fails = []
    hit = xw.filter(pl.col("player_id").is_not_null())
    # one-to-one as pre-registered: no athlete on two players, no player on two athletes (distinct ids)
    if (hit.group_by("espn_athlete_id").agg(pl.col("player_id").n_unique())["player_id"] > 1).any():
        fails.append("an ESPN athlete maps to two Transfermarkt players")
    if (hit.group_by("player_id").agg(pl.col("espn_athlete_id").n_unique())["espn_athlete_id"] > 1).any():
        fails.append(
            f"a Transfermarkt player on two ESPN athletes: {sorted(set(hit.filter(pl.col('player_id').is_duplicated())['player_id'].to_list()))}"
        )
    if (hit["match_method"] == "unmatched").any() or (
        xw.filter(pl.col("player_id").is_null())["match_method"] != "unmatched"
    ).any():
        fails.append("match_method disagrees with player_id")
    share = hit.height / max(xw.height, 1)
    if share < GATE["min_share"]:
        fails.append(f"resolved share {share:.3f} < {GATE['min_share']}")
    per = xw.group_by("espn_league").agg(pl.col("player_id").is_not_null().mean().alias("share"))
    missing = set(FIRST_TIER) - set(per["espn_league"].to_list())
    if missing:
        fails.append(f"leagues missing: {sorted(missing)}")
    fails += [
        f"{lg}: share {s:.3f} < {GATE['min_league_share']}" for lg, s in per.rows() if s < GATE["min_league_share"]
    ]
    assert hit.schema["espn_athlete_id"] == ref.schema["espn_athlete_id"] == pl.Utf8
    j = hit.join(ref, on="espn_athlete_id")
    agree = (j["player_id"] == j["ref_player_id"]).sum() / max(j.height, 1)
    if j.height == 0 or agree < GATE["min_reep_agreement"]:
        fails.append(f"reep agreement {agree:.4f} < {GATE['min_reep_agreement']} over {j.height} pairs")
    return fails


def main(argv: list[str] | None = None) -> int:
    from sportsdataverse.soccer import (
        espn_soccer_team_roster,
        load_soccer_transfermarkt_clubs,
        load_soccer_transfermarkt_players,
        load_soccer_transfermarkt_transfers,
        soccer_espn_transfermarkt_team_crosswalk,
    )

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--reep-dir", type=Path, required=True, help="directory holding reep people.csv")
    ap.add_argument("--rosters", type=Path, help="parquet of fetched ESPN rosters (read if present, else written)")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args(argv)

    clubs = load_soccer_transfermarkt_clubs()
    team_club = soccer_espn_transfermarkt_team_crosswalk().filter(pl.col("espn_league").is_in(FIRST_TIER))
    team_club = team_club.join(clubs.select("club_id"), on="club_id", how="semi")
    if args.rosters and args.rosters.exists():
        espn = pl.read_parquet(args.rosters)
    else:
        frames = []
        for tid, lg in team_club.select("espn_team_id", "espn_league").iter_rows():
            roster = espn_soccer_team_roster(league=lg, team_id=tid)
            if roster.is_empty():
                raise RuntimeError(f"ESPN roster empty for {lg} team {tid}")
            frames.append(espn_frame(roster, tid, lg))
        espn = pl.concat(frames)
        if args.rosters:
            espn.write_parquet(args.rosters)
    xw = resolve(
        espn, load_soccer_transfermarkt_players(), team_club.select("espn_team_id", "club_id"),
        load_soccer_transfermarkt_transfers(),
    )  # fmt: skip
    people = pl.read_csv(args.reep_dir / "people.csv", infer_schema=False, columns=["key_espn", "key_transfermarkt"])
    print(xw.group_by("espn_league", "match_method").len().sort("espn_league", "match_method"))
    fails = gate_failures(xw, reep_pairs(people))
    if fails:
        print("GATE FAILED:\n  " + "\n  ".join(fails))
        return 1
    args.out.parent.mkdir(parents=True, exist_ok=True)
    xw.write_csv(args.out)
    print(f"wrote {args.out} ({xw.height} rows; {xw['player_id'].is_not_null().sum()} resolved)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
