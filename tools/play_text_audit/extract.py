"""Stage 1 of the ESPN play-text audit: pull every play's text and types into one parquet.

Reads the committed season pbp parquet in cfbfastR-cfb-data
(``cfb/pbp/parquet/play_by_play_<season>.parquet``), which carries ESPN's
original play type (``orig_play_type``, ``type.id``) next to sdv-py's
re-classified one (``type.text``). Each game's own team names, mascots and
abbreviations are masked as ``{T}`` here because they are per-game; the rest of
templating is corpus-wide and lives in ``templates.py``.

    python tools/play_text_audit/extract.py --pbp <cfb/pbp/parquet> --out <data dir>
"""

from __future__ import annotations

import argparse
import glob
import os
import re
import sys

import polars as pl

KEEP = {
    "season": "season",
    "game_id": "game_id",
    "sequenceNumber": "sequence",
    "type.id": "type_id",
    "orig_play_type": "orig_play_type",
    "type.text": "play_type",
    "text": "text",
}
TEAM_COLS = [f"{side}Team{k}" for side in ("home", "away") for k in ("Name", "Mascot", "Abbrev", "NameAlt")]
# What sdv-py parsed, carried out row-for-row so stage 3 can diff it (keyed by row_id: (game_id,
# sequenceNumber) is not unique -- synthesized rows share a sequence number).
PARSER_FLAGS = [
    "rush",
    "pass",
    "pass_attempt",
    "completion",
    "sack",
    "int",
    "fumble_vec",
    "penalty_flag",
    "penalty_no_play",
    "penalty_declined",
    "td_play",
    "touchdown",
    "safety",
    "kickoff_play",
    "kickoff_tb",
    "kickoff_fair_catch",
    "kickoff_onside",
    "punt",
    "punt_tb",
    "punt_fair_catch",
    "punt_blocked",
    "fg_attempt",
    "fg_made",
    "xp_attempt",
    "xp_made",
    "kneel_down",
    "pass_breakup",
]
PARSER_FLAGS += [
    "fumble_lost",
    "turnover_vec",
    "is_turnover",
    "int_td",
    "rush_td",
    "pass_td",
    "downs_turnover",
    "firstD_by_poss",
    "firstD_by_penalty",
    "firstD_by_yards",
    "first_down_yards",
    "first_down_penalty",
]
PARSER_YARDS = [
    "yds_rushed",
    "yds_receiving",
    "yds_sacked",
    "yds_punted",
    "yds_kickoff",
    "yds_fg",
    "yds_int_return",
    "yds_fumble_return",
    "yds_punt_return",
    "yds_kickoff_return",
    "statYardage",
]
PARSER_IDS = ["pos_team_id", "def_pos_team_id", "penalty_team_id", "turnover_team"]
ROLES = [
    "passer",
    "rusher",
    "receiver",
    "sack",
    "interception",
    "pass_breakup",
    "fumble",
    "fumble_forced",
    "fumble_recovered",
    "punter",
    "punt_return",
    "kickoff",
    "kickoff_return",
    "fg_kicker",
    "fg_block",
    "punt_block",
]
PARSER_NAMES = [f"{r}_player_name" for r in ROLES] + ["xp_kicker_player_name"]
PARSER_PIDS = [f"{r}_player_id" for r in ROLES]
PARSER_STRINGS = ["yds_penalty", "pos_team", "def_pos_team"]
PARSER_COLS = [*PARSER_FLAGS, *PARSER_YARDS, *PARSER_IDS, *PARSER_NAMES, *PARSER_PIDS, *PARSER_STRINGS]


def mask_game(texts: list[str], names: list[str]) -> list[str]:
    """Mask the game's team names as {T}. Case-insensitive, except that an all-lowercase hit must be
    an exact name: Fordham's abbreviation "FOR" must not eat every "for" in its games."""
    names = sorted({n for n in names if n and len(n) > 1}, key=len, reverse=True)  # "Western Kentucky" first
    if not names:
        return texts
    exact = set(names)
    # (?<!\w)/(?!\w), not \b: a name ending in ")" -- "Miami (OH)" -- has no word boundary after it
    rx = re.compile(r"(?<!\w)(?:" + "|".join(map(re.escape, names)) + r")(?!\w)", re.IGNORECASE)

    def keep_lower(m: re.Match) -> str:
        return m.group(0) if m.group(0).islower() and m.group(0) not in exact else "{T}"

    return [rx.sub(keep_lower, t) for t in texts]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--pbp", default="/mnt/sdv_repos/cfbfastR-cfb-data/cfb/pbp/parquet")
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    os.makedirs(a.out, exist_ok=True)

    frames = []
    for path in sorted(glob.glob(os.path.join(a.pbp, "play_by_play_*.parquet"))):
        have = set(pl.read_parquet_schema(path))
        cols = [c for c in [*KEEP, *TEAM_COLS, *PARSER_COLS] if c in have]
        df = pl.read_parquet(path, columns=cols).rename({k: v for k, v in KEEP.items() if k in have})
        for c in [*TEAM_COLS, *PARSER_COLS]:
            if c not in df.columns:
                df = df.with_columns(pl.lit(None).alias(c))
        frames.append(
            df.with_columns(
                pl.col("type_id", "sequence").cast(pl.String),
                pl.col("season", "game_id").cast(pl.Int64),
                pl.col("text").fill_null("").str.strip_chars(),
                *[pl.col(c).cast(pl.String) for c in TEAM_COLS],
                *[pl.col(c).cast(pl.Boolean, strict=False) for c in PARSER_FLAGS],
                *[pl.col(c).cast(pl.Float64, strict=False).cast(pl.Int64, strict=False) for c in PARSER_YARDS],
                *[
                    pl.col(c).cast(pl.Float64, strict=False).cast(pl.Int64, strict=False)
                    for c in [*PARSER_IDS, *PARSER_PIDS]
                ],
                *[pl.col(c).cast(pl.String) for c in [*PARSER_NAMES, *PARSER_STRINGS]],
            )
        )
    plays = pl.concat(frames, how="diagonal_relaxed").sort("game_id", maintain_order=True).with_row_index("row_id")

    # Rows keep their order within each group and groups come out in sorted game order, so the
    # per-game masks extend back in exactly the frame's row order.
    masked: list[str] = []
    for _, g in plays.select("game_id", "text", *TEAM_COLS).group_by("game_id", maintain_order=True):
        masked.extend(mask_game(g["text"].to_list(), [v for c in TEAM_COLS for v in g[c].unique().to_list() if v]))
    plays = plays.with_columns(
        pl.Series("masked", masked),
        pl.coalesce("homeTeamAbbrev", "homeTeamName").alias("home_team"),
        pl.coalesce("awayTeamAbbrev", "awayTeamName").alias("away_team"),
    )
    plays.select("row_id", *PARSER_COLS).write_parquet(os.path.join(a.out, "parser.parquet"))
    plays.drop(TEAM_COLS, *PARSER_COLS).write_parquet(os.path.join(a.out, "plays_raw.parquet"))
    print(f"{plays['game_id'].n_unique():,} games, {plays['season'].n_unique()} seasons -> {plays.height:,} plays")
    return 0


def _selftest() -> None:
    names = ["Fordham", "FOR", "Rams", "Miami (OH)", "RedHawks"]
    got = mask_game(["punt for 37 yards to the FOR 27", "Miami (OH) Penalty, holding", "for the Rams"], names)
    assert got == ["punt for 37 yards to the {T} 27", "{T} Penalty, holding", "for the {T}"], got
    print("self-test ok")


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        _selftest()
    else:
        raise SystemExit(main())
