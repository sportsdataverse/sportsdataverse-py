"""Box-score builders shared by the CFB and NFL play-by-play processors.

Everything here operates on the processors' post-pipeline plays frame and the
credited-team columns :mod:`sportsdataverse.football.attribution` resolves
(``def_pos_team`` / ``forced_fumble_team`` / ``fumble_recovery_team`` /
``punt_return_team`` / ``kick_return_team``), so the two leagues emit the same
player-level defensive and specialist sections and the same air-yards keys.
"""

from __future__ import annotations

import json
from functools import reduce

import polars as pl

__all__ = [
    "air_yards_box",
    "build_defensive_players_box",
    "build_specialists_box",
    "fill_missing",
    "player_event_box",
    "sack_part",
]


def _ordered_rows(df: "pl.DataFrame", team: str, name: str, volume: str | None = None) -> "pl.DataFrame":
    """Rows in a total order: ``volume`` descending (when given), then team, then name.

    ``group_by`` emits groups in arbitrary order, so a table sorted on volume alone
    -- or not at all -- came out tied players (two receivers with one target each)
    in a different order on every run. The same game rendered twice never matched,
    and live tables reshuffled between refreshes. Team + name make the order total:
    both are the grouping keys, so no two rows share them.
    """
    keys = [c for c in (volume, team, name) if c is not None and c in df.columns]
    if not keys:
        return df
    return df.sort(keys, descending=[c == volume for c in keys], nulls_last=True, maintain_order=True)


def air_yards_box(pass_box: "pl.DataFrame", key: str) -> "pl.DataFrame":
    """Per-``key`` air-yards / YAC aggregate for the passer + receiver box scores.

    Runs on the UNFILLED pass frame: ``_fill_missing`` zero-fills numerics, and a
    zero air-yard is a real observation (a screen) while a null means ESPN's text
    carried no catch spot (pre-2025 games, sacks, throwaways). Every output is null
    when no play in the group has ``air_yards``, so a game without the data shows
    blanks, not 0.0.

    Columns: ``AirYds`` (all attempts), ``aDOT`` (mean air yards per attempt with
    data), ``CompAirYds`` (completions only), ``YAC``, and ``AirYdsPct``
    (``CompAirYds / Yds`` -- share of receiving yards earned in the air).
    """
    has_air = pl.col("air_yards").is_not_null()
    return (
        pass_box.group_by(["pos_team", key])
        .agg(
            _n_air=has_air.sum(),
            AirYds=pl.col("air_yards").sum(),
            aDOT=pl.col("air_yards").mean(),
            CompAirYds=pl.when(pl.col("completion") == True).then(pl.col("air_yards")).otherwise(None).sum(),
            YAC=pl.col("yards_after_catch").sum(),
            _yds=pl.col("yds_receiving").sum(),
        )
        .with_columns(
            AirYds=pl.when(pl.col("_n_air") > 0).then(pl.col("AirYds")).otherwise(None),
            CompAirYds=pl.when(pl.col("_n_air") > 0).then(pl.col("CompAirYds")).otherwise(None),
            YAC=pl.when(pl.col("_n_air") > 0).then(pl.col("YAC")).otherwise(None),
        )
        .with_columns(
            AirYdsPct=pl.when((pl.col("_n_air") > 0) & (pl.col("_yds") != 0))
            .then(pl.col("CompAirYds") / pl.col("_yds"))
            .otherwise(None)
        )
        .drop("_n_air", "_yds")
        .with_columns(
            pos_team=pl.col("pos_team").cast(pl.Int32),  # join key: both boxes cast pos_team Int32 first
            aDOT=pl.col("aDOT").cast(pl.Float32).round(2),
            AirYdsPct=pl.col("AirYdsPct").cast(pl.Float32).round(2),
        )
    )


def fill_missing(df: "pl.DataFrame") -> "pl.DataFrame":
    """Fill nulls for aggregation: 0.0 for numerics, False for booleans.

    ``DataFrame.fill_null(0.0)`` is a SILENT NO-OP on Boolean columns -- polars
    leaves boolean nulls untouched when the fill value is a float, with no error
    and no warning. Any `.mean()` on such a column then averages over the
    non-null rows only, which for a flag that is null-where-absent means it
    averages over exactly the True rows and returns 1.0.

    That is how `rushing_power_rate` shipped as 1.0 for every team in every
    season: `power_rush_attempt` is null on the 159,513 non-power plays of 2024
    and True on 3,437, so the rate read 1.0 instead of 3437/63017 = 0.055.
    A rate pinned at exactly 1.0 is visible; the same bug on a less extreme flag
    would not be, which is why the fill is centralized here rather than patched
    at the one call site that happened to be caught.
    """
    return df.with_columns(pl.col(pl.Boolean).fill_null(False)).fill_null(0.0)


def player_event_box(play_df, name_col, out, team_col, yds_col=None, team_out=None, extra_filter=None):
    """Count non-null occurrences of `name_col` per (team, player); sum `yds_col`.

    `team_col` is the column to group by (the resolved credited-team). `team_out`
    optionally renames it back to the section's canonical join key (e.g. group by
    `fumble_recovery_team` but emit it as `def_pos_team` so the section reduce-join
    still aligns). `extra_filter` optionally narrows the qualifying plays (e.g.
    takeaway-only fumble recoveries).
    """
    if name_col not in play_df.columns or team_col not in play_df.columns:
        return None
    f = play_df.filter(pl.col(name_col).is_not_null() & pl.col(team_col).is_not_null())
    if extra_filter is not None:
        f = f.filter(extra_filter)
    if f.height == 0:
        return None
    aggs = [pl.len().alias(out)]
    if yds_col is not None and yds_col in play_df.columns:
        aggs.append(pl.col(yds_col).sum().alias(f"{out}_yards"))
    g = f.group_by([team_col, name_col]).agg(aggs).rename({name_col: "player_name"})
    if team_out and team_out != team_col:
        g = g.rename({team_col: team_out})
    return g


def sack_part(play_df):
    """Per-player sacks with the official split-sack convention (#93).

    An assisted/split sack (``sack_player_name2`` populated) credits 0.5
    sacks -- and half the sack yardage -- to each participant, so the sum
    of player sacks always equals the team's sack-play count.
    """
    if "sack_player_name" not in play_df.columns or "def_pos_team" not in play_df.columns:
        return None
    has2 = "sack_player_name2" in play_df.columns
    split = pl.col("sack_player_name2").is_not_null() if has2 else pl.lit(False)
    credit = pl.when(split).then(pl.lit(0.5)).otherwise(pl.lit(1.0))
    yds = pl.col("yds_sacked").cast(pl.Float64).fill_null(0.0) if "yds_sacked" in play_df.columns else pl.lit(0.0)
    base = play_df.filter(pl.col("sack_player_name").is_not_null() & pl.col("def_pos_team").is_not_null())
    frames = [
        base.select(
            pl.col("def_pos_team"),
            pl.col("sack_player_name").alias("player_name"),
            credit.alias("sacks"),
            (yds * credit).alias("sacks_yards"),
        ),
    ]
    if has2:
        second = play_df.filter(
            pl.col("sack_player_name2").is_not_null() & pl.col("def_pos_team").is_not_null(),
        )
        frames.append(
            second.select(
                pl.col("def_pos_team"),
                pl.col("sack_player_name2").alias("player_name"),
                pl.lit(0.5).alias("sacks"),
                (yds * 0.5).alias("sacks_yards"),
            ),
        )
    long = pl.concat(frames)
    if long.height == 0:
        return None
    return long.group_by(["def_pos_team", "player_name"]).agg(
        pl.col("sacks").sum(),
        pl.col("sacks_yards").sum(),
    )


# #93: exclude own recoveries -- a recovery is a defensive (takeaway) event
# only when the recovering side is not the fumbling side. Rows where the
# fumbling side could not be parsed are kept (cannot prove own recovery).


def build_defensive_players_box(play_df: pl.DataFrame) -> list:
    """Per-player defensive events (sacks with the split-sack convention, pass breakups,
    interceptions, forced fumbles, takeaway fumble recoveries) as JSON-ready records."""
    takeaway_only = (
        pl.col("fumbling_team").is_null() | (pl.col("fumble_recovery_team") != pl.col("fumbling_team"))
        if "fumbling_team" in play_df.columns
        else None
    )
    def_parts = [
        sack_part(play_df),
        player_event_box(play_df, "pass_breakup_player_name", "pass_breakups", "def_pos_team"),
        player_event_box(play_df, "interception_player_name", "interceptions", "def_pos_team", "yds_int_return"),
        # #93: group by forced_fumble_team (opposite the fumbling side) so a
        # coverage player forcing a punt/kick-return fumble is credited to the
        # kicking team, not blindly to def_pos_team.
        player_event_box(
            play_df,
            "fumble_forced_player_name",
            "forced_fumbles",
            "forced_fumble_team",
            team_out="def_pos_team",
        ),
        player_event_box(
            play_df,
            "fumble_recovered_player_name",
            "fumble_recoveries",
            "fumble_recovery_team",
            "yds_fumble_return",
            team_out="def_pos_team",
            extra_filter=takeaway_only,
        ),
    ]
    def_parts = [d for d in def_parts if d is not None]
    if def_parts:
        defensive_players = (
            reduce(
                lambda left, right: left.join(right, on=["def_pos_team", "player_name"], how="full", coalesce=True),
                def_parts,
            )
            .fill_null(0)
            .with_columns(def_pos_team=pl.col("def_pos_team").cast(pl.Int32))
        )
        defensive_players_json = json.loads(
            _ordered_rows(defensive_players, "def_pos_team", "player_name").write_json()
        )
    else:
        defensive_players_json = []

    # --- specialists (0.0.53): kicking / punting / return players, attributed by player ---
    # #93: credit kickoff returns to the RECEIVING team the same way punt returns
    # are -- prefer the parsed `kick_return_team`, coalescing to `pos_team` (ESPN
    # files a kickoff under the receiving team's possession) so a return is never
    # dropped just because the returning team abbreviation did not parse.
    return defensive_players_json


def build_specialists_box(play_df: pl.DataFrame) -> list:
    """Per-player kicking / punting / return events as JSON-ready records."""
    if "kick_return_team" in play_df.columns:
        play_df = play_df.with_columns(
            _kick_return_credit_team=pl.coalesce(pl.col("kick_return_team"), pl.col("pos_team")),
        )
        _kick_return_team_col = "_kick_return_credit_team"
    else:
        _kick_return_team_col = "pos_team"
    spec_parts = [
        player_event_box(play_df, "fg_kicker_player_name", "field_goals", "pos_team", "yds_fg"),
        player_event_box(play_df, "punter_player_name", "punts", "pos_team", "yds_punted"),
        player_event_box(
            play_df,
            "kickoff_return_player_name",
            "kick_returns",
            _kick_return_team_col,
            "yds_kickoff_return",
            team_out="pos_team",
        ),
        player_event_box(
            play_df,
            "punt_return_player_name",
            "punt_returns",
            "punt_return_team",
            "yds_punt_return",
            team_out="pos_team",
        ),
    ]
    spec_parts = [s for s in spec_parts if s is not None]
    if spec_parts:
        specialists = (
            reduce(
                lambda left, right: left.join(right, on=["pos_team", "player_name"], how="full", coalesce=True),
                spec_parts,
            )
            .fill_null(0)
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )
        specialists_json = json.loads(_ordered_rows(specialists, "pos_team", "player_name").write_json())
    else:
        specialists_json = []

    # ESPN's official team + player box -- the authoritative source for countable
    # totals (turnovers, fumbles, interceptions, total/passing/rushing yards,
    # penalties, first downs, player stat lines). Surfaced as dedicated sections so
    # downstream consumers can take totals straight from ESPN; the computed sections
    # above retain the advanced/EPA metrics ESPN does not provide.
    return specialists_json
