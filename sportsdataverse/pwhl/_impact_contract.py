"""HockeyTech PWHL pbp + shifts -> the NHL player-impact column contract (task 2.13, C-NF-5).

The four stint models (``nhl_skater_rapm`` / ``nhl_unit_ratings`` / ``nhl_special_teams_value`` /
``nhl_skater_war``) read a ``load_nhl_pbp_full``-shaped pbp and a ``load_nhl_shifts``-shaped
CHANGE-event shift chart. ``load_pwhl_pbp`` / ``load_pwhl_shifts`` are HockeyTech-shaped: an
``event`` column in lower case, the shot type in ``event_type``, one row per player shift with
period-clock ``start_s`` / ``end_s``. :func:`adapt_pwhl_impact_inputs` maps one onto the other.

What the HockeyTech feed cannot supply, and how the adapter handles it:

- No missed shots: ``xg`` is :func:`sportsdataverse.pwhl.pwhl_xg_proxy.pwhl_shot_xg` on the on-net
  shots (the PWHL coordinate model ``pwhl_team_ratings`` uses), never the NHL boosters, which run
  ~1/3 low on PWHL shots (goals / sum(xg) on full 2025: 0.74 overall, 0.65 at 5v5). Penalty shots
  take ``nhl_xg``'s published penalty-shot constant. The engines are called with ``xg_scored=True``.
- Faceoffs name only the home taker (``player_id`` + ``home_win``); the away taker is absent, so the
  WAR faceoff component counts home takers only.
- Penalties name the taker; the drawer is absent (``player_two_id`` is the server), so the WAR
  penalty component counts penalties taken only.
- Strength comes from the feed's shift-derived ``strength_state`` (home v away) where
  ``strength_state_valid``. Elsewhere (430 of the 5,671 on-net shot rows of 2025, 7.6%, are flagged invalid) the
  row carries the last valid strength of its game, as every inserted ``CHANGE`` row does. A carried
  strength lags any change the feed only reports at its next event, so strength transitions land
  late: the seconds after a penalty starts or expires are credited to the previous state.

``secondary_type`` is kept for symmetry with the NHL contract (an adapted frame can still go
through ``nhl_xg``); the PWHL engines, scored with ``xg_scored=True``, do not consume it.

A required source column that is absent raises ``ValueError`` naming it; the output is checked
against :data:`IMPACT_PBP_CONTRACT` / :data:`IMPACT_SHIFTS_CONTRACT` before it is returned.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from sportsdataverse.nhl.nhl_xg import _PS_DEFAULT
from sportsdataverse.pwhl.pwhl_xg_proxy import _assert_rink_feet, pwhl_shot_xg

#: Columns (and dtypes) the NHL impact engines read off the pbp; asserted on the adapter output.
IMPACT_PBP_CONTRACT: dict[str, pl.DataType] = {
    "game_id": pl.Int64(),
    "event_id": pl.Int64(),
    "event_idx": pl.Int64(),
    "season": pl.Int64(),
    "period": pl.Int64(),
    "period_type": pl.Utf8(),
    "game_seconds": pl.Int64(),
    "event_type": pl.Utf8(),
    "secondary_type": pl.Utf8(),
    "event_team_abbr": pl.Utf8(),
    "home_abbr": pl.Utf8(),
    "away_abbr": pl.Utf8(),
    "x": pl.Float64(),
    "y": pl.Float64(),
    "x_fixed": pl.Float64(),
    "xg": pl.Float64(),
    "empty_net": pl.Boolean(),
    "strength_state": pl.Utf8(),
    "home_skaters": pl.Int64(),
    "away_skaters": pl.Int64(),
    "event_player_1_id": pl.Int64(),
    "event_player_2_id": pl.Int64(),
}

#: Columns (and dtypes) ``build_stints`` reads off the shift chart.
IMPACT_SHIFTS_CONTRACT: dict[str, pl.DataType] = {
    "game_id": pl.Int64(),
    "period": pl.Int64(),
    "game_seconds": pl.Int64(),
    "team_abbr": pl.Utf8(),
    "ids_on": pl.Utf8(),
    "ids_off": pl.Utf8(),
}

_PBP_SOURCE = (
    "game_id", "event", "event_type", "team_id", "period_of_game", "time_of_period", "x_coord", "y_coord",
    "x_coord_fixed", "player_id", "goal", "goalie_id", "home_win", "empty_net", "game_season", "home_team",
    "home_team_id", "away_team", "away_team_id", "strength_state", "strength_state_valid", "skaters_home",
    "skaters_away",
)  # fmt: skip
_SHIFTS_SOURCE = ("game_id", "player_id", "home", "period", "start_s", "end_s")

_EVENT_TYPE = {
    "faceoff": "FACEOFF",
    "hit": "HIT",
    "blocked_shot": "BLOCKED_SHOT",
    "penalty": "PENALTY",
    "goalie_change": "GOALIE_CHANGE",
}
# HockeyTech shot types -> the NHL api-web spellings `nhl_xg` normalises ("Default" / null -> null).
_SHOT_TYPE = {"Wrist": "wrist", "Snap": "snap", "Slap": "slap", "Backhand": "backhand", "Tip": "tip-in"}
_REGULATION_PERIOD_SECONDS = 1200


def _require(frame: pl.DataFrame, columns: tuple[str, ...], what: str) -> None:
    missing = [c for c in columns if c not in frame.columns]
    if missing:
        raise ValueError(f"{what} lacks {missing}: the HockeyTech feed cannot supply the impact contract without them")


def _check_contract(frame: pl.DataFrame, contract: dict[str, pl.DataType], what: str) -> pl.DataFrame:
    bad = {c: str(frame.schema.get(c)) for c, t in contract.items() if frame.schema.get(c) != t}
    if bad:
        raise ValueError(f"{what} breaks the impact contract (column: actual dtype): {bad}")
    return frame


def is_hockeytech_pbp(pbp: pl.DataFrame) -> bool:
    """True when ``pbp`` is ``load_pwhl_pbp``-shaped (HockeyTech ``event`` column), not NHL-shaped."""
    return "event" in pbp.columns and "event_id" not in pbp.columns


def adapt_pwhl_pbp(pbp: pl.DataFrame, *, xg_model: Any = None) -> pl.DataFrame:
    """Map a ``load_pwhl_pbp`` frame onto :data:`IMPACT_PBP_CONTRACT`.

    Shot rows become ``SHOT`` / ``GOAL`` by their ``goal`` flag; the HockeyTech goal-summary row that
    follows every scoring shot is dropped after lending the shot its ``empty_net`` flag, so each goal
    is counted once. Shootout rows are dropped (the engines never score them). ``xg`` is
    :func:`pwhl_shot_xg` on the on-net shots.

    Args:
        pbp: a ``load_pwhl_pbp``-shaped frame in rink feet (a raw 0-600 canvas frame raises).
        xg_model: a fitted ``PwhlCoordXGModel`` passed to :func:`pwhl_shot_xg`; ``None`` fits one
            on ``pbp`` (pass a model fit on a full season when adapting a few games).

    Returns:
        polars.DataFrame: ``pbp`` reduced to the contract columns, in feed order.

    Raises:
        ValueError: a source column is missing, or the output breaks the contract.
    """
    _require(pbp, _PBP_SOURCE, "PWHL pbp")
    _assert_rink_feet(pbp)
    pbp = pbp.with_row_index("_row")
    # pwhl_shot_xg scores the `event == "shot"` rows and returns them in pbp order.
    on_net = pbp.filter(pl.col("event") == "shot")
    scored = pwhl_shot_xg(pbp.drop("_row"), model=xg_model)
    assert scored.height == on_net.height, f"pwhl_shot_xg scored {scored.height} of {on_net.height} shots"
    for key in ("game_id", "player_id"):
        aligned = on_net[key].cast(pl.Int64).eq_missing(scored[key].cast(pl.Int64)).all()
        assert aligned, f"pwhl_shot_xg rows are not in pbp order ({key})"
    pbp = pbp.join(on_net.select("_row").with_columns(_xg=scored["xg"]), on="_row", how="left")
    period_raw = pl.col("period_of_game")
    period = (
        pl.when(period_raw.str.starts_with("OT"))
        .then(period_raw.str.slice(2).cast(pl.Int64, strict=False) + 3)
        .otherwise(period_raw.cast(pl.Int64, strict=False))
    )
    clock = pl.col("time_of_period").str.split(":")
    elapsed = clock.list.get(0).cast(pl.Int64) * 60 + clock.list.get(1).cast(pl.Int64)
    is_home = pl.col("team_id") == pl.col("home_team_id")
    side_abbr = pl.when(is_home).then(pl.col("home_team")).otherwise(pl.col("away_team"))
    shot = pl.col("event").is_in(["shot", "penaltyshot"])
    valid = pl.col("strength_state_valid") == True
    next_is_goal = pl.col("event").shift(-1).over("game_id") == "goal"
    home_won = pl.col("home_win") == "1"

    out = (
        pbp.with_columns(_empty_net=pl.when(next_is_goal).then(pl.col("empty_net").shift(-1).over("game_id")))
        .filter(pl.col("event").is_in(["goal", "shootout"]) == False)
        .with_columns(_period=period)
        .with_columns(
            event_idx=pl.int_range(pl.len(), dtype=pl.Int64),
            event_id=pl.int_range(pl.len(), dtype=pl.Int64).over("game_id"),
            season=(pl.col("game_season").cast(pl.Int64) - 1) * 10000 + pl.col("game_season").cast(pl.Int64),
            period=pl.col("_period"),
            period_type=pl.when(pl.col("_period") <= 3).then(pl.lit("REGULAR")).otherwise(pl.lit("OVERTIME")),
            game_seconds=(pl.col("_period") - 1) * _REGULATION_PERIOD_SECONDS + elapsed,
            event_type=pl.when(shot)
            .then(pl.when(pl.col("goal") == True).then(pl.lit("GOAL")).otherwise(pl.lit("SHOT")))
            .otherwise(pl.col("event").replace_strict(_EVENT_TYPE, default=None)),
            secondary_type=pl.when(pl.col("event") == "penaltyshot")
            .then(pl.lit("Penalty Shot"))
            .when(shot)
            .then(pl.col("event_type").replace_strict(_SHOT_TYPE, default=None)),
            event_team_abbr=pl.when(pl.col("event") == "faceoff")
            .then(pl.when(home_won).then(pl.col("home_team")).otherwise(pl.col("away_team")))
            .when(pl.col("team_id").is_not_null())
            .then(side_abbr),
            home_abbr=pl.col("home_team"),
            away_abbr=pl.col("away_team"),
            x=pl.col("x_coord").cast(pl.Float64),
            y=pl.col("y_coord").cast(pl.Float64),
            x_fixed=pl.col("x_coord_fixed").cast(pl.Float64),
            xg=pl.when(pl.col("event") == "penaltyshot").then(pl.lit(_PS_DEFAULT)).otherwise(pl.col("_xg")),
            empty_net=(pl.col("_empty_net") == "1").fill_null(False),
            strength_state=pl.when(valid).then(pl.col("strength_state")),
            home_skaters=pl.when(valid).then(pl.col("skaters_home").cast(pl.Int64)),
            away_skaters=pl.when(valid).then(pl.col("skaters_away").cast(pl.Int64)),
            # FACEOFF: winner / loser (only the home taker is named); PENALTY: taker / (drawer absent).
            event_player_1_id=pl.when(pl.col("event") == "faceoff")
            .then(pl.when(home_won).then(pl.col("player_id")))
            .otherwise(pl.col("player_id"))
            .cast(pl.Int64),
            event_player_2_id=pl.when((pl.col("event") == "faceoff") & (home_won == False))
            .then(pl.col("player_id"))
            .cast(pl.Int64),
        )
        .sort("_row")
        .with_columns(pl.col("game_id").cast(pl.Int64))
        .select(list(IMPACT_PBP_CONTRACT))
    )
    return _check_contract(out, IMPACT_PBP_CONTRACT, "adapted PWHL pbp")


def adapt_pwhl_shifts(shifts: pl.DataFrame, pbp: pl.DataFrame) -> pl.DataFrame:
    """Fold ``load_pwhl_shifts`` per-player rows into ``load_nhl_shifts``-style CHANGE events.

    Each shift contributes an "on" at its start and an "off" at its end; both are grouped per
    (game, second, team) so a player whose shifts abut is taken off before being put back on.
    ``start_s`` / ``end_s`` are seconds left on the period clock; ``game_seconds`` uses the same
    1200 s period base as :func:`adapt_pwhl_pbp`. Goalies (any ``goalie_id`` in that game's pbp)
    are dropped so the stints carry skaters only.

    Args:
        shifts: a ``load_pwhl_shifts``-shaped frame.
        pbp: the matching ``load_pwhl_pbp``-shaped frame (team names and goalie ids per game).

    Returns:
        polars.DataFrame: :data:`IMPACT_SHIFTS_CONTRACT` columns, one row per (game, second, team).

    Raises:
        ValueError: a source column is missing, or the output breaks the contract.
    """
    _require(shifts, _SHIFTS_SOURCE, "PWHL shifts")
    _require(pbp, ("game_id", "goalie_id", "home_team", "away_team"), "PWHL pbp")
    games = pbp.group_by("game_id").agg(pl.col("home_team").first(), pl.col("away_team").first())
    goalies = pbp.select("game_id", pl.col("goalie_id").alias("player_id")).drop_nulls().unique()
    work = shifts.with_columns(pl.col("game_id").cast(pl.Int64), pl.col("player_id").cast(pl.Int64))
    for key in ("game_id", "player_id"):
        assert work.schema[key] == goalies.schema[key], (
            f"{key} dtype mismatch: {work.schema[key]} vs {goalies.schema[key]}"
        )
    assert work.schema["game_id"] == games.schema["game_id"]
    period_len = (
        pl.when(pl.col("period") <= 3)
        .then(pl.lit(_REGULATION_PERIOD_SECONDS, dtype=pl.Int64))
        .otherwise(pl.col("start_s").max().over("game_id", "period"))
    )
    base = (pl.col("period").cast(pl.Int64) - 1) * _REGULATION_PERIOD_SECONDS
    work = (
        work.join(goalies, on=["game_id", "player_id"], how="anti")
        .join(games, on="game_id", how="inner")
        .with_columns(
            period=pl.col("period").cast(pl.Int64),
            team_abbr=pl.when(pl.col("home") == 1).then(pl.col("home_team")).otherwise(pl.col("away_team")),
            _on=base + period_len - pl.col("start_s"),
            # 666 of 81,546 2025 shifts have no end: still on the ice when the period clock ran out.
            _off=base + period_len - pl.col("end_s").fill_null(0),
            _id=pl.col("player_id").cast(pl.Utf8),
        )
    )
    keys = ["game_id", "period", "game_seconds", "team_abbr"]
    ons = work.select("game_id", "period", pl.col("_on").alias("game_seconds"), "team_abbr", pl.col("_id").alias("on"))
    offs = work.select(
        "game_id", "period", pl.col("_off").alias("game_seconds"), "team_abbr", pl.col("_id").alias("off")
    )
    events = pl.concat([ons, offs], how="diagonal_relaxed")
    out = (
        events.group_by(["game_id", "game_seconds", "team_abbr"], maintain_order=True)
        .agg(
            pl.col("period").max(),
            ids_on=pl.col("on").drop_nulls().str.join(", "),
            ids_off=pl.col("off").drop_nulls().str.join(", "),
        )
        .select(*keys, "ids_on", "ids_off")
        .sort("game_id", "game_seconds", maintain_order=True)
    )
    return _check_contract(out, IMPACT_SHIFTS_CONTRACT, "adapted PWHL shifts")


def adapt_pwhl_impact_inputs(
    pbp: pl.DataFrame, shifts: pl.DataFrame, *, xg_model: Any = None
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Adapt a HockeyTech pbp + shift pair for the NHL impact engines; NHL-shaped input passes through.

    Args:
        pbp: a ``load_pwhl_pbp``-shaped (or already ``load_nhl_pbp_full``-shaped) frame.
        shifts: the matching ``load_pwhl_shifts``-shaped (or ``load_nhl_shifts``-shaped) frame.
        xg_model: forwarded to :func:`adapt_pwhl_pbp`.

    Returns:
        tuple: ``(pbp, shifts)`` on the NHL contract. Like ``load_nhl_pbp_full``, the pbp carries a
        ``CHANGE`` row at every line change. A ``CHANGE`` row, and any event the feed flags as
        invalid strength, takes the last valid strength reported in that game, so a stint with no
        other event still gets a ``strength_state`` (``build_stints`` reads it off the events
        inside the stint). The carried strength lags a change the feed only reports at its next
        event, so strength transitions land late, never early.
    """
    if not is_hockeytech_pbp(pbp):
        return pbp, shifts
    events, chart = adapt_pwhl_pbp(pbp, xg_model=xg_model), adapt_pwhl_shifts(shifts, pbp)
    games = events.group_by("game_id").agg(pl.col("season", "home_abbr", "away_abbr").first())
    changes = (
        chart.select("game_id", "period", "game_seconds", pl.col("team_abbr").alias("event_team_abbr"))
        .join(games, on="game_id", how="inner")
        .with_columns(
            event_type=pl.lit("CHANGE"),
            period_type=pl.when(pl.col("period") <= 3).then(pl.lit("REGULAR")).otherwise(pl.lit("OVERTIME")),
            empty_net=pl.lit(False),
        )
    )
    state = ("strength_state", "home_skaters", "away_skaters")
    out = (
        pl.concat([events, changes], how="diagonal_relaxed")
        .sort("game_id", "game_seconds", maintain_order=True)
        .with_columns([pl.col(c).forward_fill().over("game_id").alias(c) for c in state])
        .with_columns(
            event_idx=pl.int_range(pl.len(), dtype=pl.Int64),
            event_id=pl.int_range(pl.len(), dtype=pl.Int64).over("game_id"),
        )
        .select(list(IMPACT_PBP_CONTRACT))
    )
    return _check_contract(out, IMPACT_PBP_CONTRACT, "adapted PWHL pbp"), chart
