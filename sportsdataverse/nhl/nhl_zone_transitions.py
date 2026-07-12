"""NHL/PWHL zone-entry / zone-exit value (T5.2 model 1, constrained 🟡).

**Constrained model.** Base NHL play-by-play does NOT label a zone entry as
*controlled* (carry/pass-in, possession retained) vs *dump* (chip-and-chase,
possession contested) -- that distinction is a manually-tagged microstat
(AllThreeZones / Corey Sznajder). This module ships a **pbp-derived
approximation**: an entry is inferred when a team's puck-owning event's
``zone_code`` flips ``N``/``D`` -> ``O`` (an exit is the inverse ``D`` -> ``N``).

``controlled`` is an **event-sequence-aware heuristic** (fleshed out from the
original "any same-team event within window" rule): the entering team must
win the **very next possession event in the whole-game event order** (not
just any later same-team event, which could follow an intervening opponent
touch) AND that event must land within ``controlled_window_s`` seconds. This
is a strictly more conservative "uninterrupted possession retention" signal
-- a same-team event 3 seconds later that was preceded by an opponent
giveaway/faceoff no longer counts as controlled, because the opponent
touched the puck in between. ``seconds_to_next`` is, correspondingly, the
time to that very-next possession event (any team), not to the entering
team's own next event.

**Unblock path:** pass a ground-truth tag feed via ``tags=`` (columns
``game_id``, ``event_idx``, ``controlled``) -- e.g. an AllThreeZones season
CSV joined on game+event -- and the heuristic ``controlled`` is overridden by
the tagged truth with no API change. The entry *rate* is a stable signal even
when the controlled/dump label is noisy, which is what the oracle gates; the
label itself is validated with a directional sanity check (controlled
entries should precede a same-team shot attempt at a higher rate than dump
entries -- see ``test_zone_entry_label_directional_sanity`` in
``tests/nhl/test_nhl_microstat_oracle.py``).

Example:
    Quick start::

        from sportsdataverse.nhl.nhl_zone_transitions import nhl_zone_transitions

        out = nhl_zone_transitions(pbp)
        print(out.sort("entry_value", descending=True).head())

    With a manual-tag feed (ground-truth controlled/dump)::

        out = nhl_zone_transitions(pbp, tags=a3z_tags)

See Also:
    * `nhl-api-py`_ -- Python NHL API client (companion data source).

.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
"""

from __future__ import annotations

from typing import Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.nhl.nhl_microstat_constants import get_constants

# Puck-owning event types -- the event owner has possession at that moment.
_POSSESSION_TYPES = ("faceoff", "shot-on-goal", "goal", "missed-shot", "takeaway", "giveaway")

TRANSITION_SCHEMA = {
    "game_id": pl.Utf8,
    "event_idx": pl.Int64,
    "player_id": pl.Utf8,
    "team_id": pl.Utf8,
    "transition_type": pl.Utf8,
    "controlled": pl.Boolean,
    "seconds_to_next": pl.Float64,
}

VALUE_SCHEMA = {
    "player_id": pl.Utf8,
    "controlled_entries": pl.Int64,
    "dump_entries": pl.Int64,
    "exits": pl.Int64,
    "controlled_entry_rate": pl.Float64,
    "entry_value": pl.Float64,
    "exit_value": pl.Float64,
}


def _seconds_expr() -> pl.Expr:
    parts = pl.col("time_in_period").str.split(":")
    return parts.list.get(0).cast(pl.Int64) * 60 + parts.list.get(1).cast(pl.Int64)


def _actor_id() -> pl.Expr:
    # The player who owns a possession event: faceoff winner or shooter.
    return (
        pl.when(pl.col("type_desc_key") == "faceoff")
        .then(pl.col("winning_player_id"))
        .otherwise(pl.col("shooting_player_id"))
        .cast(pl.Utf8)
    )


def infer_zone_transitions(
    pbp: pl.DataFrame,
    *,
    league: str = "nhl",
    tags: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Infer zone entries/exits from the pbp possession-event sequence.

    Groups possession events per game+team in event order; an entry is a
    ``zone_code`` flip ``N``/``D`` -> ``O`` (exit is ``D`` -> ``N``).
    ``controlled`` is the heuristic "same team's next possession event within
    ``controlled_window_s`` seconds" (from ``get_constants(league)``), unless
    overridden by ``tags``.

    Args:
        pbp: Parsed pbp frame (Task-0.1 contract).
        league: League key for the ``controlled_window_s`` constant.
        tags: Optional ground-truth override with columns ``game_id``,
            ``event_idx``, ``controlled`` -- when supplied, its ``controlled``
            replaces the heuristic for matched events (dtype-checked on
            ``game_id``).

    Returns:
        One row per detected transition: ``game_id``, ``event_idx``,
        ``player_id``, ``team_id``, ``transition_type`` (``"entry"``/``"exit"``),
        ``controlled`` (Boolean), ``seconds_to_next`` (Float64 -- time to the
        very next possession event in the whole-game order, any team). Zero-row
        input returns a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_zone_transitions import infer_zone_transitions

            transitions = infer_zone_transitions(pbp)
    """
    if pbp.height == 0 or "type_desc_key" not in pbp.columns:
        return pl.DataFrame(schema=TRANSITION_SCHEMA)

    window = get_constants(league).controlled_window_s

    poss = pbp.filter(pl.col("type_desc_key").is_in(_POSSESSION_TYPES)).with_columns(
        _seconds_expr().alias("_secs"),
        _actor_id().alias("_actor"),
        pl.col("event_owner_team_id").cast(pl.Utf8).alias("_team"),
        pl.col("game_id").cast(pl.Utf8).alias("_game"),
    )
    if poss.height == 0:
        return pl.DataFrame(schema=TRANSITION_SCHEMA)

    # Sort per game by event order; prior zone is computed per (game, team) so
    # entry/exit detection never leaks across games or teams. The controlled
    # signal, by contrast, needs the WHOLE-GAME event order (any team) -- see
    # the module docstring's "event-sequence-aware" note: possession retention
    # means the entering team wins the very next possession event overall, not
    # just some later event of its own with an opponent touch in between.
    poss = poss.sort(["_game", "event_idx"])
    poss = poss.with_columns(
        pl.col("zone_code").shift(1).over(["_game", "_team"]).alias("_prev_zone"),
        pl.col("_team").shift(-1).over("_game").alias("_next_event_team"),
        pl.col("_secs").shift(-1).over("_game").alias("_next_event_secs"),
    )
    poss = poss.with_columns(
        pl.when((pl.col("_prev_zone").is_in(["N", "D"])) & (pl.col("zone_code") == "O"))
        .then(pl.lit("entry"))
        .when((pl.col("_prev_zone") == "D") & (pl.col("zone_code") == "N"))
        .then(pl.lit("exit"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("transition_type"),
        (pl.col("_next_event_secs") - pl.col("_secs")).cast(pl.Float64).alias("seconds_to_next"),
    )
    transitions = poss.filter(pl.col("transition_type").is_not_null())
    if transitions.height == 0:
        return pl.DataFrame(schema=TRANSITION_SCHEMA)

    transitions = transitions.with_columns(
        (
            pl.col("seconds_to_next").is_not_null()
            & (pl.col("seconds_to_next") <= window)
            & (pl.col("_next_event_team") == pl.col("_team"))
        ).alias("controlled")
    )

    out = transitions.select(
        pl.col("_game").alias("game_id"),
        pl.col("event_idx").cast(pl.Int64),
        pl.col("_actor").alias("player_id"),
        pl.col("_team").alias("team_id"),
        pl.col("transition_type"),
        pl.col("controlled"),
        pl.col("seconds_to_next"),
    )

    if tags is not None and tags.height > 0:
        tags = tags.with_columns(pl.col("game_id").cast(pl.Utf8), pl.col("event_idx").cast(pl.Int64))
        assert out.schema["game_id"] == tags.schema["game_id"], "tags game_id dtype mismatch"
        out = (
            out.join(
                tags.select("game_id", "event_idx", pl.col("controlled").alias("_tag")),
                on=["game_id", "event_idx"],
                how="left",
            )
            .with_columns(pl.coalesce("_tag", "controlled").alias("controlled"))
            .drop("_tag")
        )

    return out


@overload
def nhl_zone_transitions(
    pbp: pl.DataFrame,
    *,
    league: str = ...,
    tags: pl.DataFrame | None = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def nhl_zone_transitions(
    pbp: pl.DataFrame,
    *,
    league: str = ...,
    tags: pl.DataFrame | None = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
def nhl_zone_transitions(
    pbp: pl.DataFrame,
    *,
    league: str = "nhl",
    tags: pl.DataFrame | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Per-player controlled/dump entry & exit rates + xG-weighted values.

    ``entry_value = controlled_entries * zone_entry_value_controlled +
    dump_entries * zone_entry_value_dump``; ``exit_value = exits *
    zone_exit_value`` -- all from ``get_constants(league)``.

    Args:
        pbp: Parsed pbp frame (Task-0.1 contract).
        league: League key for the value constants.
        tags: Optional ground-truth controlled/dump override (see
            :func:`infer_zone_transitions`).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Per-player frame: ``player_id``, ``controlled_entries``,
        ``dump_entries``, ``exits``, ``controlled_entry_rate``, ``entry_value``,
        ``exit_value``. Zero-row input returns a zero-row frame with this
        schema.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_zone_transitions import nhl_zone_transitions

            out = nhl_zone_transitions(pbp)

        PWHL::

            out_pwhl = nhl_zone_transitions(pwhl_pbp, league="pwhl")

    See Also:
        * `nhl-api-py`_ -- Python NHL API client (companion data source).

    .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    transitions = infer_zone_transitions(pbp, league=league, tags=tags)
    if transitions.height == 0:
        empty = pl.DataFrame(schema=VALUE_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    constants = get_constants(league)
    entries = transitions.filter(pl.col("transition_type") == "entry")
    exits = transitions.filter(pl.col("transition_type") == "exit")

    entry_agg = entries.group_by("player_id").agg(
        (pl.col("controlled") == True).sum().alias("controlled_entries"),  # noqa: E712
        (pl.col("controlled") == False).sum().alias("dump_entries"),  # noqa: E712
    )
    exit_agg = exits.group_by("player_id").agg(pl.len().alias("exits"))

    out = entry_agg.join(exit_agg, on="player_id", how="full", coalesce=True).with_columns(
        pl.col("controlled_entries").fill_null(0),
        pl.col("dump_entries").fill_null(0),
        pl.col("exits").fill_null(0),
    )
    out = out.with_columns(
        (pl.col("controlled_entries") / (pl.col("controlled_entries") + pl.col("dump_entries")).cast(pl.Float64)).alias(
            "controlled_entry_rate"
        ),
        (
            pl.col("controlled_entries") * constants.zone_entry_value_controlled
            + pl.col("dump_entries") * constants.zone_entry_value_dump
        ).alias("entry_value"),
        (pl.col("exits") * constants.zone_exit_value).alias("exit_value"),
    )
    out = out.select(
        pl.col("player_id").cast(pl.Utf8),
        pl.col("controlled_entries").cast(pl.Int64),
        pl.col("dump_entries").cast(pl.Int64),
        pl.col("exits").cast(pl.Int64),
        pl.col("controlled_entry_rate").cast(pl.Float64),
        pl.col("entry_value").cast(pl.Float64),
        pl.col("exit_value").cast(pl.Float64),
    )
    return out.to_pandas() if return_as_pandas else out
