"""Play-description regex layer (ported from nflfastR helper_add_nflscrapr_mutations.R).

Adds the text-derived columns the models / nflverse schema expect — chiefly
``pass_location`` (CP feature) plus ``pass_length``, ``run_location``,
``run_gap``, and the qb_kneel / qb_spike / qb_scramble / shotgun / no_huddle
indicators — and refines ``play_type`` to ``qb_kneel`` / ``qb_spike``.

:func:`add_pass_rush` (separate from :func:`add_description_features` because
it must run AFTER :func:`sportsdataverse.nfl.shield_pbp.repairs.fix_scrambles` has backfilled
``qb_scramble``) ports nflfastR ``clean_pbp``'s ``pass``/``rush`` 0/1
classification (reference §6), including the §3 ``fix_weird_pass_plays``
override applied at nflfastR's exact position — between the ``pass`` and
``rush`` derivations, so ``rush`` is computed from the *fixed* ``pass``.

``air_yards`` is NOT parsed here — it comes from the stats feed (statType
111/112) in :mod:`sportsdataverse.nfl.shield_pbp.stat_ids`.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nfl.shield_pbp.repairs import fix_weird_pass_plays


def add_description_features(df: pl.DataFrame) -> pl.DataFrame:
    """Add description-derived columns + refine play_type. Expects a ``desc`` column.

    Args:
        df: A base play frame from :func:`sportsdataverse.nfl.shield_pbp.parse.parse_game`.

    Returns:
        The frame with ``pass_length``, ``pass_location``, ``run_location``,
        ``run_gap``, ``qb_kneel``, ``qb_spike``, ``qb_scramble``, ``shotgun``,
        ``no_huddle`` added and ``play_type`` refined for kneels/spikes.
    """
    if df.height == 0:
        return df
    d = pl.col("desc").fill_null("")
    df = df.with_columns(
        pass_length=d.str.extract(r"pass (?:incomplete )?(short|deep)", 1),
        pass_location=d.str.extract(r"(?:short|deep) (left|middle|right)", 1),
        run_location_raw=d.str.extract(r" (left|middle|right)[ .]", 1),
        run_gap_raw=d.str.extract(r" (guard|tackle|end)\b", 1),
        qb_kneel=d.str.contains(" kneels").cast(pl.Int64),
        qb_spike=d.str.contains(" spiked").cast(pl.Int64),
        qb_scramble=d.str.contains(" scrambles").cast(pl.Int64),
        shotgun=d.str.contains("Shotgun").cast(pl.Int64),
        no_huddle=d.str.contains("No Huddle").cast(pl.Int64),
    )
    # run_location / run_gap only meaningful on runs.
    df = df.with_columns(
        run_location=pl.when(pl.col("play_type") == "run").then(pl.col("run_location_raw")).otherwise(None),
        run_gap=pl.when(pl.col("play_type") == "run").then(pl.col("run_gap_raw")).otherwise(None),
    ).drop("run_location_raw", "run_gap_raw")
    # Refine play_type: kneels/spikes override the rush/pass base class.
    df = df.with_columns(
        play_type=pl.when(pl.col("qb_kneel") == 1)
        .then(pl.lit("qb_kneel"))
        .when(pl.col("qb_spike") == 1)
        .then(pl.lit("qb_spike"))
        .otherwise(pl.col("play_type"))
    )
    return df


def add_pass_rush(df: pl.DataFrame) -> pl.DataFrame:
    """Derive the nflverse ``pass`` / ``rush`` 0/1 classification columns.

    Faithful port of nflfastR ``clean_pbp``'s derivation (reference §6, the
    block around ``helper_additional_functions.R`` lines 162-191), in its
    exact mutate order:

    1. ``pass = 1`` when ``desc`` matches ``( pass )|(sacked)|(scramble)``
       (case-sensitive, as in the R source) OR ``qb_scramble == 1``, else 0.
    2. ...unless a lowercase ``desc`` contains ``backward pass`` /
       ``backwards pass`` / ``lateral pass`` AND there is a rusher — then 0.
    3. ...and never on a kickoff (``kickoff_attempt == 1`` forces 0).
    4. :func:`sportsdataverse.nfl.shield_pbp.repairs.fix_weird_pass_plays` — the hardcoded 15-row
       false-positive override (reference §3) — applied HERE, before ``rush``,
       exactly where nflfastR applies it, so step 5 sees the fixed value.
    5. ``rush = 1`` when there is a rusher, ``qb_kneel == 0`` and the (fixed)
       ``pass == 0``, else 0.

    nflfastR's "is there a rusher" input is ``clean_pbp``'s regex-extracted
    ``rusher`` name; the native frame's equivalent is the structured-stats
    ``rusher_player_name`` (statIds 10-13 etc.). The two converge for this
    purpose: clean_pbp itself backfills/overwrites ``rusher`` from
    ``rusher_player_name`` on aborted/abnormal plays, and the one branch where
    they differ — 1999-2005 charting-fixed scrambles, where clean_pbp nulls
    ``rusher`` after promoting it to ``passer`` — is neutralized here because
    those rows have ``pass == 1`` (via ``qb_scramble``), which already forces
    ``rush = 0`` regardless of the rusher column.

    Must run AFTER :func:`sportsdataverse.nfl.shield_pbp.repairs.fix_scrambles`: the 1999-2005
    scramble backfill feeds step 1's ``qb_scramble == 1`` condition (in
    nflfastR, ``clean_pbp`` likewise runs long after ``fix_scrambles``).

    Args:
        df: A play frame carrying ``desc``, ``qb_scramble``, ``qb_kneel``,
            ``kickoff_attempt``, ``rusher_player_name`` (plus ``game_id`` /
            ``play_id`` for the step-4 override).

    Returns:
        ``df`` with ``pass`` and ``rush`` (Int64 0/1) added. Unchanged if
        empty or if any required input column is absent.

    Note:
        The sdv-py sibling port (``sportsdataverse/nfl/nfl_clean.py``) derives
        ``pass``/``rush`` from the CLEANED ``rusher`` column, while this module
        uses the raw ``rusher_player_name``; the two can theoretically diverge
        on lateral/direct-snap plays, where the abnormal-play overwrite can null
        the cleaned ``rusher`` without ``pass`` being forced to 1.

        Null-``desc`` divergence: step 1's ``d = pl.col("desc").fill_null("")``
        makes a null ``desc`` match none of the base regexes, so ``pass`` comes
        out ``0``, whereas nflfastR's R regex propagates ``NA`` through to
        ``pass`` on the same row.
    """
    if df.height == 0:
        return df
    required = {"desc", "qb_scramble", "qb_kneel", "kickoff_attempt", "rusher_player_name"}
    if not required <= set(df.columns):
        return df

    d = pl.col("desc").fill_null("")
    has_rusher = pl.col("rusher_player_name").is_not_null()

    # Steps 1-3: base detection, backward/lateral exclusion, kickoff exclusion.
    df = df.with_columns(
        **{
            "pass": pl.when(d.str.contains(r"( pass )|(sacked)|(scramble)") | (pl.col("qb_scramble") == 1))
            .then(1)
            .otherwise(0)
            .cast(pl.Int64)
        }
    )
    df = df.with_columns(
        **{
            "pass": pl.when(
                d.str.to_lowercase().str.contains(r"(backward pass)|(backwards pass)|(lateral pass)") & has_rusher
            )
            .then(0)
            .when(pl.col("kickoff_attempt") == 1)
            .then(0)
            .otherwise(pl.col("pass"))
        }
    )
    # Step 4: hardcoded false-positive override — BEFORE rush, per nflfastR.
    df = fix_weird_pass_plays(df)
    # Step 5: rush from the fixed pass.
    df = df.with_columns(
        rush=pl.when(has_rusher & (pl.col("qb_kneel") == 0) & (pl.col("pass") == 0)).then(1).otherwise(0).cast(pl.Int64)
    )
    return df


def add_qb_dropback(df: pl.DataFrame) -> pl.DataFrame:
    """Add ``qb_dropback`` (reference §14, ``helper_add_nflscrapr_mutations.R``
    lines 438-445).

    The R source defines ``qb_dropback = play_type == "pass" | (play_type ==
    "run" & qb_scramble == 1)``. On this frame, ``pass`` (from
    :func:`add_pass_rush`) is already ``1`` whenever ``qb_scramble == 1`` (its
    step-1 base detection is ``... | qb_scramble == 1``), so
    ``play_type == "pass"`` and ``pass == 1`` are equivalent inputs here and
    the scramble branch is already folded in — this is transcribed directly as
    ``pass == 1 or qb_scramble == 1`` (the ``qb_scramble`` term is therefore
    redundant-but-harmless, kept for exactness with the port contract).

    Must run AFTER :func:`add_pass_rush` (needs the final, fixed ``pass`` value).

    Args:
        df: A play frame carrying ``pass`` and ``qb_scramble``.

    Returns:
        ``df`` with ``qb_dropback`` (Int32 0/1) added. Unchanged if empty or
        if either required column is absent.
    """
    if df.height == 0:
        return df
    if not {"pass", "qb_scramble"} <= set(df.columns):
        return df
    df = df.with_columns(
        qb_dropback=pl.when((pl.col("pass") == 1) | (pl.col("qb_scramble") == 1)).then(1).otherwise(0).cast(pl.Int32)
    )
    return df
