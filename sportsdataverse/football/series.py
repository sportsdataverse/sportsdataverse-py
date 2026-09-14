"""cfbfastR's series / first-down decomposition, shared by the CFB and NFL
play-by-play processors (see :func:`add_series_data` for provenance)."""

from __future__ import annotations

import polars as pl

__all__ = ["add_series_data"]


def add_series_data(play_df: pl.DataFrame, *, penalty, normalplay) -> pl.DataFrame:
    """Port of cfbfastR's series / first-down decomposition.

    Provenance (verbatim port, bug-for-bug):
        * ``cfbfastR/R/pbp_prep_epa_df_after.R`` L194-298 --
          ``first_by_penalty`` / ``first_by_yards`` row flags, their lag
          ladder, ``new_series``, and the four ``firstD_by_*`` outputs.
        * ``cfbfastR/R/pbp_clean_drive_dat.R`` L18-66 + L300-312 -- the
          half-scoped ``lag(1..3)`` event-flag lags (0-filled at half
          starts) and ``drive_event_number`` (within-drive cumsum of
          non-End rows).
        * ``cfbfastR/R/pbp_clean_pbp_dat.R`` L388-390 -- ``lag_play_type``
          is UNGROUPED on the per-game frame ("End of Half" is not in the
          Timeout/End-Period skip set, so crossing the half boundary is
          inert).

    Emits TWO first-down representations, both Boolean:

    * **Earned family** (``first_down_yards`` / ``first_down_penalty`` /
      ``first_down_earned``) -- flagged on the play that EARNED the first
      down, so the row's own ``pos_team`` is the crediting team. This is
      the COUNTING surface: box/team aggregation sums these grouped by
      ``pos_team``. Mutually exclusive buckets; yards takes precedence.
    * **Series-start family** (``firstD_by_kickoff`` / ``firstD_by_poss``
      / ``firstD_by_penalty`` / ``firstD_by_yards`` + ``new_series``) --
      the verbatim cfbfastR port: flags the first play of the NEW series
      with the cause it began (1-3 rows after an earning conversion, via
      the Timeout/End-Period lag ladder). ``_poss`` / ``_kickoff`` mark
      UNEARNED (happenstance) series starts -- possession change, drive
      start, kickoff -- and must never be counted as earned first downs.
      Not a counting surface: the ladder drops drive-ending conversions
      (TDs, end-of-half) and can double-fire across a Timeout row (the
      Timeout row takes the lag1 branch, the next play the lag2 branch) --
      both faithful to R.

    Known divergence from R (documented, benign): R leaves ``firstD_by_yards``
    ``NA`` on rows 1-2 of a half (unfilled lag2/lag3); we emit ``False``.
    """
    to_ep = ["Timeout", "End Period"]
    end_rows = ["End Period", "End of Half", "End of Game"]
    # the Fox cleaning pipeline reaches this stage without the attribution
    # columns; a null penalized_team simply disables the auto-first-down
    # foul branch (penalty_1st_conv still counts)
    if "penalized_team" not in play_df.columns:
        play_df = play_df.with_columns(penalized_team=pl.lit(None, dtype=pl.Int32))
    play_df = (
        play_df.with_columns(
            first_by_penalty=pl.when(
                (pl.col("type.text").is_in(penalty)).and_(pl.col("penalty_1st_conv") == True),
            )
            .then(True)
            .when(
                (pl.col("type.text").is_in(penalty))
                .and_(pl.col("penalty_declined") == True)
                .and_(pl.col("penalty_offset") == True)
                .and_(pl.col("penalty_1st_conv") == False)
                .and_(pl.col("statYardage") > pl.col("start.distance")),
            )
            .then(True)
            .when(
                (pl.col("type.text").is_in(penalty))
                .and_(pl.col("penalty_declined") == True)
                .and_(pl.col("penalty_offset") == False)
                .and_(pl.col("penalty_1st_conv") == False)
                .and_(pl.col("statYardage") >= pl.col("start.distance")),
            )
            .then(True)
            .otherwise(False),
            first_by_yards=(
                (pl.col("type.text").is_in(normalplay)).or_(pl.col("type.text") == "Fumble Recovery (Own)")
            ).and_(pl.col("statYardage") >= pl.col("start.distance")),
            drive_event=pl.col("type.text").is_in(end_rows) == False,
        )
        .with_columns(
            # --- Earned-first-down family (conversion-row, snap-attributed) ---
            # Unlike the series-start firstD_by_* family below (which flags
            # the first play of the NEW series, 1-3 rows after the fact),
            # these flag the play that EARNED the first down, so the row's
            # own pos_team is the crediting team -- no lag attribution
            # ambiguity, and drive-ending conversions (TDs, end-of-half)
            # are captured. Kickoffs / punts / PATs / non-play rows are
            # excluded by the play-type lists. Buckets are mutually
            # exclusive; yards takes precedence (official scoring: if the
            # play reached the line to gain, penalty yardage is moot).
            first_down_yards=pl.when(
                # 1) vendor text marker -- ESPN's play text writes an
                # explicit "1ST down"/"1ST DOWN" on converting plays; where
                # present it is per-play ground truth and overrides the
                # derived rule (it counts spot/distance-data mismatches and
                # the occasional goal-to-go TD the stat crew credited).
                # Penalty-involved rows are excluded here so accepted-penalty
                # conversions keep routing to the penalty bucket.
                (pl.col("text").str.contains(r"(?i)\b1st down\b").fill_null(False))
                .and_(pl.col("text").str.contains("(?i)short of") == False)
                .and_(pl.col("text").str.contains("(?i)penalty") == False)
                .and_(
                    (
                        pl.col("type.text").is_in(
                            [
                                *normalplay,
                                "Passing Touchdown",
                                "Rushing Touchdown",
                                "Pass Reception Touchdown",
                                "Fumble Recovery (Own) Touchdown",
                            ],
                        )
                    ).or_(
                        # fumble rows keep the marker only when possession
                        # was retained (own recovery)
                        (pl.col("type.text").is_in(["Fumble", "Fumble Recovery (Own)"])).and_(
                            pl.col("change_of_pos_team") == False,
                        ),
                    ),
                ),
            )
            .then(True)
            .when(
                # 2) derived rule
                (
                    (pl.col("type.text").is_in(normalplay)).or_(
                        # offensive scrimmage TDs reach the line to gain but
                        # sit outside normalplay (own play-type strings) --
                        # the structural miss of both first_down_created
                        # (end.down != 1 after a TD) and the series ladder
                        # (next row is a kickoff / new possession).
                        pl.col("type.text").is_in(
                            [
                                "Passing Touchdown",
                                "Rushing Touchdown",
                                "Pass Reception Touchdown",
                                "Fumble Recovery (Own) Touchdown",
                            ],
                        ),
                    )
                )
                .and_(pl.col("statYardage") >= pl.col("start.distance"))
                # goal-to-go has NO line to gain, so nothing can earn a
                # first down there (a goal-to-go TD scores without one) --
                # verified vs the ESPN box: e.g. 401032062/BYU is exact
                # only when the 4 goal-to-go TDs are excluded.
                .and_(pl.col("start.distance") < pl.col("start.yardsToEndzone"))
                # a penalty-wiped play earns nothing by yards (the accepted
                # penalty path below handles any awarded first down).
                # penalty_negated_play is TRI-STATE: null means the
                # enforcement class is unknown, not that the play was
                # wiped -- only a CONFIRMED wipe blocks the yards credit
                # (null == False is null in polars and would silently
                # drop the whole branch).
                .and_(pl.col("penalty_negated_play").fill_null(False) == False)
                .and_(pl.col("penalty_no_play").fill_null(False) == False),
            )
            .then(True)
            .when(
                # 3) declined / offset penalty where the play result stood and
                # converted (cfbfastR first_by_penalty branches 2-3; officially
                # these are first downs by yards, so they bucket here)
                (pl.col("type.text").is_in(penalty))
                .and_(pl.col("penalty_declined") == True)
                .and_(pl.col("penalty_1st_conv") == False)
                .and_(
                    ((pl.col("penalty_offset") == True).and_(pl.col("statYardage") > pl.col("start.distance"))).or_(
                        (pl.col("penalty_offset") == False).and_(
                            pl.col("statYardage") >= pl.col("start.distance"),
                        ),
                    ),
                ),
            )
            .then(True)
            .otherwise(False),
        )
        .with_columns(
            first_down_penalty=(
                (pl.col("penalty_1st_conv") == True).or_(
                    # accepted automatic-first-down foul by the DEFENSE whose
                    # text lacks the "1st down" phrase (era/vendor dependent).
                    # NCAA automatic-first-down fouls ONLY: DPI, roughing the
                    # passer/kicker/snapper, targeting. Personal foul /
                    # facemask / horse collar are 15 yards WITHOUT an
                    # automatic first down in NCAA (unlike the NFL) and were
                    # measured to over-fire. Gated on the text-resolved
                    # penalized_team (binary home/away match) -- this branch
                    # was rejected while attribution ran ~50% reliable and
                    # re-added once the token matcher fixed it.
                    (
                        pl.col("text")
                        .str.contains(
                            r"(?i)pass interference|roughing the (?:passer|kicker|snapper)|targeting",
                        )
                        .fill_null(False)
                    )
                    .and_(pl.col("text").str.contains("(?i)penalty").fill_null(False))
                    .and_(pl.col("penalty_declined") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalized_team").is_not_null())
                    .and_(pl.col("penalized_team") != pl.col("pos_team"))
                    .and_(pl.col("change_of_pos_team") == False)
                    .and_(pl.col("type.text").is_in([*end_rows, "Timeout"]) == False),
                )
            )
            .and_(pl.col("first_down_yards") == False)
            # kickoff rows only: pos_team flips on kickoffs so a penalty
            # first down there would mis-credit. Punt rows are NOT
            # excluded -- roughing the kicker/snapper happens on punts and
            # awards the KICKING team (= pos_team at snap) the first down.
            .and_(pl.col("kickoff_play") == False),
        )
        .with_columns(
            first_down_earned=(pl.col("first_down_yards") == True).or_(pl.col("first_down_penalty") == True),
        )
        .with_columns(
            drive_event_number=pl.col("drive_event").cast(pl.Int32).cum_sum().over("drive.id"),
            # lag_play_type is ungrouped in R (pbp_clean_pbp_dat.R L388-390);
            # fill "" so is_in() is False like R's NA %in% -> FALSE.
            lag_play_type=pl.col("type.text").shift(1).fill_null(""),
            lag_play_type2=pl.col("type.text").shift(2).fill_null(""),
            # Half-scoped event-flag lags, 0-filled at half starts. R fills
            # via half_event_number %in% 1..k; within-half shift(k) is null
            # exactly on those rows (End rows carry all-False flags, so the
            # half_play_number nuance on scoring_play is inert).
            lag_first_by_penalty=pl.col("first_by_penalty").shift(1).over("half").fill_null(False),
            lag_first_by_penalty2=pl.col("first_by_penalty").shift(2).over("half").fill_null(False),
            lag_first_by_penalty3=pl.col("first_by_penalty").shift(3).over("half").fill_null(False),
            lag_first_by_yards=pl.col("first_by_yards").shift(1).over("half").fill_null(False),
            lag_first_by_yards2=pl.col("first_by_yards").shift(2).over("half").fill_null(False),
            lag_first_by_yards3=pl.col("first_by_yards").shift(3).over("half").fill_null(False),
            lag_change_of_pos_team_srs=pl.col("change_of_pos_team").shift(1).over("half").fill_null(False),
            lag_change_of_pos_team2_srs=pl.col("change_of_pos_team").shift(2).over("half").fill_null(False),
            lag_change_of_pos_team3_srs=pl.col("change_of_pos_team").shift(3).over("half").fill_null(False),
            lag_kickoff_play=pl.col("kickoff_play").shift(1).over("half").fill_null(False),
            lag_kickoff_play2=pl.col("kickoff_play").shift(2).over("half").fill_null(False),
            lag_punt=pl.col("punt").shift(1).over("half").fill_null(False),
            lag_punt2=pl.col("punt").shift(2).over("half").fill_null(False),
            lag_downs_turnover=pl.col("downs_turnover").shift(1).over("half").fill_null(False),
            lag_downs_turnover2=pl.col("downs_turnover").shift(2).over("half").fill_null(False),
            lag_turnover_vec=pl.col("turnover_vec").shift(1).over("half").fill_null(False),
            lag_turnover_vec2=pl.col("turnover_vec").shift(2).over("half").fill_null(False),
            lag_scoring_play=pl.col("scoring_play").shift(1).over("half").fill_null(False),
            lag_scoring_play2=pl.col("scoring_play").shift(2).over("half").fill_null(False),
            half_row=pl.int_range(pl.len()).over("half"),
            lag_drive_id=pl.col("drive.id").shift(1).over("half"),
        )
        .with_columns(
            new_series=pl.when(pl.col("half_row") == 0)
            .then(True)
            .when(pl.col("drive.id") != pl.col("lag_drive_id"))
            .then(True)
            .when(pl.col("lag_first_by_yards") == True)
            .then(True)
            .when(pl.col("lag_first_by_penalty") == True)
            .then(True)
            .otherwise(False),
            # R: kickoff_play == 1 & down == 1. ESPN's raw start.down on
            # kickoff rows is era-dependent junk (0 pre-normalization) and
            # __process_epa's kick_mask later substitutes down=1 on every
            # kickoff row -- the normalized-down semantics the R engine
            # sees natively on CFBD input -- so the down guard is vacuous.
            firstD_by_kickoff=pl.col("kickoff_play") == True,
            firstD_by_poss=pl.when(
                # 1-L.I) start by play after kickoff
                (pl.col("lag_play_type").is_in(to_ep) == False)
                .and_(pl.col("drive_event_number") == 2)
                .and_(pl.col("lag_kickoff_play") == True)
                .and_(pl.col("kickoff_play") == False),
            )
            .then(True)
            .when(
                # 1-L.II) same, one non-play event in between
                (pl.col("lag_play_type").is_in(to_ep))
                .and_(pl.col("lag_play_type2").is_in(to_ep) == False)
                .and_(pl.col("drive_event_number") == 3)
                .and_(pl.col("lag_kickoff_play2") == True)
                .and_(pl.col("kickoff_play") == False),
            )
            .then(True)
            .when(
                # 2-L.I) start by change of pos_team
                (pl.col("lag_play_type").is_in(to_ep) == False)
                .and_(pl.col("lag_change_of_pos_team_srs") == True)
                .and_(
                    (pl.col("lag_punt") == True)
                    .or_(pl.col("lag_downs_turnover") == True)
                    .or_(pl.col("lag_turnover_vec") == True),
                ),
            )
            .then(True)
            .when(
                # 2-L.II) same, one non-play event in between
                (pl.col("lag_play_type").is_in(to_ep))
                .and_(pl.col("lag_change_of_pos_team2_srs") == True)
                .and_(pl.col("lag_play_type2").is_in(to_ep) == False)
                .and_(
                    (pl.col("lag_punt2") == True)
                    .or_(pl.col("lag_downs_turnover2") == True)
                    .or_(pl.col("lag_turnover_vec2") == True),
                ),
            )
            .then(True)
            .when(
                # 3-L.I) start by opponent scoring play
                (pl.col("lag_play_type").is_in(to_ep) == False)
                .and_(pl.col("lag_scoring_play") == True)
                .and_(pl.col("kickoff_play") == False),
            )
            .then(True)
            .when(
                # 3-L.II) same, one non-play event in between
                (pl.col("lag_play_type").is_in(to_ep))
                .and_(pl.col("lag_play_type2").is_in(to_ep) == False)
                .and_(pl.col("lag_scoring_play2") == True)
                .and_(pl.col("kickoff_play") == False),
            )
            .then(True)
            .when(
                # 4) start-of-half plays start drives
                (pl.col("drive_event_number") == 1).and_(pl.col("kickoff_play") == False),
            )
            .then(True)
            .otherwise(False),
            firstD_by_penalty=pl.when(
                (pl.col("lag_first_by_penalty") == True)
                .and_(pl.col("lag_change_of_pos_team_srs") == False)
                .and_(pl.col("lag_play_type").is_in(to_ep) == False),
            )
            .then(True)
            .when(
                (pl.col("lag_first_by_penalty2") == True)
                .and_(pl.col("lag_change_of_pos_team2_srs") == False)
                .and_(pl.col("lag_play_type").is_in(to_ep)),
            )
            .then(True)
            .when(
                (pl.col("lag_first_by_penalty3") == True)
                .and_(pl.col("lag_change_of_pos_team3_srs") == False)
                .and_(pl.col("lag_play_type").is_in(to_ep))
                .and_(pl.col("lag_play_type2").is_in(to_ep)),
            )
            .then(True)
            .otherwise(False),
            firstD_by_yards=pl.when(
                (pl.col("lag_first_by_yards") == True)
                .and_(pl.col("lag_change_of_pos_team_srs") == False)
                .and_(pl.col("lag_play_type").is_in(to_ep) == False),
            )
            .then(True)
            .when(
                (pl.col("lag_first_by_yards2") == True)
                .and_(pl.col("lag_change_of_pos_team2_srs") == False)
                .and_(pl.col("lag_play_type").is_in(to_ep)),
            )
            .then(True)
            .when(
                (pl.col("lag_first_by_yards3") == True)
                .and_(pl.col("lag_change_of_pos_team3_srs") == False)
                .and_(pl.col("lag_play_type").is_in(to_ep))
                .and_(pl.col("lag_play_type2").is_in(to_ep)),
            )
            .then(True)
            .otherwise(False),
        )
        .drop(
            "first_by_penalty",
            "first_by_yards",
            "drive_event",
            "drive_event_number",
            "lag_play_type",
            "lag_play_type2",
            "lag_first_by_penalty",
            "lag_first_by_penalty2",
            "lag_first_by_penalty3",
            "lag_first_by_yards",
            "lag_first_by_yards2",
            "lag_first_by_yards3",
            "lag_change_of_pos_team_srs",
            "lag_change_of_pos_team2_srs",
            "lag_change_of_pos_team3_srs",
            "lag_kickoff_play",
            "lag_kickoff_play2",
            "lag_punt",
            "lag_punt2",
            "lag_downs_turnover",
            "lag_downs_turnover2",
            "lag_turnover_vec",
            "lag_turnover_vec2",
            "lag_scoring_play",
            "lag_scoring_play2",
            "half_row",
            "lag_drive_id",
        )
    )
    return play_df
