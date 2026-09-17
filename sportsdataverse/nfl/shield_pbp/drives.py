"""Fixed-drive derivation + drive detail columns.

Port of reference doc §8 (``helper_add_fixed_drives.R :: add_drive_results``,
lines 11-176): the 9-rule ``new_drive`` cascade -> ``fixed_drive`` (cumulative,
shared numbering across both teams) + ``fixed_drive_result`` (drive-level
category string), plus a set of ``drive_*`` detail columns.

**Divergence flagged explicitly**: in nflfastR, most ``drive_*`` detail columns
(``drive_play_count``, ``drive_time_of_possession``, ``drive_first_downs``,
``drive_inside20``, ``drive_quarter_start``/``_end``, ``drive_game_clock_start``/
``_end``, ``drive_start_yard_line``/``_end_yard_line``, ``drive_start_transition``/
``_end_transition``) are **not derived by this R function at all** — they are
raw fields lifted straight from the provider's own per-drive JSON object
(``helper_scrape_gc.R`` / ``helper_scrape_nfl.R``: ``drives <- ... $drives``,
joined onto the play frame by drive sequence number). The Shield ``driveChart``
feed this native parser reads does carry a parallel ``drives[]`` array, but it
is not yet piped into :mod:`sportsdataverse.nfl.shield_pbp.parse` as columns. Reproducing those
fields byte-for-byte therefore isn't possible from the play-level frame alone;
this module instead derives an internally-consistent **aggregate-from-plays**
equivalent for each one (documented per-column below), grouped by
``(game_id, fixed_drive)`` — never a bare/ungrouped cumulative. Only
``fixed_drive`` / ``fixed_drive_result`` (and, per ``helper_scrape_gc.R`` lines
288/290, ``drive_play_id_started``/``drive_play_id_ended`` — those two ARE a
``min``/``max`` group_by aggregate even in the R source) are exact ports.

Marker rows (``shield_play_type == "TIMEOUT"`` — timeouts + two-minute
warnings, the rows ``build.py`` drops from the final frame AFTER this module
runs) are **excluded from the drive-summary aggregation only**: they are not
plays, so they must not inflate ``drive_play_count`` or supply a drive's
first/last clock/yardline/play_id aggregate. The ``new_drive`` cascade, by
contrast, MUST keep seeing them — rules 3/4 key on Timeout/Two-Minute-Warning
rows at specific lags — so the exclusion is scoped to
:func:`_add_drive_summary` alone. A drive consisting solely of marker rows
(degenerate) gets null aggregates via the left join.
"""

from __future__ import annotations

import polars as pl

# Reference §7/§8 shared constant (``helper_add_nflscrapr_mutations.R`` line 756).
# Transcribed verbatim, including the (likely unintentional) single-bracket
# ``[:digit:]`` character class -- in both R's and Rust's regex engines a
# single-bracket class lists literal characters (':', 'd', 'i', 'g', 't'), it is
# NOT the POSIX double-bracket ``[[:digit:]]`` shorthand for a digit. Reference
# §7's port-contract note says to transcribe as-is, so this preserves that
# (extremely narrow) behavior rather than "fixing" it to ``\d``.
_KICKOFF_FINDER = (
    r"(Offside on Free Kick)|(Delay of Kickoff)|(Onside Kick formation)"
    r"|(kicks onside)|( kicks [:digit:]+ yards from)"
)

_HALF_GROUP = ["game_id", "game_half"]
_DRIVE_GROUP = ["game_id", "fixed_drive"]

_REQUIRED_DRIVE_COLUMNS = {
    "game_id",
    "game_half",
    "play_seq",
    "posteam",
    "defteam",
    "touchdown",
    "td_team",
    "fumble_lost",
    "play_type",
    "safety",
    "kickoff_attempt",
    "desc",
    "field_goal_result",
    "punt_attempt",
    "interception",
    "down",
    "yards_gained",
    "ydstogo",
    "qtr",
    "yardline_100",
    "penalty_yards",
    "play_id",
    "quarter_seconds_remaining",
    "game_seconds_remaining",
    "first_down_rush",
    "first_down_pass",
    "first_down_penalty",
}

# Columns that feed purely numeric/aggregate derivations (drive summary) and
# must be a concrete numeric dtype before arithmetic -- an all-``None`` column
# built from Python literals infers as polars ``Null`` dtype, on which even
# null-propagating ops like ``.abs()``/floor-division raise. Cast defensively.
_NUMERIC_DRIVE_COLUMNS = (
    "qtr",
    "yardline_100",
    "penalty_yards",
    "play_id",
    "quarter_seconds_remaining",
    "game_seconds_remaining",
)

# Output schema (dtype) for the degenerate-input / empty-frame fallback, so
# callers always see a stable column set (never a KeyError downstream).
_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "fixed_drive": pl.Int64,
    "fixed_drive_result": pl.Utf8,
    "drive_play_count": pl.Int64,
    "drive_first_downs": pl.Int64,
    "drive_inside20": pl.Int64,
    "drive_ended_with_score": pl.Int64,
    "drive_quarter_start": pl.Int64,
    "drive_quarter_end": pl.Int64,
    "drive_yards_penalized": pl.Int64,
    "drive_start_transition": pl.Utf8,
    "drive_end_transition": pl.Utf8,
    "drive_game_clock_start": pl.Utf8,
    "drive_game_clock_end": pl.Utf8,
    "drive_start_yard_line": pl.Int64,
    "drive_end_yard_line": pl.Int64,
    "drive_play_id_started": pl.Int64,
    "drive_play_id_ended": pl.Int64,
    "drive_time_of_possession": pl.Utf8,
}


def _own_kickoff_recovery(df: pl.DataFrame) -> pl.Expr:
    """``own_kickoff_recovery`` is a sparse stat-derived column (only present on
    games where the recovery stat actually fired) -- default to 0 when absent,
    matching the defensive fallback already used elsewhere in this pipeline.
    """
    if "own_kickoff_recovery" in df.columns:
        return pl.col("own_kickoff_recovery")
    return pl.lit(0)


def swapped_posteam(df: pl.DataFrame) -> pl.Expr:
    """Locally-swapped ``posteam``, for ``new_drive``/``new_series`` purposes only.

    Faithful port of the identical ``case_when`` block duplicated verbatim in
    both ``helper_add_fixed_drives.R`` (§8) and ``helper_add_series_data.R``
    (§7): on a kickoff recovered by the receiving team (or fumbled away by the
    kicking team), or on the reversed kickoff immediately preceding a
    penalty-forced re-kick that IS recovered, ``posteam`` is locally swapped to
    ``defteam`` -- purely as an input to the drive/series cascades. The real
    ``posteam`` column is never mutated; callers materialize this as a helper
    column and drop it before returning.
    """
    own_ko = _own_kickoff_recovery(df)
    desc = pl.col("desc").fill_null("")
    lead_recovery = own_ko.shift(-1).over("game_id") == 1
    return (
        pl.when((pl.col("kickoff_attempt") == 1) & ((own_ko == 1) | (pl.col("fumble_lost") == 1)))
        .then(pl.col("defteam"))
        .when(desc.str.contains(_KICKOFF_FINDER) & (own_ko == 0) & lead_recovery)
        .then(pl.col("defteam"))
        .otherwise(pl.col("posteam"))
    )


def drive_level_tmp_result(pos: pl.Expr) -> pl.Expr:
    """§8's per-play ``tmp_result`` case_when (no ``"QB kneel"`` bucket -- unlike
    §7's ``series_result`` vocabulary, this is a genuine divergence transcribed
    as-is, not an oversight). ``pos`` is the (locally-swapped) posteam expr.
    """
    return (
        pl.when((pl.col("touchdown") == 1) & (pos == pl.col("td_team")))
        .then(pl.lit("Touchdown"))
        .when((pl.col("touchdown") == 1) & (pos != pl.col("td_team")))
        .then(pl.lit("Opp touchdown"))
        .when(pl.col("field_goal_result") == "made")
        .then(pl.lit("Field goal"))
        .when(pl.col("field_goal_result").is_in(["blocked", "missed"]))
        .then(pl.lit("Missed field goal"))
        .when(pl.col("safety") == 1)
        .then(pl.lit("Safety"))
        .when((pl.col("play_type") == "punt") | (pl.col("punt_attempt") == 1))
        .then(pl.lit("Punt"))
        .when((pl.col("interception") == 1) | (pl.col("fumble_lost") == 1))
        .then(pl.lit("Turnover"))
        .when((pl.col("down") == 4) & (pl.col("yards_gained") < pl.col("ydstogo")) & (pl.col("play_type") != "no_play"))
        .then(pl.lit("Turnover on downs"))
        .when(pl.col("desc").fill_null("").str.contains("(END QUARTER 2)|(END QUARTER 4)|(END GAME)"))
        .then(pl.lit("End of half"))
        .otherwise(None)
    )


def last_or_first_result(df: pl.DataFrame, group: list[str], tmp_col: str, out_col: str) -> pl.DataFrame:
    """group_by-join resolution: last non-null ``tmp_col`` per ``group``, unless
    that last value is ``"End of half"`` -- then use the FIRST non-null value
    instead. Mirrors ``dplyr::if_else(last(na.omit(x)) == "End of half",
    first(na.omit(x)), last(na.omit(x)))`` from both §7 and §8.

    Deliberately shared API between :mod:`sportsdataverse.nfl.shield_pbp.drives` (for
    ``fixed_drive_result``) and :mod:`sportsdataverse.nfl.shield_pbp.series` (for
    ``series_result``) -- the two R helpers use the identical resolution
    pattern, so the port keeps one implementation.
    """
    agg = (
        df.select([*group, tmp_col])
        .filter(pl.col(tmp_col).is_not_null())
        .group_by(group, maintain_order=True)
        .agg(_first=pl.col(tmp_col).first(), _last=pl.col(tmp_col).last())
        .with_columns(
            **{out_col: pl.when(pl.col("_last") == "End of half").then(pl.col("_first")).otherwise(pl.col("_last"))}
        )
        .select([*group, out_col])
    )
    return df.join(agg, on=group, how="left")


def add_drive_detail(df: pl.DataFrame) -> pl.DataFrame:
    """Add ``fixed_drive``, ``fixed_drive_result``, and every ``drive_*`` detail
    column (reference §8).

    Must run after :func:`sportsdataverse.nfl.shield_pbp.labels.add_labels` (``field_goal_result``
    is a labels-stage column) and after :func:`sportsdataverse.nfl.shield_pbp.description.add_pass_rush`
    / description features (``play_type`` needs its qb_kneel/qb_spike refinement).

    Args:
        df: The play frame, sorted (or sortable) by ``play_seq``, carrying every
            column in :data:`_REQUIRED_DRIVE_COLUMNS` plus the per-play numeric
            columns the drive-detail aggregates read (``qtr``, ``penalty_yards``,
            ``play_id``, ``yardline_100``, ``quarter_seconds_remaining``,
            ``game_seconds_remaining``, ``first_down_rush``/``_pass``/``_penalty``).

    Returns:
        The frame with ``fixed_drive`` (Int64) + every column in
        :data:`_OUTPUT_SCHEMA` added. Empty input or missing required columns
        returns the input frame with the *absent* output columns added as typed
        nulls (never raises); output columns already present on the input are
        left untouched, so a degraded re-run cannot null out real values.
    """
    if df.height == 0 or not _REQUIRED_DRIVE_COLUMNS <= set(df.columns):
        return df.with_columns(**{c: pl.lit(None, dtype=t) for c, t in _OUTPUT_SCHEMA.items() if c not in df.columns})

    df = df.sort(["game_id", "play_seq"])
    # strict=False: some callers' ``play_id`` (or other numeric-ish columns) may
    # not be cleanly numeric (e.g. synthetic test fixtures) -- degenerate input
    # must never raise, so a non-numeric value becomes null rather than an error.
    df = df.with_columns([pl.col(c).cast(pl.Int64, strict=False) for c in _NUMERIC_DRIVE_COLUMNS])
    g = _HALF_GROUP

    df = df.with_columns(_swap=swapped_posteam(df))
    pos = pl.col("_swap")

    # Rule 1: base possession change, 3-tier null-posteam lookback.
    change = (
        (pos != pos.shift(1).over(g))
        | ((pos != pos.shift(2).over(g)) & pos.shift(1).over(g).is_null())
        | ((pos != pos.shift(3).over(g)) & pos.shift(1).over(g).is_null() & pos.shift(2).over(g).is_null())
    )
    df = df.with_columns(_new_drive=change.cast(pl.Int64))

    # Rule 2: PAT/2pt after a defensive TD (no gap) -> force 0.
    prev_def_td = (
        (pl.col("touchdown").shift(1).over(g) == 1)
        & (pos.shift(1).over(g) != pl.col("td_team").shift(1).over(g))
        & pos.shift(1).over(g).is_not_null()
    )
    df = df.with_columns(_new_drive=pl.when(prev_def_td).then(0).otherwise(pl.col("_new_drive")))

    # Rules 3/4: same, with exactly 1 or 2 intervening Timeout/2-Minute-Warning rows.
    to_desc = pl.col("desc").fill_null("").str.contains("(Timeout)|(Two-Minute Warning)")
    rule3 = (
        to_desc.shift(1).over(g)
        & (pl.col("touchdown").shift(2).over(g) == 1)
        & (pos.shift(2).over(g) != pl.col("td_team").shift(2).over(g))
    )
    df = df.with_columns(_new_drive=pl.when(rule3).then(0).otherwise(pl.col("_new_drive")))
    rule4 = (
        to_desc.shift(1).over(g)
        & to_desc.shift(2).over(g)
        & (pl.col("touchdown").shift(3).over(g) == 1)
        & (pos.shift(3).over(g) != pl.col("td_team").shift(3).over(g))
    )
    df = df.with_columns(_new_drive=pl.when(rule4).then(0).otherwise(pl.col("_new_drive")))

    # Rule 5: same team retained the ball after its own lost fumble (direct lag-1,
    # or the 2-play-back mirror when the intervening play has a null posteam) ->
    # force 1, but only when new_drive isn't already 1.
    scrimmage_types = ["punt", "pass", "run", "field_goal"]
    direct = (
        (pos == pos.shift(1).over(g))
        & (pl.col("fumble_lost").shift(1).over(g) == 1)
        & pl.col("play_type").shift(1).over(g).is_in(scrimmage_types)
        & (pl.col("touchdown").shift(1).over(g) == 0)
    )
    mirror = (
        pos.shift(1).over(g).is_null()
        & (pos == pos.shift(2).over(g))
        & (pl.col("fumble_lost").shift(2).over(g) == 1)
        & pl.col("play_type").shift(2).over(g).is_in(scrimmage_types)
        & (pl.col("touchdown").shift(2).over(g) == 0)
    )
    guard = (pl.col("_new_drive") != 1) | pl.col("_new_drive").is_null()
    df = df.with_columns(_new_drive=pl.when(guard & (direct | mirror)).then(1).otherwise(pl.col("_new_drive")))

    # Rule 6: first play of the half.
    df = df.with_columns(_row=pl.int_range(0, pl.len()).over(g))
    df = df.with_columns(_new_drive=pl.when(pl.col("_row") == 0).then(1).otherwise(pl.col("_new_drive")))

    # Rule 7: recovered onside kick / muffed kickoff return -> new drive.
    own_ko = _own_kickoff_recovery(df)
    ko_recovery = (pl.col("play_type") == "kickoff") & ((own_ko == 1) | (pl.col("fumble_lost") == 1))
    df = df.with_columns(_new_drive=pl.when(ko_recovery).then(1).otherwise(pl.col("_new_drive")))

    # Rule 8: kickoff immediately (or 2-back, with a null/no_play play in between)
    # after a safety -> new drive.
    is_kickoff = pl.col("kickoff_attempt") == 1
    ko_after_safety = (is_kickoff & (pl.col("safety").shift(1).over(g) == 1)) | (
        is_kickoff
        & (pl.col("safety").shift(2).over(g) == 1)
        & (pl.col("play_type").shift(1).over(g).is_null() | (pl.col("play_type").shift(1).over(g) == "no_play"))
    )
    df = df.with_columns(_new_drive=pl.when(ko_after_safety).then(1).otherwise(pl.col("_new_drive")))

    # Rule 9: any remaining NA -> 0.
    df = df.with_columns(_new_drive=pl.col("_new_drive").fill_null(0))

    df = df.with_columns(fixed_drive=pl.col("_new_drive").cum_sum().over("game_id").cast(pl.Int64))
    df = df.with_columns(_tmp_result=drive_level_tmp_result(pos))
    df = last_or_first_result(df, _DRIVE_GROUP, "_tmp_result", "fixed_drive_result")

    df = df.drop("_swap", "_new_drive", "_row", "_tmp_result")
    df = _add_drive_summary(df)
    return df


def _add_drive_summary(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate-from-plays ``drive_*`` detail columns, grouped by
    ``(game_id, fixed_drive)`` -- see the module docstring for why these are
    derived here rather than lifted from a raw per-drive provider payload.

    Marker rows (``shield_play_type == "TIMEOUT"`` -- the rows ``build.py``
    later drops) are excluded from the aggregation: they aren't plays, so they
    must not count toward ``drive_play_count`` or supply a first/last
    clock/yardline/play_id. The exclusion lives here (not upstream) because
    the ``new_drive`` cascade needs those rows at specific lags (rules 3/4).
    """
    grp = _DRIVE_GROUP
    df = df.with_columns(
        _fd_play=(
            (pl.col("first_down_rush") == 1) | (pl.col("first_down_pass") == 1) | (pl.col("first_down_penalty") == 1)
        ).cast(pl.Int64),
        _clock_str=pl.when(pl.col("quarter_seconds_remaining").is_not_null())
        .then(
            (pl.col("quarter_seconds_remaining") // 60).cast(pl.Int64).cast(pl.Utf8)
            + pl.lit(":")
            + (pl.col("quarter_seconds_remaining") % 60).cast(pl.Int64).cast(pl.Utf8).str.zfill(2)
        )
        .otherwise(None),
    )

    # Aggregate only over real plays; the marker rows still receive the drive's
    # summary values via the join below (they carry the same fixed_drive).
    if "shield_play_type" in df.columns:
        real_plays = df.filter(pl.col("shield_play_type").fill_null("") != "TIMEOUT")
    else:
        real_plays = df

    summary = real_plays.group_by(grp, maintain_order=True).agg(
        drive_play_count=pl.len().cast(pl.Int64),
        # Excluding TIMEOUT markers from `real_plays` assumes they never carry
        # first-down flags or penalty yards (empirically true in captured
        # fixtures, not asserted here); a real-game parity run is the
        # eventual assertion.
        drive_first_downs=pl.col("_fd_play").sum().cast(pl.Int64),
        drive_inside20=(pl.col("yardline_100").min() <= 20).fill_null(False).cast(pl.Int64),
        drive_ended_with_score=pl.col("fixed_drive_result").first().is_in(["Touchdown", "Field goal"]).cast(pl.Int64),
        drive_quarter_start=pl.col("qtr").first().cast(pl.Int64),
        drive_quarter_end=pl.col("qtr").last().cast(pl.Int64),
        drive_yards_penalized=pl.col("penalty_yards").sum().fill_null(0).cast(pl.Int64),
        drive_end_transition=pl.col("fixed_drive_result").first(),
        drive_game_clock_start=pl.col("_clock_str").first(),
        drive_game_clock_end=pl.col("_clock_str").last(),
        drive_start_yard_line=pl.col("yardline_100").first().cast(pl.Int64),
        drive_end_yard_line=pl.col("yardline_100").last().cast(pl.Int64),
        drive_play_id_started=pl.col("play_id").min().cast(pl.Int64),
        drive_play_id_ended=pl.col("play_id").max().cast(pl.Int64),
        _start_gsr=pl.col("game_seconds_remaining").first(),
        _end_gsr=pl.col("game_seconds_remaining").last(),
    )
    summary = summary.with_columns(
        drive_start_transition=pl.col("drive_end_transition").shift(1).over("game_id"),
        _top_seconds=(pl.col("_start_gsr") - pl.col("_end_gsr")).abs(),
    )
    summary = summary.with_columns(
        drive_time_of_possession=pl.when(pl.col("_top_seconds").is_not_null())
        .then(
            (pl.col("_top_seconds") // 60).cast(pl.Int64).cast(pl.Utf8)
            + pl.lit(":")
            + (pl.col("_top_seconds") % 60).cast(pl.Int64).cast(pl.Utf8).str.zfill(2)
        )
        .otherwise(None)
    ).select([*grp, *[c for c in _OUTPUT_SCHEMA if c not in ("fixed_drive", "fixed_drive_result")]])

    df = df.drop("_fd_play", "_clock_str").join(summary, on=grp, how="left")
    return df
