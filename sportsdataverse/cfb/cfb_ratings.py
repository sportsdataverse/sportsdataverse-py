"""As-of-date opponent-adjusted efficiency ratings for college football.

Thin wrapper around :mod:`sportsdataverse.cfb.cfb_adjusted_epa` -- reuses its
ridge primitives (``_prepare`` for the competitive-play filter + id casting,
``_fit_opponent_ridge`` for the offense/defense ridge fit) instead of forking
a second ridge solver. Callers are responsible for any as-of-date filtering
(e.g. via :func:`sportsdataverse.cfb.cfb_prediction_constants.as_of_ratings_split`)
before calling :func:`efficiency_ratings` -- this module performs no date
filtering of its own.

.. note::
    ``sportsdataverse/cfb/__init__.py`` does ``from .cfb_adjusted_epa import
    *``, which rebinds the package attribute ``cfb_adjusted_epa`` to the
    *function* of that name (the re-export for
    ``sportsdataverse.cfb.cfb_adjusted_epa(...)`` callers). Both
    ``from sportsdataverse.cfb import cfb_adjusted_epa as _aepa`` and
    ``import sportsdataverse.cfb.cfb_adjusted_epa as _aepa`` therefore resolve
    to that function rather than the submodule (confirmed empirically), so
    the private ridge primitives are imported by name directly from the
    submodule's fully-qualified path below -- that import path is resolved
    from ``sys.modules`` by the dotted string, not by attribute traversal on
    the already-shadowed package, and keeps mypy able to see the real
    signatures. :func:`cfb_ratings` (the public as-of-date orchestrator)
    imports ``load_cfb_pbp`` / ``load_cfb_schedule`` at module scope for the
    same reason: monkeypatch-ability in tests requires the names to live on
    *this* module's namespace, not just re-exported through the package.
"""

from __future__ import annotations

import datetime
from typing import Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.cfb.cfb_adjusted_epa import _REQUIRED_COLUMNS, _adjust_games, _fit_opponent_ridge, _prepare
from sportsdataverse.cfb.cfb_loaders import load_cfb_pbp, load_cfb_schedule
from sportsdataverse.cfb.cfb_prediction_constants import RatingsConfig, as_of_ratings_split

__all__ = ["cfb_ratings", "efficiency_ratings", "fei_ratings", "special_teams_ratings"]

_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "team_id": pl.Utf8,
    "adj_off_epa": pl.Float64,
    "adj_def_epa": pl.Float64,
    "adj_net": pl.Float64,
    "games": pl.Int64,
    "off_pace": pl.Float64,
}

_ST_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "team_id": pl.Utf8,
    "adj_st_epa": pl.Float64,
}

_FEI_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "team_id": pl.Utf8,
    "fei_off": pl.Float64,
    "fei_def": pl.Float64,
    "fei_net": pl.Float64,
}

# Documented column order + dtypes for the public `cfb_ratings` entry point --
# see its docstring for what each column means.
_RATINGS_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "adj_off_epa": pl.Float64,
    "adj_def_epa": pl.Float64,
    "adj_st_epa": pl.Float64,
    "adj_net": pl.Float64,
    "fei_off": pl.Float64,
    "fei_def": pl.Float64,
    "fei_net": pl.Float64,
    "games": pl.Int64,
    "off_pace": pl.Float64,
    "off_rank": pl.Int64,
    "def_rank": pl.Int64,
    "net_rank": pl.Int64,
    "net_z": pl.Float64,
}

# `load_cfb_pbp` serves the published `espn_cfb_pbp` asset, which ships ESPN's
# dotted field names rather than the cfbfastR-canonical ones `_prepare`
# requires. `home` maps to `homeTeamId` because the released `pos_team` is
# itself a team *id* (Int64), not a name -- `_prepare`'s HFA test compares the
# two directly, so they must share a namespace.
_RELEASED_PBP_ALIASES: dict[str, str] = {
    "pos_team_id": "start.pos_team.id",
    "def_pos_team_id": "start.def_pos_team.id",
    "home": "homeTeamId",
    # `type.text` is the reclassified play type (`orig_play_type` freezes the
    # pre-reclassification original); it is also what `cfb_pbp` itself reads
    # first, ahead of `play_type`.
    "play_type": "type.text",
    "drive_id": "drive.id",
}

# Case-insensitive keyword match over the cfbfastR `play_type` vocabulary for
# kickoffs/punts/field goals (returns, blocks, touchbacks, etc. all contain one
# of these words) -- deliberately loose since `play_type` free text varies.
_ST_PLAY_TYPE_PATTERN = "(?i)kickoff|punt|field goal"

# Kneel-down text/clock heuristic (gameonpaper `team_agg.R` / `cfb_pbp`'s own
# `kneel_down` flag): the text regexes catch narrated kneels; the clock-window
# branch catches ESPN-anonymized "TEAM run for a loss of 1-2 yards" snaps in
# the final minute of either half (2Q: 1860-1800, 4Q: 60-0 on adj_TimeSecsRem).
_KNEEL_TEXT_PATTERN = r"(?i)kneel|takes a knee"
_KNEEL_TEAM_RUN_PATTERN = r"(?i)^team run for a loss of (?:1 yard|2 yards)"
_KNEEL_TEXT_COLUMNS = ("cleaned_text", "text", "play_text")
_KNEEL_CLOCK_COLUMNS = ("adj_TimeSecsRem", "start.adj_TimeSecsRem")


def _drop_kneel_downs(plays: pl.DataFrame) -> pl.DataFrame:
    """Strip kneel-downs (gameonpaper parity) from an aliased plays frame.

    Prefers a pipeline-computed ``kneel_down`` flag when present; otherwise
    applies the text heuristic on the first available play-text column, plus
    the end-of-half TEAM-run clock heuristic when a clock column exists.
    Pass plays are never treated as kneels. A frame with neither a
    ``kneel_down`` flag nor any play-text column passes through unchanged
    (same graceful-column posture as the orchestrator's ``neutral_site``
    handling -- the released assets always carry ``text``).
    """
    if "kneel_down" in plays.columns:
        # fill_null(False): an unset flag means "not a confirmed kneel" -- keep
        # the row (a bare `== False` filter would drop null-flag rows).
        return plays.filter(pl.col("kneel_down").fill_null(False) == False)  # noqa: E712

    text_col = next((c for c in _KNEEL_TEXT_COLUMNS if c in plays.columns), None)
    if text_col is None:
        return plays
    text = pl.col(text_col).cast(pl.Utf8)
    kneel = text.str.contains(_KNEEL_TEXT_PATTERN).fill_null(False)

    clock_col = next((c for c in _KNEEL_CLOCK_COLUMNS if c in plays.columns), None)
    if clock_col is not None:
        clock = pl.col(clock_col).cast(pl.Float64)
        half_end = ((clock <= 1860) & (clock >= 1800)) | ((clock <= 60) & (clock >= 0))
        kneel = kneel | (half_end & text.str.contains(_KNEEL_TEAM_RUN_PATTERN)).fill_null(False)

    return plays.filter((pl.col("pass") == 1) | (kneel == False))  # noqa: E712


# Executing-team special-teams units (pos_team owns the play). Coverage/defense
# units are deliberately excluded -- EPA does not isolate them (see docstring).
_ST_UNIT_PATTERNS: tuple[tuple[str, str], ...] = (
    ("fg", "(?i)field goal"),
    ("punt", "(?i)punt"),
    ("kick_return", "(?i)kickoff"),  # pos_team on a kickoff is the receiving/return team
)


def _alias_released_pbp(plays: pl.DataFrame) -> pl.DataFrame:
    """Map the released `espn_cfb_pbp` field names onto the canonical ones.

    Aliases only when the canonical name is absent, so a caller passing an
    already-canonical frame (e.g. `CFBPlayProcess` output) is untouched.
    """
    aliases = {
        canon: src
        for canon, src in _RELEASED_PBP_ALIASES.items()
        if canon not in plays.columns and src in plays.columns
    }
    if not aliases:
        return plays
    return plays.with_columns([pl.col(src).alias(canon) for canon, src in aliases.items()])


def efficiency_ratings(plays: pl.DataFrame, *, config: RatingsConfig | None = None) -> pl.DataFrame:
    """One row per team: opponent-adjusted offensive/defensive efficiency.

    Fits the offense/defense ridge from :mod:`cfb_adjusted_epa` on the
    competitive plays in ``plays`` (``min_competitive_wp <= wp_before <=
    max_competitive_wp``), then nets each team's raw per-game EPA (all
    pass/rush plays, garbage time included) against the opponent's fitted
    strength and averages across games -- the R ``adjust_epa`` /
    gameonpaper ``team_agg.R`` statistic and scale (a top team nets
    ~0.30-0.40/play; the pre-2026-07-28 coefficient+intercept scale ran
    ~1.8x hotter). The ridge's dropped reference team nets normally from
    its own games.

    Args:
        plays: A cfbfastR-schema play-by-play frame carrying every column in
            ``cfb_adjusted_epa._REQUIRED_COLUMNS`` (``game_id``, ``pos_team``,
            ``pos_team_id``, ``def_pos_team_id``, ``home``, ``neutral_site``,
            ``EPA``, ``pass``, ``rush``, ``wp_before``). Callers pass an
            already as-of-date-filtered frame; this function is pure.
        config: Ratings tuning knobs. Only ``ridge_lambda`` is consulted here;
            defaults to :class:`RatingsConfig` when omitted.

    Returns:
        A ``polars.DataFrame`` with one row per ``team_id``: ``team_id``
        (Utf8), ``adj_off_epa`` / ``adj_def_epa`` / ``adj_net`` (Float64),
        ``games`` (Int64), ``off_pace`` (Float64 -- scrimmage plays per game,
        the tempo input the totals model consumes). Empty (zero-row,
        correctly-typed) when ``plays`` has no competitive plays.

    Raises:
        KeyError: If ``plays`` is missing a required column.
        ImportError: If ``scikit-learn`` is not installed.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_ratings import efficiency_ratings
            ratings = efficiency_ratings(pbp)
            ratings.sort("adj_net", descending=True).head()

        Custom ridge penalty::

            from sportsdataverse.cfb.cfb_prediction_constants import RatingsConfig
            ratings = efficiency_ratings(pbp, config=RatingsConfig(ridge_lambda=100.0))

    See Also:
        * `cfbfastR`_ -- the R implementation ``cfb_adjusted_epa`` ports.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    cfg = config or RatingsConfig()
    base, clean = _prepare(plays, _REQUIRED_COLUMNS)
    if clean.height == 0:
        return pl.DataFrame(schema=_OUTPUT_SCHEMA)

    offense, defense, _intercept = _fit_opponent_ridge(clean, cfg.ridge_lambda)

    # R `adjust_epa` netting (gameonpaper `team_agg.R` parity): each team's raw
    # per-GAME EPA over ALL pass/rush plays (garbage time included) minus the
    # opponent's fitted strength, averaged across games. The ridge only supplies
    # the opponent strengths. NOT coefficient + intercept -- that is the model's
    # competitive-play strength and runs ~1.8x hot at the top (max ~0.63 vs the
    # ~0.35 netted scale); releases before 2026-07-28 carried that hot scale.
    opp = _adjust_games(base, offense, defense, fill_strength=None)
    netted = (
        opp.group_by("pos_team_id")
        .agg(
            adj_off_epa=pl.col("adj_off_epa").mean(),
            adj_def_epa=pl.col("adj_def_epa").mean(),
        )
        .rename({"pos_team_id": "team_id"})
    )

    # `off_pace` = scrimmage plays per game (the tempo input the totals model
    # uses): `base` is already filtered to pass|rush plays by `_prepare`.
    games = (
        base.group_by("pos_team_id")
        .agg(
            pl.col("game_id").n_unique().cast(pl.Int64).alias("games"),
            pl.len().alias("_plays"),
        )
        .with_columns(off_pace=(pl.col("_plays") / pl.col("games")).cast(pl.Float64))
        .drop("_plays")
        .rename({"pos_team_id": "team_id"})
    )
    assert games.schema["team_id"] == pl.Utf8
    assert netted.schema["team_id"] == pl.Utf8

    out = (
        games.join(netted, on="team_id", how="left")
        .with_columns(
            # Netted values are centered near zero (own-effect deviations), so a
            # team whose every opponent is a ridge reference level (possible only
            # in tiny synthetic leagues) falls back to league-neutral 0.0.
            pl.col("adj_off_epa").fill_null(0.0),
            pl.col("adj_def_epa").fill_null(0.0),
            pl.col("games").fill_null(0),
            pl.col("off_pace").fill_null(0.0),
        )
        .with_columns(adj_net=pl.col("adj_off_epa") - pl.col("adj_def_epa"))
        .select("team_id", "adj_off_epa", "adj_def_epa", "adj_net", "games", "off_pace")
    )
    return out


def special_teams_ratings(plays: pl.DataFrame, *, config: RatingsConfig | None = None) -> pl.DataFrame:
    """One row per team: a per-unit special-teams EPA composite.

    Special teams was empirically found NOT to obey the offense-minus-defense
    symmetry :func:`efficiency_ratings` / :func:`fei_ratings` rely on, and not
    to benefit from opponent adjustment, when validated against the 2023 SP+
    special-teams oracle (``tests/fixtures/cfb_prediction/sp_plus_2023.parquet``
    ``sp_special``):

    * The executing ``pos_team`` owns the EPA on a kickoff / punt / field
      goal. The ``def_pos_team`` "coverage" side reflects the opposing
      returner's skill, not the coverage team's, and is not recoverable from
      EPA -- adding any coverage unit *lowers* SP+ agreement (0.77 -> 0.58),
      so coverage/defense units are excluded entirely (see the module's
      special-teams unit patterns).
    * The opponent-adjustment ridge (:func:`cfb_adjusted_epa._fit_opponent_ridge`)
      *hurts* agreement (0.72 vs 0.77) -- special teams is only weakly
      opponent-dependent, so this function does not fit a ridge at all.
    * Splitting the offense-side plays into per-phase units (field goal, punt,
      kick return) is what helps. Each unit's per-team mean EPA/play is
      centered on that unit's league-wide per-play mean and the three
      centered deviations are summed -- true EPA units. This centered form
      reached Spearman 0.865 against SP+ special teams, beating both the
      originally-shipped z-scored composite (0.768 -- dimensionless, std
      ~1.7, range +-5 under an ``_epa`` column name; replaced 2026-07-28)
      and a single-unit offense-minus-intercept ridge fit (0.703).

    ``adj_st_epa`` is therefore the sum, over the three special-teams units
    (field goal, punt, kick return), of each unit's per-team mean EPA/play
    above the unit's league average. A team with no plays in a given unit
    contributes 0 for that unit (not a penalty). ``config`` is accepted for
    signature parity with
    :func:`efficiency_ratings` / :func:`fei_ratings` but is unused -- there is
    no ridge (and therefore no ``ridge_lambda``) in this recipe.

    Args:
        plays: A cfbfastR-schema play-by-play frame carrying ``game_id``,
            ``pos_team_id``, ``EPA``, and ``play_type``. Not pre-filtered to
            special-teams plays -- this function does that filtering itself.
        config: Unused (kept for signature parity across the three rating
            functions). See the note above.

    Returns:
        A ``polars.DataFrame`` with one row per ``team_id`` appearing
        anywhere in ``plays``: ``team_id`` (Utf8), ``adj_st_epa`` (Float64,
        the sum of per-unit executing-team mean EPA/play above each unit's
        league average). Teams with no
        special-teams plays get ``adj_st_epa == 0.0``. Zero-row
        (correctly-typed) when ``plays`` has no special-teams plays.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_ratings import special_teams_ratings
            st = special_teams_ratings(pbp)
            st.sort("adj_st_epa", descending=True).head()

    See Also:
        * `cfbfastR`_ -- the R implementation this ratings spine parallels.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    roster = plays.select(pl.col("pos_team_id").cast(pl.Utf8).alias("team_id")).drop_nulls().unique()

    any_st = (
        plays.filter(pl.col("play_type").cast(pl.Utf8).str.contains(_ST_PLAY_TYPE_PATTERN))
        .filter(pl.col("EPA").is_not_null())
        .height
        > 0
    )
    if not any_st:
        return pl.DataFrame(schema=_ST_OUTPUT_SCHEMA)

    st = roster.with_columns(pl.lit(0.0).alias("adj_st_epa"))
    for _name, pat in _ST_UNIT_PATTERNS:
        u = plays.filter(pl.col("play_type").cast(pl.Utf8).str.contains(pat)).filter(pl.col("EPA").is_not_null())
        if u.height == 0:
            continue
        # Per-unit mean EPA/play centered on the unit's LEAGUE-WIDE per-play
        # mean -- true EPA units. The original z-scored composite (shipped
        # pre-2026-07-28) was dimensionless (std ~1.7, range +-5) under an
        # `_epa` column name AND tracked the SP+ special-teams oracle worse
        # (Spearman 0.768 vs 0.865 for this centered form).
        league_mean = float(u["EPA"].mean() or 0.0)
        per = u.group_by(pl.col("pos_team_id").cast(pl.Utf8).alias("team_id")).agg(
            (pl.col("EPA").mean() - league_mean).alias("dev")
        )
        st = (
            st.join(per.select("team_id", "dev"), on="team_id", how="left")
            .with_columns((pl.col("adj_st_epa") + pl.col("dev").fill_null(0.0)).alias("adj_st_epa"))
            .drop("dev")
        )
    return st.select("team_id", "adj_st_epa")


def fei_ratings(plays: pl.DataFrame, *, config: RatingsConfig | None = None) -> pl.DataFrame:
    """One row per team: opponent-adjusted per-drive efficiency (FEI-style).

    The Fremeau Efficiency Index rates teams on drive value above expectation
    given starting field position. The cfbfastR-schema ``plays`` frame this
    package works with carries no starting-field-position column, so this
    function uses the documented fallback: per-play EPA summed within each
    ``(game_id, drive_id)`` group stands in for drive value, and that
    aggregate is fit through the same opponent-adjustment ridge as
    :func:`efficiency_ratings` / :func:`special_teams_ratings` -- no forked
    solver. Offline validation against the Fremeau FEI oracle put this
    fallback's team ranking at Spearman 0.967.

    ``cfb_adjusted_epa._prepare`` filters to individual pass/rush plays and
    is not reused here (drive value should reflect every play on the drive,
    special-teams snaps included); the ``hfa`` treatment is reproduced
    directly, matching :func:`special_teams_ratings`.

    Args:
        plays: A cfbfastR-schema play-by-play frame carrying every column in
            ``cfb_adjusted_epa._REQUIRED_COLUMNS`` (``game_id``, ``pos_team``,
            ``pos_team_id``, ``def_pos_team_id``, ``home``, ``neutral_site``,
            ``EPA``, ``pass``, ``rush``, ``wp_before``) plus ``drive_id``.
            Not pre-aggregated to drives -- this function does that grouping
            itself.
        config: Ratings tuning knobs. Only ``ridge_lambda`` is consulted here;
            defaults to :class:`RatingsConfig` when omitted.

    Returns:
        A ``polars.DataFrame`` with one row per ``team_id`` appearing as
        ``pos_team_id`` on at least one drive: ``team_id`` (Utf8),
        ``fei_off`` / ``fei_def`` / ``fei_net`` (Float64). The ridge's dropped
        reference team is re-added at the shared intercept (``fei_net ==
        0.0``). Zero-row (correctly-typed) when ``plays`` has no rows with a
        non-null ``EPA``.

    Raises:
        ImportError: If ``scikit-learn`` is not installed.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_ratings import fei_ratings
            fei = fei_ratings(pbp)
            fei.sort("fei_net", descending=True).head()

    See Also:
        * `cfbfastR`_ -- the R implementation ``cfb_adjusted_epa`` ports the
          shared ridge from.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    cfg = config or RatingsConfig()
    df = (
        plays.filter(pl.col("EPA").is_not_null())
        .with_columns(
            pos_team_id=pl.col("pos_team_id").cast(pl.Utf8),
            def_pos_team_id=pl.col("def_pos_team_id").cast(pl.Utf8),
            game_id=pl.col("game_id").cast(pl.Utf8),
        )
        .with_columns(
            hfa=pl.when(pl.col("neutral_site") == True)  # noqa: E712
            .then(pl.lit(0))
            .when(pl.col("pos_team") == pl.col("home"))
            .then(pl.lit(1))
            .otherwise(pl.lit(-1))
        )
    )

    drives = df.group_by("game_id", "drive_id").agg(
        pl.col("pos_team_id").first(),
        pl.col("def_pos_team_id").first(),
        pl.col("hfa").first(),
        pl.col("EPA").sum().alias("EPA"),
    )
    if drives.height == 0:
        return pl.DataFrame(schema=_FEI_OUTPUT_SCHEMA)

    offense, defense, intercept = _fit_opponent_ridge(drives, cfg.ridge_lambda)
    assert offense.schema["team_id"] == pl.Utf8
    assert defense.schema["team_id"] == pl.Utf8

    roster = drives.select(pl.col("pos_team_id").alias("team_id")).drop_nulls().unique()

    out = (
        roster.join(offense.rename({"adjmodelOff": "fei_off"}), on="team_id", how="left")
        .join(defense.rename({"adjmodelDef": "fei_def"}), on="team_id", how="left")
        .with_columns(
            pl.col("fei_off").fill_null(intercept),
            pl.col("fei_def").fill_null(intercept),
        )
        .with_columns(fei_net=pl.col("fei_off") - pl.col("fei_def"))
        .select("team_id", "fei_off", "fei_def", "fei_net")
    )
    return out


@overload
def cfb_ratings(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    config: RatingsConfig | None = ...,
    fbs_only: bool = True,
    drop_kneels: bool = True,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
@overload
def cfb_ratings(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    config: RatingsConfig | None = ...,
    fbs_only: bool = True,
    drop_kneels: bool = True,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
def cfb_ratings(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = None,
    config: RatingsConfig | None = None,
    fbs_only: bool = True,
    drop_kneels: bool = True,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """One row per team: the full CFB ratings spine (off/def/ST EPA + FEI).

    Public orchestrator over :func:`efficiency_ratings`,
    :func:`special_teams_ratings`, and :func:`fei_ratings`. Loads play-by-play
    + schedule via :func:`sportsdataverse.cfb.cfb_loaders.load_cfb_pbp` /
    :func:`sportsdataverse.cfb.cfb_loaders.load_cfb_schedule`, joins the
    schedule's per-game date onto the plays, optionally applies the
    as-of-date leakage boundary
    (:func:`sportsdataverse.cfb.cfb_prediction_constants.as_of_ratings_split`),
    then fits all three component ratings on the (optionally filtered) plays
    and reshapes them into one wide per-team table with dense ranks and a
    net-rating z-score.

    Args:
        seasons: A single season (e.g. ``2023``) or a list of seasons to pool
            into one combined fit.
        as_of_date: When given, the leakage boundary -- only plays from games
            with ``date < as_of_date`` are used to fit the ratings (mirrors
            what was knowable heading into that date). ``None`` (default)
            uses the full season(s), unfiltered.
        config: Ratings tuning knobs forwarded to all three component
            functions. Defaults to :class:`RatingsConfig` when omitted.
        fbs_only: Keep only FBS-vs-FBS games (gameonpaper
            ``cfb-team-summaries`` parity) -- both of the schedule's
            ``home_division`` / ``away_division`` must be ``"fbs"``. Default
            True. Skipped (all games kept) when the schedule lacks the
            division columns; pass False to rate FCS opponents as regular
            teams.
        drop_kneels: Strip kneel-downs before fitting (gameonpaper parity).
            Default True. Uses a pipeline ``kneel_down`` flag when present,
            otherwise the play-text regex (``kneel`` / ``takes a knee``) plus
            the end-of-half anonymized-TEAM-run clock heuristic; skipped when
            neither a flag nor a play-text column exists. Pass False to let
            kneels with non-null EPA flow into the fit.
        return_as_pandas: If True, returns a pandas DataFrame; otherwise polars.

    Returns:
        A DataFrame with one row per ``team_id``, columns in this order:
        ``season`` (Int64 -- the single passed season for the common
        single-season call; ``null`` for a pooled multi-season call, since no
        single season applies to every row), ``team_id`` (Utf8),
        ``adj_off_epa``, ``adj_def_epa`` (Float64, from
        :func:`efficiency_ratings`), ``adj_st_epa`` (Float64, from
        :func:`special_teams_ratings`), ``adj_net`` (Float64 -- offense minus
        defense only; special teams is a separate column, not folded in),
        ``fei_off``, ``fei_def``, ``fei_net`` (Float64, from
        :func:`fei_ratings`), ``games`` (Int64), ``off_pace`` (Float64 --
        scrimmage plays per game, the tempo input the totals model uses),
        ``off_rank`` (Int64, dense
        rank on ``adj_off_epa`` descending), ``def_rank`` (Int64, dense rank
        on ``adj_def_epa`` **ascending** -- fewer EPA allowed ranks better),
        ``net_rank`` (Int64, dense rank on ``adj_net`` descending), ``net_z``
        (Float64, z-score of ``adj_net``). Zero-row (correctly-typed) when
        the requested season(s) have no published pbp/schedule asset, or when
        ``as_of_date`` filters out every play.

    Raises:
        KeyError: If the loaded plays frame is missing a required column.
        ImportError: If ``scikit-learn`` is not installed.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_ratings import cfb_ratings
            ratings = cfb_ratings(2023)
            ratings.sort("net_rank").head()

        As-of-date leakage boundary::

            import datetime as dt
            week3 = cfb_ratings(2023, as_of_date=dt.date(2023, 9, 18))

        Pandas round-trip::

            ratings_pd = cfb_ratings(2023, return_as_pandas=True)

    See Also:
        * `cfbfastR`_ -- the R implementation these ratings port from.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    cfg = config or RatingsConfig()
    season_list: list[int] = [seasons] if isinstance(seasons, int) else list(seasons)

    plays = load_cfb_pbp(season_list)
    schedule = load_cfb_schedule(season_list)
    if plays.is_empty() or schedule.is_empty():
        empty = pl.DataFrame(schema=_RATINGS_OUTPUT_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    plays = plays.with_columns(pl.col("game_id").cast(pl.Utf8))
    schedule = schedule.with_columns(pl.col("game_id").cast(pl.Utf8))
    assert plays.schema["game_id"] == schedule.schema["game_id"]

    plays = _alias_released_pbp(plays)
    if "home" in plays.columns and plays.schema["pos_team"] != plays.schema["home"]:
        # `_prepare` derives HFA from `pos_team == home`; mismatched namespaces
        # (a name on one side, an id on the other) never compare equal and would
        # silently mark every play a road play rather than fail.
        raise KeyError(
            f"cfb_ratings: `pos_team` ({plays.schema['pos_team']}) and `home` "
            f"({plays.schema['home']}) must share a namespace for the HFA term."
        )

    if "date" in schedule.columns:
        date_expr = pl.col("date").cast(pl.Date)
    else:
        # Real `load_cfb_schedule` ships `start_date` (an ISO datetime
        # string), not a bare `date` column -- take the calendar-day prefix.
        date_expr = pl.col("start_date").cast(pl.Utf8).str.slice(0, 10).str.to_date()
    sched_cols: list[pl.Expr] = [pl.col("game_id"), date_expr.alias("date")]
    if "neutral_site" not in plays.columns and "neutral_site" in schedule.columns:
        # `neutral_site` is a game attribute -- the released pbp omits it, so it
        # rides the schedule join that already runs for `date`.
        sched_cols.append(pl.col("neutral_site"))
    # FBS filter rides the schedule join like `neutral_site` above; a schedule
    # without division columns (slim fixtures) skips the filter rather than
    # raising -- the real `load_cfb_schedule` always ships both.
    apply_fbs = fbs_only and "home_division" in schedule.columns and "away_division" in schedule.columns
    if apply_fbs:
        sched_cols.extend([pl.col("home_division"), pl.col("away_division")])
    schedule_dates = schedule.select(sched_cols)

    dated_plays = plays.join(schedule_dates, on="game_id", how="left")
    if apply_fbs:
        dated_plays = dated_plays.filter((pl.col("home_division") == "fbs") & (pl.col("away_division") == "fbs")).drop(
            "home_division", "away_division"
        )
    if drop_kneels:
        dated_plays = _drop_kneel_downs(dated_plays)
    if as_of_date is not None:
        dated_plays = as_of_ratings_split(dated_plays, as_of_date)

    eff = efficiency_ratings(dated_plays, config=cfg)
    st = special_teams_ratings(dated_plays, config=cfg)
    fei = fei_ratings(dated_plays, config=cfg)

    season_value: int | None = season_list[0] if len(season_list) == 1 else None

    out = (
        eff.join(st, on="team_id", how="left")
        .join(fei, on="team_id", how="left")
        .with_columns(
            pl.col("adj_st_epa").fill_null(0.0),
            pl.col("fei_off").fill_null(0.0),
            pl.col("fei_def").fill_null(0.0),
            pl.col("fei_net").fill_null(0.0),
            pl.lit(season_value).cast(pl.Int64).alias("season"),
        )
        .with_columns(
            off_rank=pl.col("adj_off_epa").rank(method="dense", descending=True).cast(pl.Int64),
            def_rank=pl.col("adj_def_epa").rank(method="dense", descending=False).cast(pl.Int64),
            net_rank=pl.col("adj_net").rank(method="dense", descending=True).cast(pl.Int64),
        )
    )

    mean_net = float(out["adj_net"].mean() or 0.0)
    std_val = out["adj_net"].std()
    std_net = float(std_val) if std_val else 0.0
    if std_net == 0.0:
        out = out.with_columns(net_z=pl.lit(0.0).cast(pl.Float64))
    else:
        out = out.with_columns(net_z=(pl.col("adj_net") - mean_net) / std_net)

    out = out.select(
        "season",
        "team_id",
        "adj_off_epa",
        "adj_def_epa",
        "adj_st_epa",
        "adj_net",
        "fei_off",
        "fei_def",
        "fei_net",
        "games",
        "off_pace",
        "off_rank",
        "def_rank",
        "net_rank",
        "net_z",
    )
    return out.to_pandas() if return_as_pandas else out
