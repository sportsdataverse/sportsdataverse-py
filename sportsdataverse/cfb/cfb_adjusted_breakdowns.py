"""Opponent-adjusted CFB team breakdowns (success rate, down and half splits), shrunk for display.

Two stages per breakdown (a play slice and a response):

1. **Opponent removal.** The slice's plays go through :func:`cfb_adjusted_epa`'s own fit, unchanged: the plays it
   keeps (pass or rush, not garbage time by the score-margin rule), the per-team ridge at its default penalty
   (~43 plays), each game's raw value minus the opponent's fitted level, and the mean over the team's games. The
   slice's response stands in for EPA, so the "EPA, all plays" breakdown is ``cfb_adjusted_epa`` itself. The
   adjusted value is reported on the response's own scale: the league mean of the slice plus the team's residual.
2. **Display shrinkage.** ``shrunk = m + n / (n + k) * (v - m)``: ``m`` is the league mean of the slice, ``n`` the
   team's plays in it, ``k`` a frozen signal-to-noise penalty in plays for that breakdown and side.

Which breakdowns are published, and whether as an adjusted value or as a raw value with shrinkage, was decided by a
pre-registered keep rule on split-half reliability over 2014-2023, and ``k`` was fitted on the same seasons
(``tools/fit_cfb_adjusted_breakdowns.py``):

* adjusted (tier A): split-half reliability of the adjusted value at least 0.50 in every season, and at least 0.03
  above the raw value's on average;
* raw with shrinkage (tier B): otherwise, when the raw value's average reliability is at least 0.40;
* anything else is not published as a team rating.

Each published entry's reliability (train and the 2024-2025 holdout) is in the comments on ``_PUBLISHED``; the
rule and the fitter are on the model-lineage page.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

import polars as pl

from sportsdataverse.cfb.cfb_adjusted_epa import (
    _ADJ_EPA_LAMBDA,
    _ADJ_REQUIRED,
    _adjust_games,
    _fit_team_strengths,
    _prepare,
    _rank,
    _team_season,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    import pandas as pd

__all__ = ["cfb_adjusted_breakdowns"]

Side = Literal["off", "def"]
_SIDES: tuple[Side, ...] = ("off", "def")

# The released espn_cfb_pbp names first; the team-summaries build input renames two of them.
_ALIASES = {"EPA_success": "epa_success", "start.down": "down"}


def _col(df: pl.DataFrame, name: str) -> pl.Expr:
    """``name``, or its team-summaries alias when only that is present."""
    alias = _ALIASES.get(name)
    return pl.col(alias if name not in df.columns and alias in df.columns else name)


def _success(df: pl.DataFrame) -> pl.Expr:
    return _col(df, "EPA_success").cast(pl.Float64)


def _epa(_df: pl.DataFrame) -> pl.Expr:
    return pl.col("EPA")


def _explosive(df: pl.DataFrame) -> pl.Expr:
    return _col(df, "EPA_explosive").cast(pl.Float64)


def _all(_df: pl.DataFrame) -> pl.Expr:
    # ponytail: a column expression, not pl.lit(True): polars 2.2 mis-sizes a filter on lit(True).fill_null(False)
    return pl.col("EPA").is_not_null()


def _down(lo: int, hi: int) -> Callable[[pl.DataFrame], pl.Expr]:
    return lambda df: _col(df, "start.down").is_between(lo, hi)


def _flag(name: str) -> Callable[[pl.DataFrame], pl.Expr]:
    return lambda _df: pl.col(name).cast(pl.Float64) == 1


def _period(lo: int, hi: int) -> Callable[[pl.DataFrame], pl.Expr]:
    return lambda _df: pl.col("period").is_between(lo, hi)


# name -> (slice mask, response, may be opponent-adjusted). Pre-registered in full; publication is in _PUBLISHED.
_METRICS = {
    "epa_all": (_all, _epa, True),
    "success_all": (_all, _success, True),
    "epa_early_down": (_down(1, 2), _epa, True),
    "epa_standard_down": (_flag("standard_down"), _epa, True),
    "epa_first_half": (_period(1, 2), _epa, True),
    "epa_pass": (_flag("pass"), _epa, True),
    "success_pass": (_flag("pass"), _success, True),
    "epa_rush": (_flag("rush"), _epa, True),
    "success_rush": (_flag("rush"), _success, True),
    "epa_passing_down": (_flag("passing_down"), _epa, True),
    "epa_q1": (_period(1, 1), _epa, True),
    "explosive_rate": (_all, _explosive, True),
    "epa_red_zone": (_flag("rz_play"), _epa, False),
    "success_red_zone": (_flag("rz_play"), _success, False),
    "epa_late_down": (_down(3, 4), _epa, False),
    "success_late_down": (_down(3, 4), _success, False),
    "epa_q2": (_period(2, 2), _epa, False),
    "epa_q3": (_period(3, 3), _epa, False),
    "epa_q4": (_period(4, 4), _epa, False),
    "iso_ppp": (lambda df: _success(df) == 1, _epa, False),
}

# (metric, side) -> (tier, k): "adj" publishes the adjusted value, "raw" the raw value, each shrunk with k plays.
# Fitted by tools/fit_cfb_adjusted_breakdowns.py on 2014-2023; see the model-lineage page. Split-half reliability
# (Spearman-Brown) raw -> adjusted, 2014-2023 mean, and the 2024-2025 holdout: in the trailing comments. The fitter
# finds more publishable breakdowns; this table holds only the ones released so far.
_PUBLISHED: dict[tuple[str, Side], tuple[Literal["adj", "raw"], float]] = {
    ("epa_all", "off"): ("adj", 222.5),  # .699 -> .757; holdout .721 -> .774
    ("epa_all", "def"): ("adj", 244.2),  # .636 -> .740; holdout .644 -> .763
    ("success_all", "off"): ("adj", 199.4),  # .698 -> .778; holdout .695 -> .808
    ("success_all", "def"): ("adj", 229.4),  # .669 -> .751; holdout .587 -> .753
    ("epa_early_down", "off"): ("adj", 205.8),  # .667 -> .721; holdout .651 -> .708
    ("epa_early_down", "def"): ("adj", 233.3),  # .586 -> .694; holdout .608 -> .752
    ("epa_standard_down", "off"): ("adj", 252.8),  # .605 -> .650; holdout .657 -> .696
    ("epa_standard_down", "def"): ("adj", 233.7),  # .564 -> .669; holdout .605 -> .704
    ("epa_first_half", "off"): ("raw", 216.5),  # .639 -> .665 (gain < .03); holdout .605 -> .632
    ("epa_first_half", "def"): ("raw", 280.3),  # .583 -> .639 (a season < .50); holdout .588 -> .680
}

_VALUE_SCHEMA = {
    "metric": pl.Utf8,
    "side": pl.Utf8,
    "team_id": pl.Utf8,
    "pos_team": pl.Utf8,
    "raw": pl.Float64,
    "adj": pl.Float64,
    "n": pl.UInt32,
    "m": pl.Float64,
}


def _kept_plays(plays: pl.DataFrame | pd.DataFrame) -> pl.DataFrame:
    """The plays :func:`cfb_adjusted_epa` fits and scores (``method="current"``), with ``hfa``."""
    return _prepare(plays, _ADJ_REQUIRED, None)[1]


def _slice_values(clean: pl.DataFrame, metric: str, lam: float = _ADJ_EPA_LAMBDA) -> pl.DataFrame:
    """Long frame (metric, side, team_id, pos_team, raw, adj, n, m) for one breakdown, both sides, no shrinkage.

    The response replaces ``EPA`` so the shipped fit runs unchanged. ``_adjust_games`` keys each team-game on the
    team's offensive rows, so a game where the team defended plays in the slice but ran none of its own would lose its
    defensive value; the defense side is therefore read from the same fit applied to the frame with offense and
    defense swapped (each defensive game's raw value minus the opponent offense's level). When every team-game has
    plays on both sides, as on all downs, the two agree exactly with ``cfb_adjusted_epa``.
    """
    mask, response, _ = _METRICS[metric]
    sl = clean.filter(mask(clean).fill_null(False)).with_columns(EPA=response(clean).cast(pl.Float64))
    sl = sl.filter(pl.col("EPA").is_not_null())
    if sl.height == 0:
        return pl.DataFrame(schema=_VALUE_SCHEMA)
    m = float(sl["EPA"].mean())
    offense, defense, _ = _fit_team_strengths(sl, lam)
    team = _team_season(_adjust_games(sl, offense, defense, fill_strength=None))
    swapped = sl.with_columns(pos_team_id=pl.col("def_pos_team_id"), def_pos_team_id=pl.col("pos_team_id"))
    team_def = _team_season(
        _adjust_games(
            swapped,
            defense.rename({"adjmodelDef": "adjmodelOff"}),
            offense.rename({"adjmodelOff": "adjmodelDef"}),
            fill_strength=None,
        )
    )
    names = team.select("pos_team_id", "pos_team")
    out = []
    for side, key, rated in (("off", "pos_team_id", team), ("def", "def_pos_team_id", team_def)):
        raw = sl.group_by(key).agg(raw=pl.col("EPA").mean(), n=pl.len()).rename({key: "pos_team_id"})
        out.append(
            rated.select("pos_team_id", adj=pl.col("adj_off_epa") + m)
            .join(names, on="pos_team_id", how="left")
            .join(raw, on="pos_team_id", how="left")
            .select(
                metric=pl.lit(metric),
                side=pl.lit(side),
                team_id="pos_team_id",
                pos_team="pos_team",
                raw="raw",
                adj="adj",
                n=pl.col("n").cast(pl.UInt32),
                m=pl.lit(m),
            )
        )
    return pl.concat(out)


def _breakdown_values(clean: pl.DataFrame, metrics: Sequence[str]) -> pl.DataFrame:
    """:func:`_slice_values` for several breakdowns, stacked."""
    frames = [_slice_values(clean, name) for name in metrics]
    return pl.concat(frames) if frames else pl.DataFrame(schema=_VALUE_SCHEMA)


def _wide(values: pl.DataFrame, keys: Sequence[tuple[str, Side]]) -> pl.DataFrame:
    """One row per team; five columns per published (metric, side), in ``keys`` order."""
    teams = values.group_by("team_id").agg(pos_team=pl.col("pos_team").drop_nulls().first()).sort("team_id")
    for metric, side in keys:
        tier, k = _PUBLISHED[(metric, side)]
        p = f"{metric}_{side}"
        v = values.filter((pl.col("metric") == metric) & (pl.col("side") == side))
        value = pl.col("adj") if tier == "adj" else pl.col("raw")
        n = pl.col("n").cast(pl.Float64)
        v = v.select(
            "team_id",
            pl.col("raw").alias(f"{p}_raw"),
            (pl.col("adj") if tier == "adj" else pl.lit(None, dtype=pl.Float64)).alias(f"{p}_adj"),
            (pl.col("m") + n / (n + k) * (value - pl.col("m"))).alias(f"{p}_shrunk"),
            pl.col("n").alias(f"{p}_n"),
        ).with_columns(_rank(f"{p}_shrunk", descending=side == "off").alias(f"{p}_rank"))
        teams = teams.join(v, on="team_id", how="left")
    return teams


@overload
def cfb_adjusted_breakdowns(
    plays: pl.DataFrame | pd.DataFrame,
    *,
    metrics: Sequence[str] | None = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def cfb_adjusted_breakdowns(
    plays: pl.DataFrame | pd.DataFrame,
    *,
    metrics: Sequence[str] | None = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def cfb_adjusted_breakdowns(
    plays: pl.DataFrame | pd.DataFrame,
    *,
    metrics: Sequence[str] | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Season opponent-adjusted team breakdowns, shrunk toward the league mean for display.

    Each breakdown is a slice of the plays :func:`cfb_adjusted_epa` scores (the pass and rush plays that are not
    garbage time by the score-margin rule) and a response: EPA, the EPA success flag or the EPA explosive flag. The
    slice goes through the same opponent fit as :func:`cfb_adjusted_epa` with that response in place of EPA, then the
    displayed value is shrunk toward the league mean by ``n / (n + k)`` (see the module docstring). Breakdowns whose
    adjustment did not improve their reliability are published as the raw value with the same shrinkage and a null
    ``_adj``. In-sample and descriptive: the fit uses the whole season.

    Args:
        plays: A cfbfastR-schema play-by-play frame (polars or pandas) with the columns :func:`cfb_adjusted_epa`
            reads, plus the ones each requested breakdown slices on: ``EPA_success`` (or ``epa_success``),
            ``start.down`` (or ``down``), ``standard_down`` and ``period``. One season at a time.
        metrics: Breakdown names to return, in that order (default: every published one). Each name covers both
            sides that are published for it.
        return_as_pandas: Return a pandas ``DataFrame`` instead of polars.

    Returns:
        One row per team rated in any requested breakdown: ``team_id``, ``pos_team``, then for each published
        breakdown and side (``off`` = the team's offense, ``def`` = its defense) ``<metric>_<side>_raw``,
        ``_adj`` (null when the breakdown is published raw), ``_shrunk`` (the display value), ``_n`` (plays) and
        ``_rank`` (on ``_shrunk``; offense highest first, defense lowest first). A team needs two or more games with
        plays on both sides of the slice to be rated in it. No rows when every play is garbage time.

    Raises:
        KeyError: If ``plays`` is missing a required column.
        ValueError: If a name in ``metrics`` is not a published breakdown.

    Example:
        Quick start (``plays`` is a season of FBS pass / rush plays with the columns :func:`cfb_adjusted_epa` reads,
        such as the cfbfastR-cfb-data team-summaries input)::

            import polars as pl
            import sportsdataverse.cfb as cfb
            bd = cfb.cfb_adjusted_breakdowns(plays)
            bd.sort("success_all_off_rank").select("pos_team", "success_all_off_shrunk", "success_all_off_n").head()

        Only some breakdowns::

            cfb.cfb_adjusted_breakdowns(plays, metrics=["success_all", "epa_early_down"])

    See Also:
        * `cfbfastR`_ -- the R package for the season adjusted EPA these breakdowns extend.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    published = list(dict.fromkeys(m for m, _ in _PUBLISHED))
    names = published if metrics is None else list(metrics)
    unknown = [m for m in names if m not in published]
    if unknown:
        raise ValueError(f"cfb_adjusted_breakdowns: not a published breakdown: {unknown}; published: {published}")
    keys = [(m, s) for m in names for s in _SIDES if (m, s) in _PUBLISHED]
    out = _wide(_breakdown_values(_kept_plays(plays), names), keys)
    return out.to_pandas() if return_as_pandas else out
