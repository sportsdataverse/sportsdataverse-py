"""Bracketology: projected seeds + at-large bid probability.

Phase 5 of the MBB/WBB prediction & tournament stack. Blends the
opponent-adjusted ratings (``mbb_team_ratings``) with the Quad/WAB résumé
(``mbb_strength_of_schedule``) into a committee-style ``resume_score``,
selects a 68-team field (conference auto-bids + best remaining at-larges),
and assigns NET-style projected seeds.
"""

from __future__ import annotations

import datetime
from collections.abc import Iterable
from typing import Literal, Union, overload

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_college_season import _seed_field

__all__ = [
    "mbb_bracketology",
    "project_bracket",
]

# Committee-style resume blend: power (adj_em_z) and resume quality (WAB) carry
# most of the weight; schedule strength and Quad-1 wins refine the ordering.
# Each non-z input is z-scored within season before weighting. Validated
# against the bracketmatrix consensus (Task 5.3 oracle gate).
_RESUME_WEIGHTS = {"adj_em_z": 0.35, "sos": 0.20, "wab": 0.35, "quad1_w": 0.10}

_FIELD_SIZE = 68
_SEED_CAP = 16

_BRACKET_SCHEMA = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "resume_score": pl.Float64,
    "projected_seed": pl.Int64,
    "at_large_prob": pl.Float64,
    "auto_bid": pl.Boolean,
    "at_large_rank": pl.Int64,
    "bid": pl.Boolean,
}


def _z(col: str) -> pl.Expr:
    return (pl.col(col) - pl.col(col).mean().over("season")) / (pl.col(col).std().over("season") + 1e-9)


def project_bracket(
    resume: pl.DataFrame,
    auto_bids: set[str],
    *,
    league: str = "mens",
    field_size: int = _FIELD_SIZE,
) -> pl.DataFrame:
    """Select and seed a tournament field from a per-team résumé frame.

    Args:
        resume: One row per (season, team_id) with ``adj_em_z, sos, wab,
            quad1_w`` (the ratings + strength-of-schedule outputs joined).
        auto_bids: ``team_id`` set of conference auto-bid teams (see
            :func:`~sportsdataverse.mbb.mbb_college_season.conference_auto_bids`); always
            in the field.
        league: ``"mens"`` or ``"womens"`` (kept for shim parity; the blend is
            league-agnostic).
        field_size: Tournament field size (68).

    Returns:
        One row per input team: ``season, team_id, resume_score,
        projected_seed`` (1-16, null outside the field; in a 68-team field the
        last four at-large teams and the four worst auto-bids share First Four
        slots, so every line has four slots), ``at_large_prob`` (logistic in
        ``resume_score`` centred on the selection cutoff -- every selected
        at-large clears 0.5), ``auto_bid``, ``at_large_rank`` (1 = the best
        résumé without an auto-bid; null for auto-bids), ``bid`` (exactly
        ``field_size`` true).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_bracketology import project_bracket
            field = project_bracket(resume, auto_bids)
    """
    if resume.height == 0:  # preseason: nobody has a résumé yet
        return pl.DataFrame(schema=_BRACKET_SCHEMA)
    if resume.get_column("season").n_unique() > 1:
        return pl.concat(
            [
                project_bracket(sub, auto_bids, league=league, field_size=field_size)
                for _k, sub in resume.group_by("season", maintain_order=True)
            ]
        ).sort("season", "resume_score", descending=[False, True])
    score = sum(_RESUME_WEIGHTS[c] * (pl.col(c) if c == "adj_em_z" else _z(c)) for c in _RESUME_WEIGHTS)
    df = resume.with_columns(
        score.cast(pl.Float64).alias("resume_score"),
        pl.col("team_id").is_in(sorted(auto_bids)).alias("auto_bid") if auto_bids else pl.lit(False).alias("auto_bid"),
    ).sort("resume_score", descending=True)

    n_auto = df.filter(pl.col("auto_bid") == True).height  # noqa: E712
    at_large_slots = max(0, field_size - n_auto)
    df = df.with_columns(
        pl.when(pl.col("auto_bid") == False)  # noqa: E712
        .then((pl.col("auto_bid") == False).cast(pl.Int64).cum_sum().over("season"))  # noqa: E712
        .alias("at_large_rank")
    ).with_columns(
        ((pl.col("auto_bid") == True) | (pl.col("at_large_rank") <= at_large_slots)).alias("bid")  # noqa: E712
    )

    # at-large logistic: centred midway between the last at-large in and the
    # best at-large left out, scaled to a quarter of the score spread
    last_in = df.filter(pl.col("at_large_rank") == at_large_slots)
    first_out = df.filter(pl.col("at_large_rank") == at_large_slots + 1)
    if last_in.height and first_out.height:
        cutoff = 0.5 * (last_in["resume_score"][0] + first_out["resume_score"][0])
    else:
        cutoff = float(df["resume_score"].min()) - 1.0
    scale = max(float(df["resume_score"].std() or 1.0) * 0.25, 1e-9)
    df = df.with_columns((1.0 / (1.0 + (-(pl.col("resume_score") - cutoff) / scale).exp())).alias("at_large_prob"))

    seeds = _seed_field(
        df.get_column("resume_score").to_numpy(),
        df.get_column("bid").to_numpy(),
        df.get_column("auto_bid").to_numpy(),
    )
    out = (
        df.with_columns(pl.Series("projected_seed", seeds, dtype=pl.Int64))
        .with_columns(pl.when(pl.col("bid")).then(pl.col("projected_seed")).alias("projected_seed"))
        .select(*_BRACKET_SCHEMA)
        .sort("season", "resume_score", descending=[False, True])
    )
    return out


@overload
def mbb_bracketology(
    season: int,
    *,
    as_of_date: datetime.date | None = None,
    league: str = "mens",
    ineligible: Iterable[str] = (),
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_bracketology(
    season: int,
    *,
    as_of_date: datetime.date | None = None,
    league: str = "mens",
    ineligible: Iterable[str] = (),
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_bracketology(
    season: int,
    *,
    as_of_date: datetime.date | None = None,
    league: str = "mens",
    ineligible: Iterable[str] = (),
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """The daily ``bracketology`` table: the projected field, seeds and bubble as of a date.

    Builds the season résumé (:func:`~sportsdataverse.mbb.mbb_college_season.season_resume`)
    from the games strictly before ``as_of_date``, gives each conference's auto-bid to its
    tournament champion (or, before the final, the conference leader still alive), and
    selects and seeds the 68-team field with :func:`project_bracket`.

    Args:
        season: Season to project (e.g. ``2024``).
        as_of_date: Only use games strictly before this date (Selection-Sunday
            style snapshots); ``None`` uses every completed game.
        league: ``"mens"`` or ``"womens"``.
        ineligible: Team ids barred from the NCAA tournament (reclassifying programs,
            postseason bans): no at-large bid, and a barred conference champion passes the
            auto-bid to the final's runner-up. ESPN does not flag them.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per Division I team, best ``resume_score`` first: record, NET-estimate and
        RPI ranks, WAB and Quad-1 record, ``resume_score``, ``auto_bid`` /
        ``conf_champion``, ``at_large_rank``, ``bid``, ``projected_seed``,
        ``at_large_prob`` and the ``bubble`` label (``last_four_byes``, ``last_four_in``,
        ``first_four_out``, ``next_four_out``).

    Example:
        Quick start::

            import datetime
            from sportsdataverse.mbb import mbb_bracketology
            field = mbb_bracketology(2024, as_of_date=datetime.date(2024, 3, 18))

        Pipeline next step (one line)::

            field.filter(pl.col("bid") == True).sort("projected_seed")

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    from sportsdataverse.mbb.mbb_college_season import load_season_inputs, season_bracketology

    inp = load_season_inputs(season, as_of_date=as_of_date, league=league, ineligible=ineligible)
    out = season_bracketology(inp.resume, inp.auto_bids, league=league)
    return out.to_pandas() if return_as_pandas else out
