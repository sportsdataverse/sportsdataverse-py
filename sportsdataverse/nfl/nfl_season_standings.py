"""NFL standings with real NFL tiebreakers (nflseedR v2 standings engine port).

Adapted from nflseedR (MIT, Sebastian Carl & Lee Sharpe),
https://github.com/nflverse/nflseedR — R sources ``standings.R``,
``standings_init.R``, ``standings_utils.R``, ``standings_add_div_ranks.R``,
``standings_add_conf_ranks.R``, ``standings_add_draft_ranks.R`` (v2.0.2).

The public entry point is :func:`nfl_season_standings` (named to avoid the
existing ``sportsdataverse.nfl.nfl_standings`` api.nfl.com wrapper).
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Dict, Literal, Optional, Sequence, Tuple, Union, overload

import numpy as np
import polars as pl

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

__all__ = ["nfl_season_standings"]

# nflseedR::divisions (data-raw/divisions.R) — includes pre-relocation ids.
_DIVISIONS: Dict[str, Tuple[str, str]] = {
    "ARI": ("NFC", "NFC West"),
    "ATL": ("NFC", "NFC South"),
    "BAL": ("AFC", "AFC North"),
    "BUF": ("AFC", "AFC East"),
    "CAR": ("NFC", "NFC South"),
    "CHI": ("NFC", "NFC North"),
    "CIN": ("AFC", "AFC North"),
    "CLE": ("AFC", "AFC North"),
    "DAL": ("NFC", "NFC East"),
    "DEN": ("AFC", "AFC West"),
    "DET": ("NFC", "NFC North"),
    "GB": ("NFC", "NFC North"),
    "HOU": ("AFC", "AFC South"),
    "IND": ("AFC", "AFC South"),
    "JAX": ("AFC", "AFC South"),
    "KC": ("AFC", "AFC West"),
    "LA": ("NFC", "NFC West"),
    "LAC": ("AFC", "AFC West"),
    "LAR": ("NFC", "NFC West"),
    "LV": ("AFC", "AFC West"),
    "MIA": ("AFC", "AFC East"),
    "MIN": ("NFC", "NFC North"),
    "NE": ("AFC", "AFC East"),
    "NO": ("NFC", "NFC South"),
    "NYG": ("NFC", "NFC East"),
    "NYJ": ("AFC", "AFC East"),
    "OAK": ("AFC", "AFC West"),
    "PHI": ("NFC", "NFC East"),
    "PIT": ("AFC", "AFC North"),
    "SD": ("AFC", "AFC West"),
    "SEA": ("NFC", "NFC West"),
    "SF": ("NFC", "NFC West"),
    "STL": ("NFC", "NFC West"),
    "TB": ("NFC", "NFC South"),
    "TEN": ("AFC", "AFC South"),
    "WAS": ("NFC", "NFC East"),
}

# sims_exit_translate_to() in simulations_utils.R
_EXIT_TO_INT: Dict[str, int] = {
    "REG": 0,
    "WC": 1,
    "DIV": 2,
    "CON": 3,
    "SB": 4,
    "SB_WIN": 5,
}
_INT_TO_EXIT: Dict[int, str] = {v: k for k, v in _EXIT_TO_INT.items()}

_DEPTH_MAP: Dict[str, int] = {"RANDOM": 0, "PRE-SOV": 1, "SOS": 2, "POINTS": 3}

_TEAMS_DF = pl.DataFrame(
    {
        "team": list(_DIVISIONS.keys()),
        "conf": [v[0] for v in _DIVISIONS.values()],
        "division": [v[1] for v in _DIVISIONS.values()],
    }
)


def _teams_frame() -> pl.DataFrame:
    """Return the team -> (conf, division) lookup frame."""
    return _TEAMS_DF


# ---------------------------------------------------------------------------
# ranking helper (data.table frank / frankv equivalent)
# ---------------------------------------------------------------------------
def _with_frank(
    df: pl.DataFrame,
    group: Sequence[str],
    keys: Sequence[Tuple[str, bool]],
    method: str,
    rng: Optional[np.random.Generator] = None,
    out: str = "_rk",
) -> pl.DataFrame:
    """Add ``out`` = within-``group`` rank of the ``keys`` tuple.

    Mirrors ``data.table::frank(list(...), ties.method = method)`` with
    ``na.last = TRUE`` semantics (nulls rank last). ``keys`` is a list of
    ``(column, descending)`` pairs; ``method`` is one of ``min``, ``max``,
    ``dense``, ``random``.
    """
    idx = "__frank_idx"
    d = df.with_row_index(idx)
    key_cols = [c for c, _ in keys]
    desc = [bool(x) for _, x in keys]
    if method == "random":
        if rng is None:  # pragma: no cover - internal misuse guard
            raise ValueError("random tie method requires an rng")
        d = d.with_columns(pl.Series("__frank_rnd", rng.random(d.height)))
        sort_keys = key_cols + ["__frank_rnd"]
        sort_desc = desc + [False]
    else:
        sort_keys = key_cols
        sort_desc = desc
    d = d.sort(
        list(group) + sort_keys,
        descending=[False] * len(group) + sort_desc,
        nulls_last=True,
    )
    d = d.with_columns(pl.col(idx).cum_count().over(list(group)).alias("__pos"))
    if method in ("min", "max"):
        agg = pl.col("__pos").min() if method == "min" else pl.col("__pos").max()
        d = d.with_columns(agg.over(list(group) + key_cols).alias(out))
    elif method == "dense":
        d = d.with_columns(pl.col("__pos").min().over(list(group) + key_cols).alias("__minpos"))
        d = d.with_columns(pl.col("__minpos").rank(method="dense").over(list(group)).alias(out))
        d = d.drop("__minpos")
    elif method == "random":
        d = d.with_columns(pl.col("__pos").alias(out))
        d = d.drop("__frank_rnd")
    else:  # pragma: no cover - internal misuse guard
        raise ValueError(f"unknown rank method {method!r}")
    d = d.with_columns(pl.col(out).cast(pl.Int64))
    return d.sort(idx).drop([idx, "__pos"])


def _rerank_subset(
    st: pl.DataFrame,
    mask: pl.Expr,
    group: Sequence[str],
    rank_col: str,
    keys: Sequence[Tuple[str, bool]],
    method: str = "min",
    rng: Optional[np.random.Generator] = None,
) -> pl.DataFrame:
    """``st[mask, rank := min(rank) - 1 + frank(keys), by = group]``.

    The data.table subset-assign pattern used throughout the division and
    draft tiebreakers: groups are formed on the masked subset only.
    """
    sub = st.filter(mask)
    if sub.height == 0:
        return st
    sub = _with_frank(sub, group, keys, method, rng)
    sub = sub.with_columns((pl.col(rank_col).min().over(list(group)) - 1 + pl.col("_rk")).alias("__new"))
    st = st.join(sub.select("sim", "team", "__new"), on=["sim", "team"], how="left")
    return st.with_columns(pl.coalesce(pl.col("__new"), pl.col(rank_col)).alias(rank_col)).drop("__new")


def _count_ranks(st: pl.DataFrame, by: Sequence[str], out: str) -> pl.DataFrame:
    """Full recount: ``st[, out := .N, by = by]``."""
    return st.with_columns(pl.len().over(list(by)).cast(pl.Int64).alias(out))


def _any_gt_one(st: pl.DataFrame, counter: str) -> bool:
    mx = st[counter].max()
    return mx is not None and int(mx) > 1


def _tie_break_done(st: pl.DataFrame, counter: str, n_tied: int) -> bool:
    """``all(counter < n_tied, na.rm = TRUE)``."""
    s = st[counter].drop_nulls()
    if s.len() == 0:
        return True
    return bool((s < n_tied).all())


def _set_label(st: pl.DataFrame, mask: pl.Expr, label_col: str, label: Optional[str]) -> pl.DataFrame:
    lab = pl.lit(label, dtype=pl.Utf8)
    return st.with_columns(pl.when(mask).then(lab).otherwise(pl.col(label_col)).alias(label_col))


# ---------------------------------------------------------------------------
# input validation + base tables (standings_utils.R, standings_init.R)
# ---------------------------------------------------------------------------
def _validate_games(games: pl.DataFrame) -> Tuple[pl.DataFrame, bool, bool]:
    """Port of ``standings_validate_games`` (standings_utils.R L39-80)."""
    if "sim" in games.columns and "season" in games.columns:
        warnings.warn(
            "The games frame includes both 'sim' and 'season'. Will group by 'sim' and drop 'season'.",
            stacklevel=3,
        )
        games = games.drop("season")
    required = ["game_type", "week", "away_team", "home_team", "result"]
    uses_season = "season" in games.columns
    if "sim" not in games.columns and uses_season:
        games = games.rename({"season": "sim"})
    missing = [c for c in ["sim"] + required if c not in games.columns]
    if missing:
        raise ValueError(f"games must include 'sim' or 'season' plus {required}; missing: {missing}")
    if games["result"].null_count() > 0:
        raise ValueError("The games table includes missing results. Please fix and rerun.")
    has_scores = {"away_score", "home_score"}.issubset(games.columns)
    games = games.with_columns(pl.col("result").cast(pl.Int64))
    unknown = [
        t
        for t in set(games["home_team"].to_list()) | set(games["away_team"].to_list())
        if t is not None and t not in _DIVISIONS
    ]
    if unknown:
        raise ValueError(f"Unknown team abbreviations (not in nflseedR divisions): {sorted(unknown)}")
    return games, uses_season, has_scores


def _double_games(games: pl.DataFrame, has_scores: bool) -> pl.DataFrame:
    """Port of ``standings_double_games`` (standings_utils.R L1-20)."""
    base = [pl.col("sim"), pl.col("game_type"), pl.col("week")]
    away_cols = base + [
        pl.col("away_team").alias("team"),
        pl.col("home_team").alias("opp"),
        (-pl.col("result")).alias("result"),
    ]
    home_cols = base + [
        pl.col("home_team").alias("team"),
        pl.col("away_team").alias("opp"),
        pl.col("result"),
    ]
    if has_scores:
        away_cols.insert(5, pl.col("away_score").cast(pl.Int64).alias("score"))
        home_cols.insert(5, pl.col("home_score").cast(pl.Int64).alias("score"))
    out = pl.concat([games.select(away_cols), games.select(home_cols)])
    return out.with_columns(
        pl.when(pl.col("result").is_null())
        .then(None)
        .when(pl.col("result") > 0)
        .then(1.0)
        .when(pl.col("result") < 0)
        .then(0.0)
        .otherwise(0.5)
        .alias("outcome")
    )


def _standings_init(dg: pl.DataFrame, has_scores: bool) -> pl.DataFrame:
    """Port of ``standings_init`` (standings_init.R L1-87)."""
    teams = _teams_frame()
    dgj = dg.join(teams.rename({"conf": "__tconf", "division": "__tdiv"}), on="team", how="left").join(
        teams.rename({"team": "opp", "conf": "__oconf", "division": "__odiv"}),
        on="opp",
        how="left",
    )
    dgj = dgj.with_columns(
        (pl.col("__tdiv") == pl.col("__odiv")).cast(pl.Float64).alias("div_game"),
        (pl.col("__tconf") == pl.col("__oconf")).cast(pl.Float64).alias("conf_game"),
    )
    reg = dgj.filter(pl.col("game_type") == "REG")
    score = pl.col("score") if has_scores else pl.lit(3, dtype=pl.Int64)
    aggs = [
        pl.len().cast(pl.Int64).alias("games"),
        pl.col("outcome").sum().alias("wins"),
        (pl.col("outcome") == 1).sum().cast(pl.Int64).alias("true_wins"),
        (pl.col("outcome") == 0).sum().cast(pl.Int64).alias("losses"),
        (pl.col("outcome") == 0.5).sum().cast(pl.Int64).alias("ties"),
        score.sum().cast(pl.Int64).alias("pf"),
        (score - pl.col("result")).sum().cast(pl.Int64).alias("pa"),
        pl.col("outcome").mean().alias("win_pct"),
        pl.when(pl.col("div_game").sum() == 0)
        .then(0.0)
        .otherwise((pl.col("div_game") * pl.col("outcome")).sum() / pl.col("div_game").sum())
        .alias("div_pct"),
        pl.when(pl.col("conf_game").sum() == 0)
        .then(0.0)
        .otherwise((pl.col("conf_game") * pl.col("outcome")).sum() / pl.col("conf_game").sum())
        .alias("conf_pct"),
        pl.when(pl.col("conf_game").sum() == 0)
        .then(None)
        .otherwise((pl.col("conf_game") * pl.col("result")).sum())
        .cast(pl.Int64)
        .alias("conf_pd"),
    ]
    rec = reg.group_by("sim", "team").agg(aggs)
    rec = rec.join(teams, on="team", how="left")

    opp = reg.select("sim", "team", "opp", "outcome").join(
        rec.select(
            "sim",
            pl.col("team").alias("opp"),
            pl.col("wins").alias("__wins_opp"),
            pl.col("games").alias("__games_opp"),
        ),
        on=["sim", "opp"],
        how="inner",
    )
    won = (pl.col("outcome") == 1).cast(pl.Float64)
    opp_info = opp.group_by("sim", "team").agg(
        pl.when(won.sum() == 0)
        .then(0.0)
        .otherwise((pl.col("__wins_opp") * won).sum() / (pl.col("__games_opp") * won).sum())
        .alias("sov"),
        (pl.col("__wins_opp").sum() / pl.col("__games_opp").sum()).alias("sos"),
    )
    st = rec.join(opp_info, on=["sim", "team"], how="inner")
    cols = [
        "sim",
        "conf",
        "division",
        "team",
        "games",
        "wins",
        "true_wins",
        "losses",
        "ties",
    ]
    if has_scores:
        st = st.with_columns((pl.col("pf") - pl.col("pa")).alias("pd"))
        cols += ["pf", "pa", "pd"]
    else:
        st = st.drop(["pf", "pa"])
    cols += ["win_pct", "div_pct", "conf_pct", "sov", "sos", "conf_pd"]
    return st.select(cols).with_columns(pl.col("wins").cast(pl.Float64))


def _standings_h2h(dg: pl.DataFrame) -> pl.DataFrame:
    """Port of ``standings_h2h`` (standings_utils.R L22-35)."""
    return (
        dg.filter(pl.col("game_type") == "REG")
        .group_by("sim", "team", "opp")
        .agg(
            pl.len().cast(pl.Int64).alias("h2h_games"),
            pl.col("outcome").sum().alias("h2h_wins"),
            pl.col("result").sum().cast(pl.Int64).alias("h2h_pd"),
        )
    )


# ---------------------------------------------------------------------------
# division ranks (standings_add_div_ranks.R)
# ---------------------------------------------------------------------------
def _div_common_metric(st: pl.DataFrame, h2h: pl.DataFrame, n_tied: int, value_expr: pl.Expr, out: str) -> pl.DataFrame:
    """Common-games helper shared by the two division common-games steps."""
    ties = st.filter(pl.col("div_rank_counter") == n_tied).select("sim", "division", "team", "div_rank")
    cw = ties.join(h2h, on=["sim", "team"], how="inner")
    cw = cw.with_columns(
        (pl.len().over(["sim", "division", "opp", "div_rank"]) == n_tied).cast(pl.Float64).alias("__common")
    )
    return cw.group_by("sim", "team").agg(value_expr.alias(out))


def _add_div_ranks(st: pl.DataFrame, h2h: pl.DataFrame, depth: int, rng: np.random.Generator) -> pl.DataFrame:
    """Port of ``add_div_ranks`` (standings_add_div_ranks.R L1-116)."""
    grp = ["sim", "division"]
    method = "random" if depth == 0 else "min"
    st = _with_frank(st, grp, [("win_pct", True)], method, rng, out="div_rank")
    st = st.with_columns(pl.lit(None, dtype=pl.Utf8).alias("div_tie_broken_by"))
    if depth == 0:
        st = st.with_columns(pl.len().over(["sim", "division", "win_pct"]).alias("__cnt"))
        st = _set_label(st, pl.col("__cnt") > 1, "div_tie_broken_by", "Coin Toss")
        st = st.drop("__cnt")
    st = _count_ranks(st, grp + ["div_rank"], "div_rank_counter")
    cnt = pl.col("div_rank_counter")

    def _metric_step(st: pl.DataFrame, n_tied: int, metric: str, label: str) -> pl.DataFrame:
        st = _rerank_subset(st, cnt == n_tied, grp, "div_rank", [("div_rank", False), (metric, True)])
        st = _set_label(st, cnt == n_tied, "div_tie_broken_by", f"{label} ({n_tied})")
        st = _count_ranks(st, grp + ["div_rank"], "div_rank_counter")
        return _set_label(st, cnt > 1, "div_tie_broken_by", None)

    if _any_gt_one(st, "div_rank_counter"):
        for n_tied in (4, 3, 2):
            if _tie_break_done(st, "div_rank_counter", n_tied):
                continue
            # Head-to-head win pct (L118-150)
            ties = st.filter(cnt == n_tied).select("sim", "division", "team", "div_rank")
            pairs = ties.join(
                ties.select("sim", "division", "div_rank", pl.col("team").alias("opp")),
                on=["sim", "division", "div_rank"],
            ).filter(pl.col("team") != pl.col("opp"))
            h2h_pct = (
                pairs.join(h2h, on=["sim", "team", "opp"], how="inner")
                .group_by("sim", "team")
                .agg((pl.col("h2h_wins").sum() / pl.col("h2h_games").sum()).alias("__h2h_win_pct"))
            )
            st = st.join(h2h_pct, on=["sim", "team"], how="left")
            st = st.with_columns(
                pl.when((cnt == n_tied) & pl.col("__h2h_win_pct").is_null())
                .then(0.0)
                .otherwise(pl.col("__h2h_win_pct"))
                .alias("__h2h_win_pct")
            )
            st = _rerank_subset(
                st,
                cnt == n_tied,
                grp,
                "div_rank",
                [("div_rank", False), ("__h2h_win_pct", True)],
            )
            st = _count_ranks(st, grp + ["div_rank"], "div_rank_counter")
            st = _set_label(
                st,
                pl.col("__h2h_win_pct").is_not_null() & (cnt == 1),
                "div_tie_broken_by",
                f"Head-To-Head Win PCT ({n_tied})",
            )
            st = st.drop("__h2h_win_pct")
            if _tie_break_done(st, "div_rank_counter", n_tied):
                continue

            # Division record (L152-165)
            st = _metric_step(st, n_tied, "div_pct", "Division Win PCT")
            if _tie_break_done(st, "div_rank_counter", n_tied):
                continue

            # Common games win pct (L167-199)
            cwp = _div_common_metric(
                st,
                h2h,
                n_tied,
                (pl.col("__common") * pl.col("h2h_wins")).sum() / (pl.col("__common") * pl.col("h2h_games")).sum(),
                "__common_win_pct",
            )
            cwp = cwp.with_columns(
                pl.when(pl.col("__common_win_pct").is_nan())
                .then(0.0)
                .otherwise(pl.col("__common_win_pct"))
                .alias("__common_win_pct")
            )
            st = st.join(cwp, on=["sim", "team"], how="left")
            st = st.with_columns(
                pl.when((cnt == n_tied) & pl.col("__common_win_pct").is_null())
                .then(0.0)
                .otherwise(pl.col("__common_win_pct"))
                .alias("__common_win_pct")
            )
            st = _rerank_subset(
                st,
                cnt == n_tied,
                grp,
                "div_rank",
                [("div_rank", False), ("__common_win_pct", True)],
            )
            st = _count_ranks(st, grp + ["div_rank"], "div_rank_counter")
            st = _set_label(
                st,
                pl.col("__common_win_pct").is_not_null() & (cnt == 1),
                "div_tie_broken_by",
                f"Common Games Win PCT ({n_tied})",
            )
            st = st.drop("__common_win_pct")
            if _tie_break_done(st, "div_rank_counter", n_tied):
                continue

            # Conference record (L201-214)
            st = _metric_step(st, n_tied, "conf_pct", "Conference Win PCT")
            if _tie_break_done(st, "div_rank_counter", n_tied):
                continue

            if depth < 2:
                continue

            # SOV (L216-229) / SOS (L231-244)
            st = _metric_step(st, n_tied, "sov", "SOV")
            if _tie_break_done(st, "div_rank_counter", n_tied):
                continue
            st = _metric_step(st, n_tied, "sos", "SOS")
            if _tie_break_done(st, "div_rank_counter", n_tied):
                continue

            if depth < 3:
                continue

            # Combined point ranking conf/league (L246-270)
            for ptype, plabel in (("conf", "Conference"), ("league", "League")):
                sum_by = ["sim", "conf"] if ptype == "conf" else ["sim"]
                st = _with_frank(st, sum_by, [("pf", True)], "min", out="__rk_pf")
                st = _with_frank(st, sum_by, [("pa", False)], "min", out="__rk_pa")
                st = st.with_columns((pl.col("__rk_pf") + pl.col("__rk_pa")).alias("__combined")).drop(
                    ["__rk_pf", "__rk_pa"]
                )
                st = _rerank_subset(st, cnt == n_tied, grp, "div_rank", [("__combined", False)])
                st = _set_label(
                    st,
                    cnt == n_tied,
                    "div_tie_broken_by",
                    f"{plabel} Points Rank ({n_tied})",
                )
                st = _count_ranks(st, grp + ["div_rank"], "div_rank_counter")
                st = _set_label(st, cnt > 1, "div_tie_broken_by", None)
                st = st.drop("__combined")
                if _tie_break_done(st, "div_rank_counter", n_tied):
                    break
            if _tie_break_done(st, "div_rank_counter", n_tied):
                continue

            # Common games point differential (L272-295)
            cpd = _div_common_metric(
                st,
                h2h,
                n_tied,
                (pl.col("__common") * pl.col("h2h_pd")).sum(),
                "__common_pd",
            )
            st = st.join(cpd, on=["sim", "team"], how="left")
            st = _rerank_subset(
                st,
                cnt == n_tied,
                grp,
                "div_rank",
                [("div_rank", False), ("__common_pd", True)],
            )
            st = _count_ranks(st, grp + ["div_rank"], "div_rank_counter")
            st = _set_label(
                st,
                pl.col("__common_pd").is_not_null() & (cnt == 1),
                "div_tie_broken_by",
                f"Common Games Point Differential ({n_tied})",
            )
            st = st.drop("__common_pd")
            if _tie_break_done(st, "div_rank_counter", n_tied):
                continue

            # Point differential (L297-310)
            st = _metric_step(st, n_tied, "pd", "Point Differential")

        # residual ties -> coin toss (L99-110)
        if _any_gt_one(st, "div_rank_counter"):
            st = _rerank_subset(
                st,
                cnt > 1,
                grp,
                "div_rank",
                [("div_rank", False), ("win_pct", True)],
                method="random",
                rng=rng,
            )
            st = _set_label(st, cnt > 1, "div_tie_broken_by", "Coin Toss")

    return st.drop("div_rank_counter")


# ---------------------------------------------------------------------------
# conference ranks (standings_add_conf_ranks.R)
# ---------------------------------------------------------------------------
def _conf_recount_subset(st: pl.DataFrame, n_tied: int) -> pl.DataFrame:
    """``st[counter == n_tied, counter := .N, by = (sim, conf, conf_rank)]``."""
    m = pl.col("conf_rank_counter") == n_tied
    return st.with_columns(
        pl.when(m)
        .then(m.cast(pl.Int64).sum().over(["sim", "conf", "conf_rank"]))
        .otherwise(pl.col("conf_rank_counter"))
        .alias("conf_rank_counter")
    )


def _conf_eliminate(
    st: pl.DataFrame,
    n_tied: int,
    metric: str,
    descending: bool,
    label: str,
    gate: Optional[pl.Expr] = None,
) -> pl.DataFrame:
    """Winner/loser elimination step used by all conference tiebreakers.

    ``tie_winner = frankv(metric, "max") == 1`` and
    ``tie_loser = frankv(metric, "dense") != 1`` within the tied group;
    losers get ``conf_rank + 1`` and a null counter, the winner gets
    counter 1 and the tiebreaker label.
    """
    grp = ["sim", "conf", "conf_rank"]
    mask = pl.col("conf_rank_counter") == n_tied
    if gate is not None:
        mask = mask & gate
    sub = st.filter(mask).select("sim", "team", *grp[1:], metric)
    if sub.height > 0:
        sub = _with_frank(sub, grp, [(metric, descending)], "max", out="__maxrk")
        sub = _with_frank(sub, grp, [(metric, descending)], "dense", out="__densrk")
        flags = sub.select(
            "sim",
            "team",
            (pl.col("__maxrk") == 1).alias("__winner"),
            (pl.col("__densrk") != 1).alias("__loser"),
        )
        st = st.join(flags, on=["sim", "team"], how="left")
        st = st.with_columns(
            pl.when(pl.col("__loser") == True)  # noqa: E712
            .then(None)
            .otherwise(pl.col("conf_rank_counter"))
            .alias("conf_rank_counter"),
            pl.when(pl.col("__loser") == True)  # noqa: E712
            .then(pl.col("conf_rank") + 1)
            .otherwise(pl.col("conf_rank"))
            .alias("conf_rank"),
        )
        st = st.with_columns(
            pl.when(pl.col("__winner") == True)  # noqa: E712
            .then(1)
            .otherwise(pl.col("conf_rank_counter"))
            .alias("conf_rank_counter"),
        )
        st = _set_label(
            st,
            pl.col("__winner") == True,
            "conf_tie_broken_by",
            label,  # noqa: E712
        )
        st = st.drop(["__winner", "__loser"])
    return _conf_recount_subset(st, n_tied)


def _break_conf_ties_by_division(st: pl.DataFrame) -> pl.DataFrame:
    """Port of ``break_conf_ties_by_division`` (standings_add_conf_ranks.R L195-220)."""
    cnt = pl.col("conf_rank_counter")
    sub = st.filter(cnt > 1).select("sim", "team", "conf", "conf_rank", "division")
    if sub.height > 0:
        sub = sub.with_columns(
            (pl.col("division").n_unique().over(["sim", "conf", "conf_rank"]) == 1).alias("__shared")
        )
        shared = sub.select("sim", "team", "__shared")
        st = st.join(shared, on=["sim", "team"], how="left")
        mask = (cnt > 1) & (pl.col("__shared") == True)  # noqa: E712
        st = _rerank_subset(st, mask, ["sim", "conf", "conf_rank"], "conf_rank", [("div_rank", False)])
        st = _set_label(st, mask, "conf_tie_broken_by", "Division Tiebreaker")
        st = st.drop("__shared")
    return _count_ranks(st, ["sim", "conf", "conf_rank"], "conf_rank_counter")


def _conf_apply_division_reduction(st: pl.DataFrame) -> pl.DataFrame:
    """Port of ``conf_apply_division_reduction`` (standings_add_conf_ranks.R L542-570)."""
    cnt = pl.col("conf_rank_counter")
    sub = st.filter(cnt > 1).select("sim", "team", "conf_rank", "division", "div_rank")
    if sub.height == 0:
        return st
    sub = sub.with_columns(
        (pl.col("div_rank") != pl.col("div_rank").min().over(["sim", "conf_rank", "division"])).alias("__apply")
    )
    st = st.join(sub.select("sim", "team", "__apply"), on=["sim", "team"], how="left")
    st = st.with_columns(
        pl.when(pl.col("__apply") == True)  # noqa: E712
        .then(pl.col("conf_rank") + 1)
        .otherwise(pl.col("conf_rank"))
        .alias("conf_rank")
    )
    st = _count_ranks(st, ["sim", "conf", "conf_rank"], "conf_rank_counter")
    st = st.with_columns(
        pl.when(pl.col("__apply") == True)  # noqa: E712
        .then(None)
        .otherwise(pl.col("conf_rank_counter"))
        .alias("conf_rank_counter")
    )
    return st.drop("__apply")


def _conf_h2h_sweep(st: pl.DataFrame, h2h: pl.DataFrame, n_tied: int) -> pl.DataFrame:
    """Port of ``break_conf_ties_by_h2h`` (standings_add_conf_ranks.R L222-281)."""
    ties = st.filter(pl.col("conf_rank_counter") == n_tied).select("sim", "team", "conf", "conf_rank")
    pairs = ties.join(
        ties.select("sim", "conf", "conf_rank", pl.col("team").alias("opp")),
        on=["sim", "conf", "conf_rank"],
    ).filter(pl.col("team") != pl.col("opp"))
    tab = pairs.join(h2h, on=["sim", "team", "opp"], how="left")
    sweep = tab.group_by("sim", "team").agg(
        pl.when(pl.col("h2h_games").is_null().any())
        .then(None)
        .otherwise(pl.col("h2h_wins").sum() / pl.col("h2h_games").sum())
        .alias("__h2h_sweep")
    )
    # strictly inside (0, 1) -> 0.5; missing games -> 0.5
    sweep = sweep.with_columns(
        pl.when((pl.col("__h2h_sweep") > 0) & (pl.col("__h2h_sweep") < 1))
        .then(None)
        .otherwise(pl.col("__h2h_sweep"))
        .alias("__h2h_sweep")
    ).with_columns(pl.col("__h2h_sweep").fill_null(0.5))
    st = st.join(sweep, on=["sim", "team"], how="left")
    st = _conf_eliminate(st, n_tied, "__h2h_sweep", True, f"Head-To-Head Sweep ({n_tied})")
    return st.drop("__h2h_sweep")


def _conf_common_win_pct(st: pl.DataFrame, h2h: pl.DataFrame, n_tied: int) -> pl.DataFrame:
    """Port of ``break_conf_ties_by_common_win_pct`` (standings_add_conf_ranks.R L313-358)."""
    ties = st.filter(pl.col("conf_rank_counter") == n_tied).select("sim", "conf", "team", "conf_rank")
    cw = ties.join(h2h, on=["sim", "team"], how="inner")
    cw = cw.with_columns(
        (pl.len().over(["sim", "conf", "opp", "conf_rank"]) == n_tied).cast(pl.Float64).alias("__common")
    )
    agg = cw.group_by("sim", "team").agg(
        (pl.col("__common") * pl.col("h2h_games")).sum().alias("__common_games"),
        ((pl.col("__common") * pl.col("h2h_wins")).sum() / (pl.col("__common") * pl.col("h2h_games")).sum()).alias(
            "__common_win_pct"
        ),
    )
    agg = agg.with_columns(
        pl.when(pl.col("__common_win_pct").is_nan())
        .then(0.0)
        .otherwise(pl.col("__common_win_pct"))
        .alias("__common_win_pct")
    )
    st = st.join(agg, on=["sim", "team"], how="left")
    st = _conf_eliminate(
        st,
        n_tied,
        "__common_win_pct",
        True,
        f"Common Games Win PCT ({n_tied})",
        gate=pl.col("__common_games") >= 4,
    )
    return st.drop(["__common_games", "__common_win_pct"])


def _add_conf_ranks(
    st: pl.DataFrame,
    h2h: pl.DataFrame,
    depth: int,
    playoff_seeds: Optional[int],
    rng: np.random.Generator,
) -> pl.DataFrame:
    """Port of ``add_conf_ranks`` (standings_add_conf_ranks.R L1-193)."""
    method = "random" if depth == 0 else "min"
    st = st.with_columns((pl.col("div_rank") == 1).alias("__divwin"))
    st = _with_frank(st, ["sim", "conf", "__divwin"], [("win_pct", True)], method, rng, out="_rk")
    st = st.with_columns(
        pl.when(pl.col("__divwin") == True)  # noqa: E712
        .then(pl.col("_rk"))
        .otherwise(pl.col("_rk") + 4)
        .alias("conf_rank")
    ).drop(["_rk", "__divwin"])
    st = st.with_columns(pl.lit(None, dtype=pl.Utf8).alias("conf_tie_broken_by"))
    if depth == 0:
        st = st.with_columns(pl.len().over(["sim", "conf", "win_pct"]).alias("__cnt"))
        st = _set_label(st, pl.col("__cnt") > 1, "conf_tie_broken_by", "Coin Toss")
        st = st.drop("__cnt")
    if playoff_seeds is not None:
        # R: conf_rank := 50 + frankv(-win_pct, ties.method = "random") for
        # ranks beyond playoff_seeds (they're nulled after tiebreaking).
        sub = st.filter(pl.col("conf_rank") > playoff_seeds)
        if sub.height > 0:
            sub = _with_frank(sub, ["sim", "conf"], [("win_pct", True)], "random", rng)
            st = st.join(
                sub.select("sim", "team", (50 + pl.col("_rk")).alias("__new")),
                on=["sim", "team"],
                how="left",
            )
            st = st.with_columns(pl.coalesce(pl.col("__new"), pl.col("conf_rank")).alias("conf_rank")).drop("__new")
    st = _count_ranks(st, ["sim", "conf", "conf_rank"], "conf_rank_counter")

    if _any_gt_one(st, "conf_rank_counter"):
        st = _break_conf_ties_by_division(st)
        while_counter = 0
        while _any_gt_one(st, "conf_rank_counter"):
            while_counter += 1
            if while_counter > 12:
                raise RuntimeError("Entered infinite loop in conference tiebreaking procedure")
            st = _conf_apply_division_reduction(st)
            for n_tied in (4, 3, 2):
                if _tie_break_done(st, "conf_rank_counter", n_tied):
                    continue
                st = _conf_h2h_sweep(st, h2h, n_tied)
                if _tie_break_done(st, "conf_rank_counter", n_tied):
                    continue
                st = _conf_eliminate(st, n_tied, "conf_pct", True, f"Conference Win PCT ({n_tied})")
                if _tie_break_done(st, "conf_rank_counter", n_tied):
                    continue
                st = _conf_common_win_pct(st, h2h, n_tied)
                if _tie_break_done(st, "conf_rank_counter", n_tied):
                    continue
                if depth >= 2:
                    st = _conf_eliminate(st, n_tied, "sov", True, f"SOV ({n_tied})")
                    if _tie_break_done(st, "conf_rank_counter", n_tied):
                        continue
                    st = _conf_eliminate(st, n_tied, "sos", True, f"SOS ({n_tied})")
                    if _tie_break_done(st, "conf_rank_counter", n_tied):
                        continue
                if depth >= 3:
                    done = False
                    for ptype, plabel in (("conf", "Conference"), ("league", "League")):
                        sum_by = ["sim", "conf"] if ptype == "conf" else ["sim"]
                        st = _with_frank(st, sum_by, [("pf", True)], "min", out="__rk_pf")
                        st = _with_frank(st, sum_by, [("pa", False)], "min", out="__rk_pa")
                        st = st.with_columns((pl.col("__rk_pf") + pl.col("__rk_pa")).alias("__combined")).drop(
                            ["__rk_pf", "__rk_pa"]
                        )
                        st = _conf_eliminate(
                            st,
                            n_tied,
                            "__combined",
                            False,
                            f"{plabel} Points Rank ({n_tied})",
                        )
                        st = st.drop("__combined")
                        if _tie_break_done(st, "conf_rank_counter", n_tied):
                            done = True
                            break
                    if done:
                        continue
                    st = _conf_eliminate(
                        st,
                        n_tied,
                        "conf_pd",
                        True,
                        f"Conference Point Differential ({n_tied})",
                    )
                    if _tie_break_done(st, "conf_rank_counter", n_tied):
                        continue
                    st = _conf_eliminate(st, n_tied, "pd", True, f"League Point Differential ({n_tied})")
                    if _tie_break_done(st, "conf_rank_counter", n_tied):
                        continue
                # Coin toss (L524-534)
                st = _rerank_subset(
                    st,
                    pl.col("conf_rank_counter") == n_tied,
                    ["sim", "conf", "conf_rank"],
                    "conf_rank",
                    [("conf_rank", False), ("win_pct", True)],
                    method="random",
                    rng=rng,
                )
                st = _set_label(
                    st,
                    pl.col("conf_rank_counter") == n_tied,
                    "conf_tie_broken_by",
                    "Coin Toss",
                )
            st = _count_ranks(st, ["sim", "conf", "conf_rank"], "conf_rank_counter")
            st = _break_conf_ties_by_division(st)

    if playoff_seeds is not None:
        st = st.with_columns(
            pl.when(pl.col("conf_rank") > playoff_seeds).then(None).otherwise(pl.col("conf_rank")).alias("conf_rank")
        )
    return st.drop("conf_rank_counter")


# ---------------------------------------------------------------------------
# draft ranks (standings_add_draft_ranks.R)
# ---------------------------------------------------------------------------
def _draft_recount_subset(st: pl.DataFrame, n_tied: int) -> pl.DataFrame:
    m = pl.col("draft_rank_counter") == n_tied
    return st.with_columns(
        pl.when(m)
        .then(m.cast(pl.Int64).sum().over(["sim", "draft_rank"]))
        .otherwise(pl.col("draft_rank_counter"))
        .alias("draft_rank_counter")
    )


def _draft_eliminate(
    st: pl.DataFrame,
    n_tied: int,
    metric: str,
    label: str,
    gate: Optional[pl.Expr] = None,
) -> pl.DataFrame:
    """Draft variant of the elimination step. Ranks ascending: the club with
    the WORSE metric wins the earlier draft pick."""
    grp = ["sim", "draft_rank"]
    mask = pl.col("draft_rank_counter") == n_tied
    if gate is not None:
        mask = mask & gate
    sub = st.filter(mask).select("sim", "team", "draft_rank", metric)
    if sub.height > 0:
        sub = _with_frank(sub, grp, [(metric, False)], "max", out="__maxrk")
        sub = _with_frank(sub, grp, [(metric, False)], "dense", out="__densrk")
        flags = sub.select(
            "sim",
            "team",
            (pl.col("__maxrk") == 1).alias("__winner"),
            (pl.col("__densrk") != 1).alias("__loser"),
        )
        st = st.join(flags, on=["sim", "team"], how="left")
        st = st.with_columns(
            pl.when(pl.col("__loser") == True)  # noqa: E712
            .then(None)
            .otherwise(pl.col("draft_rank_counter"))
            .alias("draft_rank_counter"),
            pl.when(pl.col("__loser") == True)  # noqa: E712
            .then(pl.col("draft_rank") + 1)
            .otherwise(pl.col("draft_rank"))
            .alias("draft_rank"),
        )
        st = st.with_columns(
            pl.when(pl.col("__winner") == True)  # noqa: E712
            .then(1)
            .otherwise(pl.col("draft_rank_counter"))
            .alias("draft_rank_counter"),
        )
        st = _set_label(
            st,
            pl.col("__winner") == True,
            "draft_tie_broken_by",
            label,  # noqa: E712
        )
        st = st.drop(["__winner", "__loser"])
    return _draft_recount_subset(st, n_tied)


def _break_draft_shared(st: pl.DataFrame, unit_col: str, rank_source: str, label: str) -> pl.DataFrame:
    """Ports ``break_draft_ties_by_division`` / ``_by_conference``
    (standings_add_draft_ranks.R L146-202): if all tied clubs share the same
    division/conference, rank them by descending div_rank/conf_rank (the
    worse-ranked club picks earlier)."""
    cnt = pl.col("draft_rank_counter")
    sub = st.filter(cnt > 1).select("sim", "team", "draft_rank", unit_col, rank_source)
    if sub.height > 0:
        sub = sub.with_columns((pl.col(unit_col).n_unique().over(["sim", "draft_rank"]) == 1).alias("__shared"))
        st = st.join(sub.select("sim", "team", "__shared"), on=["sim", "team"], how="left")
        mask = (cnt > 1) & (pl.col("__shared") == True)  # noqa: E712
        st = _rerank_subset(st, mask, ["sim", "draft_rank"], "draft_rank", [(rank_source, True)])
        st = _set_label(st, mask, "draft_tie_broken_by", label)
        st = st.drop("__shared")
    return _count_ranks(st, ["sim", "draft_rank"], "draft_rank_counter")


def _draft_apply_reduction(st: pl.DataFrame) -> pl.DataFrame:
    """Port of ``draft_apply_reduction`` (standings_add_draft_ranks.R L360-399)."""
    cnt = pl.col("draft_rank_counter")
    sub = st.filter(cnt > 1).select("sim", "team", "draft_rank", "division", "conf", "div_rank", "conf_rank")
    if sub.height == 0:
        return st
    sub = sub.with_columns(
        (pl.col("div_rank") != pl.col("div_rank").max().over(["sim", "draft_rank", "division"])).alias("__apply_div"),
        # R computes max(conf_rank) without na.rm; a null in the group nulls
        # the comparison, which fifelse/filtering then treats as not-TRUE.
        pl.when(pl.col("conf_rank").is_null().any().over(["sim", "draft_rank", "conf"]))
        .then(None)
        .otherwise(pl.col("conf_rank") != pl.col("conf_rank").max().over(["sim", "draft_rank", "conf"]))
        .alias("__apply_conf"),
    )
    sub = sub.with_columns(
        (
            (pl.col("__apply_div") == True)  # noqa: E712
            | (pl.col("__apply_conf") == True)  # noqa: E712
        ).alias("__apply")
    )
    st = st.join(sub.select("sim", "team", "__apply"), on=["sim", "team"], how="left")
    st = st.with_columns(
        pl.when(pl.col("__apply") == True)  # noqa: E712
        .then(pl.col("draft_rank") + 1)
        .otherwise(pl.col("draft_rank"))
        .alias("draft_rank")
    )
    st = _count_ranks(st, ["sim", "draft_rank"], "draft_rank_counter")
    st = st.with_columns(
        pl.when(pl.col("__apply") == True)  # noqa: E712
        .then(None)
        .otherwise(pl.col("draft_rank_counter"))
        .alias("draft_rank_counter")
    )
    return st.drop("__apply")


def _draft_h2h(st: pl.DataFrame, h2h: pl.DataFrame, n_tied: int) -> pl.DataFrame:
    """Port of ``break_draft_ties_by_h2h`` (standings_add_draft_ranks.R L204-263)."""
    ties = st.filter(pl.col("draft_rank_counter") == n_tied).select("sim", "team", "draft_rank")
    pairs = ties.join(
        ties.select("sim", "draft_rank", pl.col("team").alias("opp")),
        on=["sim", "draft_rank"],
    ).filter(pl.col("team") != pl.col("opp"))
    tab = pairs.join(h2h, on=["sim", "team", "opp"], how="left")
    sweep = tab.group_by("sim", "team").agg(
        pl.when(pl.col("h2h_games").is_null().any())
        .then(None)
        .otherwise(pl.col("h2h_wins").sum() / pl.col("h2h_games").sum())
        .alias("__h2h_sweep")
    )
    sweep = sweep.with_columns(
        pl.when((pl.col("__h2h_sweep") > 0) & (pl.col("__h2h_sweep") < 1))
        .then(None)
        .otherwise(pl.col("__h2h_sweep"))
        .alias("__h2h_sweep")
    ).with_columns(pl.col("__h2h_sweep").fill_null(0.5))
    st = st.join(sweep, on=["sim", "team"], how="left")
    st = _draft_eliminate(st, n_tied, "__h2h_sweep", f"Head-To-Head ({n_tied})")
    return st.drop("__h2h_sweep")


def _draft_common_win_pct(st: pl.DataFrame, h2h: pl.DataFrame, n_tied: int) -> pl.DataFrame:
    """Port of ``break_draft_ties_by_common_win_pct`` (standings_add_draft_ranks.R L265-310)."""
    ties = st.filter(pl.col("draft_rank_counter") == n_tied).select("sim", "team", "draft_rank")
    cw = ties.join(h2h, on=["sim", "team"], how="inner")
    cw = cw.with_columns((pl.len().over(["sim", "opp", "draft_rank"]) == n_tied).cast(pl.Float64).alias("__common"))
    agg = cw.group_by("sim", "team").agg(
        (pl.col("__common") * pl.col("h2h_games")).sum().alias("__common_games"),
        ((pl.col("__common") * pl.col("h2h_wins")).sum() / (pl.col("__common") * pl.col("h2h_games")).sum()).alias(
            "__common_win_pct"
        ),
    )
    agg = agg.with_columns(
        pl.when(pl.col("__common_win_pct").is_nan())
        .then(0.0)
        .otherwise(pl.col("__common_win_pct"))
        .alias("__common_win_pct")
    )
    st = st.join(agg, on=["sim", "team"], how="left")
    st = _draft_eliminate(
        st,
        n_tied,
        "__common_win_pct",
        f"Common Games Win PCT ({n_tied})",
        gate=pl.col("__common_games") >= 4,
    )
    return st.drop(["__common_games", "__common_win_pct"])


def _add_draft_ranks(
    st: pl.DataFrame,
    h2h: pl.DataFrame,
    dg: Optional[pl.DataFrame],
    depth: int,
    rng: np.random.Generator,
) -> pl.DataFrame:
    """Port of ``add_draft_ranks`` (standings_add_draft_ranks.R L1-144)."""
    if dg is not None:
        dg2 = dg.with_columns(
            pl.when((pl.col("game_type") == "SB") & (pl.col("result") > 0))
            .then(pl.lit("SB_WIN"))
            .otherwise(pl.col("game_type"))
            .alias("game_type")
        )
        exit_df = dg2.group_by("sim", "team").agg(pl.col("game_type").sort_by("week").last().alias("__exit_chr"))
        exit_df = exit_df.with_columns(
            pl.col("__exit_chr").replace_strict(_EXIT_TO_INT, return_dtype=pl.Int64).alias("exit")
        ).drop("__exit_chr")
        st = st.join(exit_df, on=["sim", "team"], how="left")
        st = st.with_columns(pl.col("exit").fill_null(0))

    method = "random" if depth == 0 else "min"
    st = _with_frank(
        st,
        ["sim"],
        [("exit", False), ("win_pct", False), ("sos", False)],
        method,
        rng,
        out="draft_rank",
    )
    st = st.with_columns(pl.lit(None, dtype=pl.Utf8).alias("draft_tie_broken_by"))
    if depth == 0:
        st = st.with_columns(pl.len().over(["sim", "exit", "win_pct", "sos"]).alias("__cnt"))
        st = _set_label(st, pl.col("__cnt") > 1, "draft_tie_broken_by", "Coin Toss")
        st = st.drop("__cnt")
    st = _count_ranks(st, ["sim", "draft_rank"], "draft_rank_counter")

    if _any_gt_one(st, "draft_rank_counter"):
        st = _break_draft_shared(st, "division", "div_rank", "Division Tiebreaker")
        st = _break_draft_shared(st, "conf", "conf_rank", "Conference Tiebreaker")
        while_counter = 0
        while _any_gt_one(st, "draft_rank_counter"):
            while_counter += 1
            if while_counter > 18:
                raise RuntimeError("Entered infinite loop in draft tiebreaking procedure")
            st = _draft_apply_reduction(st)
            n_tied = 2
            if not _tie_break_done(st, "draft_rank_counter", n_tied):
                st = _draft_h2h(st, h2h, n_tied)
            if not _tie_break_done(st, "draft_rank_counter", n_tied):
                st = _draft_common_win_pct(st, h2h, n_tied)
            if not _tie_break_done(st, "draft_rank_counter", n_tied):
                st = _draft_eliminate(st, n_tied, "sov", f"SOV ({n_tied})")
            if not _tie_break_done(st, "draft_rank_counter", n_tied):
                # Coin toss (L342-352)
                st = _rerank_subset(
                    st,
                    pl.col("draft_rank_counter") == n_tied,
                    ["sim", "draft_rank"],
                    "draft_rank",
                    [("exit", False), ("win_pct", False), ("sos", False)],
                    method="random",
                    rng=rng,
                )
                st = _set_label(
                    st,
                    pl.col("draft_rank_counter") == n_tied,
                    "draft_tie_broken_by",
                    "Coin Toss",
                )
            st = _count_ranks(st, ["sim", "draft_rank"], "draft_rank_counter")
            st = _break_draft_shared(st, "division", "div_rank", "Division Tiebreaker")
            st = _break_draft_shared(st, "conf", "conf_rank", "Conference Tiebreaker")
    return st.drop("draft_rank_counter")


# ---------------------------------------------------------------------------
# engine + public API
# ---------------------------------------------------------------------------
def _standings_core(
    games: pl.DataFrame,
    *,
    ranks: str,
    depth: int,
    playoff_seeds: Optional[int],
    rng: np.random.Generator,
    has_scores: bool,
) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Run the standings engine on a validated games frame (must carry a
    ``sim`` column and non-null results). Returns ``(standings, h2h, dg)``."""
    dg = _double_games(games, has_scores)
    st = _standings_init(dg, has_scores)
    h2h = _standings_h2h(dg)
    if ranks == "NONE":
        return st, h2h, dg
    st = _add_div_ranks(st, h2h, depth, rng)
    if ranks == "DIV":
        return st, h2h, dg
    st = _add_conf_ranks(st, h2h, depth, playoff_seeds, rng)
    if ranks == "CONF":
        return st, h2h, dg
    st = _add_draft_ranks(st, h2h, dg, depth, rng)
    return st, h2h, dg


def _finalize_standings(st: pl.DataFrame, uses_season: bool) -> pl.DataFrame:
    """Port of ``finalize_standings`` (standings_utils.R L82-97)."""
    sort_cols = ["sim", "division"] + (["div_rank"] if "div_rank" in st.columns else [])
    st = st.sort(sort_cols)
    if "exit" in st.columns:
        st = st.with_columns(pl.col("exit").replace_strict(_INT_TO_EXIT, return_dtype=pl.Utf8))
    if "conf_pd" in st.columns:
        st = st.drop("conf_pd")
    if uses_season:
        st = st.rename({"sim": "season"})
    return st


@overload
def nfl_season_standings(
    games: pl.DataFrame,
    *,
    ranks: str = ...,
    tiebreaker_depth: str = ...,
    playoff_seeds: Optional[int] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def nfl_season_standings(
    games: pl.DataFrame,
    *,
    ranks: str = ...,
    tiebreaker_depth: str = ...,
    playoff_seeds: Optional[int] = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...


def nfl_season_standings(
    games: pl.DataFrame,
    *,
    ranks: str = "CONF",
    tiebreaker_depth: str = "SOS",
    playoff_seeds: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Compute NFL standings with the real NFL tiebreaking procedures.

    Faithful polars port of ``nflseedR::nfl_standings()`` (v2 engine,
    ``R/standings.R`` L82-155): initializes records, points, win
    percentages, SOV and SOS from a games frame, then resolves division
    ranks, conference ranks (playoff seeds) and draft order through the
    full NFL tiebreaker cascades.

    Args:
        games: Games frame with one row per game. Required columns:
            ``sim`` or ``season`` (identifier), ``game_type`` (``'REG'``,
            ``'WC'``, ``'DIV'``, ``'CON'``, ``'SB'``), ``week``,
            ``away_team``, ``home_team``, and ``result`` (home score minus
            away score; no missing values allowed). ``away_score`` /
            ``home_score`` are additionally required for
            ``tiebreaker_depth='POINTS'`` and enable the ``pf``/``pa``/``pd``
            output columns.
        ranks: One of ``'DIV'``, ``'CONF'`` (default), ``'DRAFT'``, or
            ``'NONE'`` — which rank columns (and thus tiebreakers) to
            compute. ``'DRAFT'`` implies ``'CONF'`` implies ``'DIV'``.
        tiebreaker_depth: One of ``'SOS'`` (default), ``'PRE-SOV'``,
            ``'POINTS'``, or ``'RANDOM'``. Controls how deep the tiebreaker
            cascade goes before falling back to a coin toss.
        playoff_seeds: If not ``None``, only conference ranks up to this
            value are resolved with tiebreakers; deeper ranks are returned
            as null. Must be in 1-16.
        return_as_pandas: If ``True``, return a pandas DataFrame.

    Returns:
        A standings frame with one row per (sim/season, team) including
        records, ``win_pct``/``div_pct``/``conf_pct``, ``sov``, ``sos``, and
        the requested ``div_rank``/``conf_rank``/``draft_rank`` columns plus
        ``*_tie_broken_by`` bookkeeping. ``conf_rank`` is the playoff seed.

    Raises:
        ValueError: If required columns are missing, results contain
            missing values, an argument is out of range, or a team
            abbreviation is unknown.

    Example:
        Standings + seeds for a completed season::

            import sportsdataverse.nfl as nfl
            games = nfl.load_schedules([2024])
            standings = nfl.nfl_season_standings(games, ranks="DRAFT")
            print(standings.shape)

        Playoff seeds only, pandas output::

            df = nfl.nfl_season_standings(
                games, ranks="CONF", playoff_seeds=7, return_as_pandas=True
            )

        Pipeline next step (one line)::

            standings.filter(pl.col("conf_rank") <= 7).sort("conf", "conf_rank")

    See Also:
        * `nflseedR`_ -- the canonical R implementation this ports.
        * `nflreadpy`_ -- nflverse schedules/data in Python.

    .. _nflseedR: https://nflseedr.com
    .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    if ranks not in ("DIV", "CONF", "DRAFT", "NONE"):
        raise ValueError("ranks must be one of 'DIV', 'CONF', 'DRAFT', 'NONE'")
    if tiebreaker_depth not in _DEPTH_MAP:
        raise ValueError("tiebreaker_depth must be one of 'SOS', 'PRE-SOV', 'POINTS', 'RANDOM'")
    depth = _DEPTH_MAP[tiebreaker_depth]
    if playoff_seeds is not None and not (1 <= playoff_seeds <= 16):
        raise ValueError("playoff_seeds must be in range 1 - 16")
    games, uses_season, has_scores = _validate_games(games)
    if depth == 3 and not has_scores:
        raise ValueError("tiebreaker_depth='POINTS' requires away_score and home_score columns")
    rng = np.random.default_rng()
    st, _, _ = _standings_core(
        games,
        ranks=ranks,
        depth=depth,
        playoff_seeds=playoff_seeds,
        rng=rng,
        has_scores=has_scores,
    )
    out = _finalize_standings(st, uses_season)
    if return_as_pandas:
        return out.to_pandas()
    return out
