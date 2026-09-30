# tools/validation/cfb_fourth_down_impact.py
"""Regulation impact of a CFB win-probability change on the fourth-down bot, 2014-2025.

Built for sdv-py #626 (the overtime correction, ``cfb_wp_overtime``); rerun it whenever
that correction is refit or the WP boosters change.

* ``score`` re-scores every published fourth-down DECISION of a season (the
  ``football.tendencies`` rule: a fourth-down rush, pass, punt or field goal that stood)
  through the importable sdv-py's ``get_4th_down_probs``, with the overtime possession
  order the pipeline computes (``cfb_pbp._with_ot_second``), and keeps the published
  columns beside the new ones.
* ``report`` compares the two: recommendation flips and confidence-tier changes per
  season, where ``|delta go_wp_diff|`` / ``|delta go_boost|`` concentrate (quarter,
  margin, time left), the regulation WP calibration of late one-score states (Q4,
  ``|margin| <= 8``) before vs after against observed results, and the coach
  WP-left boards.

"Before" is the published ``cfbfastR-cfb-data`` pbp. That is origin/main's output: for every
season 2014-2025, main's ``get_4th_down_probs`` (its package extracted with ``git archive``)
reproduces the published go/FG/punt WP, go_boost and recommendation exactly (max |diff| 0) on
every late one-score regulation decision plus 400 sampled others (13,423 decisions), and its
``wp_spread`` booster reproduces the published ``wp_before`` (max |diff| 3e-5). Re-check that
when the pbp is reprocessed, and re-score every season after a refit of the correction (a
score run reads the card once, at its first prediction).

Usage::

    for s in $(seq 2014 2025); do
        uv run python -m tools.validation.cfb_fourth_down_impact score --season $s --data <dir>
    done
    uv run python -m tools.validation.cfb_fourth_down_impact report --data <dir> \\
        --out tools/validation/reports/cfb_fourth_down_impact_2014_2025.md
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

import polars as pl

from sportsdataverse.cfb.cfb_fourth_down import _PBP_COLS, get_4th_down_probs
from sportsdataverse.cfb.cfb_pbp import _with_ot_second
from sportsdataverse.cfb.cfb_wp_overtime import adjust_wp, reach_overtime_prob
from sportsdataverse.cfb.model_cards import load_model_card
from sportsdataverse.cfb.model_vars import wp_final_names, wp_start_columns
from sportsdataverse.football.tendencies import _fourth_counts, _prepare
from tools.fit_cfb_wp_overtime import brier_delta_ci, load_states

SEASONS = range(2014, 2026)
HOLDOUT = (2022, 2025)  # the correction is fitted on 2004-2021 (card)
OUT = ["go_wp", "fg_wp", "punt_wp", "go_boost", "go_wp_diff", "fourth_down_recommendation"]
STATE = [c for c in _PBP_COLS.values() if c != _PBP_COLS["ot_second"]]
KEYS = ["game_id", "game_play_number"]
FLAGS = ["scrimmage_play", "penalty_no_play", "rush", "pass", "punt", "fg_attempt", "first_down_created", "touchdown"]
#: GOP DecisionCard's confidence label reads |go_boost| (pp): LOW < 1 <= MEDIUM < 3 <= STRONG < 10 <= VERY STRONG.
TIER_CUTS, TIER_LABELS = [1.0, 3.0, 10.0], ["LOW", "MEDIUM", "STRONG", "VERY STRONG"]
MARGIN = (
    [-9, -4, -1, 0, 3, 8],
    ["<=-9", "-8..-4", "-3..-1", "0", "1..3", "4..8", ">=9"],
)
TIME_LEFT = (  # start.adj_TimeSecsRem (game seconds left); overtime is its own bucket
    [60, 120, 300, 600, 900, 1800],
    ["Q4 0-1m", "Q4 1-2m", "Q4 2-5m", "Q4 5-10m", "Q4 10-15m", "2nd half Q3", "1st half"],
)
MINUTE = ([60, 120, 180, 240, 300, 600], ["0-1m", "1-2m", "2-3m", "3-4m", "4-5m", "5-10m", "10-15m"])


def _pbp(root: Path, season: int) -> Path:
    return root / f"pbp/parquet/play_by_play_{season}.parquet"


def score(args: argparse.Namespace) -> None:
    path = _pbp(args.pbp_root, args.season)
    have = pl.read_parquet_schema(path)
    cols = [*KEYS, "seasonType", "pos_team_id", "start.pos_team.id", "type.text", *STATE, *FLAGS, *OUT]
    plays = (
        pl.read_parquet(path, columns=[c for c in dict.fromkeys(cols) if c in have])
        .filter(pl.col("seasonType").is_in([2, 3]))
        .sort(KEYS)
    )
    plays = _with_ot_second(plays)  # the possession order the pipeline hands the fourth-down surface
    dec = _prepare(plays, None).filter(pl.col("t_fourth_decision"))
    assert dec.select(KEYS).is_unique().all(), "duplicate (game_id, game_play_number)"
    new = pl.from_pandas(get_4th_down_probs(dec.select(*KEYS, *STATE, "ot_second_possession"))[[*KEYS, *OUT]])
    out = dec.select(
        *KEYS,
        "season",
        "pos_team_id",
        "period",
        "start.adj_TimeSecsRem",
        "pos_score_diff_start",
        "start.down",
        "start.distance",
        "start.yardsToEndzone",
        "t_fourth_decision",
        "t_fourth_went",
        "t_converted",
        *OUT,
    ).join(new.rename({c: f"new_{c}" for c in OUT}), on=KEYS, how="left")
    assert (
        out.height == dec.height
        and out["new_fourth_down_recommendation"].null_count() <= out["fourth_down_recommendation"].null_count()
    ), "the re-score dropped decisions"
    args.data.mkdir(parents=True, exist_ok=True)
    out.write_parquet(args.data / f"fourth_{args.season}.parquet")
    print(args.season, out.height, "decisions")


# ---- report --------------------------------------------------------------------------------


def _tier(c: str) -> pl.Expr:
    return pl.col(c).abs().cut(TIER_CUTS, labels=TIER_LABELS, left_closed=True).cast(pl.Utf8).fill_null("none")


def _buckets(f: pl.DataFrame) -> pl.DataFrame:
    t = pl.col("start.adj_TimeSecsRem")
    return f.with_columns(
        quarter=pl.when(pl.col("period") >= 5).then(pl.lit("OT")).otherwise(pl.format("Q{}", pl.col("period"))),
        margin=pl.col("pos_score_diff_start").cut(MARGIN[0], labels=MARGIN[1]).cast(pl.Enum(MARGIN[1])),
        time_left=pl.when(pl.col("period") >= 5)
        .then(pl.lit("OT"))
        .otherwise(t.cut(TIME_LEFT[0], labels=TIME_LEFT[1]).cast(pl.Utf8))
        .cast(pl.Enum([*TIME_LEFT[1], "OT"])),
        late_one_score=(pl.col("period") == 4) & (t <= 300) & (pl.col("pos_score_diff_start").abs() <= 8),
    )


def _load(data: Path) -> pl.DataFrame:
    f = pl.concat([pl.read_parquet(data / f"fourth_{s}.parquet") for s in SEASONS], how="vertical_relaxed")
    return _buckets(f).with_columns(
        flip=pl.col("fourth_down_recommendation").ne_missing(pl.col("new_fourth_down_recommendation")),
        tier=_tier("go_boost"),
        new_tier=_tier("new_go_boost"),
        d_diff=(pl.col("new_go_wp_diff") - pl.col("go_wp_diff")).abs() * 100,  # pp, like go_boost
        d_boost=(pl.col("new_go_boost") - pl.col("go_boost")).abs(),
    )


def _board(f: pl.DataFrame, rec: str, boost: str, keys: list[str]) -> pl.DataFrame:
    """``football.tendencies``' own fourth-down counts, on the given recommendation columns."""
    return _fourth_counts(f.with_columns(t_rec=pl.col(rec), t_go_boost=pl.col(boost)), keys)


def _dist(f: pl.DataFrame, by: str) -> pl.DataFrame:
    return (
        f.group_by(by)
        .agg(
            n=pl.len(),
            flips=pl.col("flip").sum(),
            flip_rate=pl.col("flip").mean(),
            tier_changes=(pl.col("tier") != pl.col("new_tier")).sum(),
            d_diff_median=pl.col("d_diff").median(),
            d_diff_p90=pl.col("d_diff").quantile(0.9),
            d_diff_p99=pl.col("d_diff").quantile(0.99),
            d_diff_max=pl.col("d_diff").max(),
            d_diff_gt1pp=(pl.col("d_diff") > 1).mean(),
            d_boost_median=pl.col("d_boost").median(),
            d_boost_p99=pl.col("d_boost").quantile(0.99),
            sum_d_boost=pl.col("d_boost").sum(),
        )
        .with_columns(share_of_all_d_boost=pl.col("sum_d_boost") / pl.col("sum_d_boost").sum())
        .drop("sum_d_boost")
        .sort(by)
    )


def _logloss(p: pl.Expr) -> pl.Expr:
    q = p.clip(1e-6, 1 - 1e-6)
    return -(pl.col("y") * q.log() + (1 - pl.col("y")) * (1 - q).log()).mean()


def _cells(s: pl.DataFrame, by: list[str], before: str, after: str) -> pl.DataFrame:
    """Per cell: level, Brier and log loss before/after, and a game-clustered 95% interval on the Brier change."""
    rows = []
    for key, g in s.group_by(by, maintain_order=True):
        ci = brier_delta_ci(g["game_id"].to_numpy(), g[before].to_numpy(), g[after].to_numpy(), g["y"].to_numpy())
        rows.append({**dict(zip(by, key)), "brier_delta": ci["delta"], "ci_lo": ci["ci95"][0], "ci_hi": ci["ci95"][1]})
    stats = s.group_by(by).agg(
        n=pl.len(),
        games=pl.col("game_id").n_unique(),
        won=pl.col("y").mean(),
        before=pl.col(before).mean(),
        after=pl.col(after).mean(),
        brier_before=((pl.col(before) - pl.col("y")) ** 2).mean(),
        brier_after=((pl.col(after) - pl.col("y")) ** 2).mean(),
        logloss_before=_logloss(pl.col(before)),
        logloss_after=_logloss(pl.col(after)),
    )
    ci = pl.DataFrame(rows).cast({k: stats.schema[k] for k in by})
    return stats.join(ci, on=by).with_columns(
        gap_before=pl.col("before") - pl.col("won"), gap_after=pl.col("after") - pl.col("won")
    )


def _calibration(states: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Regulation WP at Q4 one-score scrimmage snaps: published booster vs the corrected runtime.

    Also the correction's two parts per cell: q against the observed overtime rate, and the booster
    against the win rate of games settled in regulation (what it was trained to estimate).
    """
    s = states.filter(
        (pl.col("period") == 4)
        & (pl.col("pos_score_diff_start").abs() <= 8)
        & (pl.col("scrimmage_play") == True)  # noqa: E712
        & pl.col("wp_before").is_not_null()
    )
    X = s.select([pl.col(src).alias(name) for src, name in zip(wp_start_columns, wp_final_names)])
    s = s.with_columns(
        after=pl.Series(adjust_wp(s["wp_before"].to_numpy(), X)),
        q=pl.Series(reach_overtime_prob(X.to_pandas())),
        y=pl.col("won").cast(pl.Float64),
        sample=pl.when(pl.col("season").is_between(*HOLDOUT))
        .then(pl.lit("holdout 2022-25"))
        .otherwise(pl.lit("in-sample 2014-21")),
        minute=pl.col("start.adj_TimeSecsRem").cut(MINUTE[0], labels=MINUTE[1]).cast(pl.Enum(MINUTE[1])),
        state=pl.when(pl.col("pos_score_diff_start") == 0)
        .then(pl.lit("tied"))
        .when(pl.col("pos_score_diff_start") > 0)
        .then(pl.lit("leading 1-8"))
        .otherwise(pl.lit("trailing 1-8")),
    )
    s = pl.concat([s, s.with_columns(state=pl.lit("all one-score"))])
    tab = _cells(s, ["sample", "state", "minute"], "wp_before", "after").sort("sample", "state", "minute")
    parts = (
        s.group_by("sample", "state", "minute")
        .agg(
            q=pl.col("q").mean(),
            ot_rate=pl.col("reached_ot").cast(pl.Float64).mean(),
            booster_reg=pl.col("wp_before").filter(~pl.col("reached_ot")).mean(),
            won_reg=pl.col("y").filter(~pl.col("reached_ot")).mean(),
        )
        .with_columns(q_gap=pl.col("q") - pl.col("ot_rate"), booster_gap=pl.col("booster_reg") - pl.col("won_reg"))
        .sort("sample", "state", "minute")
    )
    return tab, parts


def _chosen_branch(f: pl.DataFrame, states: pl.DataFrame, root: Path) -> pl.DataFrame:
    """The WP of the option the team chose, against the result: what the board's decisions stand on."""
    kicks = pl.concat(
        [
            pl.read_parquet(_pbp(root, s), columns=[*KEYS, "fg_attempt", "punt"]).with_columns(
                pl.col("fg_attempt").cast(pl.Boolean, strict=False).fill_null(False),
                pl.col("punt").cast(pl.Boolean, strict=False).fill_null(False),
            )
            for s in SEASONS
        ],
        how="vertical_relaxed",
    )
    won = states.select(*KEYS, "won").unique(KEYS)
    for right in (kicks, won):
        assert all(f.schema[k] == right.schema[k] for k in KEYS), "join-key dtypes differ"
    d = f.join(kicks, on=KEYS, how="left").join(won, on=KEYS, how="inner")

    def chosen(prefix: str) -> pl.Expr:
        return (
            pl.when(pl.col("t_fourth_went"))
            .then(pl.col(f"{prefix}go_wp"))
            .when(pl.col("fg_attempt"))
            .then(pl.col(f"{prefix}fg_wp"))
            .when(pl.col("punt"))
            .then(pl.col(f"{prefix}punt_wp"))
        )

    d = d.with_columns(
        chosen_before=chosen(""),
        chosen_after=chosen("new_"),
        y=pl.col("won").cast(pl.Float64),
        sample=pl.when(pl.col("season").is_between(*HOLDOUT))
        .then(pl.lit("holdout 2022-25"))
        .otherwise(pl.lit("in-sample 2014-21")),
        slice=pl.when(pl.col("period") >= 5)
        .then(pl.lit("overtime"))
        .when(pl.col("late_one_score"))
        .then(pl.lit("late one-score"))
        .otherwise(pl.lit("rest of regulation")),
    ).filter(pl.col("chosen_before").is_not_null() & pl.col("chosen_after").is_not_null())
    return _cells(d, ["sample", "slice"], "chosen_before", "chosen_after").sort("sample", "slice")


def _md(df: pl.DataFrame) -> str:
    with pl.Config(
        tbl_rows=500,
        tbl_cols=40,
        tbl_width_chars=10_000,
        tbl_formatting="MARKDOWN",
        tbl_hide_dataframe_shape=True,
        tbl_hide_column_data_types=True,
        float_precision=4,
    ):
        return str(df.drop([c for c in df.columns if c.startswith("__")]))


def report(args: argparse.Namespace) -> None:
    f = _load(args.data)
    # a no-op guard: 92% of late one-score decisions move by more than 0.1 pp (2019/22/25)
    late_moved = f.filter(pl.col("late_one_score")).select((pl.col("d_boost") > 0.1).mean()).item()
    assert late_moved > 0.5, f"late one-score decisions barely moved ({late_moved:.2f}): a no-op?"
    big = pl.max_horizontal(pl.col("go_boost").abs(), pl.col("new_go_boost").abs()) >= 1.0
    unscored = f.select(
        before=pl.col("fourth_down_recommendation").is_null().sum(),
        after=pl.col("new_fourth_down_recommendation").is_null().sum(),
    ).row(0)
    per_season = (
        f.group_by("season")
        .agg(
            decisions=pl.len(),
            flips=pl.col("flip").sum(),
            flip_rate=pl.col("flip").mean(),
            go_to_kick=(pl.col("flip") & (pl.col("fourth_down_recommendation") == "go")).sum(),
            kick_to_go=(pl.col("flip") & (pl.col("new_fourth_down_recommendation") == "go")).sum(),
            fg_punt=(
                pl.col("flip")
                & pl.col("fourth_down_recommendation").is_in(["field_goal", "punt"])
                & pl.col("new_fourth_down_recommendation").is_in(["field_goal", "punt"])
            ).sum(),
            tier_changes=(pl.col("tier") != pl.col("new_tier")).sum(),
            tier_change_rate=(pl.col("tier") != pl.col("new_tier")).mean(),
            flips_late_one_score=(pl.col("flip") & pl.col("late_one_score")).sum(),
            flips_ot=(pl.col("flip") & (pl.col("period") >= 5)).sum(),
            flips_medium_plus=(pl.col("flip") & big).sum(),
        )
        .sort("season")
    )
    total = per_season.select(pl.lit(0).alias("season"), pl.exclude("season").sum()).with_columns(
        flip_rate=pl.col("flips") / pl.col("decisions"), tier_change_rate=pl.col("tier_changes") / pl.col("decisions")
    )
    per_season = pl.concat([per_season, total.cast(per_season.schema)])
    trans = (
        f.group_by("fourth_down_recommendation", "new_fourth_down_recommendation")
        .len()
        .pivot(on="new_fourth_down_recommendation", index="fourth_down_recommendation", values="len")
        .sort("fourth_down_recommendation")
    )
    tiers = f.group_by("tier", "new_tier").len().pivot(on="new_tier", index="tier", values="len").sort("tier")
    agree = pl.concat(
        [
            _board(f, rec, boost, ["season"]).with_columns(side=pl.lit(side))
            for side, rec, boost in (
                ("before", "fourth_down_recommendation", "go_boost"),
                ("after", "new_fourth_down_recommendation", "new_go_boost"),
            )
        ]
    ).with_columns(agreement=pl.col("fourth_agreed") / pl.col("fourth_decisions"))
    agree = agree.pivot(on="side", index="season", values=["agreement", "fourth_wp_left"]).sort("season")
    by_tier = (
        f.group_by("tier")
        .agg(
            n=pl.len(),
            flips=pl.col("flip").sum(),
            flip_rate=pl.col("flip").mean(),
            flips_low_after=(pl.col("flip") & (pl.col("new_tier") == "LOW")).sum(),
            flips_late_one_score=(pl.col("flip") & pl.col("late_one_score")).sum(),
        )
        .with_columns(share_of_flips=pl.col("flips") / pl.col("flips").sum())
        .sort("tier")
    )
    late = f.group_by("late_one_score").agg(
        n=pl.len(),
        flips=pl.col("flip").sum(),
        flip_rate=pl.col("flip").mean(),
        d_boost_sum=pl.col("d_boost").sum(),
    )
    late = late.with_columns(
        share_of_decisions=pl.col("n") / pl.col("n").sum(),
        share_of_flips=pl.col("flips") / pl.col("flips").sum(),
        share_of_d_boost=pl.col("d_boost_sum") / pl.col("d_boost_sum").sum(),
    ).drop("d_boost_sum")
    q4_cross = (
        f.filter(pl.col("period") == 4)
        .group_by("time_left", "margin")
        .agg(
            n=pl.len(),
            flips=pl.col("flip").sum(),
            flip_rate=pl.col("flip").mean(),
            d_diff_median=pl.col("d_diff").median(),
        )
        .sort("time_left", "margin")
    )

    # coach boards: football.tendencies' counts per team-season, the published coach per team-season
    coaches = pl.concat(
        [
            pl.read_parquet(
                args.pbp_root / f"coach_tendencies/parquet/coach_tendencies_{s}.parquet",
                columns=["season", "pos_team_id", "pos_team", "coach", "fourth_decisions", "fourth_wp_left"],
            )
            for s in SEASONS
        ],
        how="vertical_relaxed",
    ).with_columns(pl.col("pos_team_id").cast(pl.Int64), pl.col("season").cast(pl.Int64))
    team = f.with_columns(pl.col("pos_team_id").cast(pl.Int64), pl.col("season").cast(pl.Int64))
    b = _board(team, "fourth_down_recommendation", "go_boost", ["season", "pos_team_id"]).select(
        "season", "pos_team_id", "fourth_decisions", wp_left_before="fourth_wp_left"
    )
    a = _board(team, "new_fourth_down_recommendation", "new_go_boost", ["season", "pos_team_id"]).select(
        "season", "pos_team_id", wp_left_after="fourth_wp_left"
    )
    coaches = coaches.rename({"fourth_decisions": "published_decisions"})
    board = coaches.join(b, on=["season", "pos_team_id"]).join(a, on=["season", "pos_team_id"])
    assert board.height == coaches.filter(pl.col("published_decisions") > 0).height, "the coach join dropped rows"
    assert (board["published_decisions"] == board["fourth_decisions"]).all(), "decision counts differ"
    drift = (board["fourth_wp_left"] - board["wp_left_before"]).abs().max()
    assert drift is not None and drift < 1e-6, f"'before' board does not reproduce coach_tendencies: {drift}"

    def ranked(g: pl.DataFrame) -> pl.DataFrame:
        return g.with_columns(
            per_before=pl.col("wp_left_before") / pl.col("fourth_decisions"),
            per_after=pl.col("wp_left_after") / pl.col("fourth_decisions"),
        ).with_columns(
            rank_before=pl.col("per_before").rank("min").cast(pl.Int32),
            rank_after=pl.col("per_after").rank("min").cast(pl.Int32),
        )

    b25 = ranked(board.filter(pl.col("season") == 2025))
    pooled = ranked(
        board.group_by("coach")
        .agg(
            seasons=pl.len(),
            fourth_decisions=pl.col("fourth_decisions").sum(),
            wp_left_before=pl.col("wp_left_before").sum(),
            wp_left_after=pl.col("wp_left_after").sum(),
        )
        .filter(pl.col("fourth_decisions") >= args.min_pooled_decisions)
    )
    cols25 = ["coach", "pos_team", "fourth_decisions", "per_before", "per_after", "rank_before", "rank_after"]
    colsp = ["coach", "seasons", "fourth_decisions", "per_before", "per_after", "rank_before", "rank_after"]

    states = load_states(args.pbp_root, SEASONS)
    cal, parts = _calibration(states)
    branch = _chosen_branch(f, states, args.pbp_root)
    card = load_model_card("wp_ot_reach")
    sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
    board_n, pooled_n = b25.height, pooled.height
    sections = [
        "# CFB fourth-down bot: regulation impact of the WP overtime correction, 2014-2025\n",
        f"Generated by `tools/validation/cfb_fourth_down_impact.py` at sdv-py `{sha}`; `wp_ot_reach` card trained "
        f"{card['trained_date']} (fit {card['training_seasons']}, holdout {card['holdout_seasons']}). "
        "Before = the published cfbfastR-cfb-data pbp (origin/main's output, see the module docstring); after = the "
        "same decisions re-scored by this branch. Decisions = `football.tendencies`' rule (seasonType 2-3). "
        "`d_diff` = |delta go_wp_diff| and `d_boost` = |delta go_boost|, both in WP points (pp). Confidence tier = "
        "GOP's label on |go_boost|: LOW < 1 <= MEDIUM < 3 <= STRONG < 10 <= VERY STRONG. late_one_score = Q4, "
        f"5:00 or less, |margin| <= 8. Decisions with no recommendation: {unscored[0]} before, {unscored[1]} after "
        "(a null on one side only counts as a flip).\n",
        "## Recommendation flips and confidence-tier changes, per season (season 0 = all)\n",
        _md(per_season),
        "\n### Recommendation, before (rows) x after (columns)\n",
        _md(trans),
        "\n### Confidence tier, before (rows) x after (columns)\n",
        _md(tiers),
        "\n### Flips by the confidence tier before (LOW = |go_boost| < 1 pp: near indifference)\n",
        _md(by_tier),
        "\n### Agreement rate and total WP left (pp), per season\n",
        _md(agree),
        "\n## Where the changes concentrate\n",
        "### Late one-score (Q4, <= 5:00, |margin| <= 8) vs the rest\n",
        _md(late),
        "\n### By quarter\n",
        _md(_dist(f, "quarter")),
        "\n### By score margin at the snap (team with the ball)\n",
        _md(_dist(f, "margin")),
        "\n### By time left\n",
        _md(_dist(f, "time_left")),
        "\n### Q4 flips by time left x margin\n",
        _md(q4_cross),
        "\n## Regulation WP calibration, Q4 one-score scrimmage snaps\n",
        "Before = the published `wp_before` (the `wp_spread` booster); after = `cfb_wp_overtime.adjust_wp` on the "
        "same rows; won = the team with the ball won. 2014-21 is inside the correction's fit window; 2022-25 is "
        "its holdout. `brier_delta` = after - before, with a game-clustered bootstrap 95% interval "
        "(`ci_lo`, `ci_hi`).\n",
        _md(cal),
        "\n### The correction's parts, per cell\n",
        "`q` = mean P(reach overtime) vs `ot_rate` observed; `booster_reg` = the booster on games settled in "
        "regulation (what it estimates) vs `won_reg` there. A leader's WP falls by q x (booster - tie value): "
        "where q and the tie value match, a remaining gap is the booster's own.\n",
        _md(parts),
        "\n## The chosen option's WP against the result (fourth-down decisions)\n",
        "Per decision, the WP of what the team did (go / FG / punt), before vs after, against the game result: "
        "the values the board's WP-left is built from.\n",
        _md(branch),
        f"\n## Coach WP-left board (WP left per decision, pp; rank 1 = least left), 2025 ({board_n} coaches)\n",
        "The 'before' board reproduces the published `coach_tendencies_2025` `fourth_wp_left` exactly.\n",
        "### Least WP left, after\n",
        _md(b25.sort("per_after").head(10).select(cols25)),
        "\n### Most WP left, after\n",
        _md(b25.sort("per_after", descending=True).head(10).select(cols25)),
        "\n### Most WP left, before\n",
        _md(b25.sort("per_before", descending=True).head(10).select(cols25)),
        f"\n## Coach WP-left board, pooled 2014-2025 (>= {args.min_pooled_decisions} decisions, {pooled_n} coaches)\n",
        "### Least WP left, after\n",
        _md(pooled.sort("per_after").head(10).select(colsp)),
        "\n### Most WP left, after\n",
        _md(pooled.sort("per_after", descending=True).head(10).select(colsp)),
        "\n### Most WP left, before\n",
        _md(pooled.sort("per_before", descending=True).head(10).select(colsp)),
        "\n### Largest rank moves (pooled)\n",
        _md(
            pooled.with_columns(move=pl.col("rank_after") - pl.col("rank_before"))
            .sort(pl.col("move").abs(), descending=True)
            .head(10)
            .select(*colsp, "move")
        ),
    ]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(sections) + "\n", encoding="utf-8")
    print(f"wrote {args.out}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("score", "report"):
        p = sub.add_parser(name)
        p.add_argument("--pbp-root", type=Path, default=Path("/mnt/sdv_repos/cfbfastR-cfb-data/cfb"))
        p.add_argument("--data", type=Path, required=True, help="per-season re-scored decisions (parquet)")
    sub.choices["score"].add_argument("--season", type=int, required=True)
    sub.choices["report"].add_argument("--out", type=Path, required=True)
    sub.choices["report"].add_argument("--min-pooled-decisions", type=int, default=300)
    args = ap.parse_args()
    if args.cmd == "score":
        score(args)
    else:
        report(args)


if __name__ == "__main__":
    main()
