"""ESPN-vs-alternate parity harness and pinned gates for the basketball game page (private, experimental).

BB-D15 of the GOP basketball roadmap (spec ``2026-11-03-gop-basketball-impact-lineups-design.md`` §9). An
alternate source joins :data:`~sportsdataverse.basketball.sources.dispatch.SOURCE_ORDER`, and so becomes
selectable in Game on Paper, only once every game of a real captured corpus passes its gates here.

:func:`_compare_game` runs ESPN and the alternate through the dispatch leg a served game takes
(:func:`~sportsdataverse.basketball.sources.dispatch._run_source`: adapter, contract, ``helper_<lg>_pbp``,
:func:`~sportsdataverse.nba.nba_game_page.game_page_frames`) and measures, per game:

* the score after every period and the final (mismatches, so the gate is 0);
* the team box totals (cells that differ);
* player minutes and points on the players both sides carry, and the share of ESPN's players paired;
* the play count ratio, the share of ESPN's play ids the source emits, and the shared plays whose score
  and clock agree row for row;
* the order of the free-throw and technical-foul rows;
* the shot-coordinate error in feet (median and p95) and the share of ESPN's located shots located;
* the Pearson correlation of the home win probability, aligned on play id, with each source's own prior
  and with the prior equalised (the alternate re-scored with ESPN's line and neutral-site flag, through
  :func:`~sportsdataverse.nba.nba_game_page.score_win_prob`).

A source with its own play ids (:data:`OWN_PLAY_IDS`: stats.ncaa.org, BB-D16) has its plays paired with ESPN's by
:func:`_align_play_ids` first, so every play-level metric compares the same plays.

Gates are **derived from observed values and never lowered** (the football harness's rule,
:mod:`sportsdataverse.football.sources.parity`): :data:`GATES` records each bound with the corpus worst
case it came from, :meth:`ParityReport.check` names every breach.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from difflib import SequenceMatcher
from typing import Any

import polars as pl

from sportsdataverse.basketball.sources import dispatch
from sportsdataverse.nba.nba_game_page import score_win_prob

#: Team-box columns compared cell for cell (with every ``*_made`` / ``*_attempted`` / ``*_pct`` /
#: ``*rebounds`` / ``*assists`` / ``*turnovers`` column both sides carry).
_TEAM_BOX_EXTRA = ("team_score", "steals", "blocks", "fouls", "technical_fouls", "turnover_points")
_TEAM_BOX_SUFFIXES = ("_made", "_attempted", "_pct", "rebounds", "assists", "turnovers")
#: ESPN's NBA / WNBA "Free Throw - 1 of 2" and its college "MadeFreeThrow" (no space), and every technical.
_FT_OR_TECH = "(?i)free ?throw|technical"

#: ``metric -> (lower, upper)``; None is unbounded. Derived 2026-11-03 from the BB-D15 corpus (27 real games:
#: 18 NBA 2025-26, 9 WNBA 2026; provenance ``tests/basketball/fixtures/parity/README.md``): each bound is the
#: observed worst case plus the margin stated beside it, at most 0.01 on a share or correlation and 0.05 ft on
#: a distance. Never loosened; tightened when the corpus improves. ``test_sources_parity.py`` fails if a game
#: breaches one, or if a bound drifts further than that margin from the corpus worst case.
LIVE_GATES: dict[str, tuple[float | None, float | None]] = {
    # exact on all 27 games: every period's closing score and the final
    "period_score_mismatches": (None, 0),
    # exact on all 27 games (after the half-up percentage fix; 42.5% is 43 on ESPN, round() gave 42)
    "team_box_mismatches": (None, 0),
    # exact on all 544 players with minutes; every one is paired through the id map
    "player_paired_share": (1.0, None),
    "player_minutes_agreement": (1.0, None),
    "player_points_agreement": (1.0, None),
    # worst 0.9838 (WNBA 401918020: ESPN's replay-review rows liveData lacks); margin 0.0038.
    # Upper: worst 1.0 (NBA 401866759: liveData's split double fouls offset four review rows); margin 0.005
    "play_count_ratio": (0.98, 1.005),
    # worst 0.9838 (WNBA 401918020, the same review rows); margin 0.0038
    "play_id_share": (0.98, None),
    # worst 0.9897 (NBA 401809837: a dunk and a technical free throw at one clock, logged in the other
    # order, so the running score between them differs; every period still closes level); margin 0.0047
    "score_row_agreement": (0.985, None),
    # worst 0.9981 (NBA 401810551: the NBA moved a rebound from 3:27 to 3:29 after ESPN copied it); margin 0.0031
    "clock_row_agreement": (0.995, None),
    # exact on all 27 games: 1,314 free-throw / technical rows in ESPN's relative order
    "ft_tech_order_agreement": (1.0, None),
    # worst 0.9834 (NBA 401810433: 3 of 181); every one of the 18 unlocated shots in the corpus is an
    # end-of-period heave liveData logs with no location; margin 0.0034
    "shot_located_share": (0.98, None),
    # worst median 0.510 ft (NBA 401809788) and p95 0.856 ft (NBA 401866759): ESPN rounds to whole feet,
    # liveData is tenths; margin 0.04 / 0.044 ft
    "shot_error_median_ft": (None, 0.55),
    "shot_error_p95_ft": (None, 0.9),
    # each source's own prior: liveData has no line, so the prior is home court (KNOWN_LOSSY). Worst 0.97329
    # (WNBA 401918020) since #1075's WP refit, which weighs the prior less (0.7905 before); margin 0.0033
    "wp_corr": (0.97, None),
    # ESPN's line and neutral site given to both: worst 0.999705 (NBA 401809837); margin 0.000205
    "wp_corr_equal_prior": (0.9995, None),
}
#: stats.ncaa.org (BB-D16), derived 2026-11-12 from its 16-game corpus (8 MBB + 8 WBB 2026-27 finals; provenance
#: ``tests/basketball/fixtures/ncaa_parity/README.md``) by the same rule: the observed worst case plus the stated
#: margin, never loosened. These pin the adapter against regression; they are NOT the bar for selection. "ncaa"
#: stays out of :data:`~sportsdataverse.basketball.sources.dispatch.SOURCE_ORDER` until the four hard gates are
#: exact on every game (period scores and team box 0, player minutes / points and free-throw order 1.0), which
#: ``test_sources_ncaa.py`` asserts they are not yet.
NCAA_GATES: dict[str, tuple[float | None, float | None]] = {
    # exact on all 16 games: the score columns, put back on the right sides where stats.ncaa.org lists away first
    "period_score_mismatches": (None, 0),
    # worst 4 cells: MBB 401920769 (a basket missing from the plays, an extra team turnover) and WBB 401926100
    # (four free throws missing); 14 of 16 games are exact
    "team_box_mismatches": (None, 4),
    # worst 0.9524 (MBB 401917515: a player the season's roster lacks, so no crosswalk row); margin 0.0024
    "player_paired_share": (0.95, None),
    # worst 0.3333 (WBB 401906979): the pipeline's stint minutes, not the official box's; margin 0.0033
    "player_minutes_agreement": (0.33, None),
    # exact on all 16 games
    "player_points_agreement": (1.0, None),
    # worst 0.9783 (MBB 401909514: rows ESPN carries and stats.ncaa.org does not); margin 0.0033. Upper: worst 0.998; margin 0.007
    "play_count_ratio": (0.975, 1.005),
    # worst 0.9160 (MBB 401917515: the roster-less player's rows carry no athlete id); margin 0.006
    "play_id_share": (0.91, None),
    # worst 0.8806 (MBB 401917515): one second's rows in another order; margin 0.0006
    "score_row_agreement": (0.88, None),
    # only plays one feed re-timed pair across clocks (_align_play_ids pass 3): worst 0.9979 (WBB 401906979);
    # margin 0.0009
    "clock_row_agreement": (0.997, None),
    # worst 0.7308 (MBB 401917515): one second's rows in another order; margin 0.0008
    "ft_tech_order_agreement": (0.73, None),
    # worst 0.9145 (MBB 401917515: the roster-less player's shots carry no athlete id, so they do not pair);
    # margin 0.0045
    "shot_located_share": (0.91, None),
    # worst median 0.5815 ft and p95 0.8735 ft (WBB 401918712 / 401919679): ESPN rounds to whole feet;
    # margin 0.0185 / 0.0265 ft
    "shot_error_median_ft": (None, 0.6),
    "shot_error_p95_ft": (None, 0.9),
    # each source's own prior (no line on stats.ncaa.org): worst 0.9409 (MBB 401909514); margin 0.0009
    "wp_corr": (0.94, None),
    # ESPN's line and neutral site given to both: worst 0.9966 (MBB 401917515); margin 0.0006
    "wp_corr_equal_prior": (0.996, None),
}
#: The gates an alternate must meet exactly, on every corpus game, before it joins ``SOURCE_ORDER``.
HARD_GATES: dict[str, tuple[float | None, float | None]] = {
    "period_score_mismatches": (None, 0),
    "team_box_mismatches": (None, 0),
    # minutes and points are compared on paired players only, so every ESPN player must pair
    "player_paired_share": (1.0, None),
    "player_minutes_agreement": (1.0, None),
    "player_points_agreement": (1.0, None),
    "ft_tech_order_agreement": (1.0, None),
}
GATES: dict[tuple[str, str], dict[str, tuple[float | None, float | None]]] = {
    ("nba", "live"): LIVE_GATES,
    ("wnba", "live"): LIVE_GATES,
    ("mbb", "ncaa"): NCAA_GATES,
    ("wbb", "ncaa"): NCAA_GATES,
}


@dataclass
class ParityReport:
    """Result of :func:`_compare_game` / :func:`_compare_frames` for one game.

    Returns:
        A dataclass with these fields.

        | field | type | description |
        |---|---|---|
        | league | str | ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` |
        | espn_id | int | the ESPN event id |
        | source | str | the alternate source compared against ESPN |
        | n_plays_espn | int | rows of ESPN's game-page ``pbp`` |
        | n_plays_source | int | rows of the source's game-page ``pbp`` |
        | play_count_ratio | float | ``n_plays_source / n_plays_espn`` |
        | play_id_share | float | share of ESPN's play ids the source emits |
        | score_row_agreement | float | shared plays whose ``home_score`` and ``away_score`` both agree |
        | clock_row_agreement | float | shared plays whose ``clock_display_value`` agrees |
        | period_score_mismatches | list[str] | periods (and ``"final"``) whose closing score differs |
        | team_box_mismatches | list[str] | ``team_id:column`` cells of the team box that differ |
        | n_players | int | ESPN athletes with minutes in the box (inactive / DNP rows have none) |
        | player_paired_share | float | share of those the source carries (joined on ``athlete_id``) |
        | player_minutes_agreement | float | paired players whose ``minutes`` agree |
        | player_points_agreement | float | paired players whose ``points`` agree |
        | n_ft_tech | int | ESPN free-throw and technical rows |
        | ft_tech_order_agreement | float | share of those the source carries in ESPN's relative order |
        | n_shots | int | ESPN's located shots |
        | shot_located_share | float | share of those the source also locates |
        | shot_error_median_ft | float | median distance between the two locations, feet |
        | shot_error_p95_ft | float | 95th percentile of that distance, feet |
        | wp_corr | float | Pearson r of ``home_win_prob`` on shared play ids, each source's own prior |
        | wp_corr_equal_prior | float | the same after re-scoring the source with ESPN's line and neutral site |
        | prior | dict | pregame home win probability: ``espn``, ``source`` and ``source_equalised`` (equals ``espn``) |
    """

    league: str
    espn_id: int
    source: str
    n_plays_espn: int = 0
    n_plays_source: int = 0
    play_count_ratio: float = math.nan
    play_id_share: float = math.nan
    score_row_agreement: float = math.nan
    clock_row_agreement: float = math.nan
    period_score_mismatches: list[str] = field(default_factory=list)
    team_box_mismatches: list[str] = field(default_factory=list)
    n_players: int = 0
    player_paired_share: float = math.nan
    player_minutes_agreement: float = math.nan
    player_points_agreement: float = math.nan
    n_ft_tech: int = 0
    ft_tech_order_agreement: float = math.nan
    n_shots: int = 0
    shot_located_share: float = math.nan
    shot_error_median_ft: float = math.nan
    shot_error_p95_ft: float = math.nan
    wp_corr: float = math.nan
    wp_corr_equal_prior: float = math.nan
    prior: dict[str, Any] = field(default_factory=dict)

    def metrics(self) -> dict[str, float]:
        """The gated numbers: every float field, and the two mismatch lists as counts."""
        out = {k: v for k, v in asdict(self).items() if isinstance(v, float)}
        out["period_score_mismatches"] = float(len(self.period_score_mismatches))
        out["team_box_mismatches"] = float(len(self.team_box_mismatches))
        return out

    def check(self, gates: dict[str, tuple[float | None, float | None]] | None = None) -> list[str]:
        """Every gate this game breaches: a value outside its ``(lower, upper)``, or not a finite number.

        Args:
            gates: ``metric -> (lower, upper)``; default :data:`GATES` for ``(league, source)``.

        Returns:
            One message per breach; empty when the game passes.

        Raises:
            KeyError: no gates are pinned for ``(league, source)`` and none were passed.
        """
        gates = GATES[(self.league, self.source)] if gates is None else gates
        observed = self.metrics()
        fails: list[str] = []
        for name, (lo, hi) in gates.items():
            got = observed.get(name)
            # NaN compares False against every bound, so an unmeasured metric would pass forever
            if got is None or not math.isfinite(got):
                fails.append(f"{name} is {got} (not measured)")
            elif (lo is not None and got < lo) or (hi is not None and got > hi):
                fails.append(f"{name} {got:.4f} outside [{lo}, {hi}]")
        # the cells behind a breached count (a count within its bound, as NCAA_GATES allows, is no breach)
        breached = {f.split()[0] for f in fails}
        if self.period_score_mismatches and "period_score_mismatches" in breached:
            fails.append(f"periods: {self.period_score_mismatches}")
        if self.team_box_mismatches and "team_box_mismatches" in breached:
            fails.append(f"team box: {self.team_box_mismatches[:5]}")
        return fails


def _pair(espn: pl.DataFrame, source: pl.DataFrame, on: str) -> pl.DataFrame:
    """Inner join on ``on`` (source columns suffixed ``_src``), one row per key per side, dtypes asserted."""
    if espn.schema[on] != source.schema[on]:
        # one dtype per id, fixed at the boundary: a silent zero-row join would read as a perfect gate
        raise ValueError(f"join key {on!r} dtype differs: ESPN {espn.schema[on]}, source {source.schema[on]}")
    left = espn.drop_nulls(on).unique(subset=on, keep="first", maintain_order=True)
    right = source.drop_nulls(on).unique(subset=on, keep="first", maintain_order=True)
    return left.join(right, on=on, how="inner", suffix="_src")


def _share(frame: pl.DataFrame, expr: pl.Expr) -> float:
    return float(frame.select(expr.fill_null(False).mean()).item()) if frame.height else math.nan


def _period_scores(final: dict, frames: dict) -> dict[str, tuple]:
    by_period = (
        frames["pbp"].group_by("period_number", maintain_order=True).agg(pl.col("home_score", "away_score").last())
    )
    out: dict[str, tuple] = {str(p): (h, a) for p, h, a in by_period.rows()}
    competitors = final["header"]["competitions"][0]["competitors"]
    out["final"] = tuple(sorted((c["homeAway"], str(c["team"]["id"]), str(c["score"])) for c in competitors))
    return out


def _team_box_mismatches(espn: pl.DataFrame, source: pl.DataFrame) -> list[str]:
    j = _pair(espn, source, "team_id")
    if j.height != espn.height:
        return [f"paired {j.height} of {espn.height} team rows"]
    cols = [c for c in espn.columns if c.endswith(_TEAM_BOX_SUFFIXES) or c in _TEAM_BOX_EXTRA]
    out = []
    for c in cols:
        if f"{c}_src" not in j.columns:
            out.append(f"*:{c} missing")
            continue
        bad = j.filter(pl.col(c).cast(pl.Utf8).ne_missing(pl.col(f"{c}_src").cast(pl.Utf8)))
        out += [f"{t}:{c}" for t in bad["team_id"].cast(pl.Utf8)]
    return out


def _order_agreement(espn_pbp: pl.DataFrame, source_pbp: pl.DataFrame) -> tuple[int, float]:
    """ESPN's free-throw / technical play ids, and the share the source carries in the same relative order."""
    ids = espn_pbp.filter(pl.col("type_text").str.contains(_FT_OR_TECH))["id"].to_list()
    wanted = set(ids)
    theirs = [i for i in source_pbp["id"].to_list() if i in wanted]
    if not ids:
        return 0, 1.0
    matched = sum(b.size for b in SequenceMatcher(None, ids, theirs, autojunk=False).get_matching_blocks())
    return len(ids), matched / len(ids)


def _wp_corr(espn_pbp: pl.DataFrame, source_pbp: pl.DataFrame) -> float:
    j = _pair(espn_pbp.select("id", "home_win_prob"), source_pbp.select("id", "home_win_prob"), "id").drop_nulls()
    if j.height < 2:
        return math.nan
    r = j.select(pl.corr("home_win_prob", "home_win_prob_src")).item()
    return float(r) if r is not None else math.nan


def _equal_prior(espn_frames: dict, source_frames: dict, league: str) -> pl.DataFrame:
    """The source's plays re-scored with ESPN's line and neutral-site flag (the prior ESPN's WP started from)."""
    pbp, prior = source_frames["pbp"], espn_frames["prior"]
    pbp = pbp.with_columns(
        pl.lit(prior["home_team_spread"]).cast(pbp.schema["home_team_spread"]).alias("home_team_spread"),
        pl.lit(prior["game_spread_available"]).cast(pbp.schema["game_spread_available"]).alias("game_spread_available"),
    )
    return score_win_prob(
        pbp,
        league=league,
        regulation_periods=source_frames["regulation_periods"],
        neutral_site=espn_frames["neutral_site"],
    )


#: Sources whose play ids are their own, not ESPN's (stats.ncaa.org has no event number): their plays are paired
#: with ESPN's by :func:`_align_play_ids` before any play-level metric is measured.
OWN_PLAY_IDS = frozenset({"ncaa"})

#: What two feeds' plays must share to align: period, team, ESPN type and first athlete (and, in the first two
#: passes of :func:`_align_play_ids`, the clock). Score is left out, so ``score_row_agreement`` still measures it.
_ALIGN_KEY = ("period_number", "team_id", "type_text", "athlete_id_1")


def _align_play_ids(espn_pbp: pl.DataFrame, source_pbp: pl.DataFrame) -> pl.DataFrame:
    """The source's plays with each one that aligns to an ESPN play renamed to that play's ``id``.

    Three passes, exact first, so a repeated key can never claim the wrong ESPN play while its exact twin exists:

    1. a longest-matching-block alignment (:class:`difflib.SequenceMatcher`) of the two play sequences on
       :data:`_ALIGN_KEY` **plus the clock**, which respects order;
    2. each play still unpaired, with the first unpaired ESPN play of the same key at the same clock (a feed
       that logs one game second's rows in another order, as stats.ncaa.org does);
    3. the plays left on both sides, aligned in order on the key alone (a play one feed re-timed), so only
       these pairs can disagree on the clock.

    A play left unaligned keeps its own id, which never collides with an ESPN id.
    """

    def keys(frame: pl.DataFrame) -> list[tuple]:
        return frame.select(pl.col(c).cast(pl.Utf8) for c in (*_ALIGN_KEY, "clock_display_value")).rows()

    ekeys, skeys = keys(espn_pbp), keys(source_pbp)
    renamed, espn_ids = source_pbp["id"].to_list(), espn_pbp["id"].to_list()
    free_e, free_s = list(range(len(ekeys))), list(range(len(skeys)))

    def pair(e: list[int], s: list[int], width: int) -> None:
        a, b = [ekeys[i][:width] for i in e], [skeys[j][:width] for j in s]
        for block in SequenceMatcher(None, a, b, autojunk=False).get_matching_blocks():
            for k in range(block.size):
                renamed[s[block.b + k]] = espn_ids[e[block.a + k]]
                done_e.add(e[block.a + k])
                done_s.add(s[block.b + k])

    done_e: set[int] = set()
    done_s: set[int] = set()
    pair(free_e, free_s, len(_ALIGN_KEY) + 1)
    waiting: dict[tuple, list[int]] = {}
    for i in free_e:
        if i not in done_e:
            waiting.setdefault(ekeys[i], []).append(i)
    for j in free_s:
        if j not in done_s and (queue := waiting.get(skeys[j])):
            i = queue.pop(0)
            renamed[j] = espn_ids[i]
            done_e.add(i)
            done_s.add(j)
    pair([i for i in free_e if i not in done_e], [j for j in free_s if j not in done_s], len(_ALIGN_KEY))
    return source_pbp.with_columns(pl.Series("id", renamed, dtype=source_pbp.schema["id"]))


def _compare_frames(
    league: str, espn_id: int, source: str, espn: tuple[dict, dict], alternate: tuple[dict, dict]
) -> ParityReport:
    """Measure one game from both sides' ``(helper_<lg>_pbp final, game_page_frames)`` pairs.

    Args:
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        espn_id: the ESPN event id.
        source: the alternate source's name (selects its :data:`GATES`).
        espn: ESPN's ``(final, frames)``.
        alternate: the alternate's ``(final, frames)``.

    Returns:
        :class:`ParityReport`.

    Raises:
        ValueError: a join key carries a different dtype on each side.
    """
    ef, efr = espn
    sf, sfr = alternate
    if source in OWN_PLAY_IDS:
        sfr = {**sfr, "pbp": _align_play_ids(efr["pbp"], sfr["pbp"])}
    epbp, spbp = efr["pbp"], sfr["pbp"]
    rep = ParityReport(league, espn_id, source, n_plays_espn=epbp.height, n_plays_source=spbp.height)
    rep.play_count_ratio = spbp.height / epbp.height if epbp.height else math.nan
    rep.play_id_share = _share(epbp, pl.col("id").is_in(spbp["id"].implode()))
    rows = _pair(epbp, spbp, "id")
    rep.score_row_agreement = _share(
        rows,
        pl.col("home_score").eq_missing(pl.col("home_score_src"))
        & pl.col("away_score").eq_missing(pl.col("away_score_src")),
    )
    rep.clock_row_agreement = _share(rows, pl.col("clock_display_value").eq_missing(pl.col("clock_display_value_src")))

    ours, theirs = _period_scores(ef, efr), _period_scores(sf, sfr)
    rep.period_score_mismatches = [k for k in ours if theirs.get(k) != ours[k]] + [k for k in theirs if k not in ours]
    rep.team_box_mismatches = _team_box_mismatches(efr["team_box"], sfr["team_box"])

    # a player ESPN lists with no minutes (inactive, DNP) has no MIN / PTS to compare
    eplayers = efr["player_box"].drop_nulls(["athlete_id", "minutes"])
    pj = _pair(eplayers, sfr["player_box"], "athlete_id")
    rep.n_players = eplayers.height
    rep.player_paired_share = pj.height / eplayers.height if eplayers.height else math.nan
    rep.player_minutes_agreement = _share(pj, pl.col("minutes").eq_missing(pl.col("minutes_src")))
    rep.player_points_agreement = _share(pj, pl.col("points").eq_missing(pl.col("points_src")))

    rep.n_ft_tech, rep.ft_tech_order_agreement = _order_agreement(epbp, spbp)

    located = epbp.filter(pl.col("x_legacy").is_not_null())
    shots = _pair(located.select("id", "x_legacy", "y_legacy"), spbp.select("id", "x_legacy", "y_legacy"), "id")
    shots = shots.drop_nulls()
    rep.n_shots = located.height
    rep.shot_located_share = shots.height / located.height if located.height else math.nan
    if shots.height:
        err = shots.select(
            (
                (pl.col("x_legacy") - pl.col("x_legacy_src")) ** 2 + (pl.col("y_legacy") - pl.col("y_legacy_src")) ** 2
            ).sqrt()
            / 10.0
        ).to_series()
        rep.shot_error_median_ft = float(err.median())
        rep.shot_error_p95_ft = float(err.quantile(0.95, interpolation="linear"))

    rep.wp_corr = _wp_corr(epbp, spbp)
    equalised = _equal_prior(efr, sfr, league)
    rep.wp_corr_equal_prior = _wp_corr(epbp, equalised)
    rep.prior = {
        "espn": efr["prior"]["home_win_prob"],
        "source": sfr["prior"]["home_win_prob"],
        "source_equalised": equalised["pregame_home_prob"][0] if equalised.height else None,
    }
    return rep


def _compare_game(league: str, espn_id: int, payloads: dict[str, Any], *, source: str = "live") -> ParityReport:
    """Run ESPN and ``source`` through dispatch on the same game and measure their parity.

    The alternate does not have to be in :data:`~sportsdataverse.basketball.sources.dispatch.SOURCE_ORDER`:
    deciding whether it may join is this harness's job. Both sides take
    :func:`~sportsdataverse.basketball.sources.dispatch._run_source`, the leg a served game takes.

    Args:
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        espn_id: the ESPN event id.
        payloads: native payloads by source, ``{"espn": raw_summary, source: ...}``; a missing entry fetches.
        source: the alternate source (default ``"live"``, the cdn.nba.com / cdn.wnba.com adapter; ``"ncaa"``
            for stats.ncaa.org on MBB / WBB).

    Returns:
        :class:`ParityReport`; ``.check()`` lists the gates it breaches.

    Raises:
        ValueError: no adapter is registered for ``(league, source)``, or a join key's dtype differs.
        sportsdataverse.basketball.sources.dispatch._SourceFailed: either side's adapter, contract or
            parser failed (a game that cannot be served cannot pass).

    Example:
        Quick start::

            from sportsdataverse.basketball.sources.parity import _compare_game
            report = _compare_game("wnba", 401856986, {})
            report.wp_corr_equal_prior, report.check()
    """
    legs = []
    for src in ("espn", source):
        adapter = dispatch._adapter_for(league, src)
        if adapter is None:
            raise ValueError(f"no adapter registered for ({league!r}, {src!r})")
        _, _, final, frames = dispatch._run_source(league, espn_id, adapter, payloads.get(src))
        legs.append((final, frames))
    return _compare_frames(league, espn_id, source, legs[0], legs[1])
