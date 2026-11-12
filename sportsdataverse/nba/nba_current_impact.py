"""Current impact: an as-of-date player rating, a Kalman filter on how far a player sits from his box prior.

The season rating (:func:`~sportsdataverse.nba.nba_reml_rapm.nba_reml_rapm`) answers "what did he do this
season". The current rating answers "what do we expect from him on this date", from every game before it:

* **Expectation.** The box prior v2 (:mod:`sportsdataverse.nba.nba_box_prior`), scored on the box lines of
  the :data:`CURRENT_IMPACT_PRIOR_WINDOW_DAYS` before the date (:func:`current_impact_prior`). The window is
  about one season of games at any date, so on opening night the prior is last season's, not empty.
* **Deviation.** Each player's offense and defense deviation from that prior is the state of one joint
  Kalman filter over all players (the RAPM design: a possession's points are the league level plus the
  five offensive and five defensive players' ratings). Each game updates it exactly.
* **Shocks and decay.** A new player's deviation starts at ``N(0, entry_sd^2)``; a new season adds
  ``season_sd^2`` to every returning player's variance and a team change adds ``team_sd^2``; every day adds
  ``drift_sd^2``, so older games weigh less (time decay). The four SDs are fitted by
  ``tools/fit_current_impact.py`` on 28-day forward error, NBA 2007-2013 (entry 0.98, season 0.42 per 100;
  the team-change SD and the drift come out near zero).

With no drift and no shocks, one season and a fixed prior, the filter's mean is the prior-informed ridge
of :func:`~sportsdataverse.nba.nba_reml_rapm.fit_reml_prior_ridge` at the penalty ``noise_var / entry_sd^2``.
"""

from __future__ import annotations

import datetime as dt
import itertools
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_box_prior import BoxPriorModel, apply_box_prior, box_prior_features

#: SD (per 100 possessions) of a new player's deviation from his box prior, on offense and on defense.
#: Fitted by ``tools/fit_current_impact.py`` (28-day forward error, NBA regular seasons 2007-2013; Nelder-Mead
#: stopped at its 80-evaluation cap on a surface flat to 1e-6 in MSE, so the team SD and the drift below are
#: not identified: setting either to 0 changes the 2014-2026 gate error by under 0.03 bp).
CURRENT_IMPACT_ENTRY_SD: float = 0.983
#: SD (per 100) added to every returning player's deviation at a new season. Fitted as above.
CURRENT_IMPACT_SEASON_SD: float = 0.417
#: SD (per 100) added to a player's deviation at a team change (his first game for a new team). Fitted as above.
CURRENT_IMPACT_TEAM_SD: float = 0.026
#: SD (per 100) the deviation drifts per square-root day: the time decay. Fitted as above.
CURRENT_IMPACT_DRIFT_SD: float = 0.0115
#: SD (points per possession) of the league level at a season start. Chosen a priori to be diffuse
#: (a level of 1.10 +- 0.05); one day of games pins it. Not fitted.
CURRENT_IMPACT_LEVEL_SD: float = 0.05
#: Variance of a possession's points used for the first season of the input, when no earlier season gives
#: one. Chosen a priori inside the 1.21 (2005-06) to 1.44 (2025-26) range of NBA regular seasons. Not fitted.
CURRENT_IMPACT_NOISE_VAR_FALLBACK: float = 1.3
#: Days of box lines the as-of-date prior is scored on. Chosen a priori (about one season of games at any
#: date, the span the box prior was trained on). Not fitted.
CURRENT_IMPACT_PRIOR_WINDOW_DAYS: int = 365

_OFF = [f"off_player_{i}" for i in range(1, 6)]
_DEF = [f"def_player_{i}" for i in range(1, 6)]

#: Output columns of :func:`nba_current_impact`, in order.
CURRENT_IMPACT_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "player_id": pl.Int64,
    "as_of": pl.Date,
    "season": pl.Int64,
    "o_current_impact": pl.Float64,
    "d_current_impact": pl.Float64,
    "current_impact": pl.Float64,
    "o_prior": pl.Float64,
    "d_prior": pl.Float64,
    "off_poss": pl.Int64,
    "def_poss": pl.Int64,
    "last_game_date": pl.Date,
}

#: Columns of :func:`current_impact_prior`.
CURRENT_IMPACT_PRIOR_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "player_id": pl.Int64,
    "prior_date": pl.Date,
    "o_prior": pl.Float64,
    "d_prior": pl.Float64,
}


@dataclass(frozen=True)
class CurrentImpactParams:
    """The filter's shock and decay SDs, per 100 possessions (``level_sd`` per possession).

    Attributes:
        entry_sd: SD of a new player's deviation.
        season_sd: SD added at each new season.
        team_sd: SD added at a team change.
        drift_sd: SD of the daily drift, per square-root day.
        level_sd: SD of the league level at a season start.
    """

    entry_sd: float = CURRENT_IMPACT_ENTRY_SD
    season_sd: float = CURRENT_IMPACT_SEASON_SD
    team_sd: float = CURRENT_IMPACT_TEAM_SD
    drift_sd: float = CURRENT_IMPACT_DRIFT_SD
    level_sd: float = CURRENT_IMPACT_LEVEL_SD


def _date(d: object) -> dt.date:
    return d if isinstance(d, dt.date) else dt.date.fromisoformat(str(d)[:10])


def current_impact_prior(
    player_logs: pl.DataFrame,
    team_logs: pl.DataFrame,
    possessions: pl.DataFrame,
    bio: pl.DataFrame,
    model: BoxPriorModel,
    as_of: dt.date | str,
    *,
    window_days: int = CURRENT_IMPACT_PRIOR_WINDOW_DAYS,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """The box prior v2 as of a date: features from the box lines of the ``window_days`` before it.

    Only games dated before ``as_of`` (and on or after ``as_of - window_days``) are read, so the prior holds
    nothing from the date itself or later. The logs and possessions may span two seasons.

    Args:
        player_logs: Per-player-game box lines (``leaguegamelog`` ``P``) with ``game_id`` and ``game_date``.
        team_logs: Per-team-game lines (``leaguegamelog`` ``T``).
        possessions: Possession+lineup rows of the same games (``game_id``, ``points``, the lineups).
        bio: ``player_id``, ``age``, ``height_in``, ``position_num`` for the season of ``as_of``.
        model: The :class:`~sportsdataverse.nba.nba_box_prior.BoxPriorModel` for that season (one that did
            not learn from it: :func:`~sportsdataverse.nba.nba_box_prior.box_prior_training_seasons`).
        as_of: The date the prior is for.
        window_days: Days of box lines to score.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        :data:`CURRENT_IMPACT_PRIOR_SCHEMA`: one row per player on court in the window, ``prior_date`` =
        ``as_of``, ``o_prior`` / ``d_prior`` per 100 (``d`` positive = good defense). Zero rows when the
        window has no games.

    Raises:
        ValueError: If ``game_id`` has a different dtype in the logs and the possessions, or the window has
            games but none of their possessions (a silent empty prior otherwise). A window game without
            possessions is left out of the box lines too, so both come from the same games.

    Example:
        Opening night's prior is last season's box lines::

            from sportsdataverse.nba.nba_current_impact import current_impact_prior
            prior = current_impact_prior(logs_p, logs_t, possessions, bio, model_2025, "2024-10-22")
    """
    day = _date(as_of)
    lo = day - dt.timedelta(days=window_days)
    dates = pl.col("game_date").cast(pl.Utf8).str.slice(0, 10).str.to_date()
    if player_logs.schema["game_id"] != possessions.schema["game_id"]:  # "0021300001" never matches 21300001
        raise ValueError(
            f"game_id dtypes differ: logs {player_logs.schema['game_id']}, possessions {possessions.schema['game_id']}"
        )
    gids = player_logs.filter((dates < day) & (dates >= lo))["game_id"].unique().to_list()
    poss = possessions.filter(pl.col("game_id").is_in(gids))
    if gids and poss.height == 0:
        raise ValueError(f"{len(gids)} games in the window before {day} but no possession matches their game_id")
    if poss.height == 0:
        out = pl.DataFrame({c: pl.Series([], dtype=t) for c, t in CURRENT_IMPACT_PRIOR_SCHEMA.items()})
    else:  # box lines and on-court counts from the same games: drop a window game with no possessions
        matched = poss["game_id"].unique().to_list()
        feats = box_prior_features(player_logs, team_logs, poss, bio, game_ids=matched)
        out = apply_box_prior(feats, model).select(
            "player_id", pl.lit(day, pl.Date).alias("prior_date"), "o_prior", "d_prior"
        )
    return out.to_pandas() if return_as_pandas else out


class CurrentImpactFilter:
    """The joint Kalman filter of :func:`nba_current_impact`, fed one season and one game at a time.

    State, per possession and in the RAPM design's sign: each player's offense deviation, defense
    deviation (negative = good defense) and the league level. Within a season the covariance is joint
    over the season's players; across seasons each player's mean and variance carry (the cross terms do
    not: ponytail, carry the block if a gate ever wants it).

    Args:
        params: The shock and decay SDs.
    """

    def __init__(self, params: CurrentImpactParams | None = None) -> None:
        p = params or CurrentImpactParams()
        self.params = p
        self._entry, self._season, self._team = ((x / 100.0) ** 2 for x in (p.entry_sd, p.season_sd, p.team_sd))
        self._drift = (p.drift_sd / 100.0) ** 2
        self._carry: dict[int, tuple[float, float, float, float]] = {}  # pid -> (m_o, m_d, v_o, v_d)
        self._team_of: dict[int, int] = {}
        self._last: dict[int, dt.date] = {}
        self._prior: dict[int, tuple[float, float]] = {}
        self._level = 1.0
        self.pids: np.ndarray = np.zeros(0, dtype=np.int64)
        self._P = self._L = 0
        self.mu = np.zeros(0)
        self._noise = CURRENT_IMPACT_NOISE_VAR_FALLBACK

    def start_season(self, pids: Sequence[int], noise_var: float | None = None) -> None:
        """Open a season for ``pids``: carried players get ``season_sd^2`` more variance, the level resets."""
        if self.pids.size:
            self.end_season()
        self.pids = np.unique(np.asarray(pids, dtype=np.int64))
        n = self.pids.size
        self._P, self._L = n, 2 * n
        self.m = np.zeros(2 * n + 1)
        self.cov = np.zeros((2 * n + 1, 2 * n + 1))
        self.seen: np.ndarray = np.zeros(n, dtype=bool)
        self.off_poss: np.ndarray = np.zeros(n, dtype=np.int64)
        self.def_poss: np.ndarray = np.zeros(n, dtype=np.int64)
        for i, p in enumerate(self.pids.tolist()):
            if p in self._carry:
                mo, md, vo, vd = self._carry[p]
                self.m[i], self.m[n + i] = mo, md
                self.cov[i, i], self.cov[n + i, n + i] = vo + self._season, vd + self._season
                self.seen[i] = True
        self.m[-1] = self._level
        self.cov[-1, -1] = self.params.level_sd**2
        if noise_var is not None:
            self._noise = float(noise_var)
        self.set_prior(self._prior)

    def end_season(self) -> None:
        """Store each seen player's mean and variance for later seasons."""
        n = self._P
        d = np.diag(self.cov)
        for i in np.flatnonzero(self.seen):
            self._carry[int(self.pids[i])] = (self.m[i], self.m[n + i], d[i], d[n + i])
        self._level = float(self.m[-1])

    def set_prior(self, prior: dict[int, tuple[float, float]]) -> None:
        """Set the expectation ``{pid: (o_prior, d_prior)}`` per 100; absent players get 0 (league average)."""
        self._prior = prior
        o = np.array([prior.get(int(p), (0.0, 0.0))[0] for p in self.pids], dtype=np.float64)
        d = np.array([prior.get(int(p), (0.0, 0.0))[1] for p in self.pids], dtype=np.float64)
        self.mu = np.concatenate([o, -d]) / 100.0

    def drift(self, days: float) -> None:
        """Add ``days`` of drift variance to every player's deviation (the time decay)."""
        if days > 0 and self._P:
            idx = np.arange(self._L)
            self.cov[idx, idx] += self._drift * days

    def update_game(
        self, off: np.ndarray, de: np.ndarray, points: np.ndarray, teams: dict[int, int], day: dt.date
    ) -> None:
        """Update on one game's possessions.

        Args:
            off: ``(n, 5)`` offensive player ids.
            de: ``(n, 5)`` defensive player ids.
            points: ``(n,)`` possession points.
            teams: ``{pid: team_id}`` for every player in the game (for the team-change shock).
            day: The game date.
        """
        n = self._P
        oi = np.searchsorted(self.pids, off)
        di = np.searchsorted(self.pids, de)
        if not (np.array_equal(self.pids[oi], off) and np.array_equal(self.pids[di], de)):
            raise ValueError("a player in this game was not opened by start_season")
        for i in np.unique(np.concatenate([oi.ravel(), di.ravel()])).tolist():
            p, t = int(self.pids[i]), teams.get(int(self.pids[i]))
            if not self.seen[i]:  # league entry: a fresh deviation (no cross terms exist yet)
                self.seen[i] = True
                self.m[i] = self.m[n + i] = 0.0
                self.cov[i, i] = self.cov[n + i, n + i] = self._entry
            elif t is not None and self._team_of.get(p, t) != t:
                self.cov[i, i] += self._team
                self.cov[n + i, n + i] += self._team
            if t is not None:
                self._team_of[p] = t
            self._last[p] = day
        np.add.at(self.off_poss, oi.ravel(), 1)
        np.add.at(self.def_poss, di.ravel(), 1)
        rows: np.ndarray = np.repeat(np.arange(points.size), 5)
        cols = np.unique(np.concatenate([oi.ravel(), n + di.ravel()]))
        J: np.ndarray = np.append(cols, self._L)
        H = np.zeros((points.size, J.size))
        np.add.at(H, (rows, np.searchsorted(cols, oi.ravel())), 1.0)  # add, as the RAPM design does for a
        np.add.at(H, (rows, np.searchsorted(cols, n + di.ravel())), 1.0)  # (rare) repeated id in a lineup
        H[:, -1] = 1.0
        resid = points - self.mu[oi].sum(1) - self.mu[n + di].sum(1)
        A = H.T @ H / self._noise
        b = H.T @ resid / self._noise
        PJ = self.cov[:, J]
        G = np.linalg.solve(np.eye(J.size) + A @ PJ[J], np.eye(J.size))
        self.m += PJ @ (G @ (b - A @ self.m[J]))
        self.cov -= PJ @ (G @ A) @ PJ.T

    def symmetrize(self) -> None:
        """Average the covariance with its transpose (rounding drift; :func:`run_current_impact` calls it daily)."""
        self.cov = 0.5 * (self.cov + self.cov.T)

    def ratings(self) -> dict[int, tuple[float, float, float, float, int, int, dt.date | None]]:
        """Every seen player's ``(o, d, o_prior, d_prior, off_poss, def_poss, last_game)``.

        Ratings and priors per 100 (``d`` positive = good defense): the prior in force plus the deviation.
        A player carried from an earlier season who is not in this one's matrix gets his carried deviation
        plus the prior in force for him (0 when he has none). Possessions count this season's games.
        """
        n = self._P
        out: dict[int, tuple[float, float, float, float, int, int, dt.date | None]] = {}
        for p, (mo, md, _vo, _vd) in self._carry.items():
            po, pd_ = self._prior.get(p, (0.0, 0.0))
            out[p] = (po + 100.0 * mo, pd_ - 100.0 * md, po, pd_, 0, 0, self._last.get(p))
        for i in np.flatnonzero(self.seen[:n]) if n else []:
            p = int(self.pids[i])
            po, pd_ = 100.0 * self.mu[i], -100.0 * self.mu[n + i]
            out[p] = (po + 100.0 * self.m[i], pd_ - 100.0 * self.m[n + i], po, pd_,
                      int(self.off_poss[i]), int(self.def_poss[i]), self._last.get(p))  # fmt: skip
        return out


def _games(possessions: pl.DataFrame) -> pl.DataFrame:
    """Full-lineup possessions with a Date ``game_date``, ordered by date and game."""
    p = possessions.drop_nulls(["points", "game_date", "season", *_OFF, *_DEF])
    return p.with_columns(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("game_date").cast(pl.Utf8).str.slice(0, 10).str.to_date(),
        pl.col("season").cast(pl.Int64),
        pl.col("points").cast(pl.Float64),
    ).sort("game_date", "game_id", maintain_order=True)


def _teams(off: np.ndarray, de: np.ndarray, ot: np.ndarray, dt_: np.ndarray) -> dict[int, int]:
    """``{pid: team_id}`` of one game, from the offense / defense team of the possessions he is on court for."""
    pids = np.concatenate([off.ravel(), de.ravel()])
    teams = np.concatenate([np.repeat(ot, 5), np.repeat(dt_, 5)])
    return dict(zip(pids.tolist(), teams.tolist(), strict=True))


def run_current_impact(
    possessions: pl.DataFrame,
    priors: pl.DataFrame,
    as_of: Sequence[dt.date],
    *,
    params: CurrentImpactParams | None = None,
) -> dict[dt.date, tuple[int, dict[int, tuple[float, float, float, float, int, int, dt.date | None]]]]:
    """One pass of the filter over ``possessions``, snapshotting before each ``as_of`` date.

    Args:
        possessions: Possessions with ``game_id``, ``game_date``, ``season``, ``points``,
            ``offense_team_id``, ``defense_team_id`` and the lineups, any number of seasons.
        priors: :data:`CURRENT_IMPACT_PRIOR_SCHEMA` rows; a game uses the latest ``prior_date`` on or before
            its date (the caller builds each from games before that date: :func:`current_impact_prior`).
        as_of: Snapshot dates; a snapshot holds every game dated strictly before it.
        params: The shock and decay SDs.

    Returns:
        ``{as_of: (season, ratings)}``: ``ratings`` as :meth:`CurrentImpactFilter.ratings`, ``season`` of
        the last game before the date (0 when none).
    """
    filt = CurrentImpactFilter(params)
    games = _games(possessions)
    snaps = sorted({_date(d) for d in as_of})
    pr = priors.with_columns(pl.col("prior_date").cast(pl.Utf8).str.slice(0, 10).str.to_date()).sort("prior_date")
    prior_dates = pr["prior_date"].unique().sort().to_list()
    by_date = {d: {int(r[0]): (float(r[1]), float(r[2])) for r in f.select("player_id", "o_prior", "d_prior").iter_rows()}
               for (d,), f in pr.group_by("prior_date")}  # fmt: skip
    out: dict[dt.date, tuple[int, dict[int, tuple[float, float, float, float, int, int, dt.date | None]]]] = {}
    season, last_day, prior_in_force, si = 0, None, None, 0
    prev_noise: float | None = None

    def prior_at(day: dt.date) -> dt.date | None:
        k = int(np.searchsorted(np.array(prior_dates, dtype="datetime64[D]"), np.datetime64(day), side="right"))
        return prior_dates[k - 1] if k else None

    def snapshot(upto: dt.date) -> None:
        nonlocal si, prior_in_force
        while si < len(snaps) and snaps[si] <= upto:
            pd_ = prior_at(snaps[si])
            if pd_ is not None and pd_ != prior_in_force and filt.pids.size:
                filt.set_prior(by_date[pd_])
                prior_in_force = pd_
            out[snaps[si]] = (season, filt.ratings())
            si += 1

    off = games.select(_OFF).to_numpy().astype(np.int64)
    de = games.select(_DEF).to_numpy().astype(np.int64)
    pts = games["points"].to_numpy()
    ot, dtm = games["offense_team_id"].to_numpy(), games["defense_team_id"].to_numpy()
    seasons = games["season"].to_numpy()
    days = games["game_date"].to_list()
    gid = games["game_id"].to_numpy()
    starts = (np.flatnonzero(np.r_[True, gid[1:] != gid[:-1]]).tolist() if games.height else []) + [games.height]
    for a, b in itertools.pairwise(starts):
        day, s = days[a], int(seasons[a])
        if day != last_day:
            if last_day is not None:
                filt.symmetrize()
            snapshot(day)
            if s != season:
                in_s = seasons == s
                filt.start_season(np.unique(np.concatenate([off[in_s].ravel(), de[in_s].ravel()])), prev_noise)
                v = float(pts[in_s].var(ddof=1)) if in_s.sum() > 1 else float("nan")
                prev_noise = v if np.isfinite(v) and v > 0 else None  # None: the fallback
                season, prior_in_force, last_day = s, None, day
            filt.drift((day - last_day).days)
            last_day = day
            pd_ = prior_at(day)
            if pd_ != prior_in_force:
                filt.set_prior(by_date.get(pd_, {}) if pd_ is not None else {})
                prior_in_force = pd_
        filt.update_game(off[a:b], de[a:b], pts[a:b], _teams(off[a:b], de[a:b], ot[a:b], dtm[a:b]), day)
    snapshot(dt.date.max)
    return out


def nba_current_impact(
    possessions: pl.DataFrame,
    priors: pl.DataFrame,
    as_of: dt.date | str | Sequence[dt.date | str],
    *,
    params: CurrentImpactParams | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Current impact: each player's expected offense / defense impact per 100 as of a date.

    The box prior in force on the date plus the Kalman-filtered deviation from it (see the module
    docstring), from games dated strictly before the date only: possessions dated on or after the latest
    ``as_of`` are dropped before the filter runs, and so are priors dated after it (a prior dated on a date
    is built from games before it and is in force on it).

    Measured by ``tools/fit_current_impact.py`` on NBA regular seasons 2014-2026 (the SDs fitted on
    2007-2013), weekly dates, each scored on the possessions of the next 28 days: it beats the season-to-date
    rating (:func:`~sportsdataverse.nba.nba_reml_rapm.nba_reml_rapm`, fixed penalty, on the season's games
    before the date at the BB-D38 in-season box prior) by 5.53 bp of MSE (season-cluster bootstrap SE 0.35),
    in all 13 seasons: 13.2 bp in a season's first four weeks, 3.3 bp from its thirteenth. Against the
    strongest season-to-date variant (the window prior, penalty tuned on 2007-2013) it is still 1.91 bp better
    (SE 0.20, 13 of 13), so the carried deviation adds beyond the prior. The window prior alone is 5.10 bp
    worse than the filter. The fitted team-change SD and drift barely move the ratings
    (without either the gate error changes by under 0.03 bp; without drift it is 0.03 bp lower).

    Args:
        possessions: Possession+lineup rows with ``game_id``, ``game_date``, ``season``, ``points``,
            ``offense_team_id``, ``defense_team_id``, ``off_player_1..5``, ``def_player_1..5``, from the
            first season the filter should learn from (regular season: the fit used ``002`` games).
        priors: :data:`CURRENT_IMPACT_PRIOR_SCHEMA` rows, e.g. one :func:`current_impact_prior` per week;
            a game uses the latest ``prior_date`` on or before its date.
        as_of: One date or several (one pass of the filter serves all of them).
        params: The shock and decay SDs (default the fitted constants).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        :data:`CURRENT_IMPACT_SCHEMA`, one row per date and player seen before it: ``o_current_impact`` /
        ``d_current_impact`` / ``current_impact`` (per 100, ``d`` positive = good defense,
        ``current_impact = o + d``), the prior in force (``o_prior`` / ``d_prior``), the season-to-date
        possessions (``off_poss`` / ``def_poss``, 0 for a player who has not played this season) and the
        player's last game date. ``season`` is that of the last game before the date.

    Example:
        Ratings on two dates from three seasons of possessions and weekly priors::

            from sportsdataverse.nba import nba_current_impact
            cur = nba_current_impact(possessions, priors, ["2025-01-15", "2025-03-01"])
            print(cur.filter(pl.col("as_of") == pl.date(2025, 3, 1)).sort("current_impact", descending=True).head())
    """
    days = [_date(as_of)] if isinstance(as_of, (str, dt.date)) else [_date(d) for d in as_of]
    if not days:
        raise ValueError("as_of must hold at least one date")
    cut = max(days)
    games = _games(possessions).filter(pl.col("game_date") < cut)
    pr = priors.with_columns(pl.col("prior_date").cast(pl.Utf8).str.slice(0, 10).str.to_date())
    snaps = run_current_impact(games, pr.filter(pl.col("prior_date") <= cut), days, params=params)
    rows = [
        (p, d, season, o, dd, o + dd, po, pd_, op, dp, last)
        for d, (season, r) in snaps.items()
        for p, (o, dd, po, pd_, op, dp, last) in r.items()
    ]
    out = pl.DataFrame(rows, schema=CURRENT_IMPACT_SCHEMA, orient="row").sort("as_of", "player_id")
    return out.to_pandas() if return_as_pandas else out
