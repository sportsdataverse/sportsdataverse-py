"""Paper Index: who won each football game on paper, and the season luck it implies.

One share per team in [0, 1] from eight performance margins in their advanced-box
forms -- success rate, explosive-play rate, explosiveness (EPA per successful play),
scoring-opportunity conversion rate, points per opportunity, starting field position
in expected points, havoc, and turnovers -- squashed through an intercept-free
logistic, so a dead-even game reads 50/50. A team's share is its *deserved win*
probability; summed over a season it is :func:`deserved_wins`, and the gap to the
real wins is luck.

Port of Game on Paper's ``python/paper_index.py`` (``team_inputs``,
``share_from_inputs``, ``compute``; game-on-paper-app commit ``770ba849``,
2026-09-15). The weights are fitted per league by
``game-on-paper-app/python/tools/fit_paper_index.py`` (non-negative,
intercept-free logistic on P(home won), real finals from that league's released
``espn_{league}_pbp``) and are copied here VERBATIM at the 4 decimals GOP ships.
The fit's provenance -- season split, gates, holdout metrics, input identities and
the field-position curve fingerprint -- lives in the oracle fixtures it wrote,
copied to ``tests/fixtures/paper_index/paper_index_oracle{,_nfl}.json``; the
tests replay their 24 real holdout games through this module.

Fit spans (from ``SPLITS`` in fit_paper_index.py; :data:`TRAIN_SEASONS`,
:data:`HOLDOUT_SEASONS`):

====== ========== =============== =========== ============ ============= ==============
league fitted     train seasons   train games holdout      holdout games holdout Brier
====== ========== =============== =========== ============ ============= ==============
cfb    2026-09-07 2016-2023       6,637       2024-2025    1,894         0.0657
nfl    2026-09-15 2016-2021       1,615       2022-2025    1,137         0.1174
====== ========== =============== =========== ============ ============= ==============

(The cfb oracle fixture was regenerated 2026-09-15 with the 2026-09-07 weights.)
Holdout Brier is as measured on the pbp each fit read. The NFL release the fit read
still reproduces it (0.1174, every season's game count exact). ``espn_cfb_pbp`` was
rebuilt after the fit (sportsdataverse-py #643-#651): on the rebuilt 2024-2025 files
the same weights score 1,901 games at Brier 0.0727 (2024) and 0.0638 (2025), still
under the fit's 0.09 gate -- the college weights are due a refit on the rebuilt pbp.

A season inside the train span is IN-SAMPLE: its shares were part of the fit.
Holdout seasons were scored out of sample, though not fully clean -- both leagues'
EP models (the EPA, success and explosiveness inputs) were trained on spans that
include the holdout, and the NFL field-position curve was fitted on 2016-2025
drives (its train-only refit moved holdout Brier 0.1174 -> 0.1177). Seasons before
2016 and after 2025 were never seen by the fit (out of span: scored, never
evaluated). Field position is valued in POINTS on each league's bundled
EP-by-yardline curve (:func:`sportsdataverse.cfb.cfb_field_position.load_fp_curve`,
:func:`sportsdataverse.nfl.nfl_field_position.load_nfl_fp_curve`); the weights were
fitted against those exact parquets (sha256 pinned by the tests).

Example:
    Quick start::

        from sportsdataverse.cfb import load_cfb_pbp
        from sportsdataverse.paper_index import PBP_COLUMNS, deserved_wins, paper_index_games

        games = paper_index_games(load_cfb_pbp(2024).select(PBP_COLUMNS), "cfb")
        luck = deserved_wins(games).sort("luck_wins", descending=True)
"""

from __future__ import annotations

import warnings
from typing import Any, Optional, Union

import polars as pl

from sportsdataverse.cfb.cfb_field_position import load_fp_curve
from sportsdataverse.nfl.nfl_field_position import load_nfl_fp_curve
from sportsdataverse.rolling_windows import _ID_DTYPES

__all__ = [
    "DESERVED_WINS_SCHEMA",
    "GAMES_SCHEMA",
    "HOLDOUT_SEASONS",
    "LEAGUE_PTS_PER_OPP",
    "MARGINS",
    "MIN_PLAYS_PER_TEAM",
    "OPP_POINTS_INCLUDE_FG",
    "PBP_COLUMNS",
    "TRAIN_SEASONS",
    "WEIGHTS",
    "deserved_wins",
    "paper_index_game",
    "paper_index_games",
]

# Fitted by game-on-paper-app/python/tools/fit_paper_index.py, copied verbatim from
# GOP python/paper_index.py (770ba849). A margin the active-set fit pins to zero
# would ship as 0.0; neither league pins one today.
# cfb: train 2016-2023 / holdout 2024-2025.
# nfl: train 2016-2021 / holdout 2022-2025 (holdout widened for power after the
# 2024-25 holdout failed the paired EPA-only gate), on espn_nfl_pbp with
# scoring_opp keyed to start.yardsToEndzone (sportsdataverse-py #495), made field
# goals counted in points per opportunity, Pro Bowls dropped.
WEIGHTS: dict[str, dict[str, float]] = {
    "cfb": {
        "success": 23.8447,
        "explosive": 2.8098,
        "explosive_epa": 3.2703,
        "opp_conversion": 3.4280,
        "pts_per_opp": 0.1860,
        "field_position": 4.0046,
        "havoc": 5.9958,
        "turnovers": 0.6005,
    },
    "nfl": {
        "success": 8.0168,
        "explosive": 1.3517,
        "explosive_epa": 1.7687,
        "opp_conversion": 1.9426,
        "pts_per_opp": 0.3468,
        "field_position": 2.5097,
        "havoc": 5.7830,
        "turnovers": 0.4545,
    },
}

#: league average points per scoring opportunity over the train seasons' fitted games:
#: the neutral ``pts_per_opp`` of a team with no opportunity trips (a fitted constant)
LEAGUE_PTS_PER_OPP: dict[str, float] = {"cfb": 3.3603, "nfl": 3.7306}

#: whether a made field goal (a NON-scrimmage row flagged ``scoring_opp``) counts toward
#: points per opportunity; the college weights were fitted without them
OPP_POINTS_INCLUDE_FG: dict[str, bool] = {"cfb": False, "nfl": True}

#: (first, last) train season per league -- seasons the fit saw (in-sample)
TRAIN_SEASONS: dict[str, tuple[int, int]] = {"cfb": (2016, 2023), "nfl": (2016, 2021)}
#: (first, last) holdout season per league -- scored out of sample at fit time
HOLDOUT_SEASONS: dict[str, tuple[int, int]] = {"cfb": (2024, 2025), "nfl": (2022, 2025)}

#: the fit's snap floor: a game where either side ran fewer scrimmage plays is a broken feed
MIN_PLAYS_PER_TEAM = 20
# the trainer's JOIN_KEEP_FLOOR: losing more decided games than this to missing inputs
# is a feed problem, not noise
_KEEP_FLOOR = 0.98
#: ESPN NFL franchise ids (31/32 are the Pro Bowl AFC/NFC squads, which the fit dropped)
_NFL_FRANCHISE_IDS = tuple(range(1, 31)) + (33, 34)

#: the eight margins, in WEIGHTS order; each is a ``{name}_margin`` column (team minus opponent)
MARGINS: tuple[str, ...] = tuple(WEIGHTS["cfb"])

#: released ``espn_{cfb,nfl}_pbp`` columns :func:`paper_index_games` reads (project to these)
PBP_COLUMNS: tuple[str, ...] = (
    "game_id",
    "season",
    "seasonType",
    "week",
    "status_type_completed",
    "game_play_number",
    "homeTeamId",
    "awayTeamId",
    "homeScore",
    "awayScore",
    "pos_team_id",
    "scrimmage_play",
    "EPA",
    "EPA_success",
    "EPA_explosive",
    "havoc",
    "scoring_opp",
    "drive.id",
    "drive.isScore",
    "start.yardsToEndzone",
    "is_pos_team_turnover",
    "pos_score_pts",
    "fg_made",
)
# what one team's inputs need (paper_index_game reads only these)
_INPUT_COLUMNS = ("game_play_number", *PBP_COLUMNS[10:])

_INPUTS = (
    "success_rate",
    "explosive_rate",
    "explosiveness_epa",
    "opp_conversion",
    "pts_per_opp",
    "avg_start_ep",
    "havoc_allowed_rate",
    "turnovers_committed",
)

#: :func:`paper_index_games` columns besides the ids (``game_id`` / ``team_id`` keep the pbp's dtype)
GAMES_SCHEMA: dict[str, Any] = {
    "season": pl.Int64,
    "season_type": pl.Int64,
    "week": pl.Int64,
    "won": pl.Boolean,
    "paper_share": pl.Float64,
    "opp_share": pl.Float64,
    **{f"{m}_margin": pl.Float64 for m in MARGINS},
}

#: :func:`deserved_wins` columns besides ``team_id`` (which keeps the games frame's dtype)
DESERVED_WINS_SCHEMA: dict[str, Any] = {
    "season": pl.Int64,
    "games": pl.Int64,
    "wins": pl.Int64,
    "deserved_wins": pl.Float64,
    "luck_wins": pl.Float64,
    "luck_z": pl.Float64,
}


def _check_league(league: str) -> None:
    # a league without its own fit gets no index, not another league's weights
    if league not in WEIGHTS:
        raise ValueError(f"league must be one of {sorted(WEIGHTS)}, got {league!r}")


def _check_columns(pbp: pl.DataFrame, cols: tuple[str, ...], league: Optional[str] = None) -> None:
    # fg_made is read only by a league whose fit counts made field goals (GOP _needed)
    skip = {"fg_made"} if league is not None and not OPP_POINTS_INCLUDE_FG[league] else set()
    missing = [c for c in cols if c not in pbp.columns and c not in skip]
    if missing:
        raise ValueError(f"pbp is missing columns: {missing}")


def _check_ids(pbp: pl.DataFrame, cols: tuple[str, ...]) -> None:
    for c in cols:
        if pbp.schema[c] not in _ID_DTYPES:
            raise TypeError(f"{c} is {pbp.schema[c]}; ids must be integer or string, never float")


def _ep_curve(league: str) -> pl.DataFrame:
    return load_fp_curve() if league == "cfb" else load_nfl_fp_curve()


def _team_inputs(pbp: pl.DataFrame, league: str, keys: list[str]) -> pl.DataFrame:
    """The eight model inputs (+ snaps) per ``keys`` group of offensive plays.

    GOP ``team_inputs``, vectorized the way fit_paper_index.py's
    ``season_ext_rows`` does it: rates over scrimmage snaps; drive starts and
    opportunity trips over the snaps' drives (EP of the start on the league's
    curve); opportunity points over the WHOLE frame through the league's mask (a
    made field goal is a non-scrimmage row). A group with no drive ids or no
    successful snap has no row (GOP returns None for it), as does one with a null
    or non-finite input.
    """
    snaps = pbp.filter(pl.col("scrimmage_play") == True)  # noqa: E712
    rates = snaps.group_by(keys).agg(
        plays=pl.len(),
        success_rate=(pl.col("EPA_success") == True).mean(),  # noqa: E712
        explosive_rate=(pl.col("EPA_explosive") == True).mean(),  # noqa: E712
        explosiveness_epa=pl.col("EPA").filter(pl.col("EPA_success") == True).mean(),  # noqa: E712
        havoc_allowed_rate=(pl.col("havoc") == True).mean(),  # noqa: E712
        turnovers_committed=(pl.col("is_pos_team_turnover") == True).sum().cast(pl.Float64),  # noqa: E712
    )
    drives = (
        snaps.filter(pl.col("drive.id").is_not_null())
        .group_by([*keys, "drive.id"])
        .agg(
            opp=pl.col("scoring_opp").any(),
            scored=pl.col("drive.isScore").any(),
            # the drive's first snap by play order, not by frame row order
            start_yte=pl.col("start.yardsToEndzone").sort_by("game_play_number").first(),
        )
        .with_columns(yardline_own=(100 - pl.col("start_yte")).cast(pl.Int64).clip(1, 99))
        .join(_ep_curve(league), on="yardline_own", how="left")
        .group_by(keys)
        .agg(
            opp_trips=pl.col("opp").sum().cast(pl.Float64),
            opp_converted=(pl.col("opp") & pl.col("scored")).sum().cast(pl.Float64),
            avg_start_yards_to_endzone=pl.col("start_yte").mean(),
            avg_start_ep=pl.col("ep").mean(),
        )
    )
    on_snap = pl.col("scrimmage_play") == True  # noqa: E712
    if OPP_POINTS_INCLUDE_FG[league]:
        on_snap = on_snap | (pl.col("fg_made") == True)  # noqa: E712
    opp_points = (
        pbp.filter((pl.col("scoring_opp") == True) & on_snap)  # noqa: E712
        .group_by(keys)
        .agg(opp_points=pl.col("pos_score_pts").fill_null(0).sum().cast(pl.Float64))
    )
    trips = pl.col("opp_trips")
    return (
        rates.join(drives, on=keys, how="inner")
        .join(opp_points, on=keys, how="left")
        .with_columns(
            # no opportunities is neutral finishing, not zero-percent finishing
            opp_conversion=pl.when(trips > 0).then(pl.col("opp_converted") / trips).otherwise(0.5),
            pts_per_opp=pl.when(trips > 0)
            .then(pl.col("opp_points").fill_null(0) / trips)
            .otherwise(LEAGUE_PTS_PER_OPP[league]),
        )
        .drop_nulls(list(_INPUTS))
        .filter(pl.all_horizontal(pl.col(c).is_finite() for c in _INPUTS))
        .select(*keys, "plays", *_INPUTS, "avg_start_yards_to_endzone")
    )


def _score(paired: pl.DataFrame, league: str) -> pl.DataFrame:
    """Margins (team minus opponent) and both shares from a frame of team inputs
    beside the opponent's (``*_opp``) -- GOP ``share_from_inputs``."""

    def diff(col: str) -> pl.Expr:
        return pl.col(col) - pl.col(f"{col}_opp")

    margins = {
        "success_margin": diff("success_rate"),
        "explosive_margin": diff("explosive_rate"),
        "explosive_epa_margin": diff("explosiveness_epa"),
        "opp_conversion_margin": diff("opp_conversion"),
        "pts_per_opp_margin": diff("pts_per_opp"),
        # field position in POINTS: EP of the average drive start; higher is better
        "field_position_margin": diff("avg_start_ep"),
        # havoc the team's defense created = havoc allowed on the opponent's snaps
        "havoc_margin": -diff("havoc_allowed_rate"),
        "turnovers_margin": -diff("turnovers_committed"),
    }
    weights = WEIGHTS[league]
    # a plain sum, so a null margin gives a null share (sum_horizontal would read it as 0)
    z = sum((pl.col(f"{m}_margin") * weights[m] for m in MARGINS), pl.lit(0.0))
    return (
        paired.with_columns(**margins)
        .with_columns(_z=z)
        .with_columns(
            paper_share=1.0 / (1.0 + (-pl.col("_z")).exp()),
            opp_share=1.0 / (1.0 + pl.col("_z").exp()),
        )
        .drop("_z")
    )


def paper_index_game(
    pbp: pl.DataFrame,
    home_id: Union[int, str],
    away_id: Union[int, str],
    league: str,
) -> Optional[dict[str, Any]]:
    """Paper Index of one game: each side's deserved-win share and the eight margins.

    The served path, as Game on Paper's ``paper_index.compute``: no snap floor and
    no tie rule, just the model applied to the plays given.

    Args:
        pbp: one game's released ``espn_{cfb,nfl}_pbp`` plays: ``game_play_number``
            and the :data:`PBP_COLUMNS` from ``pos_team_id`` on (``fg_made`` only for
            the NFL; ``game_id`` optional).
        home_id: the home team's id as ``pos_team_id`` carries it (int or str).
        away_id: the away team's id.
        league: ``"cfb"`` or ``"nfl"`` -- picks the fitted weights, the field-position
            curve and the field-goal rule.

    Returns:
        dict | None: ``{"home_share": float, "away_share": float, "margins": {name:
        float}}`` with the eight :data:`MARGINS` from the home side (home minus away;
        havoc and turnovers signed so positive favors home). ``away_share`` is
        ``1 - home_share``. None when either side has no usable inputs (no
        scrimmage snap, no drive id, or no successful play).

    Raises:
        ValueError: unknown ``league``, missing columns, plays from more than one game,
            or ``home_id`` and ``away_id`` naming the same team.
        TypeError: ``pos_team_id`` is float.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.cfb import load_cfb_pbp
            from sportsdataverse.paper_index import paper_index_game

            pbp = load_cfb_pbp(2024).filter(pl.col("game_id") == 401628337)
            out = paper_index_game(pbp, 2, 25, "cfb")  # Auburn (home) vs California
            out["home_share"], out["margins"]["success"]

        See Also:
            * :func:`paper_index_games` -- every completed game of a pbp frame, one row per team.
            * `Game on Paper`_ -- the live app this model serves.

        .. _Game on Paper: https://gameonpaper.com
    """
    _check_league(league)
    _check_columns(pbp, _INPUT_COLUMNS, league)
    _check_ids(pbp, ("pos_team_id",))
    if "game_id" in pbp.columns and pbp["game_id"].n_unique() > 1:
        raise ValueError("pbp holds more than one game_id; use paper_index_games for a batch")
    if str(home_id) == str(away_id):
        # a team scored against itself is a clean 50/50 with zero margins: a wrong answer, not an error
        raise ValueError(f"home_id and away_id are the same team ({home_id!r})")
    inputs = _team_inputs(pbp, league, ["pos_team_id"])
    key = pl.col("pos_team_id").cast(pl.Utf8)
    home = inputs.filter(key == str(home_id))
    away = inputs.filter(key == str(away_id))
    if home.height != 1 or away.height != 1:
        return None
    row = _score(pl.concat([home, away.rename(lambda c: f"{c}_opp")], how="horizontal"), league).row(0, named=True)
    return {
        "home_share": row["paper_share"],
        "away_share": row["opp_share"],
        "margins": {m: row[f"{m}_margin"] for m in MARGINS},
    }


def paper_index_games(pbp: pl.DataFrame, league: str) -> pl.DataFrame:
    """Paper Index of every completed game in a pbp frame, one row per team per game.

    A game counts when it passes the trainer's filters (fit_paper_index.py
    ``season_game_rows``): it has a winner (ties dropped), both sides ran at least
    :data:`MIN_PLAYS_PER_TEAM` scrimmage snaps, and (NFL) both teams are franchises
    (the Pro Bowl dropped). The port adds two checks of its own: the game's last
    play is marked completed (``status_type_completed``), and both sides' eight
    inputs are computable and finite (where the trainer required a finite mean EPA).
    Final scores and the home/away ids are the last play's (by ``game_play_number``).
    On the NFL seasons the fit read, this selects exactly the fit's games.

    Args:
        pbp: released ``espn_{cfb,nfl}_pbp`` plays, any number of games and seasons;
            project to :data:`PBP_COLUMNS` first (``load_cfb_pbp(s).select(PBP_COLUMNS)``).
        league: ``"cfb"`` or ``"nfl"``.

    Returns:
        pl.DataFrame: keyed by ``(game_id, team_id)``, sorted by them; ``game_id`` and
        ``team_id`` keep the pbp's ``game_id`` / ``pos_team_id`` dtype (never through
        float), the rest is :data:`GAMES_SCHEMA`:

        * ``season``, ``season_type`` (ESPN ``seasonType``: 2 regular, 3 post), ``week``.
        * ``won``: the team outscored its opponent.
        * ``paper_share``: the team's deserved-win probability; ``opp_share`` the
          opponent's (they sum to 1).
        * eight ``{margin}_margin`` columns, team minus opponent, positive favors the team.

        An empty frame with the same columns when no game qualifies.

    Warns:
        UserWarning: inputs were computable for fewer than 98% of the completed,
            decided games (the trainer's join keep-floor) -- a feed problem would
            otherwise shrink every team's ``games`` and still give believable luck.

    Raises:
        ValueError: unknown ``league`` or missing columns.
        TypeError: a float id column, or ``homeTeamId`` / ``awayTeamId`` typed unlike
            ``pos_team_id`` (the join would silently miss).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.paper_index import PBP_COLUMNS, paper_index_games

            url = (
                "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
                "espn_nfl_pbp/play_by_play_2024.parquet"
            )
            games = paper_index_games(pl.read_parquet(url, columns=list(PBP_COLUMNS), use_pyarrow=True), "nfl")

        Pipeline next step (one line)::

            games.filter(~pl.col("won") & (pl.col("paper_share") > 0.8))  # deserved to win, lost

        See Also:
            * :func:`deserved_wins` -- the season roll-up (deserved wins, luck).
            * :func:`paper_index_game` -- one game's shares and margins.
    """
    _check_league(league)
    _check_columns(pbp, PBP_COLUMNS, league)
    _check_ids(pbp, ("game_id", "pos_team_id", "homeTeamId", "awayTeamId"))
    for c in ("homeTeamId", "awayTeamId"):
        if pbp.schema[c] != pbp.schema["pos_team_id"]:
            raise TypeError(f"{c} is {pbp.schema[c]} but pos_team_id is {pbp.schema['pos_team_id']}")
    keys = ["game_id", "pos_team_id"]
    finals = (
        pbp.sort("game_play_number")
        .group_by("game_id")
        .agg(
            pl.col("season").last(),
            pl.col("seasonType").last().alias("season_type"),
            pl.col("week").last(),
            pl.col("status_type_completed").last().alias("completed"),
            pl.col("homeTeamId").last().alias("home_id"),
            pl.col("awayTeamId").last().alias("away_id"),
            pl.col("homeScore").last().alias("home_score"),
            pl.col("awayScore").last().alias("away_score"),
        )
        .filter((pl.col("completed") == True) & (pl.col("home_score") != pl.col("away_score")))  # noqa: E712
    )
    if league == "nfl":
        ids = pl.Series(_NFL_FRANCHISE_IDS).cast(pbp.schema["pos_team_id"]).to_list()
        finals = finals.filter(pl.col("home_id").is_in(ids) & pl.col("away_id").is_in(ids))
    home_view = finals.select(
        "game_id", "season", "season_type", "week",
        pl.col("home_id").alias("team_id"), pl.col("away_id").alias("opp_id"),
        (pl.col("home_score") > pl.col("away_score")).alias("won"),
    )  # fmt: skip
    away_view = finals.select(
        "game_id", "season", "season_type", "week",
        pl.col("away_id").alias("team_id"), pl.col("home_id").alias("opp_id"),
        (pl.col("away_score") > pl.col("home_score")).alias("won"),
    )  # fmt: skip
    inputs = _team_inputs(pbp, league, keys).rename({"pos_team_id": "team_id"})
    opp_inputs = inputs.rename(lambda c: c if c == "game_id" else f"{c}_opp")
    paired = (
        pl.concat([home_view, away_view])
        .join(inputs, on=["game_id", "team_id"], how="inner")
        .join(opp_inputs, left_on=["game_id", "opp_id"], right_on=["game_id", "team_id_opp"], how="inner")
    )
    scored = paired["game_id"].n_unique()
    if scored < _KEEP_FLOOR * finals.height:
        warnings.warn(
            f"paper_index_games: inputs computable for {scored} of {finals.height} completed, decided "
            "games; a drop this large is a pbp feed problem (missing drive ids, EPA or snaps)",
            UserWarning,
            stacklevel=2,
        )
    paired = paired.filter((pl.col("plays") >= MIN_PLAYS_PER_TEAM) & (pl.col("plays_opp") >= MIN_PLAYS_PER_TEAM))
    ids = {"game_id": pbp.schema["game_id"], "team_id": pbp.schema["pos_team_id"]}
    return _score(paired, league).select(*ids, *GAMES_SCHEMA).cast({**ids, **GAMES_SCHEMA}).sort("game_id", "team_id")


def deserved_wins(games: pl.DataFrame) -> pl.DataFrame:
    """Season deserved wins and luck per team from :func:`paper_index_games` rows.

    With ``p_i`` the team's ``paper_share`` in game ``i`` of the season and ``W`` its
    real wins: ``deserved_wins = sum(p_i)``, ``luck_wins = W - deserved_wins`` and
    ``luck_z = luck_wins / sqrt(sum(p_i * (1 - p_i)))``. Under the model a season's
    wins are a sum of independent Bernoulli(``p_i``) draws, so ``luck_z`` is luck in
    standard deviations; it is null when that variance is 0 (every share exactly 0
    or 1).

    Args:
        games: :func:`paper_index_games` output (needs ``season``, ``team_id``,
            ``won``, ``paper_share``), any number of seasons of ONE league: ESPN team
            ids overlap across leagues (2 is Auburn in college and Buffalo in the NFL).

    Returns:
        pl.DataFrame: one row per ``(season, team_id)``, sorted by them: ``games``,
        ``wins``, ``deserved_wins``, ``luck_wins``, ``luck_z``
        (:data:`DESERVED_WINS_SCHEMA`; ``team_id`` keeps its dtype). An empty frame
        with the same columns when ``games`` is empty.

        Read with care:

        * ``games`` / ``wins`` count only the games :func:`paper_index_games` scored,
          so they can differ from the official record: ties and snap-floor games are
          out (NFL ties: 1 in 2021, 2 in 2022), postseason games are in.
        * Luck includes home field: the share has no intercept, and home teams beat
          their shares in every 2022-2025 season (+0.013 to +0.047 wins per home game;
          fit holdouts: college 1,204 home wins vs 1,171 deserved, NFL 628 vs 596).
          A team's ``luck_wins`` is biased by about that rate x (home - away games),
          and the nominal home side at a neutral site gets it too.
        * Small samples: college FCS opponents appear with 1-2 games; filter on a
          minimum ``games`` before ranking. Over 2022-2025 the spread of ``luck_z``
          for teams with 8+ games was 1.03-1.11 (college) and 0.85-1.28 (NFL).

    Raises:
        ValueError: ``games`` is missing a needed column.

    Example:
        Quick start::

            from sportsdataverse.cfb import load_cfb_pbp
            from sportsdataverse.paper_index import PBP_COLUMNS, deserved_wins, paper_index_games

            luck = deserved_wins(paper_index_games(load_cfb_pbp(2024).select(PBP_COLUMNS), "cfb"))
            luck.sort("luck_z", descending=True).head(10)  # the season's luckiest teams

        See Also:
            * :func:`paper_index_games` -- the per-game shares this rolls up.
            * :data:`TRAIN_SEASONS` -- seasons inside the fit are in-sample.
    """
    _check_columns(games, ("season", "team_id", "won", "paper_share"))
    p = pl.col("paper_share")
    out = (
        games.group_by("season", "team_id")
        .agg(
            games=pl.len(),
            wins=(pl.col("won") == True).sum(),  # noqa: E712
            deserved_wins=p.sum(),
            _var=(p * (1.0 - p)).sum(),
        )
        .with_columns(luck_wins=pl.col("wins") - pl.col("deserved_wins"))
        .with_columns(
            luck_z=pl.when(pl.col("_var") > 0).then(pl.col("luck_wins") / pl.col("_var").sqrt()),
        )
    )
    return (
        out.select("season", "team_id", *list(DESERVED_WINS_SCHEMA)[1:])
        .cast(DESERVED_WINS_SCHEMA)
        .sort("season", "team_id")
    )
