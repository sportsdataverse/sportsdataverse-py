# sportsdataverse/nba/nba_game_lines.py
"""Betting lines, no-vig market win probability, player props and WP variants for basketball games (BB-D33).

The odds come from the SportsDataverse odds store (the ``odds`` schema: ``lines_checkpoints``,
``props_checkpoints``, ``espn_crosswalk`` and each league's ``props_player_xwalk``), one row per
event x book x market x outcome with the book's first (``open_*``) and last pre-tip (``close_*``)
quote. These functions join them to ESPN games; they read nothing themselves.

* :func:`game_lines` -- per ESPN game: opening and closing spread, total and moneylines, the no-vig
  market win probability, and (completed games) the result against the spread and the total.
* :func:`player_props` -- per ESPN game x athlete x stat: the closing line, prices and no-vig over
  probability.
* :func:`wp_variants` -- two extra per-play home WP columns from
  :func:`~sportsdataverse.nba.nba_game_page.score_win_prob` with a different prior: naive (0.5, score
  and clock only) and market (the closing no-vig market WP).

Consensus across books is the median: of the line (spread, total), of the decimal price (moneylines,
reported back in American odds) and of each book's no-vig probability. A book's no-vig probability
is its two raw implied probabilities scaled to sum to 1 (the multiplicative method,
:func:`~sportsdataverse.wexp.market.devig_multiplicative`); since each book's pair sums to 1, so do
the medians. Exchanges (``betfair``, ``polymarket``) and a book whose two prices imply an overround
outside [1, :data:`MAX_OVERROUND`] are left out of the probability; a closing quote taken after tip
(``close_mins_before < 0``) is left out of every closing column.

Spreads are in the betting convention: ``spread_home_close`` is negative when the home side is
favoured (it is minus the expected home margin, the pbp frames' ``home_team_spread``). The odds
event's home side is the listed one; an ESPN crosswalk row matched with the teams swapped
(``match_method`` ``flipped`` / ``contains_flip``, common at neutral sites) is turned round, so every
``_home`` column is ESPN's home team.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_game_page import _resolve_league, score_win_prob

#: The odds store's sport key per league. ``basketball_wncaab`` (women's college) has lines from
#: the 2023-24 season (odds year 2024) and no player props.
ODDS_SPORT_KEYS: dict[str, str] = {
    "nba": "basketball_nba",
    "wnba": "basketball_wnba",
    "mbb": "basketball_ncaab",
    "wbb": "basketball_wncaab",
}
#: Exchanges quote back / lay prices with no bookmaker margin; they are not sportsbook lines.
EXCHANGES: tuple[str, ...] = ("betfair", "polymarket")
#: The largest raw implied-probability sum a book's two-way price may carry before it is treated as
#: a broken quote (US books run about 1.02-1.06 on game lines, 1.06-1.12 on props).
MAX_OVERROUND = 1.15
#: Prop market -> ``stat`` in :func:`player_props`.
PROP_MARKETS: dict[str, str] = {
    "player_points": "points",
    "player_rebounds": "rebounds",
    "player_assists": "assists",
    "player_threes": "threes",
    "player_points_rebounds_assists": "pra",
}
_FLIPPED = ("flipped", "contains_flip")

GAME_LINES_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int32(),
    "season": pl.Int32(),
    "event_id": pl.String(),
    "n_books": pl.UInt32(),
    "spread_home_open": pl.Float64(),
    "spread_home_close": pl.Float64(),
    "total_open": pl.Float64(),
    "total_close": pl.Float64(),
    "ml_home_open": pl.Float64(),
    "ml_away_open": pl.Float64(),
    "ml_home_close": pl.Float64(),
    "ml_away_close": pl.Float64(),
    "market_home_wp_open": pl.Float64(),
    "market_away_wp_open": pl.Float64(),
    "market_home_wp_close": pl.Float64(),
    "market_away_wp_close": pl.Float64(),
    "home_margin": pl.Int32(),
    "ats_result": pl.String(),
    "total_result": pl.String(),
}
PLAYER_PROPS_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int32(),
    "season": pl.Int32(),
    "athlete_id": pl.Int32(),
    "team_id": pl.Int32(),
    "player_name": pl.String(),
    "stat": pl.String(),
    "line": pl.Float64(),
    "n_books": pl.UInt32(),
    "over_price": pl.Float64(),
    "under_price": pl.Float64(),
    "over_prob": pl.Float64(),
}


def _decimal(american: pl.Expr) -> pl.Expr:
    return pl.when(american < 0).then(1 - 100 / american).otherwise(1 + american / 100)


def _american(decimal: pl.Expr) -> pl.Expr:
    return pl.when(decimal >= 2).then((decimal - 1) * 100).otherwise(-100 / (decimal - 1))


def _devig(a: pl.Expr, b: pl.Expr) -> pl.Expr:
    """The no-vig probability of side ``a`` from two American prices; null when the pair is not a sane two-way quote."""
    pa, pb = 1 / _decimal(a), 1 / _decimal(b)
    over = pa + pb
    return pl.when((over >= 1) & (over <= MAX_OVERROUND)).then(pa / over)


def _games(crosswalk: pl.DataFrame, schedule: pl.DataFrame, sport_key: str) -> pl.DataFrame:
    """Crosswalk rows of the league's events joined to the schedule: event_id, game_id, season, flipped, home_id, away_id."""
    xw = crosswalk.filter(pl.col("sport_key") == sport_key, pl.col("espn_game_id").is_not_null()).select(
        "event_id",
        pl.col("espn_game_id").cast(pl.Int64).cast(pl.Int32).alias("game_id"),
        pl.col("match_method").is_in(_FLIPPED).alias("flipped"),
    )
    sched = schedule.select(
        pl.col("game_id").cast(pl.Int32),
        pl.col("season").cast(pl.Int32),
        *[pl.col(c).cast(pl.Int32) for c in ("home_id", "away_id") if c in schedule.columns],
    )
    assert xw.schema["game_id"] == sched.schema["game_id"]
    return xw.join(sched, on="game_id", how="inner")


def _sided(odds: pl.DataFrame, games: pl.DataFrame) -> pl.DataFrame:
    """Sportsbook game-line quotes on ESPN games, ``side`` home / away / over / under from ESPN's point of view."""
    rows = odds.filter(
        pl.col("market_key").is_in(["h2h", "spreads", "totals"]), ~pl.col("bookmaker_key").is_in(EXCHANGES)
    ).join(games.select("event_id", "game_id", "season", "flipped"), on="event_id", how="inner")
    listed = (
        pl.when(pl.col("outcome_name") == pl.col("home_team"))
        .then(pl.lit("home"))
        .when(pl.col("outcome_name") == pl.col("away_team"))
        .then(pl.lit("away"))
        .when(pl.col("outcome_name") == "Over")
        .then(pl.lit("over"))
        .when(pl.col("outcome_name") == "Under")
        .then(pl.lit("under"))
    )
    swap = pl.when(listed == "home").then(pl.lit("away")).when(listed == "away").then(pl.lit("home")).otherwise(listed)
    closed = pl.col("close_mins_before") >= 0
    return rows.with_columns(
        pl.when(pl.col("flipped")).then(swap).otherwise(listed).alias("side"),
        pl.when(closed).then(pl.col("close_price")).alias("close_price"),
        pl.when(closed).then(pl.col("close_point")).alias("close_point"),
    ).filter(pl.col("side").is_not_null())


def game_lines(
    lines_checkpoints: pl.DataFrame,
    crosswalk: pl.DataFrame,
    schedule: pl.DataFrame,
    *,
    league: str | None = None,
) -> pl.DataFrame:
    """Per ESPN game: opening and closing spread, total and moneylines, the no-vig market WP, and the result.

    Args:
        lines_checkpoints: The odds store's ``lines_checkpoints`` rows (any sports and years: rows of
            other sports are ignored). Needs ``event_id``, ``sport_key``, ``home_team``, ``away_team``,
            ``bookmaker_key``, ``market_key``, ``outcome_name``, ``open_price`` / ``open_point``,
            ``close_price`` / ``close_point`` and ``close_mins_before``.
        crosswalk: The odds store's ``espn_crosswalk`` (``sport_key``, ``event_id``, ``espn_game_id``,
            ``match_method``).
        schedule: The league's ESPN schedule (``load_<league>_schedule``): ``game_id``, ``season``,
            ``home_score``, ``away_score``, ``status_type_completed``. Only its games are returned, with
            its ``season``.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).

    Returns:
        One row per game with a line, :data:`GAME_LINES_SCHEMA`: ``game_id``, ``season``, the odds
        ``event_id`` (the one with the most quotes when the crosswalk maps two events to a game),
        ``n_books`` (books with a closing quote), ``spread_home_open`` / ``_close`` (betting
        convention, negative when home is favoured), ``total_open`` / ``_close``, ``ml_home`` /
        ``ml_away`` ``_open`` / ``_close`` (American), ``market_home_wp`` / ``market_away_wp``
        ``_open`` / ``_close`` (no vig, the pair sums to 1), and for completed games ``home_margin``,
        ``ats_result`` (``"home"`` / ``"away"`` / ``"push"``: who covered the closing spread) and
        ``total_result`` (``"over"`` / ``"under"`` / ``"push"`` against the closing total). A column
        the books did not quote is null; an empty frame with those columns when nothing matches.

    Raises:
        ValueError: If the league is unknown.
        polars.exceptions.ColumnNotFoundError: If a required column is missing.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_game_lines import game_lines
            from sportsdataverse.nba.nba_loaders import load_nba_schedule
            odds = "odds-data/odds"  # a local checkout of the odds store
            lines = game_lines(
                pl.read_parquet(f"{odds}/lines_checkpoints_202[45].parquet"),
                pl.read_parquet(f"{odds}/espn_crosswalk.parquet"),
                load_nba_schedule([2025]),
                league="nba",
            )

        Against the spread::

            lines.group_by("ats_result").len()

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college basketball
            * `wehoop`_ -- the R companion for WNBA and women's college basketball

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, None)
    games = _games(crosswalk, schedule, ODDS_SPORT_KEYS[lg])
    rows = _sided(lines_checkpoints.filter(pl.col("sport_key") == ODDS_SPORT_KEYS[lg]), games)
    if rows.is_empty():
        return pl.DataFrame(schema=GAME_LINES_SCHEMA)
    # one game per event; a game two events map to keeps the one with the most quotes
    event = (
        rows.group_by("game_id", "event_id")
        .len()
        .sort(["game_id", "len", "event_id"], descending=[False, True, False])
        .unique("game_id", keep="first", maintain_order=True)
        .select("game_id", "event_id")
    )
    rows = rows.join(event, on=["game_id", "event_id"], how="semi")
    book = ["game_id", "event_id", "season", "bookmaker_key"]
    per_book = (
        rows.group_by(book)
        .agg(
            *[
                pl.col(f"{t}_{v}").filter(pl.col("market_key") == m, pl.col("side") == s).first().alias(f"{m}_{s}_{t}")
                for m, sides in (("h2h", ("home", "away")), ("spreads", ("home",)), ("totals", ("over", "under")))
                for s in sides
                for t in ("open", "close")
                for v in (("price",) if m == "h2h" else ("point",))
            ],
        )
        .with_columns(
            *[_devig(pl.col(f"h2h_home_{t}"), pl.col(f"h2h_away_{t}")).alias(f"wp_{t}") for t in ("open", "close")],
            pl.any_horizontal(pl.col("^(h2h|spreads|totals)_.*_close$").is_not_null()).alias("closed"),
        )
    )
    out = per_book.group_by("game_id", "event_id", "season").agg(
        pl.col("closed").sum().alias("n_books"),
        *[pl.col(f"spreads_home_{t}").median().alias(f"spread_home_{t}") for t in ("open", "close")],
        *[pl.col(f"totals_over_{t}").median().alias(f"total_{t}") for t in ("open", "close")],
        *[
            _american(_decimal(pl.col(f"h2h_{s}_{t}")).median()).round(1).alias(f"ml_{s}_{t}")
            for t in ("open", "close")
            for s in ("home", "away")
        ],
        *[pl.col(f"wp_{t}").median().alias(f"market_home_wp_{t}") for t in ("open", "close")],
        *[(1 - pl.col(f"wp_{t}")).median().alias(f"market_away_wp_{t}") for t in ("open", "close")],
    )
    done = pl.col("status_type_completed").fill_null(False)
    result = schedule.select(
        pl.col("game_id").cast(pl.Int32),
        pl.when(done)
        .then(pl.col("home_score").cast(pl.Int32) - pl.col("away_score").cast(pl.Int32))
        .alias("home_margin"),
        pl.when(done)
        .then(pl.col("home_score").cast(pl.Float64) + pl.col("away_score").cast(pl.Float64))
        .alias("_points"),
    ).unique("game_id")
    ats = pl.col("home_margin") + pl.col("spread_home_close")
    tot = pl.col("_points") - pl.col("total_close")
    return (
        out.join(result, on="game_id", how="left")
        .with_columns(
            pl.when(ats > 0)
            .then(pl.lit("home"))
            .when(ats < 0)
            .then(pl.lit("away"))
            .when(ats == 0)
            .then(pl.lit("push"))
            .alias("ats_result"),
            pl.when(tot > 0)
            .then(pl.lit("over"))
            .when(tot < 0)
            .then(pl.lit("under"))
            .when(tot == 0)
            .then(pl.lit("push"))
            .alias("total_result"),
        )
        .select(pl.col(c).cast(t) for c, t in GAME_LINES_SCHEMA.items())
        .sort("game_id")
    )


def player_props(
    props_checkpoints: pl.DataFrame,
    crosswalk: pl.DataFrame,
    player_xwalk: pl.DataFrame,
    schedule: pl.DataFrame,
    *,
    league: str | None = None,
) -> pl.DataFrame:
    """Per ESPN game x athlete x stat: the closing player-prop line, its prices and the no-vig over probability.

    The line is the one most books closed on (the lower line on a tie); the prices and the over
    probability are the medians over the books at that line. Only the five main markets are read
    (:data:`PROP_MARKETS`), not the alternates.

    Args:
        props_checkpoints: The odds store's ``props_checkpoints`` rows (``event_id``, ``sport_key``,
            ``bookmaker_key``, ``market_key``, ``outcome_name`` ``Over`` / ``Under``,
            ``outcome_description`` (the player), ``close_price``, ``close_point``,
            ``close_mins_before``).
        crosswalk: The odds store's ``espn_crosswalk``.
        player_xwalk: The league's ``props_player_xwalk`` (``season``, ``team``: ESPN team id,
            ``player_name``, ``espn_athlete_id``), keyed by ESPN season.
        schedule: The league's ESPN schedule: ``game_id``, ``season``, ``home_id``, ``away_id``.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).

    Returns:
        :data:`PLAYER_PROPS_SCHEMA`: ``game_id``, ``season``, ``athlete_id`` / ``team_id`` (null when
        the name is not in the crosswalk for either team in the game), ``player_name`` (as the books
        list it), ``stat`` (``points``, ``rebounds``, ``assists``, ``threes``, ``pra``), ``line``,
        ``n_books`` (books at that line), ``over_price`` / ``under_price`` (American) and
        ``over_prob``; an empty frame with those columns when nothing matches.

    Raises:
        ValueError: If the league is unknown.
        polars.exceptions.ColumnNotFoundError: If a required column is missing.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_game_lines import player_props
            from sportsdataverse.nba.nba_loaders import load_nba_schedule
            odds = "odds-data/odds"  # a local checkout of the odds store
            props = player_props(
                pl.read_parquet(f"{odds}/checkpoints/props_2025_0[1-4]_nba.parquet"),
                pl.read_parquet(f"{odds}/espn_crosswalk.parquet"),
                pl.read_parquet(f"{odds}/nba/props_player_xwalk_2025.parquet"),
                load_nba_schedule([2025]),
                league="nba",
            )

        One player's points lines::

            props.filter(pl.col("stat") == "points", pl.col("athlete_id") == 3112335)

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college basketball
            * `wehoop`_ -- the R companion for WNBA and women's college basketball

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, None)
    games = _games(crosswalk, schedule, ODDS_SPORT_KEYS[lg])
    rows = (
        props_checkpoints.filter(
            pl.col("sport_key") == ODDS_SPORT_KEYS[lg],
            pl.col("market_key").is_in(list(PROP_MARKETS)),
            ~pl.col("bookmaker_key").is_in(EXCHANGES),
            pl.col("close_mins_before") >= 0,
            pl.col("outcome_name").is_in(["Over", "Under"]),
            pl.col("close_point").is_not_null(),
        )
        .join(games.select("event_id", "game_id"), on="event_id", how="inner")
        .rename({"outcome_description": "player_name"})
    )
    if rows.is_empty() or not {"home_id", "away_id"} <= set(games.columns):
        return pl.DataFrame(schema=PLAYER_PROPS_SCHEMA)
    key = ["game_id", "market_key", "player_name"]
    per_book = (
        rows.group_by([*key, "bookmaker_key", "close_point"])
        .agg(
            pl.col("close_price").filter(pl.col("outcome_name") == "Over").first().alias("over"),
            pl.col("close_price").filter(pl.col("outcome_name") == "Under").first().alias("under"),
        )
        .filter(pl.col("over").is_not_null() & pl.col("under").is_not_null())
        .with_columns(_devig(pl.col("over"), pl.col("under")).alias("p_over"))
    )
    line = (
        per_book.group_by([*key, "close_point"])
        .len()
        .sort([*key, "len", "close_point"], descending=[False, False, False, True, False])
        .unique(key, keep="first", maintain_order=True)
        .select(*key, "close_point")
    )
    out = (
        per_book.join(line, on=[*key, "close_point"], how="semi")
        .group_by([*key, "close_point"])
        .agg(
            pl.len().cast(pl.UInt32).alias("n_books"),
            _american(_decimal(pl.col("over")).median()).round(1).alias("over_price"),
            _american(_decimal(pl.col("under")).median()).round(1).alias("under_price"),
            pl.col("p_over").median().alias("over_prob"),
        )
        .rename({"close_point": "line"})
        .with_columns(pl.col("market_key").replace_strict(PROP_MARKETS).alias("stat"))
    )
    # the athlete: the name in the crosswalk for one of the game's two teams that season
    sides = (
        games.unique("game_id")
        .select("game_id", "season", pl.concat_list("home_id", "away_id").alias("team_id"))
        .explode("team_id")
    )
    xw = player_xwalk.select(
        pl.col("season").cast(pl.Int32),
        pl.col("team").cast(pl.Int64).cast(pl.Int32).alias("team_id"),
        "player_name",
        pl.col("espn_athlete_id").cast(pl.Int64).cast(pl.Int32).alias("athlete_id"),
    ).filter(pl.col("athlete_id").is_not_null())
    for k in ("season", "team_id"):
        assert sides.schema[k] == xw.schema[k]
    who = (
        out.select("game_id", "player_name")
        .unique()
        .join(sides, on="game_id", how="inner")
        .join(xw, on=["season", "team_id", "player_name"], how="inner")
        .unique(["game_id", "player_name"], keep="first", maintain_order=True)
        .select("game_id", "player_name", "athlete_id", "team_id")
    )
    season = games.unique("game_id").select("game_id", "season")
    return (
        out.join(season, on="game_id", how="left")
        .join(who, on=["game_id", "player_name"], how="left")
        .select(pl.col(c).cast(t) for c, t in PLAYER_PROPS_SCHEMA.items())
        .sort("game_id", "player_name", "stat")
    )


def wp_variants(
    pbp: pl.DataFrame,
    market_home_prob: float | None = None,
    *,
    league: str | None = None,
    regulation_periods: int | None = None,
    neutral_site: bool | None = False,
) -> pl.DataFrame:
    """Two extra per-play home WP columns for one game: naive (prior 0.5) and market-anchored.

    Both rescore the plays with :func:`~sportsdataverse.nba.nba_game_page.score_win_prob`, the same
    in-game model as ``home_win_prob``, with only the pregame prior swapped: 0.5 for
    ``home_win_prob_naive`` (score and clock alone), ``market_home_prob`` for
    ``home_win_prob_market``. End-of-game rows settle to 1 / 0 as in ``score_win_prob``.

    Args:
        pbp: One game in the basketball pbp schema, as :func:`score_win_prob` takes it.
        market_home_prob: The game's pregame no-vig market home WP (:func:`game_lines`
            ``market_home_wp_close``); ``None`` or NaN leaves ``home_win_prob_market`` null.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        regulation_periods: The game's regulation periods; see :func:`score_win_prob`.
        neutral_site: Passed to :func:`score_win_prob` (it does not change a given prior).

    Returns:
        ``game_id`` (when present), ``game_play_number`` (when present), ``home_win_prob_naive`` and
        ``home_win_prob_market``, one row per play in play order; empty with those columns for an
        empty ``pbp``.

    Raises:
        ValueError: If the league is unknown, ``market_home_prob`` is outside [0, 1], or ``pbp`` holds
            more than one game.
        polars.exceptions.ColumnNotFoundError: If a column :func:`score_win_prob` needs is missing.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_game_lines import wp_variants
            from sportsdataverse.nba.nba_loaders import load_nba_pbp
            one = load_nba_pbp([2025]).filter(pl.col("game_id") == 401704638)
            wp = wp_variants(one, 0.31, league="nba")

        Where the market prior moved the curve most::

            wp.with_columns((pl.col("home_win_prob_market") - pl.col("home_win_prob_naive")).alias("d")).sort("d")

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, None)
    market = None if market_home_prob is None or market_home_prob != market_home_prob else float(market_home_prob)
    if market is not None and not 0.0 <= market <= 1.0:
        raise ValueError(f"market_home_prob must be in [0, 1]; got {market_home_prob!r}")
    keep = [c for c in ("game_id", "game_play_number") if c in pbp.columns]
    # no line: score_win_prob then takes rated_home_prob as the prior (home_prior's "rated" branch)
    lineless = pbp.with_columns(pl.lit(False).alias("game_spread_available"))

    def _wp(prior: float) -> pl.Series:
        return score_win_prob(
            lineless, league=lg, regulation_periods=regulation_periods, neutral_site=neutral_site, rated_home_prob=prior
        )["home_win_prob"]

    naive = _wp(0.5)
    return pbp.select(keep).with_columns(
        naive.alias("home_win_prob_naive"),
        (_wp(market) if market is not None else pl.Series([None] * pbp.height, dtype=pl.Float64)).alias(
            "home_win_prob_market"
        ),
    )
