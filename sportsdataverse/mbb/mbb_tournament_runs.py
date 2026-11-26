"""College tournament runs (BB-D43 for MBB / WBB): ``mbb.tournament_run_games`` / ``_teams`` / ``_players``.

A run is one team's games in one tournament: the NCAA tournament (First Four onward), a
conference tournament, the NIT / WNIT, the CBI, the Crown, the WBIT and the other postseason
events, keyed and scored from the ESPN schedule, with the team and player totals summed from the
ESPN box scores. The rules (which games, how a tournament is named and keyed, champion /
eliminated / alive) are in :mod:`sportsdataverse._common.tournament_runs`. ``league="womens"``
reads the WBB releases; :mod:`sportsdataverse.wbb.wbb_tournament_runs` binds it.

Every id is ESPN's (game, team, athlete), the keys of the ``mbb`` / ``wbb`` game schemas.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import polars as pl

from sportsdataverse._common.tournament_runs import tournament_run_games, tournament_run_players, tournament_run_teams

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["mbb_tournament_run_players", "mbb_tournament_run_teams", "mbb_tournament_runs"]


def _loaders(league: str) -> tuple[Any, Any, Any]:
    """``(schedule, team box, player box)`` loaders for ``league`` (ESPN releases)."""
    if league == "womens":
        from sportsdataverse.wbb.wbb_loaders import load_wbb_player_boxscore, load_wbb_schedule, load_wbb_team_boxscore

        return load_wbb_schedule, load_wbb_team_boxscore, load_wbb_player_boxscore
    if league != "mens":
        raise ValueError(f"league must be 'mens' or 'womens', not {league!r}")
    from sportsdataverse.mbb.mbb_loaders import load_mbb_player_boxscore, load_mbb_schedule, load_mbb_team_boxscore

    return load_mbb_schedule, load_mbb_team_boxscore, load_mbb_player_boxscore


def _runs(seasons: int | list[int], league: str) -> tuple[pl.DataFrame, list[int], Any, Any]:
    load_schedule, load_team_box, load_player_box = _loaders(league)
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    return tournament_run_games(load_schedule(seasons_list)), seasons_list, load_team_box, load_player_box


def mbb_tournament_runs(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Every tournament game, once per side, keyed to that team's run, with the run's record after it.

    A run is one team's games in one tournament. The games are the postseason (ESPN
    ``season_type`` 3: the NCAA tournament from the First Four, the NIT / WNIT, the CBI, the
    Crown, the WBIT ...) and the conference tournaments (a ``tournament_id`` with a conference
    group, from mid-February). ``run_id`` is ``"{season}-{tournament_key}-{team_id}"``;
    ``tournament_key`` is ESPN's ``tournament_id``, or the headline's event name where the id is
    missing (older seasons). A conference tournament ESPN did not tag is not a run.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_tournament_runs.wbb_tournament_runs`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        Two rows per tournament game (one per side): ``season``, ``run_id``, ``tournament_key``,
        ``tournament_id``, ``tournament_name`` (the event name most of its headlines carry),
        ``tournament_kind`` (``ncaa``, ``conference`` or ``postseason``), ``conference_id``,
        ``round_name``, ``game_number`` (by tip-off), ``game_id``, ``game_date``, ``completed``,
        ``home_away`` (``neutral`` at a neutral site), the team's and the opponent's id,
        abbreviation, display name and score, ``winner_team_id``, ``wins`` / ``losses`` (after
        the game), ``run_games`` and ``run_result`` (``champion``: won the tournament's last listed
        game, once final; ``eliminated``: lost its last listed game; else ``alive``).

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"`` (checked before any load).

    See Also:
        :func:`mbb_tournament_run_teams`, :func:`mbb_tournament_run_players`; the cores in
        :mod:`sportsdataverse._common.tournament_runs`.

    Example:
        The 2025 NCAA champion's run::

            from sportsdataverse.mbb.mbb_tournament_runs import mbb_tournament_runs
            runs = mbb_tournament_runs(2025)
            runs.filter((runs["tournament_kind"] == "ncaa") & (runs["run_result"] == "champion"))
    """
    games, _, _, _ = _runs(seasons, league)
    return games.to_pandas() if return_as_pandas else games


def mbb_tournament_run_teams(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Each run's record and box totals: the sum of the team's game boxes in the tournament.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"``.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per run with a final game, keyed ``(season, run_id, team_id)`` with
        ``team_abbreviation``: ``games``, ``wins``, ``losses``, ``pts``, ``opp_pts`` and the
        summed counts ``fgm``, ``fga``, ``fg3m``, ``fg3a``, ``ftm``, ``fta``, ``oreb``, ``dreb``,
        ``reb``, ``ast``, ``stl``, ``blk``, ``tov``, ``pf``.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    See Also:
        :func:`mbb_tournament_runs`, :func:`mbb_tournament_run_players`.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_tournament_runs import mbb_tournament_run_teams
            mbb_tournament_run_teams(2025).sort("wins", descending=True).head()
    """
    games, seasons_list, load_team_box, _ = _runs(seasons, league)
    out = tournament_run_teams(games, load_team_box(seasons_list))
    return out.to_pandas() if return_as_pandas else out


def mbb_tournament_run_players(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Each player's totals and per-game line over a run, against the player's regular season.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"``.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per player who played in a final game of the run, keyed
        ``(season, run_id, team_id, athlete_id)``: the columns of
        :func:`sportsdataverse.nba.nba_playoff_series.nba_playoff_series_players` (totals,
        ``<stat>_pg``, ``ts_pct``, ``reg_games`` / ``reg_<stat>_pg`` / ``reg_ts_pct`` and the shift
        ``<stat>_pg_diff`` / ``ts_pct_diff``). The regular season leaves out the conference
        tournament games.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    See Also:
        :func:`mbb_tournament_runs`, :func:`mbb_tournament_run_teams`.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_tournament_runs import mbb_tournament_run_players
            mbb_tournament_run_players(2025).sort("pts_pg_diff", descending=True).head()
    """
    games, seasons_list, _, load_player_box = _runs(seasons, league)
    out = tournament_run_players(games, load_player_box(seasons_list))
    return out.to_pandas() if return_as_pandas else out
