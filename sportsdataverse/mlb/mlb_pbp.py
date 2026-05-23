from __future__ import annotations

from typing import Dict

from sportsdataverse.dl_utils import download


def espn_mlb_pbp(game_id: int, raw: bool = False, **kwargs) -> Dict:
    """espn_mlb_pbp - pull the full ESPN game-summary payload for one MLB game.

    Wraps the Site v2 endpoint::

        http://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event={game_id}

    Args:
        game_id (int): ESPN game id (the "event id"). Obtainable from
            :func:`espn_mlb_schedule`.
        raw (bool): When True, returns the full nested payload unchanged.
            When False (default), the same payload is returned for now — full
            parsing into a tidy plays / boxscore dict is **not yet implemented**;
            see the TODO below.

    Returns:
        Dict: The Site v2 summary payload. Top-level keys typically include
        ``header``, ``boxscore``, ``plays``, ``leaders``, ``scoringPlays``,
        ``gameInfo``, ``winprobability``, ``pickcenter``, ``news``, ``videos``,
        ``standings``, ``article``, ``seasonseries``, ``broadcasts``,
        ``predictor``.

    Example:
        Pull a single game's raw feed (Opening Day 2024)::

            from sportsdataverse.mlb import espn_mlb_pbp
            game = espn_mlb_pbp(game_id=401569461, raw=True)
            sorted(game.keys())
            print(game.get("header", {}).get("competitions", [{}])[0].get("date"))

        Iterate the plays array::

            plays = game.get("plays") or []
            print(f"{len(plays)} plays")
            for p in plays[:3]:
                print(p.get("text"))

    TODO:
        Full PBP parsing into a tidy pitch-by-pitch dataframe is unimplemented.
        The reference shape to target is :func:`espn_nhl_pbp` — return a dict
        with keys ``gameId, plays, boxscore, header, broadcasts, videos,
        playByPlaySource, standings, leaders, seasonseries, pickcenter,
        againstTheSpread, odds, gameInfo, season`` plus an MLB-specific
        ``boxscore.batters`` / ``boxscore.pitchers`` split. Statcast-level
        detail (exit velocity, launch angle, spin rate) is NOT in ESPN's
        payload — use :func:`mlb_api_pbp` (statsapi.mlb.com) for that.
    """
    url = f"http://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event={game_id}"
    resp = download(url=url, **kwargs)
    if resp is None:
        return {}
    payload = resp.json()
    if raw:
        return payload
    # TODO: tidy-parse into plays / boxscore frames; for now we return the
    # full Site v2 payload as a dict so callers can mine it directly.
    return payload
