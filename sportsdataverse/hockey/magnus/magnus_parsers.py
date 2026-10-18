"""Parsers for the generated ``magnus`` wrappers (Ligue Magnus, ``liguemagnus.com``).

Two body shapes come back from the wrappers:

* **admin-ajax envelopes** -- ``{"success": true, "data": {"data": ..., "time": ...}}``.
  The rows are ``data.data`` (competitions, phases, teams, rounds), ``data.data`` beside
  Laravel ``links`` / ``meta`` (``get_rencontres`` games), ``data.data.rencontres`` (one
  round's games) or ``data.data.positions`` (standings). Every parser also accepts the
  inner ``data`` object, so ``parse_magnus_schedule(body["data"])`` works too.
* **the game sheet** -- the object embedded in ``/rencontre/{id}/`` (the runtime has already
  extracted it): ``evenements`` (events), ``joueurs`` (roster), ``tirs_aux_but`` (shootout),
  ``scores``, ``receveur`` / ``visiteur`` (home / away), ``periodes``, ``mvps``, ...

Every frame follows the package parser contract: polars by default, pandas via
``return_as_pandas=True``, snake_case columns, and a zero-row frame with the documented
columns (never an exception) on an empty or malformed payload. Each parser builds its rows
against a fixed schema, so a leaf of the wrong type becomes null rather than raising.

Ids and their key spaces (every numeric id is ``Int64``; none is ever a float):

* ``game_id`` -- the game (``get_rencontres`` ``data[].id`` = ``/rencontre/{id}/``).
* ``team_id`` -- the team, stable across seasons (``get_equipes`` ``id`` = game
  ``receveur.id`` / ``visiteur.id`` = standings ``equipe.id`` = event ``equipe.id``).
* ``*_team_participant_id`` / ``team_participant_id`` -- a team's entry in ONE phase (its own
  key space: game ``score[].participant_id`` = standings ``participant_id``). Not a team id.
* ``player_id`` (and every ``*_player_id``) -- the PERSON id (``joueurs[].id`` =
  ``joueurs[].personne.id``), stable across games. It is the only player key the frames
  carry as a scalar.
* ``player_participant_id`` (lineups only) -- the player's roster entry in THIS game, a
  per-game key that changes every game. Event on-ice lists (``joueurs_plus_ids`` /
  ``joueurs_moins_ids``) and ``mvps[]`` are in this space; the parsers translate them to
  person ids through the game's roster. ``mvps[].id`` is an MVP record id and is not output.
* ``competition_id`` (one per season), ``phase_id`` (one per phase per season), ``round_id``,
  ``attempt_id`` (a shootout attempt), ``venue_id``, ``club_id`` (the legal entity behind a
  team), ``standings_id``.
* ``licence_number`` (FFHG ``code_adherent``) and ``event_id`` (``BUT99238``) are ``String``:
  licence numbers keep their leading zeros.

Staff birth dates are never output (the runtime drops ``staffs[].ddn``; no parser reads it).
"""

from __future__ import annotations

import math
import re
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple, Union

import pandas as pd
import polars as pl

__all__ = [
    "parse_magnus_competitions",
    "parse_magnus_game",
    "parse_magnus_lineups",
    "parse_magnus_pbp",
    "parse_magnus_phases",
    "parse_magnus_rounds",
    "parse_magnus_schedule",
    "parse_magnus_shootout",
    "parse_magnus_standings",
    "parse_magnus_teams",
]

Frame = Union[pl.DataFrame, pd.DataFrame]
Getter = Callable[[Mapping[str, Any]], Any]
Spec = Dict[str, Tuple[Any, Getter]]

#: Game times are published in French local time.
TZ = "Europe/Paris"
_DATETIME = pl.Datetime("us", TZ)
_IDS = pl.List(pl.Int64)
#: Regulation period length in seconds; ``temps`` is elapsed game seconds.
PERIOD_SECONDS = 1200

#: Raw event ``type`` -> English ``event_type``.
EVENT_TYPES: Dict[str, str] = {
    "BUT": "goal",
    "SANCTION": "penalty",
    "TIR": "shot",
    "ENGA": "faceoff",
    "REMPLACANT": "goalie_change",
    "TEMPS_MORT": "timeout",
}
# Order of events that share a second: the shot, the goal it produced, then the penalty and
# the stoppages, and the faceoff that restarts play last. A heuristic tie-break only: the
# sheet groups events by type and carries no sequence number.
_TYPE_ORDER = {"TIR": 0, "BUT": 1, "SANCTION": 2, "REMPLACANT": 3, "TEMPS_MORT": 4, "ENGA": 5}
_DECISIONS = {"S": "regulation", "PRL": "overtime", "TAB": "shootout"}
_POSITIONS = {"A": "forward", "D": "defence", "GB": "goalie"}
_DIGITS = re.compile(r"\d+")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _out(df: pl.DataFrame, return_as_pandas: bool) -> Frame:
    return df.to_pandas() if return_as_pandas else df


def _at(obj: Any, *keys: str) -> Any:
    """``obj[k1][k2]...``; ``None`` as soon as a level is not an object."""
    for key in keys:
        obj = obj.get(key) if isinstance(obj, dict) else None
    return obj


def _list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _dicts(value: Any) -> List[Dict[str, Any]]:
    return [v for v in _list(value) if isinstance(v, dict)]


def _int(value: Any) -> Optional[int]:
    """An int or a digit string as ``int``; anything else (a bool, a float, an object) -> ``None``."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def _coerce(value: Any, dtype: Any) -> Any:
    """``value`` as its column's dtype allows, else ``None`` (a leaf of the wrong type never raises)."""
    if value is None:
        return None
    if isinstance(dtype, pl.List):
        return [_int(v) for v in value] if isinstance(value, list) else None
    if dtype == pl.Boolean:
        return value if isinstance(value, bool) else None
    if dtype == pl.Int64:
        return _int(value)
    if isinstance(value, (bool, dict, list)):
        return None
    if isinstance(value, str):
        return value if value.strip() else None
    return str(value) if isinstance(value, (int, float)) else None


def _paris(col: str) -> pl.Expr:
    """A published game time as a Europe/Paris datetime: local ``YYYY-MM-DD HH:MM[:SS]`` or UTC ISO ``...Z``."""
    c = pl.col(col)
    local = pl.coalesce(
        c.str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False),
        c.str.to_datetime("%Y-%m-%d %H:%M", strict=False),
    ).dt.replace_time_zone(TZ, ambiguous="earliest", non_existent="null")
    utc = c.str.to_datetime("%Y-%m-%dT%H:%M:%S%.fZ", strict=False).dt.replace_time_zone("UTC").dt.convert_time_zone(TZ)
    return pl.coalesce(local, utc).dt.cast_time_unit("us").alias(col)


def _frame(rows: Iterable[Mapping[str, Any]], spec: Spec) -> pl.DataFrame:
    """One row per mapping, each column read by its getter and coerced to its dtype.

    Datetime / Date columns are read as strings and parsed after the frame exists (polars
    owns the time-zone database, so no tzdata is needed).
    """
    rows = list(rows)
    raw_dtype = {c: (pl.String if d in (_DATETIME, pl.Date) else d) for c, (d, _) in spec.items()}
    data = {c: [_coerce(get(r), raw_dtype[c]) for r in rows] for c, (_, get) in spec.items()}
    df = pl.DataFrame(data, schema=raw_dtype)
    df = df.with_columns(_paris(c) for c, (d, _) in spec.items() if d == _DATETIME)
    return df.with_columns(
        pl.col(c).str.to_date("%d/%m/%Y", strict=False) for c, (d, _) in spec.items() if d == pl.Date
    )


def _payload(raw: Any) -> Any:
    """The admin-ajax ``data.data`` of an envelope, or of its inner ``data`` object."""
    body = raw if isinstance(raw, dict) else {}
    if "success" in body:
        return _at(body, "data", "data")
    return body.get("data")


def _get(*keys: str) -> Getter:
    return lambda row: _at(row, *keys)


# ---------------------------------------------------------------------------
# admin-ajax
# ---------------------------------------------------------------------------

_COMPETITIONS: Spec = {
    "competition_id": (pl.Int64, _get("id")),
    "season": (pl.Int64, _get("saison")),
    "competition": (pl.String, _get("libelle")),
    "competition_type_id": (pl.Int64, _get("type", "id")),
    "competition_type_code": (pl.String, _get("type", "code")),
    "competition_type": (pl.String, _get("type", "libelle")),
    "external_url": (pl.String, _get("parametres", "url_externe")),
    "published": (pl.Boolean, _get("parametres", "publier_sur_site")),
}


def parse_magnus_competitions(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """One row per competition (season) from a ``get_competition_by_type`` body.

    Args:
        raw: the ``get_competition_by_type`` body (envelope or its ``data``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        ``competition_id``, ``season`` (END year: 2027 = 2026-27), names, the competition
        type and the publication flags; zero rows on an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.magnus import magnus_competitions
            df = magnus_competitions()
            print(df.select("season", "competition_id"))

    See Also:
        * :func:`sportsdataverse.hockey.magnus.magnus_competitions` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return _out(_frame(_dicts(_payload(raw)), _COMPETITIONS), return_as_pandas)


_PHASES: Spec = {
    "phase_id": (pl.Int64, _get("id")),
    "phase": (pl.String, _get("libelle")),
    "phase_type_code": (pl.String, _get("type_phase")),
    "visible": (pl.Boolean, _get("visible")),
    "competition": (pl.String, _get("competition_libelle")),
    "n_teams": (pl.Int64, _get("parametrage", "nombre_participant")),
    "overtime": (pl.Boolean, _get("parametrage", "prolongation")),
    "games_per_matchup": (pl.Int64, _get("parametrage", "nombre_match_par_confrontation")),
}


def parse_magnus_phases(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """One row per phase (regular season, playoffs, relegation) from a ``get_phases`` body.

    Args:
        raw: the ``get_phases`` body (envelope or its ``data``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        ``phase_id`` (the key of the phase-scoped wrappers), the phase name and type code
        (``C`` round robin, ``T`` playoff bracket) and its format; zero rows when empty.

    Example:
        Quick start::

            from sportsdataverse.hockey.magnus import magnus_phases
            df = magnus_phases(season=2027)
            print(df.select("phase_id", "phase", "phase_type_code"))

    See Also:
        * :func:`sportsdataverse.hockey.magnus.magnus_phases` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return _out(_frame(_dicts(_payload(raw)), _PHASES), return_as_pandas)


def _team_spec(prefix: str, *path: str) -> Spec:
    """The team columns of a team object at ``path`` (a get_equipes row when ``path`` is empty)."""
    return {
        f"{prefix}team_id": (pl.Int64, _get(*path, "id")),
        f"{prefix}team": (pl.String, _get(*path, "libelle_complet")),
        f"{prefix}team_abbreviation": (pl.String, _get(*path, "abreviation")),
    }


_TEAMS: Spec = {
    **_team_spec(""),
    "team_short_name": (pl.String, _get("libelle_court")),
    "team_label": (pl.String, _get("libelle")),
    "gender": (pl.String, _get("sexe")),
    "logo_url": (pl.String, _get("logo")),
    "club_id": (pl.Int64, _get("structure", "id")),
    "club": (pl.String, _get("structure", "nom")),
    "club_code": (pl.String, _get("structure", "code")),
    "competition_ids": (_IDS, _get("id_compet")),
}


def parse_magnus_teams(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """One row per team of a phase from a ``get_equipes`` body.

    Args:
        raw: the ``get_equipes`` body (envelope or its ``data``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        ``team_id`` (stable across seasons; joins games and standings), display names, the
        club behind the team and ``competition_ids`` (every competition it entered, all
        seasons and levels); zero rows when empty.

    Example:
        Quick start::

            from sportsdataverse.hockey.magnus import magnus_teams
            df = magnus_teams(phase_id=714)
            print(df.select("team_id", "team"))

    See Also:
        * :func:`sportsdataverse.hockey.magnus.magnus_teams` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return _out(_frame(_dicts(_payload(raw)), _TEAMS), return_as_pandas)


def _score(game: Mapping[str, Any], side: str, key: str) -> Any:
    """``key`` of the score row of ``side``'s team, matched by team id.

    ``score[]`` is not positional: it can list the away team first, and some finished games
    carry extra 0-goal rows for teams that did not play (2026-27 games 77751 / 77753 / 77754).
    """
    team = _int(_at(game, side, "id"))
    hits = [
        s
        for s in _dicts(game.get("score") or game.get("scores"))
        if team is not None and _int(s.get("equipe_id")) == team
    ]
    return hits[0].get(key) if len(hits) == 1 else None


def _result(game: Mapping[str, Any], side: str) -> Optional[int]:
    """A side's goals once the game is final or live; unplayed games publish a placeholder 0-0."""
    played = game.get("etat") == "T" or game.get("en_cours") is True
    return _int(_score(game, side, "score")) if played else None


def _status(game: Mapping[str, Any]) -> str:
    if game.get("etat") == "T":
        return "final"
    return "in_progress" if game.get("en_cours") is True else "scheduled"


_SCHEDULE: Spec = {
    "game_id": (pl.Int64, _get("id")),
    "season": (pl.Int64, _get("competition", "saison")),
    "competition_id": (pl.Int64, _get("competition", "id")),
    "phase_id": (pl.Int64, lambda g: _at(g, "phase_id") if _at(g, "phase_id") is not None else _at(g, "phase", "id")),
    "phase": (pl.String, _get("phase", "libelle")),
    "round": (pl.Int64, _get("tour")),
    "game_datetime": (_DATETIME, _get("date_rencontre")),
    "status": (pl.String, _status),
    "decision": (pl.String, lambda g: _DECISIONS.get(str(g.get("victoire_par"))) if g.get("etat") == "T" else None),
    **_team_spec("home_", "receveur"),
    **_team_spec("away_", "visiteur"),
    "home_score": (pl.Int64, lambda g: _result(g, "receveur")),
    "away_score": (pl.Int64, lambda g: _result(g, "visiteur")),
    "home_team_participant_id": (pl.Int64, lambda g: _score(g, "receveur", "participant_id")),
    "away_team_participant_id": (pl.Int64, lambda g: _score(g, "visiteur", "participant_id")),
    "venue_id": (pl.Int64, _get("lieu_de_pratique", "id")),
    "venue": (pl.String, _get("lieu_de_pratique", "nom")),
    "game_label": (pl.String, _get("rencontre_libelle")),
}


def parse_magnus_schedule(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """One row per game from a ``get_rencontres`` body or a ``get_rencontrestours`` round.

    A round body's games carry no competition, phase or round of their own; they inherit the
    round's. ``get_rencontres`` publishes local times and a round publishes UTC ISO times:
    both land in ``game_datetime`` as a Europe/Paris datetime.

    Args:
        raw: the ``get_rencontres`` or ``get_rencontrestours`` body (envelope or its ``data``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per game: ids, round, time, ``status`` (``final`` / ``in_progress`` /
        ``scheduled``), ``decision`` (``regulation`` / ``overtime`` / ``shootout``, final games
        only), home / away teams, goals (null until the game starts: unplayed games publish
        a 0-0 placeholder) and the team participant ids that join the standings; zero rows
        when empty.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.hockey.magnus import magnus_schedule
            df = magnus_schedule(season=2027)
            print(df.filter(pl.col("status") == "final").height)

    See Also:
        * :func:`sportsdataverse.hockey.magnus.magnus_round_games` - the route this works on.
        * :func:`sportsdataverse.hockey.magnus.magnus_schedule` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    payload = _payload(raw)
    if isinstance(payload, dict):  # one round: its games plus the round's own scope
        scope = {
            "competition": payload.get("competition"),
            "phase": payload.get("phase"),
            "phase_id": payload.get("phase_id"),
            "tour": payload.get("tour"),
        }
        games: List[Dict[str, Any]] = [{**scope, **g} for g in _dicts(payload.get("rencontres"))]
    else:
        games = _dicts(payload)
    return _out(_frame(games, _SCHEDULE), return_as_pandas)


_ROUNDS: Spec = {
    "round_id": (pl.Int64, _get("id")),
    "round": (pl.Int64, _get("tour")),
    "round_label": (pl.String, _get("libelle")),
    "phase_id": (pl.Int64, _get("phase_id")),
    "phase": (pl.String, _get("phase", "libelle")),
    "competition_id": (pl.Int64, _get("competition", "id")),
    "season": (pl.Int64, _get("competition", "saison")),
    "start_date": (pl.Date, _get("date_debut")),
    "end_date": (pl.Date, _get("date_fin")),
}


def parse_magnus_rounds(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """One row per round (journée) of a phase from a ``get_liste_tours`` body.

    Args:
        raw: the ``get_liste_tours`` body (envelope or its ``data``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        ``round`` (the value ``magnus_round_games`` and ``magnus_schedule`` take), its id,
        phase, season and date span (the body's DD/MM/YYYY dates as ``Date``); zero rows when
        empty.

    Example:
        Quick start::

            from sportsdataverse.hockey.magnus import magnus_rounds
            df = magnus_rounds(phase_id=714)
            print(df.select("round", "start_date"))

    See Also:
        * :func:`sportsdataverse.hockey.magnus.magnus_rounds` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return _out(_frame(_dicts(_payload(raw)), _ROUNDS), return_as_pandas)


def _form(position: Mapping[str, Any]) -> Optional[str]:
    """The last results as ``W`` / ``L`` / ``T`` letters, in the order the body lists them."""
    me = _int(position.get("participant_id"))
    letters = []
    for game in _dicts(position.get("dernier_resultats")):
        if game.get("match_nul") is True:
            letters.append("T")
        elif me is not None and _int(game.get("participant_gagnant_id")) is not None:
            letters.append("W" if _int(game.get("participant_gagnant_id")) == me else "L")
    return "".join(letters) or None


_STANDINGS: Spec = {
    "rank": (pl.Int64, _get("position")),
    "team_id": (pl.Int64, _get("equipe", "id")),
    "team": (pl.String, _get("equipe", "nom_court")),
    "team_abbreviation": (pl.String, _get("equipe", "abreviation")),
    "team_participant_id": (pl.Int64, _get("participant_id")),
    "games_played": (pl.Int64, _get("nombre_rencontres_joues")),
    "wins": (pl.Int64, _get("nombre_victoire")),
    "ot_wins": (pl.Int64, _get("nombre_vprl")),
    "ot_losses": (pl.Int64, _get("nombre_dprl")),
    "losses": (pl.Int64, _get("nombre_defaite")),
    "ties": (pl.Int64, _get("nombre_nul")),
    "points": (pl.Int64, _get("nombre_point")),
    "goals_for": (pl.Int64, _get("nombre_but_marque")),
    "goals_against": (pl.Int64, _get("nombre_but_concede")),
    "penalty_minutes": (pl.Int64, _get("nombre_penalites")),
    "last_results": (pl.String, _form),
    "standings_id": (pl.Int64, _get("standings_id")),
}


def parse_magnus_standings(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """One row per team, in rank order, from a ``get_classementsphase`` body.

    ``wins`` / ``losses`` are regulation results; overtime and shootout results are
    ``ot_wins`` / ``ot_losses``.

    Args:
        raw: the ``get_classementsphase`` body (envelope or its ``data``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        Rank, team, record, points, goals, penalty minutes and ``last_results`` (the form
        guide as ``W`` / ``L`` / ``T`` letters); zero rows when empty.

    Example:
        Quick start::

            from sportsdataverse.hockey.magnus import magnus_standings
            df = magnus_standings(phase_id=714)
            print(df.select("rank", "team", "points"))

    See Also:
        * :func:`sportsdataverse.hockey.magnus.magnus_standings` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    table = _payload(raw)
    standings_id = _at(table, "classement_id")
    rows = [{**p, "standings_id": standings_id} for p in _dicts(_at(table, "positions"))]
    return _out(_frame(rows, _STANDINGS), return_as_pandas)


# ---------------------------------------------------------------------------
# game sheet
# ---------------------------------------------------------------------------


def _sheet(raw: Any) -> Dict[str, Any]:
    return raw if isinstance(raw, dict) else {}


def _game_id(sheet: Mapping[str, Any]) -> Optional[int]:
    """The game id: the sheet has no id of its own, its ``scores[]`` rows carry it."""
    ids = {_int(s.get("sportif_rencontre_id")) for s in _dicts(sheet.get("scores"))} - {None}
    return ids.pop() if len(ids) == 1 else None


def _person_id(obj: Any) -> Optional[int]:
    """The person id of a player object (``id``, else ``personne.id``)."""
    pid = _int(_at(obj, "id"))
    return pid if pid is not None else _int(_at(obj, "personne", "id"))


_GAME: Spec = {
    "game_id": (pl.Int64, _game_id),
    "competition": (pl.String, _get("competition")),
    "phase": (pl.String, _get("phase")),
    "round": (pl.Int64, _get("journee")),
    "game_datetime": (_DATETIME, _get("date_rencontre_non_formate")),
    "status": (
        pl.String,
        lambda s: (
            "final"
            if s.get("est_terminee") is True or s.get("etat") == "T"
            else "in_progress"
            if s.get("est_debute") is True
            else "scheduled"
        ),
    ),
    **_team_spec("home_", "receveur"),
    **_team_spec("away_", "visiteur"),
    "home_score": (pl.Int64, lambda s: _score(s, "receveur", "score")),
    "away_score": (pl.Int64, lambda s: _score(s, "visiteur", "score")),
    "home_team_participant_id": (pl.Int64, lambda s: _score(s, "receveur", "participant_id")),
    "away_team_participant_id": (pl.Int64, lambda s: _score(s, "visiteur", "participant_id")),
    "has_shootout": (pl.Boolean, lambda s: bool(_dicts(s.get("tirs_aux_but")))),
    "n_periods": (pl.Int64, _get("periodes", "nombre")),
    "period_minutes": (pl.Int64, _get("periodes", "duree")),
    "attendance": (pl.Int64, _get("nombre_spectateurs")),
    "venue": (pl.String, _get("patinoire")),
}


def parse_magnus_game(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """The game header: one row per game sheet (``/rencontre/{id}/``).

    Args:
        raw: the game sheet object (``magnus_game(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row: ids, competition / phase / round names, the Europe/Paris start time, status,
        home / away teams and goals, team participant ids, shootout flag, period format,
        attendance and rink; zero rows on an empty or malformed sheet.

    Example:
        Fetch the sheet once, parse it four ways::

            from sportsdataverse.hockey.magnus import magnus_game, parse_magnus_game, parse_magnus_pbp
            sheet = magnus_game(game_id=77779, return_parsed=False)
            header, pbp = parse_magnus_game(sheet), parse_magnus_pbp(sheet)

    See Also:
        * :func:`sportsdataverse.hockey.magnus.magnus_game` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    sheet = _sheet(raw)
    is_sheet = "evenements" in sheet or "scores" in sheet  # an envelope or an error object is not a game
    return _out(_frame([sheet] if is_sheet else [], _GAME), return_as_pandas)


def _period(event_type: Any, seconds: Optional[int]) -> Optional[int]:
    """The period of an event from its elapsed game seconds (regulation periods are 1200 s).

    A faceoff at exactly 1200 / 2400 / 3600 opens the next period (period = s // 1200 + 1);
    any other event at a boundary belongs to the period ending there (period =
    max(1, ceil(s / 1200))): game 77779 has shots at exactly 1200 that interleave with
    period-1 shots. Overtime is period 4 (and up).
    """
    if seconds is None:
        return None
    if event_type == "ENGA":
        return seconds // PERIOD_SECONDS + 1
    return max(1, math.ceil(seconds / PERIOD_SECONDS))


def _event_row(event: Mapping[str, Any], game_id: Optional[int], roster: Mapping[int, Optional[int]]) -> Dict[str, Any]:
    raw_type = event.get("type")
    seconds = _int(event.get("temps"))
    period = _period(raw_type, seconds)
    actor = {
        "BUT": "buteur",
        "TIR": "tireur",
        "SANCTION": "joueur",
        "ENGA": "gagnant",
    }.get(str(raw_type))
    assists = _dicts(event.get("assistants"))
    on_ice = {
        side: [roster.get(p) for p in (_int(v) for v in _list(event.get(key))) if p is not None]
        if isinstance(event.get(key), list)
        else None
        for side, key in (("plus", "joueurs_plus_ids"), ("minus", "joueurs_moins_ids"))
    }
    return {
        "game_id": game_id,
        "event_id": event.get("id"),
        "event_type": EVENT_TYPES.get(str(raw_type), str(raw_type).lower() if raw_type else None),
        "event_type_raw": raw_type,
        "period": period,
        "period_seconds": seconds - (period - 1) * PERIOD_SECONDS if seconds is not None and period else None,
        "game_seconds": seconds,
        "team_id": _at(event, "equipe", "id"),
        "team_abbreviation": _at(event, "equipe", "abreviation"),
        "player_id": _person_id(event.get(actor)) if actor else None,
        "player_name": _at(event, actor, "nom_complet") if actor else None,
        "assist1_player_id": _person_id(assists[0]) if assists else None,
        "assist1_player_name": assists[0].get("nom_complet") if assists else None,
        "assist2_player_id": _person_id(assists[1]) if len(assists) > 1 else None,
        "assist2_player_name": assists[1].get("nom_complet") if len(assists) > 1 else None,
        "goalie_player_id": _person_id(event.get("gardien")),
        "goalie_player_name": _at(event, "gardien", "nom_complet"),
        "faceoff_home_player_id": _person_id(event.get("receveur")) if raw_type == "ENGA" else None,
        "faceoff_away_player_id": _person_id(event.get("visiteur")) if raw_type == "ENGA" else None,
        "strength_code": _at(event, "surnombre", "code"),
        "penalty_code": _at(event, "sanction", "code"),
        "penalty": _at(event, "sanction", "libelle"),
        "penalty_minutes": event.get("temps_penalite"),
        "penalty_start_seconds": event.get("temps_debut_sanction"),
        "penalty_end_seconds": event.get("temps_fin_sanction"),
        "goalie_in_player_id": _person_id(event.get("entrant")),
        "goalie_out_player_id": _person_id(event.get("sortant")),
        "on_ice_plus_player_ids": on_ice["plus"],
        "on_ice_minus_player_ids": on_ice["minus"],
    }


_PBP: Spec = {
    name: (dtype, _get(name))
    for name, dtype in {
        "game_id": pl.Int64,
        "event_id": pl.String,
        "event_type": pl.String,
        "event_type_raw": pl.String,
        "period": pl.Int64,
        "period_seconds": pl.Int64,
        "game_seconds": pl.Int64,
        "team_id": pl.Int64,
        "team_abbreviation": pl.String,
        "player_id": pl.Int64,
        "player_name": pl.String,
        "assist1_player_id": pl.Int64,
        "assist1_player_name": pl.String,
        "assist2_player_id": pl.Int64,
        "assist2_player_name": pl.String,
        "goalie_player_id": pl.Int64,
        "goalie_player_name": pl.String,
        "faceoff_home_player_id": pl.Int64,
        "faceoff_away_player_id": pl.Int64,
        "strength_code": pl.String,
        "penalty_code": pl.String,
        "penalty": pl.String,
        "penalty_minutes": pl.Int64,
        "penalty_start_seconds": pl.Int64,
        "penalty_end_seconds": pl.Int64,
        "goalie_in_player_id": pl.Int64,
        "goalie_out_player_id": pl.Int64,
        "on_ice_plus_player_ids": _IDS,
        "on_ice_minus_player_ids": _IDS,
    }.items()
}


def _roster(sheet: Mapping[str, Any]) -> Dict[int, Optional[int]]:
    """Player participant id (per game) -> person id, from ``joueurs``."""
    out: Dict[int, Optional[int]] = {}
    for player in _dicts(sheet.get("joueurs")):
        pid = _int(player.get("participant_id"))
        if pid is not None:
            out[pid] = _person_id(player)
    return out


def _event_key(row: Mapping[str, Any]) -> Tuple[Any, ...]:
    digits = _DIGITS.findall(str(row.get("event_id") or ""))
    return (
        row["period"] if row["period"] is not None else math.inf,
        row["game_seconds"] if row["game_seconds"] is not None else math.inf,
        _TYPE_ORDER.get(str(row.get("event_type_raw")), len(_TYPE_ORDER)),
        int(digits[-1]) if digits else 0,
    )


def parse_magnus_pbp(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Play-by-play from a game sheet's ``evenements``, in game order.

    The sheet groups events by type (every goal, then every penalty, ...), so the rows are
    sorted by period, elapsed second, then a same-second tie-break (shot, goal, penalty,
    goalie change, timeout, faceoff) and the event id. ``period`` comes from the elapsed
    seconds (see the module rule: a faceoff at 1200 / 2400 / 3600 opens the next period).
    There are no shot coordinates, and 2020-21 sheets carry no faceoff events.

    Every player column is a PERSON id (``joueurs[].id``); the on-ice lists, which the sheet
    gives in per-game participant ids, are translated through the roster (an id the roster
    lacks becomes null inside the list).

    Args:
        raw: the game sheet object (``magnus_pbp(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per event: ``event_type`` in English (``goal``, ``penalty``, ``shot``,
        ``faceoff``, ``goalie_change``, ``timeout``) beside ``event_type_raw`` (``BUT``,
        ``SANCTION``, ``TIR``, ``ENGA``, ``REMPLACANT``, ``TEMPS_MORT``), period and clock,
        team, the acting player (scorer, shooter, penalised player, faceoff winner),
        assists, the goalie, faceoff takers, goal strength, penalty details, goalie changes
        and the on-ice player lists of a goal; zero rows when empty.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.hockey.magnus import magnus_pbp
            df = magnus_pbp(game_id=77779)
            print(df.filter(pl.col("event_type") == "goal"))

    See Also:
        * :func:`sportsdataverse.hockey.magnus.magnus_pbp` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    sheet = _sheet(raw)
    game_id, roster = _game_id(sheet), _roster(sheet)
    rows = sorted((_event_row(e, game_id, roster) for e in _dicts(sheet.get("evenements"))), key=_event_key)
    return _out(_frame(rows, _PBP), return_as_pandas)


def _home_away(sheet: Mapping[str, Any], team_id: Any) -> Optional[str]:
    team = _int(team_id)
    if team is not None and team == _int(_at(sheet, "receveur", "id")):
        return "home"
    if team is not None and team == _int(_at(sheet, "visiteur", "id")):
        return "away"
    return None


_LINEUPS: Spec = {
    "game_id": (pl.Int64, _get("_game_id")),
    "team_id": (pl.Int64, _get("equipe_id")),
    "home_away": (pl.String, _get("_home_away")),
    "player_id": (pl.Int64, _person_id),
    "player_participant_id": (pl.Int64, _get("participant_id")),
    "player_name": (pl.String, _get("nom_complet")),
    "jersey_number": (pl.String, _get("numero")),
    "registered_jersey_number": (pl.String, _get("personne", "numero")),
    "licence_number": (pl.String, _get("code_adherent")),
    "position_code": (pl.String, _get("position", "code")),
    "position": (pl.String, lambda p: _POSITIONS.get(str(_at(p, "position", "code")))),
    "captaincy": (
        pl.String,
        lambda p: "".join(str(a.get("code")) for a in _dicts(p.get("attributs")) if a.get("code")) or None,
    ),
    "is_mvp": (pl.Boolean, _get("_is_mvp")),
    "photo_url": (pl.String, _get("personne", "photo_url")),
}


def parse_magnus_lineups(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """The game roster: one row per dressed player from a game sheet's ``joueurs``.

    Args:
        raw: the game sheet object (``magnus_lineups(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per player: ``player_id`` (the person id, stable across games) beside
        ``player_participant_id`` (this game's roster entry, the key the sheet's on-ice lists
        and MVPs use), name, the jersey worn and the registered one, FFHG licence number,
        position (``forward`` / ``defence`` / ``goalie``), captaincy (``C`` / ``A``) and
        whether the player was named the team's MVP; zero rows when empty.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.hockey.magnus import magnus_lineups
            df = magnus_lineups(game_id=77779)
            print(df.filter(pl.col("position") == "goalie"))

    See Also:
        * :func:`sportsdataverse.hockey.magnus.magnus_lineups` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    sheet = _sheet(raw)
    game_id = _game_id(sheet)
    mvps = {_int(m.get("participant_joueur_id")) for m in _dicts(sheet.get("mvps"))} - {None}
    rows = [
        {
            **p,
            "_game_id": game_id,
            "_home_away": _home_away(sheet, p.get("equipe_id")),
            "_is_mvp": _int(p.get("participant_id")) in mvps,
        }
        for p in _dicts(sheet.get("joueurs"))
    ]
    return _out(_frame(rows, _LINEUPS), return_as_pandas)


_SHOOTOUT: Spec = {
    "game_id": (pl.Int64, _get("_game_id")),
    "attempt_id": (pl.Int64, _get("id")),
    "shootout_round": (pl.Int64, _get("numero")),
    "team_id": (pl.Int64, _get("equipe", "id")),
    "team_abbreviation": (pl.String, _get("equipe", "abreviation")),
    "shooter_player_id": (pl.Int64, lambda a: _person_id(a.get("tireur"))),
    "shooter_name": (pl.String, _get("tireur", "nom_complet")),
    "goalie_player_id": (pl.Int64, lambda a: _person_id(a.get("gardien"))),
    "goalie_name": (pl.String, _get("gardien", "nom_complet")),
    "scored": (pl.Boolean, _get("marque")),
}


def parse_magnus_shootout(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Shootout attempts from a game sheet's ``tirs_aux_but``, in round order.

    ``shootout_round`` is the round (one attempt per team per round), not a jersey number.
    Trailing rounds that were not needed are listed with no shooter and ``scored`` null.

    Args:
        raw: the game sheet object (``magnus_shootout(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per attempt: round, team, shooter and goalie person ids and names, and
        ``scored``; zero rows when the game had no shootout.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.hockey.magnus import magnus_shootout
            df = magnus_shootout(game_id=77779)
            print(df.group_by("team_id").agg(pl.col("scored").sum()))

    See Also:
        * :func:`sportsdataverse.hockey.magnus.magnus_shootout` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    sheet = _sheet(raw)
    game_id = _game_id(sheet)
    rows = [{**a, "_game_id": game_id} for a in _dicts(sheet.get("tirs_aux_but"))]
    df = _frame(rows, _SHOOTOUT).sort(["shootout_round", "attempt_id"], nulls_last=True, maintain_order=True)
    return _out(df, return_as_pandas)
