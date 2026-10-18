"""Per-competition convenience families over the multi-competition basketball platforms.

FIBA (GDAP + game pages), Genius LiveStats and Sportradar Atrium each serve many competitions through one
parameterised family (``comp`` / ``org`` + ``comp_id`` / ``website_id``). ``mint_fiba(entry)``,
``mint_genius(entry)`` and ``mint_atrium(entry)`` bind one registry entry (see
``tools/codegen/basketball_competitions.yaml``, shipped as :mod:`._competitions_registry`) and return a dict of
``<prefix>_<suffix>`` callables, like HockeyTech's ``build_family``. Only the headline frames are minted
(schedule, standings, teams, the box / play-by-play / shots of a game, and the competition listing): the full
surface stays on the parameterised wrappers, so a new competition costs one registry line and ~8 functions.

Each homes' ``*_competitions_runtime`` module calls the minter and spreads the result into its namespace; the
minted callables carry that module as ``__module__`` so they file under the platform's source in the docs.
"""

from __future__ import annotations

import datetime as _dt
import inspect
import re
import time
from typing import Any, Callable, Dict, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse.basketball._competitions_registry import REGISTRY
from sportsdataverse.errors import AssetFetchError, NoDataError

Entry = Dict[str, Any]

_SEE_ALSO = (
    "\n        See Also:\n"
    "            * `hoopR`_ - SportsDataverse men's basketball (R)\n"
    "            * `wehoop`_ - SportsDataverse women's basketball (R)\n\n"
    "        .. _hoopR: https://hoopR.sportsdataverse.org\n"
    "        .. _wehoop: https://wehoop.sportsdataverse.org\n"
)
_FLAGS = "return_parsed: parse into a polars DataFrame (default True); False returns the raw body.\n"
_PANDAS = "return_as_pandas: with return_parsed, return a pandas DataFrame instead of polars.\n"
_KW = "**kwargs: Forwarded to the underlying HTTP getter.\n"


_TTL = 3600.0
_CACHE: Dict[tuple, Any] = {}


def _cached(key: tuple, fetch: Callable[[], Any]) -> Any:
    """``fetch()`` memoised per process for an hour: every minted call needs the same listing, so don't refetch it."""
    hit = _CACHE.get(key)
    if hit is not None and time.monotonic() - hit[0] < _TTL:
        return hit[1]
    value = fetch()
    if getattr(value, "height", 1):  # an empty listing is not worth remembering
        _CACHE[key] = (time.monotonic(), value)
    return value


def entries(platform: str) -> list[Entry]:
    """The registry entries of one platform (``fiba``, ``genius`` or ``atrium``)."""
    return list(REGISTRY[platform])


def _section(doc: str, name: str) -> str:
    """The body of a ``<name>:`` section of a generated wrapper's docstring, re-indented one level (8 spaces)."""
    m = re.search(rf"^{name}:\n(.*?)(?=^[A-Z][A-Za-z ]*:\n|\Z)", inspect.cleandoc(doc or ""), re.S | re.M)
    return _indent(m.group(1).rstrip(), "    ").rstrip("\n") if m else ""


def _indent(block: str, pad: str = "        ") -> str:
    return "".join(f"{pad}{ln}\n" if ln else "\n" for ln in block.splitlines())


class _BoundDoc(str):
    """A minted docstring that remembers the base wrapper it was built from (read by :func:`_finish`)."""

    base: Callable[..., Any]


_GENIUS_RAW = (
    "the body or page record (``dict`` / ``list``): ``data.json`` as served plus ``_request`` "
    "(the match id asked for), or the sdv-internal-refs capture shape of a page"
)


def _doc(
    entry: Entry,
    base: Callable[..., Any],
    summary: str,
    args: str,
    example: str,
    raises_extra: str = "",
    notes_extra: str = "",
    returns_raw: str = "",
) -> str:
    """Assemble a minted callable's docstring: Args from ``args``, Returns / Raises / Notes inherited from ``base``."""
    gender = {"men": "men's", "women": "women's", "both": "men's and women's"}[entry["gender"]]
    base_doc = base.__doc__ or ""
    returns = (
        _section(base_doc, "Returns")
        or "        A polars/pandas DataFrame, or the raw body when ``return_parsed=False``."
    )
    if returns_raw:  # a route with no match id: the base text describes the match routes' raw body
        if _GENIUS_RAW not in returns:
            raise RuntimeError(f"{base.__name__}: base Returns prose changed; update _GENIUS_RAW")
        returns = returns.replace(_GENIUS_RAW, returns_raw)
    raises = _section(base_doc, "Raises")
    notes = _section(base_doc, "Notes")
    out = f"{summary}\n\n    {entry['label']} ({gender}); season rule: {entry['season_rule']}.\n\n"
    out += f"    Convenience wrapper: a thin binding of :func:`{base.__name__}` to this competition, minted from the\n"
    out += "    competition registry (``tools/codegen/basketball_competitions.yaml``).\n\n"
    extra = [ln for ln in notes_extra.splitlines() if ln]
    if entry["gender"] in ("women", "both"):
        extra.append("Women's competition data: flagged for the wehoop mirror (Phase C).")
    # the docs page renders no Notes block, so the terms-of-use and owner flags ride in the description too
    bullets = re.split(r"(?m)^\s*\* ", notes)  # the first bullet is the terms-of-use flag; the rest is reference detail
    first = " ".join(bullets[1].split()) if len(bullets) > 1 else " ".join(notes.split())
    flat = f"{first} {' '.join(extra)}".strip()
    if flat.strip():
        out += f"    Read before use: {flat.strip()}\n\n"
    out += "    Args:\n" + _indent(args.rstrip("\n")) + "\n"
    out += "    Returns:\n" + returns + "\n\n"
    out += (
        "    Raises:\n"
        + (raises + "\n" if raises else "")
        + (raises_extra and _indent(raises_extra.rstrip("\n")))
        + "\n"
    )
    out += "    Example:\n        Quick start::\n\n" + _indent(example.rstrip("\n"), "            ") + _SEE_ALSO + "\n"
    out += "    Notes:\n" + notes + ("\n" if notes else "") + "".join(f"        * {ln}\n" for ln in extra)
    bound = _BoundDoc(out.rstrip() + "\n")
    bound.base = base
    return bound


def _finish(fn: Callable[..., Any], name: str, module: str, doc: str) -> Callable[..., Any]:
    """Name the minted callable and point it at its base wrapper.

    ``__sdv_base__`` is the base wrapper's name and ``__sdv_schema__`` its native returns schema
    (``native/<stem>/<short>``): the docs renderer reads the minted function's returns table from there,
    so the columns have one source of truth (``tools/codegen/schemas/native``)."""
    fn.__name__ = name
    fn.__qualname__ = name
    fn.__module__ = module
    fn.__doc__ = doc
    base = getattr(doc, "base", None)
    if base is not None:
        stem = base.__module__.rsplit(".", 1)[-1]
        fn.__sdv_base__ = base.__name__  # type: ignore[attr-defined]
        fn.__sdv_schema__ = f"native/{stem}/{base.__name__[len(stem) + 1 :]}"  # type: ignore[attr-defined]
    return fn


# --------------------------------------------------------------------------------------------------- FIBA


def _fiba_comp(family_id: int, season: Optional[int], comp: Optional[int], label: str, **kw: Any) -> int:
    """The GDAP competition id of one edition: ``comp`` as given, else the family's edition of ``season`` (newest started one if None)."""
    from sportsdataverse.basketball.fiba_hub import fiba_gdap as gdap
    from sportsdataverse.basketball.fiba_hub import fiba_runtime

    if comp is not None:
        # the family of an edition is in its getGdapCompetitionById record, which the runtime caches per process
        # and the game page needs anyway (its URL is built from the family): no extra request for a game
        fam = int(fiba_runtime._competition_slug(int(comp)).split("-", 1)[0])
        if fam != family_id:
            raise AssetFetchError(
                f"FIBA competition {comp} is an edition of family {fam}, not of {label} (family {family_id})"
            )
        return int(comp)
    ed = _cached(
        ("fiba", family_id, season),
        lambda: gdap.fiba_gdap_editions(family_id=family_id, season=season, to_season=season, **kw),
    )
    if season is None and ed.height and "start" in ed.columns:
        started = ed.filter(pl.col("start").cast(pl.Utf8) <= _dt.date.today().isoformat())
        ed = started if started.height else ed
    if ed.height == 0:
        raise NoDataError(f"FIBA lists no {label} edition for season={season}")
    if season is None:
        ed = ed.filter(pl.col("season") == ed["season"].max())
    ids = sorted({int(i) for i in ed["competition_id"].drop_nulls().to_list()})
    if not ids:
        raise NoDataError(f"FIBA lists no {label} edition id for season={season}")
    if len(ids) > 1:
        raise ValueError(f"{label} season={season} lists several editions {ids}; pass comp= to pick one")
    return ids[0]


def mint_fiba(entry: Entry, module: str) -> Dict[str, Callable[..., Any]]:
    """Mint the ``<prefix>_*`` callables of one FIBA family entry (``family_id`` = GDAP competition family)."""
    from sportsdataverse.basketball.fiba_hub import fiba_gamepage as gp
    from sportsdataverse.basketball.fiba_hub import fiba_gdap as gdap

    p, fam, label = entry["prefix"], int(entry["family_id"]), entry["label"]
    season_arg = (
        "season: Edition season key (see the season rule above). Omitted = the newest edition that has started.\n"
        "comp: FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions.\n"
        "    Checked to be an edition of this competition (one cached getGdapCompetitionById request).\n"
    )
    wrong_comp = "AssetFetchError: ``comp`` is an edition of another competition family.\n"
    out: Dict[str, Callable[..., Any]] = {}

    def editions(
        season: Optional[int] = None,
        to_season: Optional[int] = None,
        *,
        return_parsed: bool = True,
        return_as_pandas: bool = False,
        **kwargs: Any,
    ) -> Union[pl.DataFrame, pd.DataFrame, Dict]:
        return gdap.fiba_gdap_editions(
            family_id=fam,
            season=season,
            to_season=to_season,
            return_parsed=return_parsed,
            return_as_pandas=return_as_pandas,
            **kwargs,
        )

    out[f"{p}_editions"] = _finish(
        editions,
        f"{p}_editions",
        module,
        _doc(
            entry,
            gdap.fiba_gdap_editions,
            f"Every edition of {label}.",
            "season: First season (inclusive); omitted = all editions.\nto_season: Last season, inclusive.\n"
            + _FLAGS
            + _PANDAS
            + _KW,
            f"from sportsdataverse.basketball.fiba_hub import {p}_editions\n{p}_editions(season={entry['example_season']})\n",
        ),
    )

    def _comp_fn(base_name: str, suffix: str, what: str) -> None:
        base = getattr(gdap, base_name)

        def fn(
            season: Optional[int] = None,
            comp: Optional[int] = None,
            *,
            return_parsed: bool = True,
            return_as_pandas: bool = False,
            **kwargs: Any,
        ) -> Union[pl.DataFrame, pd.DataFrame, Dict]:
            return getattr(gdap, base_name)(
                comp=_fiba_comp(fam, season, comp, label, **kwargs),
                return_parsed=return_parsed,
                return_as_pandas=return_as_pandas,
                **kwargs,
            )

        out[f"{p}_{suffix}"] = _finish(
            fn,
            f"{p}_{suffix}",
            module,
            _doc(
                entry,
                base,
                what,
                season_arg + _FLAGS + _PANDAS + _KW,
                f"from sportsdataverse.basketball.fiba_hub import {p}_{suffix}\n{p}_{suffix}(season={entry['example_season']})\n",
                "ValueError: The season lists several editions (pass ``comp``).\nNoDataError: FIBA lists no edition for the season.\n"
                + wrong_comp,
            ),
        )

    _comp_fn("fiba_gdap_schedule", "schedule", f"Schedule and results of one {label} edition, one row per game.")
    _comp_fn("fiba_gdap_final_standings", "standings", f"Final standings of one {label} edition.")
    _comp_fn("fiba_gdap_teams", "teams", f"Teams of one {label} edition.")

    def _game_fn(base_name: str, suffix: str, what: str) -> None:
        base = getattr(gp, base_name)

        def fn(
            game_id: int,
            season: Optional[int] = None,
            comp: Optional[int] = None,
            *,
            return_parsed: bool = True,
            return_as_pandas: bool = False,
            **kwargs: Any,
        ) -> Union[pl.DataFrame, pd.DataFrame, Dict]:
            if season is None and comp is None:
                raise ValueError(
                    f"pass season (the edition's END year) or comp (the GDAP competition id) for a {label} game"
                )
            return getattr(gp, base_name)(
                game_id=game_id,
                comp=_fiba_comp(fam, season, comp, label, **kwargs),
                return_parsed=return_parsed,
                return_as_pandas=return_as_pandas,
                **kwargs,
            )

        out[f"{p}_{suffix}"] = _finish(
            fn,
            f"{p}_{suffix}",
            module,
            _doc(
                entry,
                base,
                what,
                "game_id: FIBA game id (game_id in the schedule).\n" + season_arg + _FLAGS + _PANDAS + _KW,
                f"from sportsdataverse.basketball.fiba_hub import {p}_schedule, {p}_{suffix}\n"
                + (
                    f"{p}_{suffix}(game_id=100720, season=2023)\n"
                    if entry["key"] == "world_cup"
                    else f"games = {p}_schedule(season={entry['example_season']})\n"
                    f"{p}_{suffix}(game_id=int(games['game_id'][0]), season={entry['example_season']})\n"
                ),
                "ValueError: Neither ``season`` nor ``comp`` is given, or the season lists several editions (pass ``comp``).\nNoDataError: FIBA lists no edition for the season.\n"
                + wrong_comp,
            ),
        )

    _game_fn("fiba_gamepage_box_players", "player_box", f"Player box score of one {label} game.")
    _game_fn("fiba_gamepage_box_teams", "team_box", f"Team box score of one {label} game.")
    _game_fn("fiba_gamepage_pbp", "pbp", f"Play-by-play of one {label} game.")
    _game_fn("fiba_gamepage_shots", "shots", f"Shot chart of one {label} game.")
    return out


# ------------------------------------------------------------------------------------------------ Genius


def _label_rule(entry: Entry) -> pl.Expr:
    """The entry's label rule over a ``label`` column: matches ``label_include`` and not ``label_exclude`` (any case)."""
    label = pl.col("label").fill_null("")
    return label.str.contains("(?i)" + entry["label_include"]) & (
        label.str.contains("(?i)" + entry["label_exclude"]) == False  # noqa: E712
    )


def _genius_check(entry: Entry, match_id: int, base_name: str, **kw: Any) -> None:
    """Refuse a match of another competition: ``data.json`` names none, so read the game's ``/u/`` shell.

    One request per match (cached per process, so the four game frames share it). The shell exists only under
    the game's own LiveStats ORG, and its competition name must pass the entry's label rule.
    """
    from sportsdataverse.basketball.genius import genius as g

    org, label = entry["live_org"], entry["label"]
    try:
        rec = _cached(
            ("genius-u", org, int(match_id)),
            lambda: g.genius_match_competition(org=org, match_id=match_id, return_parsed=False, **kw),
        )
    except NoDataError:
        raise AssetFetchError(
            f"match {match_id} is not a {label} game: Genius has no /u/{org}/{match_id}/ shell "
            f"(another org's game); read it with {base_name}(match_id=...)"
        ) from None
    name = rec.get("competition_name")
    if not pl.DataFrame({"label": [name]}, schema={"label": pl.Utf8}).filter(_label_rule(entry)).height:
        raise AssetFetchError(
            f"match {match_id} belongs to {name!r} (comp_id {rec.get('comp_id')}), not to {label}; "
            f"read it with {base_name}(match_id=...)"
        )


def _genius_comp(entry: Entry, season: Optional[int], comp_id: Optional[int], **kw: Any) -> int:
    """The Genius competition id of ``season``: ``comp_id`` as given, else the org competition whose label matches the entry."""
    from sportsdataverse.basketball.genius import genius as g

    if comp_id is not None:
        return int(comp_id)
    frame = _cached(("genius", entry["org"]), lambda: g.genius_competitions(org=entry["org"], **kw))
    hit = frame.filter(_label_rule(entry))
    if season is None:
        hit = hit.filter(pl.col("season") == hit["season"].max()) if hit.height else hit
    else:
        hit = hit.filter(pl.col("season") == season)
    if hit.height == 0:
        raise NoDataError(f"Genius lists no {entry['label']} competition for season={season}")
    ids = sorted({int(i) for i in hit["comp_id"].drop_nulls().to_list()})
    if not ids:
        raise NoDataError(f"Genius lists no {entry['label']} competition id for season={season}")
    if len(ids) > 1:
        raise ValueError(
            f"{entry['label']} season={season} matches several competitions {ids}; pass comp_id= to pick one"
        )
    return ids[0]


def mint_genius(entry: Entry, module: str) -> Dict[str, Callable[..., Any]]:
    """Mint the ``<prefix>_*`` callables of one Genius entry (``org`` + a label rule that picks the headline competition)."""
    from sportsdataverse.basketball.genius import genius as g

    p, org, label = entry["prefix"], entry["org"], entry["label"]
    out: Dict[str, Callable[..., Any]] = {}

    def competitions(
        *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
    ) -> Union[pl.DataFrame, pd.DataFrame, Dict]:
        return g.genius_competitions(org=org, return_parsed=return_parsed, return_as_pandas=return_as_pandas, **kwargs)

    out[f"{p}_competitions"] = _finish(
        competitions,
        f"{p}_competitions",
        module,
        _doc(
            entry,
            g.genius_competitions,
            f"Every competition of the {org} Genius org, with its END-year season.",
            _FLAGS + _PANDAS + _KW,
            f"from sportsdataverse.basketball.genius import {p}_competitions\n{p}_competitions()\n",
            returns_raw="the chooser page record (``dict``): the sdv-internal-refs capture shape plus ``_request`` (the org asked for)",
        ),
    )

    def schedule(
        season: Optional[int] = None,
        comp_id: Optional[int] = None,
        round_number: Optional[int] = -1,
        *,
        return_parsed: bool = True,
        return_as_pandas: bool = False,
        **kwargs: Any,
    ) -> Union[pl.DataFrame, pd.DataFrame, Dict]:
        return g.genius_schedule(
            org=org,
            comp_id=_genius_comp(entry, season, comp_id, **kwargs),
            round_number=round_number,
            return_parsed=return_parsed,
            return_as_pandas=return_as_pandas,
            **kwargs,
        )

    out[f"{p}_schedule"] = _finish(
        schedule,
        f"{p}_schedule",
        module,
        _doc(
            entry,
            g.genius_schedule,
            f"Every match of one {label} season (hosted WebStats): ids, status, venue, teams and scores.",
            "season: END-year season (see the season rule above). Omitted = the newest season the org lists.\n"
            "comp_id: Genius competition id; skips the season lookup (needed when the label rule matches several competitions).\n"
            "round_number: Round to list; -1 (default) lists every round.\n" + _FLAGS + _PANDAS + _KW,
            f"from sportsdataverse.basketball.genius import {p}_schedule\n{p}_schedule(season=2026)\n",
            "ValueError: The season matches several competitions (pass ``comp_id``).\nNoDataError: The org lists no matching competition.\n",
            f"The competition is picked from {org}'s chooser by the label rule include={entry['label_include']!r} "
            f"exclude={entry['label_exclude']!r}; cups and play-offs have their own comp_id.\n",
            returns_raw="the schedule page record (``dict``): the sdv-internal-refs capture shape of the page",
        ),
    )

    def _game_fn(base_name: str, suffix: str, what: str) -> None:
        base = getattr(g, base_name)

        def fn(
            match_id: int, *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
        ) -> Union[pl.DataFrame, pd.DataFrame, Dict]:
            res = getattr(g, base_name)(
                match_id=match_id, return_parsed=return_parsed, return_as_pandas=return_as_pandas, **kwargs
            )
            _genius_check(entry, match_id, base_name, **kwargs)
            return res

        out[f"{p}_{suffix}"] = _finish(
            fn,
            f"{p}_{suffix}",
            module,
            _doc(
                entry,
                base,
                what,
                "match_id: Genius match id (match_id in the schedule) of a game of this competition.\n"
                + _FLAGS
                + _PANDAS
                + _KW,
                f"from sportsdataverse.basketball.genius import {p}_schedule, {p}_{suffix}\n"
                f"games = {p}_schedule()\n"
                f"{p}_{suffix}(match_id=int(games['match_id'][0]))\n",
                f"AssetFetchError: The match is not a game of this competition (another org's game, or a cup or "
                f"play-off of {org}).\n",
                notes_extra="Genius match ids are one global sequence shared by every customer, and data.json names no "
                f"competition, so the id is checked on the game's LiveStats shell (/u/{entry['live_org']}/<match_id>/: one "
                f"extra request per game, cached per process): its competition name must pass the label rule. "
                f"{base_name}(match_id=...) reads any game unchecked.\n",
            ),
        )

    _game_fn("genius_team_box", "team_box", f"Team box score of a finished {label} game.")
    _game_fn("genius_player_box", "player_box", f"Player box score of a finished {label} game.")
    _game_fn("genius_pbp", "pbp", f"Play-by-play of a finished {label} game.")
    _game_fn("genius_shots", "shots", f"Shot chart of a finished {label} game.")
    return out


# ------------------------------------------------------------------------------------------------ Atrium


def _atrium_check(site: int, label: str, res: Any, **kw: Any) -> None:
    """Refuse a game of another site: its ``seasonId`` (in the body and every game frame) must be one of the site's.

    Atrium seasonIds are never shared across sites (sdv-internal-refs atrium-embed/README.md), and a cache sits in
    front of the API, so the game routes' ``links.self`` echo alone does not prove the site.
    """
    from sportsdataverse.basketball.atrium import atrium_runtime

    if isinstance(res, dict):
        raw = [(res.get("data") or {}).get("seasonId")]
    else:
        raw = list(res["season_id"]) if "season_id" in res.columns else []
    got = {s for s in raw if isinstance(s, str)}
    listed = {r.get("season_id") for r in atrium_runtime._fetch_seasons(site, **kw)}
    stray = sorted(got - listed)
    if listed and stray:
        raise AssetFetchError(f"Atrium site {site}: season {stray[0]} is not a {label} season (a game of another site)")


def mint_atrium(entry: Entry, module: str) -> Dict[str, Callable[..., Any]]:
    """Mint the ``<prefix>_*`` callables of one Atrium site entry (``website_id``)."""
    from sportsdataverse.basketball.atrium import atrium as a

    p, site, label = entry["prefix"], int(entry["website_id"]), entry["label"]
    out: Dict[str, Callable[..., Any]] = {}
    sel = (
        "season: Season END year (see the season rule above). Omit season and season_id for the site's current season.\n"
        "season_id: Season UUID (season_id in the seasons listing); skips the END-year lookup.\n"
        "competition_id: Competition UUID (a site carries many: conferences, cups); needed when several share the season.\n"
    )

    def seasons(
        locale: Optional[str] = None, *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
    ) -> Union[pl.DataFrame, pd.DataFrame, Dict]:
        return a.atrium_seasons(
            website_id=site, locale=locale, return_parsed=return_parsed, return_as_pandas=return_as_pandas, **kwargs
        )

    out[f"{p}_seasons"] = _finish(
        seasons,
        f"{p}_seasons",
        module,
        _doc(
            entry,
            a.atrium_seasons,
            f"Every season and competition id of {label}.",
            "locale: Label locale, e.g. en-EN; omitted = the site's own language.\n" + _FLAGS + _PANDAS + _KW,
            f"from sportsdataverse.basketball.atrium import {p}_seasons\n{p}_seasons()\n",
        ),
    )

    def _sel_fn(base_name: str, suffix: str, what: str) -> None:
        base = getattr(a, base_name)

        def fn(
            season: Optional[int] = None,
            season_id: Optional[str] = None,
            competition_id: Optional[str] = None,
            locale: Optional[str] = None,
            *,
            return_parsed: bool = True,
            return_as_pandas: bool = False,
            **kwargs: Any,
        ) -> Union[pl.DataFrame, pd.DataFrame, Dict]:
            return getattr(a, base_name)(
                website_id=site,
                season=season,
                season_id=season_id,
                competition_id=competition_id,
                locale=locale,
                return_parsed=return_parsed,
                return_as_pandas=return_as_pandas,
                **kwargs,
            )

        out[f"{p}_{suffix}"] = _finish(
            fn,
            f"{p}_{suffix}",
            module,
            _doc(
                entry,
                base,
                what,
                sel + "locale: Label locale, e.g. en-EN; omitted = the site's own language.\n" + _FLAGS + _PANDAS + _KW,
                f"from sportsdataverse.basketball.atrium import {p}_{suffix}\n{p}_{suffix}(season=2026)\n",
            ),
        )

    _sel_fn("atrium_fixtures", "schedule", f"Fixtures (schedule and results) of a {label} season.")
    _sel_fn("atrium_standings", "standings", f"Standings of a {label} season.")
    _sel_fn("atrium_teams", "teams", f"Teams of a {label} season.")

    def _game_fn(base_name: str, suffix: str, what: str) -> None:
        base = getattr(a, base_name)

        def fn(
            fixture_id: str,
            locale: Optional[str] = None,
            *,
            return_parsed: bool = True,
            return_as_pandas: bool = False,
            **kwargs: Any,
        ) -> Union[pl.DataFrame, pd.DataFrame, Dict]:
            res = getattr(a, base_name)(
                website_id=site,
                fixture_id=fixture_id,
                locale=locale,
                return_parsed=return_parsed,
                return_as_pandas=return_as_pandas,
                **kwargs,
            )
            _atrium_check(site, label, res, **kwargs)
            return res

        out[f"{p}_{suffix}"] = _finish(
            fn,
            f"{p}_{suffix}",
            module,
            _doc(
                entry,
                base,
                what,
                "fixture_id: Fixture UUID (fixture_id in the schedule).\nlocale: Label locale, e.g. en-EN; omitted = the site's own language.\n"
                + _FLAGS
                + _PANDAS
                + _KW,
                f"from sportsdataverse.basketball.atrium import {p}_schedule, {p}_{suffix}\n"
                f"games = {p}_schedule()\n"
                f"{p}_{suffix}(fixture_id=games['fixture_id'][0])\n",
                "AssetFetchError: The game's season is not one of this site's seasons (a fixture of another site).\n",
                notes_extra="The game is checked against this site: its seasonId must be in the site's season list "
                "(one request per site, cached for an hour, the same list the season lookup reads).\n",
            ),
        )

    _game_fn("atrium_box_score", "player_box", f"Player box score of a {label} game.")
    _game_fn("atrium_team_box_score", "team_box", f"Team box score of a {label} game.")
    _game_fn("atrium_pbp", "pbp", f"Play-by-play of a {label} game.")
    _game_fn("atrium_shots", "shots", f"Shot chart of a {label} game.")
    return out
