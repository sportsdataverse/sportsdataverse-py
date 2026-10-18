"""Generate the ``fiba_gdap`` and ``fiba_gamepage`` flat-API endpoint YAMLs, returns-schemas, pinned
dtypes and column-description notes from the FIBA OpenAPI specs (``sdv-internal-refs/fiba/``).

Idempotent: same specs + same fixtures -> byte-identical output. Modeled on ``gen_magnus.py``.

Two sources, one home (``sportsdataverse.basketball.fiba_hub``):

* **``fiba_gdap``**: the GDAP JSON API (``digital-api.fiba.basketball/hapi/<method>``). One wrapper per
  parsed view; a route whose body holds two tables (a game: header, periods, officials; a roster:
  players, coaches) gets one wrapper per table, all reading the same route. ``getcustomgateway`` is two
  wrappers (``gateway_federation_ranking``, ``gateway_competition_ids``) with ``fibaFunction`` fixed.
  The competition is the ``comp`` argument of every competition-scoped route.
* **``fiba_gamepage``**: box score, play-by-play and shots, which exist only in the game page's RSC flight
  payload. The wrappers take ``game_id`` and ``comp``; the runtime builds the real URL (the spec's three
  templates) from GDAP, so the route here is the pseudo-path ``/en/games/{game_id}``.

Dtypes are derived from every fixture of a parser (the FULL bodies under ``tests/fixtures/<stem>/full/``
plus the truncated refs captures) with the committed dtypes switched off, merged int + float -> float
and rate-named columns -> float, then written to ``fiba_dtypes.py``; the returns-schemas list those same
columns and types. Column descriptions are NOT written into the schemas: the generator owns their text
(``tools/codegen/notes/fiba_*_descriptions.yaml``, taken from ``fiba-returns.md`` and the tables below),
which Integration merges into ``manual_column_descriptions.yaml``. A snapshot of the spec's routes goes to
``tests/fixtures/fiba_gdap/routes.json`` so the tests never read the private refs checkout.

Run: ``python tools/codegen/gen_fiba.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import ROOT, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.fiba_hub import fiba_dtypes, fiba_frames
from sportsdataverse.basketball.fiba_hub import fiba_gamepage_parsers as pp
from sportsdataverse.basketball.fiba_hub import fiba_gdap_parsers as gp
from sportsdataverse.dl_utils import underscore

GDAP_SPEC = "fiba-gdap.openapi.yaml"
PAGE_SPEC = "fiba-gamepage.openapi.yaml"
GDAP_HOST = "https://digital-api.fiba.basketball/hapi"
SITE_HOST = "https://www.fiba.basketball"
GETTER = "sportsdataverse.basketball.fiba_hub.fiba_runtime"
FIXTURES = ROOT / "tests" / "fixtures"
NOTES_DIR = ROOT / "tools" / "codegen" / "notes"
HOME = "basketball.fiba_hub"

# --------------------------------------------------------------------------------------------------------------------
# routes: short -> (spec method, params, fixed params, parser module, fixtures)
# a param is (python name, wire key, type, required, transform, default)
# --------------------------------------------------------------------------------------------------------------------
COMP = ("comp", "gdapCompetitionId", "int", True, "_int_id", None)
GAME = ("game_id", "gdapGameId", "int", True, "_int_id", None)
TEAM = ("team_id", "gdapTeamId", "int", True, "_int_id", None)
PLAYER = ("player_id", "gdapPlayerId", "int", True, "_int_id", None)
FAMILY = ("family_id", "competitionMarketingNameId", "int", True, "_int_id", None)
DISCIPLINE = ("discipline_format", "disciplineFormat", "str", False, None, None)
_C = "gdap__"


def _fx(method: str, *labels: str) -> List[str]:
    return [f"{_C}{method}__{label}.json" for label in labels]


GDAP: Dict[str, Dict[str, Any]] = {
    "schedule": dict(
        method="getgdapgamesbycompetitionid",
        params=[COMP],
        fixtures=_fx(
            "getgdapgamesbycompetitionid", "bcl2025", "elw2025", "paris2024-men", "wc1994", "wc2023", "wwc2022"
        )
        + ["full/getgdapgamesbycompetitionid__paris2024-men.json"],
        example={"comp": 208182},
    ),
    "games_between": dict(
        method="getgdapgamesbetweentwodates",
        params=[
            ("date_from", "dateFrom", "str", True, "_window_date", None),
            ("date_to", "dateTo", "str", True, "_window_date", None),
        ],
        fixtures=_fx("getgdapgamesbetweentwodates", "wc2023-final-weekend"),
        example={"date_from": "2023-09-08", "date_to": "2023-09-11"},
    ),
    "game": dict(
        method="getgdapgamebyid",
        params=[GAME],
        fixtures=_fx("getgdapgamebyid", "bcl2025", "elw2025", "paris2024-final", "wc2023-final", "wwc2022-final"),
        example={"game_id": 100720},
    ),
    "game_periods": dict(
        method="getgdapgamebyid",
        params=[GAME],
        fixtures=_fx("getgdapgamebyid", "bcl2025", "elw2025", "paris2024-final", "wc2023-final", "wwc2022-final"),
        example={"game_id": 100720},
    ),
    "game_officials": dict(
        method="getgdapgamebyid",
        params=[GAME],
        fixtures=_fx("getgdapgamebyid", "bcl2025", "elw2025", "paris2024-final", "wc2023-final", "wwc2022-final"),
        example={"game_id": 100720},
    ),
    "competition": dict(
        method="getGdapCompetitionById",
        params=[COMP],
        fixtures=_fx("getGdapCompetitionById", "wc1994", "wc2023"),
        example={"comp": 208182},
    ),
    "editions": dict(
        method="getgdapcompetitionsbyseasonsandcompetitionmarketingname",
        params=[
            FAMILY,
            ("season", "fromSeason", "int", False, "_season", None),
            ("to_season", "toSeason", "int", False, "_season", None),
        ],
        fixtures=_fx("getgdapcompetitionsbyseasonsandcompetitionmarketingname", "bcl-2025", "world-cup-1950-2027"),
        example={"family_id": 201, "season": 2023},
    ),
    "teams": dict(
        method="getgdapcompetitionteamsbycompetitionid",
        params=[COMP],
        fixed={"profile": "true"},
        fixtures=_fx("getgdapcompetitionteamsbycompetitionid", "wc2023"),
        example={"comp": 208182},
    ),
    "rounds": dict(
        method="getgdaproundsbycompetitionid",
        params=[COMP],
        fixtures=_fx("getgdaproundsbycompetitionid", "bcl2025", "wc2023")
        + ["full/getgdaproundsbycompetitionid__wc2023.json"],
        example={"comp": 208182},
    ),
    "round_standings": dict(
        method="getgdaproundsbycompetitionid",
        params=[COMP],
        fixtures=_fx("getgdaproundsbycompetitionid", "bcl2025", "wc2023")
        + ["full/getgdaproundsbycompetitionid__wc2023.json"],
        example={"comp": 208182},
    ),
    "final_standings": dict(
        method="getgdapcompetitionfinalstandingbycompetitionid",
        params=[COMP],
        fixed={"profile": "true"},
        fixtures=_fx("getgdapcompetitionfinalstandingbycompetitionid", "wc2023"),
        example={"comp": 208182},
    ),
    "round_gamedays": dict(
        method="getgdapcompetitionroundgamedaysbyid",
        params=[COMP],
        fixtures=_fx("getgdapcompetitionroundgamedaysbyid", "wc2023"),
        example={"comp": 208182},
    ),
    "awards": dict(
        method="getgdapcompetitionawardsbyid",
        params=[COMP, DISCIPLINE],
        fixtures=_fx("getgdapcompetitionawardsbyid", "wc2023"),
        example={"comp": 208182},
    ),
    "nominations": dict(
        method="getgdapcompetitionnominationsbyid",
        params=[COMP, ("level", "level", "str", False, None, "competition")],
        fixtures=_fx("getgdapcompetitionnominationsbyid", "wc2023"),
        example={"comp": 208182},
    ),
    "team_staff": dict(
        method="getgdapteamstaffbycompetitionid",
        params=[COMP],
        fixtures=_fx("getgdapteamstaffbycompetitionid", "wc2023"),
        example={"comp": 208182},
    ),
    "roster": dict(
        method="getgdapcompetitionteamrosterbyteamid",
        params=[TEAM, DISCIPLINE],
        fixtures=_fx("getgdapcompetitionteamrosterbyteamid", "wc2023-ger")
        + ["full/getgdapcompetitionteamrosterbyteamid__wc2023-ger.json"],
        example={"team_id": 279569},
    ),
    "roster_coaches": dict(
        method="getgdapcompetitionteamrosterbyteamid",
        params=[TEAM, DISCIPLINE],
        fixtures=_fx("getgdapcompetitionteamrosterbyteamid", "wc2023-ger")
        + ["full/getgdapcompetitionteamrosterbyteamid__wc2023-ger.json"],
        example={"team_id": 279569},
    ),
    "roster_latest": dict(
        method="getgdapcompetitionteamlatestrosterbyteamid",
        params=[TEAM],
        fixtures=_fx("getgdapcompetitionteamlatestrosterbyteamid", "bcl2025-oostende"),
        example={"team_id": 282183},
    ),
    "team_stats": dict(
        method="getgdapcompetitionteamstatisticsbyteamid",
        params=[TEAM],
        fixtures=_fx("getgdapcompetitionteamstatisticsbyteamid", "wc2023-ger")
        + ["full/getgdapcompetitionteamstatisticsbyteamid__wc2023-ger.json"],
        example={"team_id": 279569},
    ),
    "team_player_stats": dict(
        method="getgdapcompetitionteamstatisticsbyteamid",
        params=[TEAM],
        fixtures=_fx("getgdapcompetitionteamstatisticsbyteamid", "wc2023-ger")
        + ["full/getgdapcompetitionteamstatisticsbyteamid__wc2023-ger.json"],
        example={"team_id": 279569},
    ),
    "team_games": dict(
        method="getgdapgameswithleaderdetailsbyteamid",
        params=[TEAM],
        fixtures=_fx("getgdapgameswithleaderdetailsbyteamid", "wc2023-ger"),
        example={"team_id": 279569},
    ),
    "player_info": dict(
        method="getgdapplayerinfobycompetitionid",
        params=[PLAYER, COMP],
        fixtures=_fx("getgdapplayerinfobycompetitionid", "wc2023-bonga"),
        example={"player_id": 211501, "comp": 208182},
    ),
    "player_stats": dict(
        method="getgdapplayercompetitionstatisticsbyplayerid",
        params=[PLAYER, COMP],
        fixtures=_fx("getgdapplayercompetitionstatisticsbyplayerid", "wc2023-bonga"),
        example={"player_id": 211501, "comp": 208182},
    ),
    "player_game_stats": dict(
        method="getgdapplayergamestatisticsbyplayerid",
        params=[PLAYER, COMP],
        fixtures=_fx("getgdapplayergamestatisticsbyplayerid", "wc2023-bonga")
        + ["full/getgdapplayergamestatisticsbyplayerid__wc2023-bonga.json"],
        example={"player_id": 211501, "comp": 208182},
    ),
    "federation_games": dict(
        method="getgdapfederationgamesbyfederationid",
        params=[("federation_id", "gdapFederationId", "int", True, "_int_id", None)],
        fixtures=_fx("getgdapfederationgamesbyfederationid", "ger"),
        example={"federation_id": 64},
    ),
    "national_teams": dict(
        method="getgdapnationalteams", params=[], fixtures=_fx("getgdapnationalteams", "all"), example={}
    ),
    "medal_count": dict(
        method="getgdapmedalcount",
        params=[FAMILY],
        fixtures=_fx("getgdapmedalcount", "world-cup"),
        example={"family_id": 201},
    ),
    "ranking_zones": dict(
        method="getgdaprankingzones", params=[], fixtures=_fx("getgdaprankingzones", "all"), example={}
    ),
    "federation_ranking": dict(
        method="getgdapfederationsranking",
        params=[
            ("category", "gdapCategory", "str", False, None, "men"),
            ("zone_id", "zoneRankingId", "int", False, "_int_id", 1),
            ("as_of_date", "asOfDate", "str", False, None, None),
        ],
        fixtures=_fx("getgdapfederationsranking", "men-world-2025-09-15"),
        example={"category": "men", "zone_id": 1, "as_of_date": "2025-09-15"},
    ),
    "gateway_federation_ranking": dict(
        method="getcustomgateway",
        params=[
            ("gender", "gender", "str", False, None, "men"),
            ("zone_id", "zoneId", "int", False, "_int_id", 1),
            ("as_of_date", "asOfDate", "str", False, None, None),
        ],
        fixed={"fibaFunction": "federationRanking"},
        fixtures=_fx("getcustomgateway", "federationRanking-men-world"),
        example={"gender": "men", "zone_id": 1, "as_of_date": "2025-09-15"},
    ),
    "gateway_competition_ids": dict(
        method="getcustomgateway",
        params=[
            ("start_date", "startDate", "str", True, "_window_date", None),
            ("end_date", "endDate", "str", True, "_window_date", None),
        ],
        fixed={"fibaFunction": "competitionIdsBetweenTwoDates"},
        fixtures=_fx("getcustomgateway", "competitionIdsBetweenTwoDates-2025-01-14"),
        example={"start_date": "2025-01-14", "end_date": "2025-01-16"},
    ),
}

PAGE_FIXTURES = [
    "page__bcl__bcl2027-current.json",
    "page__events__elw2025.json",
    "page__events__paris2024-final.json",
    "page__history__bcl2025.json",
    "page__history__wc2023-final.json",
    "page__history__wwc2022-final.json",
    "full/page__wc2023-final.json",
    "full/page__elw2025.json",
]
PAGE: Dict[str, Dict[str, Any]] = {
    short: dict(fixtures=PAGE_FIXTURES)
    for short in ("pbp", "shots", "box_players", "box_teams", "period_scores", "lineup")
}

#: spec methods deliberately not wrapped (none: the one uncaptured route, getgdapteamrankingincompetitionbycompetitionid,
#: answered {} for every team probed on 2026-10-12 and is not in the spec)
UNWRAPPED: set = set()

NOTES = [
    "FIBA's terms restrict this source. The GDAP developer-portal terms limit API use to 'Authorized Users' "
    "(federations, organisers, FIBA partners) and say any broader use 'is not authorised'; the fiba.basketball "
    "terms say no portion of the site may be duplicated, redistributed or manipulated. The package owner reviewed "
    "both clauses on 2026-10-12, accepted them and approved the full intake, published data releases included. "
    "Users who are not themselves authorised by FIBA should check the terms before using the wrappers.",
    "The GDAP subscription key is public (fiba.basketball ships it in every page); the runtime reads it from the "
    "live page at run time and never logs it. Officials' contact details and coaches' and officials' dates of "
    "birth are dropped from every body.",
    "Id key spaces: competition_id is one per edition (2023 World Cup = 208182) and game_id is global across "
    "competitions (the <gameId> of a game-page URL). team_id is per team per competition (Germany is 279569 at the "
    "2023 World Cup and 281545 at Paris 2024), while organisation_id is the stable federation or club (64 = Germany, "
    "every year); the game page's T_<id> and the play-by-play oId are organisation ids, not team ids.",
]
RAISES = [
    "sportsdataverse.errors.NoDataError: FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for "
    "an unknown id, a game the competition does not list, or a game page with no box score).",
    "sportsdataverse.errors.AssetFetchError: the fetch failed, or the answer is not about the request (an error "
    "envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, "
    "federation, family or season, the HTTP 200 'not-found shell' of a wrong game page, a live game).",
    "ValueError: HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp.",
]
SEE_ALSO = [
    {
        "name": "FIBA",
        "url": "https://www.fiba.basketball/",
        "note": "the federation whose events and clubs the wrappers read",
    },
]

# --------------------------------------------------------------------------------------------------------------------
# dtypes
# --------------------------------------------------------------------------------------------------------------------
_CODE = {pl.Int64: "i", pl.Float64: "f", pl.Boolean: "b", pl.String: "s"}
_DOC = {"i": "integer", "f": "numeric", "b": "logical", "s": "character"}


def _load(stem: str, name: str) -> Any:
    return json.loads((FIXTURES / stem / name).read_text(encoding="utf-8"))


def _merge(codes: List[str], col: str) -> str:
    seen = set(codes)
    if fiba_frames._RATE.search(col) and seen <= {"i", "f"}:
        return "f"
    if not seen:
        return fiba_frames._code_for(col)
    if seen == {"i", "f"}:
        return "f"
    if len(seen) == 1:
        return seen.pop()
    return "s"


def _derive(parser: Any, stem: str, fixtures: List[str]) -> Dict[str, List[str]]:
    """Ordered ``{column: [observed codes]}`` for a parser, from every fixture, with the committed dtypes switched off."""
    saved, fiba_dtypes.DTYPES = fiba_dtypes.DTYPES, {}
    try:
        order: List[str] = []
        seen: Dict[str, List[str]] = {}
        for name in sorted(fixtures, key=lambda n: (not n.startswith("full/"), n)):  # FULL bodies first
            df = parser(_load(stem, name))
            if df.height == 0:
                raise SystemExit(f"{parser.__name__}: fixture {name} parses to zero rows")
            for col, dtype in df.schema.items():
                if col not in seen:
                    order.append(col)
                    seen[col] = []
                if df[col].null_count() < df.height:  # an all-null column is typed by its name: not an observation
                    seen[col].append(_CODE.get(dtype, "s"))
        return {col: seen[col] for col in order}
    finally:
        fiba_dtypes.DTYPES = saved


def _family_codes(observed: Dict[str, Dict[str, List[str]]]) -> Dict[str, Dict[str, str]]:
    """``{parser: {column: code}}`` for one family; a column no fixture of its parser fills takes the codes the
    family's other parsers observed for it, so one column name has one dtype across the family (the game page's
    team lines carry no ``BSR``, so ``box_teams.blocks_received`` is always null: Int64 like ``box_players``)."""
    family: Dict[str, List[str]] = {}
    for cols in observed.values():
        for col, codes in cols.items():
            family.setdefault(col, []).extend(codes)
    return {p: {c: _merge(codes or family[c], c) for c, codes in cols.items()} for p, cols in observed.items()}


# --------------------------------------------------------------------------------------------------------------------
# descriptions
# --------------------------------------------------------------------------------------------------------------------
_ROW = re.compile(r"^\|\s*`([^`]+)`\s*\|\s*([^|]*?)\s*\|\s*(.*?)\s*\|\s*$")
_SECTION = re.compile(r"^## `GET /([A-Za-z]+)`(?: \(`([A-Za-z]+)`\))?")


def _sections(md: Path) -> Dict[str, List[Tuple[str, str, str]]]:
    """``{method[:view]: [(path, type, description)]}`` from ``fiba-returns.md``; ``GamePage`` for the page objects."""
    out: Dict[str, List[Tuple[str, str, str]]] = {}
    key: Optional[str] = None
    for line in md.read_text(encoding="utf-8").splitlines():
        m = _SECTION.match(line)
        if m:
            key = m.group(1) + (f":{m.group(2)}" if m.group(2) else "")
            out[key] = []
        elif line.startswith("## `GamePage`"):
            key = "GamePage"
            out[key] = []
        elif line.startswith("# "):
            key = None if key is None or line.startswith("# Game") is False else key
        m = _ROW.match(line)
        if m and key and m.group(1) != "col_name":
            out[key].append((m.group(1), m.group(2), m.group(3)))
    return out


_DROP = re.compile(r"(^|\.)(\[\]|<period>)(?=\.|$)")


def _tokens(path: str) -> List[str]:
    return [t for t in re.split(r"\.", path) if t not in ("[]", "<period>", "items")] or [path]


def _desc_maps(rows: List[Tuple[str, str, str]]) -> Dict[str, str]:
    """``{snake column: description}`` for every md path, under every suffix of its path (longest first wins)."""
    out: Dict[str, str] = {}
    for path, typ, desc in sorted(rows, key=lambda r: len(r[0]), reverse=True):
        toks = _tokens(path)
        text = desc.rstrip(".") + ("" if typ not in ("array",) else "") + "."
        for i in range(len(toks)):
            key = underscore("_".join(toks[i:]))
            out.setdefault(key, text)
    return out


# ids and parser-added head columns, described once and used by name where the md has no row for them
HEAD: Dict[str, str] = {
    "game_id": "FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL.",
    "competition_id": "FIBA competition id (Int64): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); the comp argument.",
    "player_id": "FIBA person id (Int64), global across competitions and roles: roster, statistics, play-by-play pId and box P_<id> all use it.",
    "team_id": "FIBA team id (Int64), one per team PER COMPETITION (Germany: 279569 at the 2023 World Cup, 281545 at Paris 2024); join across seasons on organisation_id.",
    "team_code": "Team code (GER, USA, OOST), the three letters of a game-page URL slug.",
    "team_name": "Team name as registered for the competition.",
    "organisation_id": "Stable federation or club id (Int64), the same in every competition and season (64 = Germany); the game page's T_<id> and the play-by-play oId.",
    "round_id": "Round id (Int64) of the competition; the first segment of a game's gameName.",
    "round_name": "Round name (Preliminary Round, Final...).",
    "round_code": "Round code (F final, G group...).",
    "group_name": "Group name within the round (A, B...).",
    "group_status_code": "Group status code (COMPL completed).",
    "team_side": "A or B: whether the row is the schedule's team A or team B.",
    "statistic_system": "Live-stats system of the game: OVR (World Cup, Olympics) or FLS (club competitions, most qualifiers).",
    "season": "Season key (Int64): the END year for club competitions, the event year for national-team events, the target tournament year for qualifiers.",
}


def _clean(text: str) -> str:
    return " ".join(text.split())


def _describe(col: str, short: str, md: Dict[str, str], extra: Dict[str, str], listy: bool) -> str:
    for src in (extra, md, HEAD):
        if col in src:
            text = _clean(src[col])
            return text + (" Stored as a JSON string." if listy and "JSON" not in text else "") if src is md else text
    return ""


# --------------------------------------------------------------------------------------------------------------------
# endpoint YAML
# --------------------------------------------------------------------------------------------------------------------
def _spec_params(spec: dict, method: str) -> Dict[str, dict]:
    return {p["name"]: p for p in spec["paths"][f"/{method}"]["get"].get("parameters", []) if p["in"] == "query"}


def _clean_text(text: Any) -> str:
    return " ".join(str(text or "").split())


def _param_entry(spec_p: Dict[str, dict], p: tuple) -> Dict[str, Any]:
    name, wire, typ, required, transform, default = p
    desc = _clean_text(spec_p[wire].get("description"))
    entry: Dict[str, Any] = {"name": name, "query_key": wire, "type": typ, "description": desc}
    if required:
        entry["required"] = True
        if not desc.startswith("Required"):
            entry["description"] = f"Required. {desc}"
    if transform:
        entry["transform"] = transform
    if default is not None:
        entry["default"] = default
    return entry


EXTRA_PARAM_DOC = {
    "comp": "Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row "
    "(208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). Find it with fiba_gdap_editions or "
    "fiba_gdap_games_between.",
    "season": "Season key: the END year for club competitions (BCL 2024-25 = 2025), the event year for national-team "
    "events (World Cup 2023 = 2023); qualifiers carry the TARGET tournament year (World Cup 2027 qualifiers played in "
    "2025 = 2027). Sent unchanged. First season, inclusive; omitted = no lower bound.",
    "to_season": "Last season, inclusive (same key as season). Omitted with season given = the same season.",
    "date_from": "Required. Window start as YYYY-MM-DD, compared with the venue-local gameDateTime (not UTC).",
    "date_to": "Required. Window end as YYYY-MM-DD, EXCLUSIVE (date_from = date_to returns nothing).",
    "family_id": "Required. Competition family id (competitionMarketingName.competitionMarketingNameId): 201 = World Cup, "
    "320 = men's Olympics, 306 = Women's World Cup, 112 = BCL tier 1, 211 = EuroLeague Women.",
    "start_date": "Required. Window start as YYYY-MM-DD.",
    "end_date": "Required. Window end as YYYY-MM-DD.",
}


def _gdap_entry(short: str, cfg: Dict[str, Any], spec: dict) -> Dict[str, Any]:
    spec_p = _spec_params(spec, cfg["method"])
    params = [_param_entry(spec_p, p) for p in cfg["params"]]
    for entry in params:
        if entry["name"] in EXTRA_PARAM_DOC:
            entry["description"] = EXTRA_PARAM_DOC[entry["name"]]
    wire = {p["query_key"] for p in params} | set(cfg.get("fixed", {}))
    missing = {n for n, p in spec_p.items() if p.get("required")} - wire
    if missing:
        raise SystemExit(f"fiba_gdap {short}: required spec params not sent: {sorted(missing)}")
    parser = f"parse_fiba_gdap_{short}"
    doc = (getattr(gp, parser).__doc__ or "").strip().splitlines()[0]
    entry: Dict[str, Any] = {
        "short": short,
        "summary": doc,
        "path": f"/{cfg['method']}",
        "extra_params": params,
        "example_args": cfg["example"],
        "parser": parser,
        "returns_schema": f"native/fiba_gdap/{short}",
    }
    if cfg.get("fixed"):
        entry["fixed_params"] = dict(cfg["fixed"])
    return entry


def _page_entry(short: str, summary: str) -> Dict[str, Any]:
    return {
        "short": short,
        "summary": summary,
        "path": "/en/games/{game_id}",
        "path_params": [
            {
                "name": "game_id",
                "type": "int",
                "required": True,
                "transform": "_int_id",
                "description": "FIBA game id (global): gameId of a gdap schedule row, e.g. 100720 = the World Cup 2023 final.",
            }
        ],
        "extra_params": [
            {
                "name": "comp",
                "query_key": "comp",
                "type": "int",
                "required": True,
                "transform": "_int_id",
                "description": EXTRA_PARAM_DOC["comp"].replace(
                    "Find it with", "The game must belong to it. Find it with"
                ),
            }
        ],
        "example_args": {"game_id": 100720, "comp": 208182},
        "parser": f"parse_fiba_gamepage_{short}",
        "returns_schema": f"native/fiba_gamepage/{short}",
        "docstring": copy.deepcopy(PAGE_DOC),
    }


PAGE_RAW = (
    "the game-page ``Dict`` (``game``, ``playByPlay``, ``gameDetails``, ``playersTeamA``, ``playersTeamB``) cut out of "
    "the RSC flight payload with its tokens resolved"
)
PAGE_DOC: Dict[str, Any] = {
    "example_import": True,
    "raw_doc": PAGE_RAW,
    "raises": RAISES,
    "see_also": SEE_ALSO,
    "notes": NOTES
    + [
        "The wrapper takes the game id and its competition (comp) and builds the real page URL from GDAP (the "
        "competition's marketing name gives the history slug, its schedule gives the two team codes; a competition "
        "with an event page is tried at its event URL when the archive URL answers the not-found shell, and the "
        "Champions League host last). The 'Example URL' above is the wrapper's route, not a page FIBA serves.",
        "Box scores start about 2006; play-by-play with shot x/y about 2012 (OVR events) and 2017-18 (FLS club "
        "competitions). A game with no box score raises NoDataError; a game still live raises AssetFetchError. "
        "fiba.basketball answers plain requests with HTTP 403, so the page transport is curl_cffi with Chrome TLS "
        "(extra: pip install sportsdataverse[all]). Shot x/y are image pixels with an unverified scale.",
    ],
}


def _doc_family(kind: str) -> Dict[str, Any]:
    return {"example_import": True, "raises": RAISES, "see_also": SEE_ALSO, "notes": NOTES}


def _family_yaml(stem: str, endpoints: List[Dict[str, Any]], runtime_imports: List[str]) -> Dict[str, Any]:
    doc: Dict[str, Any] = {
        "api": stem,
        "host": GDAP_HOST if stem == "fiba_gdap" else SITE_HOST,
        "name_pattern": f"{stem}_{{short}}",
        "module": stem,
        "parser_module": f"{HOME}.{stem}_parsers",
        "getter_module": GETTER,
        "runtime_imports": runtime_imports,
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        "schema_prefix": f"{stem}_",
        "docstring": _doc_family(stem) if stem == "fiba_gdap" else copy.deepcopy(PAGE_DOC),
        "endpoints": endpoints,
    }
    return doc


# --------------------------------------------------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------------------------------------------------
def _wc_fixture_note() -> str:
    return "tests/fixtures/<stem>/ holds the committed refs captures (truncated to 3 rows per array) and full/ holds untruncated bodies."


def _write_descriptions(
    stem: str, parsers: Dict[str, Any], dtypes: Dict[str, Dict[str, str]], notes: Dict[str, Dict[str, str]]
) -> None:
    """Write the notes file for a family; a column left undescribed is an error (the coverage ratchet is 0)."""
    out: Dict[str, Dict[str, str]] = {}
    missing: List[str] = []
    for short in sorted(parsers):
        cols = dtypes[parsers[short]]
        out[f"{stem}_{short}"] = {}
        for col in cols:
            text = notes[short].get(col, "")
            if len(text) < 15 or text.lower() == col.lower():
                missing.append(f"{stem}_{short}.{col}")
            out[f"{stem}_{short}"][col] = text
    if missing:
        raise SystemExit(f"{stem}: {len(missing)} undescribed columns, e.g. {missing}")
    write_yaml(NOTES_DIR / f"{stem}_descriptions.yaml", out)


def _route_snapshot(gdap_spec: dict, page_spec: dict) -> Dict[str, Any]:
    methods = {
        p[1:]: sorted(q["name"] for q in op["get"].get("parameters", []) if q["in"] == "query" and q.get("required"))
        for p, op in gdap_spec["paths"].items()
    }
    return {
        "spec_methods": methods,
        "unwrapped": sorted(UNWRAPPED),
        "page_templates": sorted(page_spec["paths"]),
        "gdap": {s: {"method": c["method"], "fixtures": c["fixtures"]} for s, c in sorted(GDAP.items())},
        "gamepage": {s: {"fixtures": c["fixtures"]} for s, c in sorted(PAGE.items())},
    }


def main() -> None:
    from gen_fiba_descriptions import gamepage_notes, gdap_notes  # the description tables (same directory)

    refs = refs_dir("fiba", GDAP_SPEC)
    gdap_spec, page_spec = load_spec(refs, GDAP_SPEC), load_spec(refs, PAGE_SPEC)
    spec_methods = {p[1:] for p in gdap_spec["paths"]}
    wrapped = {c["method"] for c in GDAP.values()}
    if spec_methods - wrapped != UNWRAPPED:
        raise SystemExit(f"fiba: spec methods changed: unwrapped {sorted(spec_methods - wrapped)}")
    if len(page_spec["paths"]) != 3:
        raise SystemExit("fiba: the game-page templates changed")

    # dtypes, from every fixture of every parser
    dtypes: Dict[str, Dict[str, str]] = {}
    dtypes.update(
        _family_codes(
            {
                f"parse_fiba_gdap_{s}": _derive(getattr(gp, f"parse_fiba_gdap_{s}"), "fiba_gdap", c["fixtures"])
                for s, c in GDAP.items()
            }
        )
    )
    dtypes.update(
        _family_codes(
            {
                f"parse_fiba_gamepage_{s}": _derive(
                    getattr(pp, f"parse_fiba_gamepage_{s}"), "fiba_gamepage", c["fixtures"]
                )
                for s, c in PAGE.items()
            }
        )
    )
    body = ",\n".join(
        f'    "{p}": {{\n' + "".join(f'        "{c}": "{t}",\n' for c, t in cols.items()) + "    }"
        for p, cols in sorted(dtypes.items())
    )
    (ROOT / "sportsdataverse/basketball/fiba_hub/fiba_dtypes.py").write_text(
        '"""Column dtypes of the FIBA parsers, one ordered map per parser function.\n\n'
        "Generated by ``tools/codegen/gen_fiba.py`` from the FULL fixture bodies plus the refs captures; do not edit.\n"
        "Codes: ``i`` Int64, ``f`` Float64, ``b`` Boolean, ``s`` String. ``fiba_frames._finish`` gives these dtypes to a\n"
        'populated column, an all-null column and an empty frame, so an empty frame stacks onto a populated one."""\n\n'
        "from typing import Dict\n\n"
        f"DTYPES: Dict[str, Dict[str, str]] = {{\n{body},\n}}\n",
        encoding="utf-8",
    )
    importlib_reload()

    # endpoints
    gdap_eps = [_gdap_entry(s, c, gdap_spec) for s, c in sorted(GDAP.items())]
    page_eps = []
    for short in sorted(PAGE):
        summary = (getattr(pp, f"parse_fiba_gamepage_{short}").__doc__ or "").strip().splitlines()[0]
        page_eps.append(_page_entry(short, summary))
    for ep in gdap_eps + page_eps:  # the renderer substitutes {league} / {sport} as ESPN slugs
        assert not {"league", "sport"} & {p["name"] for p in ep.get("path_params", [])}, ep["path"]
    write_yaml(
        ROOT / "tools/codegen/endpoints/fiba_gdap.yaml",
        _family_yaml("fiba_gdap", gdap_eps, ["_get", "_int_id", "_season", "_window_date"]),
    )
    write_yaml(
        ROOT / "tools/codegen/endpoints/fiba_gamepage.yaml",
        _family_yaml("fiba_gamepage", page_eps, ["_get", "_int_id"]),
    )

    # returns-schemas (descriptions live in the notes files)
    for stem, shorts in (("fiba_gdap", sorted(GDAP)), ("fiba_gamepage", sorted(PAGE))):
        schema_dir = ROOT / "tools/codegen/schemas/native" / stem
        rewrite_schema_dir(schema_dir)
        for short in shorts:
            cols = dtypes[f"parse_{stem}_{short}"]
            write_yaml(
                schema_dir / f"{short}.yaml",
                {
                    "schema": f"{stem}_{short}",
                    "kind": "dataframe",
                    "columns": [{"name": c, "type": _DOC[t], "description": ""} for c, t in cols.items()],
                },
            )

    # descriptions
    sections = _sections(refs / "fiba-returns.md")
    _write_descriptions(
        "fiba_gdap",
        {s: f"parse_fiba_gdap_{s}" for s in GDAP},
        dtypes,
        gdap_notes(sections, dtypes, _desc_maps),
    )
    _write_descriptions(
        "fiba_gamepage",
        {s: f"parse_fiba_gamepage_{s}" for s in PAGE},
        dtypes,
        gamepage_notes(sections, dtypes, _desc_maps),
    )

    # route snapshot for the tests
    (FIXTURES / "fiba_gdap").mkdir(parents=True, exist_ok=True)
    (FIXTURES / "fiba_gdap" / "routes.json").write_text(
        json.dumps(_route_snapshot(gdap_spec, page_spec), indent=1, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"fiba: {len(gdap_eps)} fiba_gdap + {len(page_eps)} fiba_gamepage endpoints")


def importlib_reload() -> None:
    import importlib

    importlib.reload(fiba_dtypes)


if __name__ == "__main__":
    main()
