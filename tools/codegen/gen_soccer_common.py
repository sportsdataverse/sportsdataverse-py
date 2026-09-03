"""Shared helpers for the three soccer flat-API generators (ASA / MLS / NWSL).

``gen_asa.py``, ``gen_mls.py`` and ``gen_nwsl.py`` all read a frozen OpenAPI spec
plus the committed sample payloads from the same ``sdv-internal-refs`` checkout
and emit an endpoint YAML + per-endpoint returns-schemas. Everything they do
identically lives here so the three scripts carry only their provider-specific
route curation.

Returns-schema columns are derived by **running the real parser over the
committed capture** rather than by reading the spec's declared 200-response
component. The captures are ground truth -- the MLS spec, for instance, declares
bare arrays for four routes that actually answer with an envelope -- and a
capture-derived column list is guaranteed to match what the parser hands a
caller. Column *descriptions* come from the spec, then the reference repo's
``*-returns.md`` tables, then the curated :data:`_LEAF_DESCRIPTIONS` prose for the
site plumbing neither documents -- every source is committed, so re-running a
generator on unchanged inputs is byte-identical.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

import polars as pl
import yaml

from sportsdataverse.dl_utils import underscore

ROOT = Path(__file__).resolve().parents[2]

# Windows dev-box fallback, matching the sibling generators (gen_cbs, gen_on3).
_WINDOWS_REFS = Path("C:/Users/saiem/Documents/sdv-internal-refs")

# polars dtype -> the R-style type vocabulary the returns-schemas use.
_NUMERIC_KINDS = ("integer", "numeric")

# JSON-schema type -> the same vocabulary, for the spec-sourced column path.
_JSON_TYPE = {
    "integer": "integer",
    "number": "numeric",
    "boolean": "logical",
    "string": "character",
    "object": "character",
    "array": "character",
}

# Container prefixes ``pandas.json_normalize`` bolts onto a nested object's fields,
# longest first. Stripping one turns e.g. ``home_short_name`` back into the
# documented ``short_name``; the label qualifies the resolved prose so the reader
# still knows which side/entity the column belongs to. An empty label means the
# prefix adds no information worth repeating.
_PREFIX_LABELS = [
    ("home_editorial_", "Home club editorial: "),
    ("away_editorial_", "Away club editorial: "),
    ("team_editorial_", "Club editorial: "),
    ("editorial_broadcasters_", "Broadcast listing: "),
    ("competition_logo_dark_", "Competition dark-theme logo: "),
    ("competition_logo_light_", "Competition light-theme logo: "),
    ("first_party_tickets_", "MLS-operated ticketing link: "),
    ("third_party_tickets_", "Third-party ticketing link: "),
    ("club_color_one_", "Primary club colour: "),
    ("club_color_two_", "Secondary club colour: "),
    ("club_color_three_", "Tertiary club colour: "),
    ("league_promo_image_", "League promo image: "),
    ("priority_match_", "Priority-match window: "),
    ("qualification_", "Qualification band: "),
    ("editorial_", "Editorial metadata: "),
    ("shirt_three_", "Third-choice kit: "),
    ("shirt_one_", "First-choice kit: "),
    ("shirt_two_", "Second-choice kit: "),
    ("match_set_", "Match day (round): "),
    ("competition_", "Competition: "),
    ("references_", "Contentful reference list: "),
    ("thumbnail_", "Player thumbnail image: "),
    ("provider_", "Provider-reported: "),
    ("stadium_", "Stadium: "),
    ("season_", "Season: "),
    ("venue_", "Venue: "),
    ("fields_", "Season content field: "),
    ("apple_", "Apple broadcast: "),
    ("home_", "Home club: "),
    ("away_", "Away club: "),
    ("team_", "Club: "),
    ("club_", ""),
    ("social_", ""),
    ("maps_geo_code_", ""),
]

# Curated prose for columns neither the OpenAPI spec nor the reference repo's
# ``*-returns.md`` tables document -- almost all of it is site plumbing (brand
# colours, kit colours, CMS bookkeeping, broadcast/ticketing links, social
# handles) rather than match data. Keyed by the *unprefixed* leaf name so one
# entry covers every ``home_`` / ``away_`` / ``team_`` variant.
_LEAF_DESCRIPTIONS = {
    # --- Contentful / CMS bookkeeping (MLS dapi) ---
    "_entity_id": "Contentful entity id for the content entry.",
    "_translation_id": "Contentful translation-group id shared by every locale of this entry.",
    "_list_availability": "Contentful flag controlling whether the entry appears in listings.",
    "self_url": "Canonical API URL of this content entry.",
    "slug": "URL slug.",
    "title": "Display title.",
    "tags": "Content tags, JSON-encoded.",
    "relations": "Related content entries, JSON-encoded.",
    "created_by": "User or service that created the content entry.",
    "last_updated_by": "User or service that last updated the content entry.",
    "last_updated_date": "Timestamp of the last content update (ISO 8601).",
    "content_date": "Editorial content date (ISO 8601).",
    "featured": "Whether the entry is flagged as featured (1 = featured).",
    "entity_code": "Contentful entity-type code.",
    "name": "Display name.",
    "sportec_id_overwrite": "Whether the season's Sportec id is manually overridden in the CMS.",
    "home_advantage": "Points of home advantage applied by the playoff seeding rules.",
    "home_advantage_legend": "Prose legend explaining the home-advantage rule.",
    "standings_legend": "Prose legend explaining the standings qualification bands.",
    "top_clubs": "Number of clubs qualifying from the top of the table.",
    "top_clubs_legend": "Prose legend explaining the top-clubs cut line.",
    "playoff_qualified_east_conference": "Count of Eastern Conference clubs that have clinched a playoff berth.",
    "playoff_qualified_west_conference": "Count of Western Conference clubs that have clinched a playoff berth.",
    "list_of_clubs_clinched_e": "Reference list of clubs that have clinched, list variant e.",
    "list_of_clubs_clinched_s": "Reference list of clubs that have clinched, list variant s.",
    "list_of_clubs_clinched_x": "Reference list of clubs that have clinched, list variant x.",
    "list_of_clubs_clinched_y": "Reference list of clubs that have clinched, list variant y.",
    # --- branding / imagery ---
    "background_color": "Brand background colour (hex).",
    "logo_bw_slug": "Asset slug for the black-and-white logo.",
    "logo_color_slug": "Asset slug for the full-colour logo.",
    "logo_color_url": "URL of the full-colour logo asset.",
    "crest_color_slug": "Asset slug for the full-colour club crest.",
    "bw_slug": "Asset slug for the black-and-white variant.",
    "color_slug": "Asset slug for the full-colour variant.",
    "color_url": "URL of the full-colour asset.",
    "background_image_slug": "Asset slug for the background image.",
    "asset_url": "URL of the image asset.",
    "format": "Image format of the asset.",
    "template_url": "Templated image URL with substitutable size tokens.",
    "thumbnail_url": "URL of the rendered thumbnail image.",
    "all_season_imagery": "Per-season crest/kit imagery variants, JSON-encoded.",
    "imagery": "CDN image URLs (crest/badge/photo variants), JSON-encoded.",
    # --- kit + club colours ---
    "shirt_main_color": "Main shirt colour name.",
    "shirt_main_color_rgb": "Main shirt colour as an RGB hex string.",
    "shirt_secondary_color": "Secondary shirt colour name.",
    "shirt_secondary_color_rgb": "Secondary shirt colour as an RGB hex string.",
    "shirt_number_color": "Shirt-number colour name.",
    "shirt_number_color_rgb": "Shirt-number colour as an RGB hex string.",
    "club_color": "Club colour name.",
    "club_color_rgb": "Club colour as an RGB hex string.",
    "primary_colour": "Club primary colour (hex).",
    "secondary_colour": "Club secondary colour (hex).",
    "text_colour": "Club text colour (hex).",
    # --- broadcast / ticketing / commerce ---
    "broadcasters": "Broadcast listings for the match, JSON-encoded.",
    "club_broadcasters": "Club-specific broadcast listings, JSON-encoded.",
    "advertisement_category": "Apple advertising category for the stream.",
    "stream_url": "Apple TV stream URL for the match.",
    "subscription_tier": "Apple subscription tier required to watch.",
    "calendar_url": "Calendar (.ics) subscription URL for the match.",
    "ecal_widget_id": "Identifier of the eCal calendar-subscription widget.",
    "widget_id": "Identifier of the embedded provider widget.",
    "mgm_id": "MGM sportsbook partner identifier.",
    "accessible_text": "Accessible (screen-reader) label for the ticketing link.",
    "display_text": "Button label for the ticketing link.",
    "is_visible": "Whether the link is shown.",
    "open_in_new_tab": "Whether the link opens in a new tab.",
    "url": "Link URL.",
    "shop_url": "Club shop URL.",
    "tickets_url": "Ticketing URL.",
    "website_url": "Official website URL.",
    "sponsor": "Sponsor name attached to the priority match.",
    "sponsor_image": "Sponsor image asset, JSON-encoded.",
    "theme_night": "Theme-night promotion attached to the match.",
    "broadcaster_national1": "First national broadcaster carrying the match.",
    "broadcaster_national2": "Second national broadcaster carrying the match.",
    "broadcaster_national3": "Third national broadcaster carrying the match.",
    "broadcaster_international1": "First international broadcaster carrying the match.",
    "broadcaster_international2": "Second international broadcaster carrying the match.",
    "broadcaster_international3": "Third international broadcaster carrying the match.",
    # --- social handles ---
    "facebook": "Facebook handle or URL.",
    "instagram": "Instagram handle or URL.",
    "linked_in": "LinkedIn handle or URL.",
    "tik_tok": "TikTok handle or URL.",
    "x": "X (Twitter) handle or URL.",
    "you_tube": "YouTube handle or URL.",
    "highlights_url": "Match highlights URL.",
    "highlights_national_url": "Nationally-geofenced match highlights URL.",
    "highlights_international_url": "Internationally-geofenced match highlights URL.",
    "editorials": "Editorial blurbs attached to the record, JSON-encoded.",
    "player_role_within_team": "Editorial description of the player's role in the side.",
    # --- match / competition plumbing ---
    "block_header_name": "Header label used for this competition on the site.",
    "nextgen_ecal_match_hub_display": "Whether the match hub shows the eCal subscribe control.",
    "player_headshot_thumbnail_field": "Content field the site reads player headshots from.",
    "phase": "Competition phase of the match (regular season, playoffs, ...).",
    "match_date": "Scheduled kickoff timestamp (ISO 8601).",
    "match_type": "Match classification for the competition (league, cup, friendly).",
    "match_title": "Home:Away title of the match.",
    "league_match_title": "Home:Away match title as rendered in league listings.",
    "is_time_tbd": "Whether the kickoff time is still to be confirmed.",
    "delayed_match": "Whether the match is flagged as delayed.",
    "round_name": "Name of the round or series this match belongs to.",
    "round_group": "Group label within the round (tournaments).",
    "round_id": "Composite id of the round.",
    "bracket_structure_id": "Identifier of the playoff bracket structure.",
    "club_rank": "Club's standings rank at the time of the request.",
    "date_from": "Start of the display window (ISO 8601).",
    "date_to": "End of the display window (ISO 8601).",
    "full_name": "Full club name.",
    "short_name": "Short display name.",
    "opta_id": "Parallel Opta integer id for the entity.",
    "sportec_id": "Sportec opaque id for the entity (Utf8 join key, never numeric).",
    "venue_sportec_id": "Sportec opaque id of the venue (Utf8 join key).",
    "competition_opta_id": "Parallel Opta integer id of the competition.",
    "competition_sportec_id": "Sportec opaque id of the competition (Utf8 join key).",
    # --- NWSL SDP plumbing ---
    "media_name": "Media-style display name.",
    "media_short_name": "Short media-style display name.",
    "media_first_name": "Media-style first name.",
    "media_last_name": "Media-style last name.",
    "official_name": "Official registered name.",
    "acronym_name": "Acronym form of the name.",
    "acronym_name_localized": "Localized acronym form of the name.",
    "country_code": "ISO country code.",
    "team_type": "Team classification (club, national team, ...).",
    "is_team_fake": "Whether the entry is a placeholder rather than a real club.",
    "overall_summary": "Provider summary blurb for the entity.",
    "stadium": "Home stadium block, JSON-encoded.",
    "score_push": "Live score pushed by the feed for this side.",
    "away_score": "Away side's score.",
    "home_score": "Home side's score.",
    "penalty_score_away": "Away side's penalty-shootout score.",
    "penalty_score_home": "Home side's penalty-shootout score.",
    "previous_legs_result": "Aggregate result of previous legs in a two-legged tie.",
    "additional_time": "Stoppage time added, in minutes.",
    "average_x_position": "Average pitch x-coordinate of the player over the match.",
    "average_y_position": "Average pitch y-coordinate of the player over the match.",
    "city_name": "City the stadium is in.",
    "address": "Street address of the stadium.",
    "capacity": "Seating capacity of the stadium.",
    "country": "Country the stadium is in.",
    "year_of_construction": "Year the stadium was built.",
    "latitude": "Latitude in decimal degrees.",
    "longitude": "Longitude in decimal degrees.",
    "matchday_status": "Status of the match day (scheduled, in progress, completed).",
    "index": "Ordinal position of the match day within the season.",
    "format_id": "Identifier of the match-day format.",
    "match_set_format_id": "Identifier of the match-day format.",
    "match_set_id": "Composite match-day (match set) id.",
    "start_date_utc": "Start of the period, in UTC (ISO 8601).",
    "end_date_utc": "End of the period, in UTC (ISO 8601).",
    "stage_id": "Composite id of the competition stage.",
    "qualification_id": "Identifier of the qualification band.",
    "qualification_label": "Display label of the qualification band.",
    "type": "Type discriminator for the record.",
    "id": "Provider identifier for the entity (Utf8 join key).",
}

# Markdown-table keys never lifted into the flat description map: the table
# headers themselves, plus names whose meaning depends entirely on the resource
# the table describes. ``sportec_id`` documented once as "Player Sportec id" would
# otherwise be reused on a match row, so those resolve through the curated generic
# prose in :data:`_LEAF_DESCRIPTIONS` instead.
_SKIP_MARKDOWN_KEYS = frozenset(
    {"col_name", "field", "param", "name", "opta_id", "sportec_id", "id", "type", "slug", "url"},
)

_TABLE_ROW = re.compile(r"^\s*\|(.+)\|\s*$")
_SEPARATOR_ROW = re.compile(r"^[\s|:-]+$")


def refs_dir(source: str, marker: str) -> Path:
    """Resolve ``sdv-internal-refs/<source>`` (env -> sibling checkout -> default).

    Args:
        source: sub-directory of the reference repo (``"asa"``, ``"mls"``, ``"nwsl"``).
        marker: a file that must exist inside it, used to validate the candidate.

    Returns:
        The resolved directory.

    Raises:
        SystemExit: when no candidate contains ``marker``.
    """
    env = os.environ.get("SDV_INTERNAL_REFS_REPO")
    candidates = [Path(env)] if env else []
    candidates += [ROOT.parent / "sdv-internal-refs", _WINDOWS_REFS]
    for base in candidates:
        if (base / source / marker).exists():
            return base / source
    raise SystemExit(f"{source}: spec not found -- set SDV_INTERNAL_REFS_REPO to the sdv-internal-refs checkout")


def load_spec(refs: Path, filename: str) -> dict:
    """Parse a frozen OpenAPI spec from the reference repo."""
    return yaml.safe_load((refs / filename).read_text(encoding="utf-8"))


def get_ops(spec: dict) -> List[tuple]:
    """``[(path, get_operation), ...]`` for every GET path, in spec order."""
    return [(path, ops["get"]) for path, ops in spec["paths"].items() if "get" in ops]


def spec_descriptions(spec: dict) -> Dict[str, str]:
    """``{snake_cased_property: description}`` unioned over every component schema.

    Keyed by the snake_cased name because that is what the parsers emit. First
    definition wins, so a property documented once is documented everywhere it
    appears -- adequate for a docs table and stable across runs (component order
    in the frozen spec is fixed).
    """
    out: Dict[str, str] = {}
    for schema in (spec.get("components", {}).get("schemas") or {}).values():
        for name, prop in ((schema or {}).get("properties") or {}).items():
            desc = (prop or {}).get("description")
            key = underscore(str(name))
            if desc and key not in out:
                out[key] = str(desc).strip()
    return out


def markdown_descriptions(path: Path) -> Dict[str, str]:
    """``{snake_cased_column: description}`` from the markdown tables in a returns doc.

    Reads every ``| col | type | ... | description |`` table in the file and keys
    the last cell by the first. The reference repo scopes those tables by
    resource; this map is flat, so a column name reused across two resources with
    different prose keeps the first definition. That is a documentation-only
    trade-off -- the column *set* always comes from the capture, never from here.
    """
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        match = _TABLE_ROW.match(line)
        if not match or _SEPARATOR_ROW.match(match.group(1)):
            continue
        cells = [c.strip() for c in match.group(1).split("|")]
        if len(cells) < 3:
            continue
        name = underscore(cells[0].strip("`").strip())
        desc = cells[-1]
        if not name or name in _SKIP_MARKDOWN_KEYS or not desc:
            continue
        if name not in out:
            out[name] = desc
    return out


def _column_type(dtype: pl.DataType) -> str:
    """R-style returns-schema type for a polars dtype."""
    if dtype == pl.Boolean:
        return "logical"
    if dtype.is_integer():
        return _NUMERIC_KINDS[0]
    if dtype.is_float():
        return _NUMERIC_KINDS[1]
    return "character"


# Proper nouns that must keep their capital when a description is spliced in
# after a container label (multi-capital acronyms are handled separately).
_KEEP_CASE = frozenset(
    {"Sportec", "Opta", "StatsPerform", "Contentful", "Apple", "Akamai", "Home:Away", "Composite", "Match", "Season"},
)


def _decapitalize(text: str) -> str:
    """Lower the first letter so resolved prose reads as a clause after a label.

    Left alone when the first word is an acronym (``URL``, ``ISO``, ``CDN``) --
    ``"URL of the asset"`` must not become ``"uRL of the asset"``. A run of two
    capitals is the tell.
    """
    if len(text) > 1 and text[0].isupper() and text[1].isupper():
        return text
    if text.split(" ", 1)[0].rstrip(":,.") in _KEEP_CASE:
        return text
    return text[:1].lower() + text[1:]


def describe(name: str, sources: Sequence[Dict[str, str]]) -> str:
    """Resolve a description for one snake_cased column name.

    Three tiers, in order:

    1. an exact hit in ``sources`` (the spec's property docs, then the reference
       repo's ``*-returns.md`` tables);
    2. an exact hit in :data:`_LEAF_DESCRIPTIONS`, the curated prose for the site
       plumbing neither of those documents (brand colours, CMS bookkeeping,
       broadcast/ticketing links, social handles);
    3. a **prefixed** variant: ``json_normalize`` turns one nested object into many
       dotted columns, so ``home_short_name`` / ``away_short_name`` /
       ``team_short_name`` are all the documented ``short_name`` wearing a
       container prefix. The longest matching prefix is stripped, the remainder is
       resolved recursively, and its prose is qualified with the container's label.

    Args:
        name: snake_cased column name.
        sources: description maps consulted in order.

    Returns:
        The description, or ``""`` when nothing resolves.
    """
    for source in sources:
        if source.get(name):
            return source[name]
    if name in _LEAF_DESCRIPTIONS:
        return _LEAF_DESCRIPTIONS[name]
    for prefix, label in _PREFIX_LABELS:
        if name.startswith(prefix) and len(name) > len(prefix):
            inner = describe(name[len(prefix) :], sources)
            if inner:
                return label + (_decapitalize(inner) if label else inner)
    return ""


def columns_from_frame(df: pl.DataFrame, descriptions: Sequence[Dict[str, str]]) -> List[Dict[str, str]]:
    """``[{name, type, description}, ...]`` for a parsed frame.

    Args:
        df: the frame the endpoint's parser produced from its capture.
        descriptions: description maps consulted in order; the first hit wins.

    Returns:
        One entry per column, in frame order.
    """
    cols: List[Dict[str, str]] = []
    for name, dtype in df.schema.items():
        cols.append({"name": name, "type": _column_type(dtype), "description": describe(name, descriptions)})
    return cols


def component_columns(spec: dict, component: str, descriptions: Sequence[Dict[str, str]]) -> List[Dict[str, str]]:
    """``[{name, type, description}, ...]`` from a spec component's properties.

    Used only where a route's committed capture legitimately parses to nothing --
    the NWSL ``/stages`` sample is a real ``{"stages": null}`` body for a
    pure-league season -- so the documented columns come from the spec rather than
    from an empty frame.
    """
    props = ((spec.get("components", {}).get("schemas") or {}).get(component) or {}).get("properties") or {}
    cols: List[Dict[str, str]] = []
    for name, prop in props.items():
        key = underscore(str(name))
        desc = str((prop or {}).get("description") or "").strip()
        if not desc:
            desc = next((s[key] for s in descriptions if s.get(key)), "")
        # OpenAPI 3.1 writes a nullable property as ``type: [string, "null"]``.
        jtype = (prop or {}).get("type")
        if isinstance(jtype, list):
            jtype = next((t for t in jtype if t != "null"), None)
        cols.append({"name": key, "type": _JSON_TYPE.get(jtype, "character"), "description": desc})
    return cols


def parse_capture(
    path: Optional[Path],
    parser: Callable[..., Any],
    primary: Optional[str] = None,
) -> pl.DataFrame:
    """Run ``parser`` over a committed capture and return the frame to document.

    Args:
        path: capture file, or ``None`` when the route has no committed sample.
        parser: the parser the endpoint YAML declares.
        primary: sub-frame key to document when the parser returns a mapping.

    Returns:
        The parsed frame -- empty when there is no capture (so the schema is
        emitted with an empty column list rather than a wrong one).
    """
    import json

    if path is None or not path.exists():
        return pl.DataFrame()
    parsed = parser(json.loads(path.read_text(encoding="utf-8")))
    if isinstance(parsed, dict):
        return parsed.get(primary or next(iter(parsed), ""), pl.DataFrame())
    return parsed


def write_yaml(path: Path, obj: Any) -> None:
    """Write ``obj`` as deterministic, LF-terminated YAML."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        yaml.safe_dump(obj, fh, sort_keys=True, default_flow_style=False, allow_unicode=True)


def rewrite_schema_dir(path: Path) -> None:
    """Empty a generated schema directory so a removed route leaves no orphan."""
    path.mkdir(parents=True, exist_ok=True)
    for schema_path in path.glob("*.yaml"):
        schema_path.unlink()
