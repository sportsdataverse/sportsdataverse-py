"""NCAA SVG shot-map parser (``ShotEventParser``, cbb-explorer port).

Faithful Python port of hoop-explorer's ``cbb-explorer`` (Scala 2.12,
``utest``) ``ShotEventParser.scala`` -- the fifth of seven Phase-5e modules.
Parses the SVG shot-map embedded in an NCAA game page (``circle.shot``
elements, one per shot, each carrying its pixel location + a ``<title>``
describing the play) into a list of :class:`~sportsdataverse.mbb
.mbb_ncaa_models.ShotEvent`. **v1-only** -- SVG shot maps did not exist in
the legacy (v0) NCAA page format, so unlike every other 5e parser there is
no ``v0_builders`` table here.

Ported surface (Scala anchors in each docstring):

* :data:`ShotMapDimensions` -- the SVG's pixel<->feet conversion constants
  (``ShotEventParser.ShotMapDimensions``, ``:568-582``).
* :class:`ShotEventBuilders` / :data:`v1_builders` -- the HTML finder-function
  table (``ShotEventParser.base_builders``/``v1_builders``, ``:37-172``).
* :func:`create_shot_event_data` -- the entry point (``:175-259``).
* :func:`shot_js_to_html` -- converts the client-side-JS ``addShot(...)``
  fallback into parseable HTML circles, for pages where the shot map is
  built on the fly rather than baked into the initial HTML (``:266-283``).
* :func:`parse_shot_html` / :func:`build_base_event` -- per-``<circle>``
  extraction (``:285-410``).
* :func:`phase1_shot_event_enrichment` -- the court-geometry pipeline:
  ascending-time conversion, side-to-shoot-on inference, coordinate
  transform, geo synthesis, and the self-correcting side-flip re-run
  (``:412-528``).
* :func:`get_ascending_time` / :func:`is_team_shooting_left_to_start` /
  :func:`is_women_game` / :func:`transform_shot_location` -- the geometry
  pipeline's building blocks (``:530-620``).

**Scala idiom decision: ``shot_js_to_html``'s ``builders``/``browser``
parameters are dropped.** The Scala signature is
``shot_js_to_html(js: String, builders: base_builders, browser: Browser)``,
but the function body never reads ``builders`` -- it hardcodes
``v1_builders.shot_event_finder(...)`` on its last line regardless of what
was passed in (verified: there is exactly one call site,
``create_shot_event_data``, and it always passes ``builders = v1_builders``
anyway, so the dead parameter never observably differs). ``browser`` exists
only to call ``browser.parseString(html)``, which this port's
:func:`~sportsdataverse.mbb.mbb_ncaa_html.parse_html` already does
statelessly with no injectable instance. The Python :func:`shot_js_to_html`
therefore takes a single ``js: str`` parameter and calls the module-private
:func:`_v1_shot_event_finder` directly, matching the Scala's actual
(hardcoded-v1) runtime behavior exactly while dropping the two parameters
that carried no information.

**Scala idiom decision: the ``phase1_shot_event_enrichment`` flip-detection
``println`` is ported as a real (unconditional) ``print``, not dropped.**
Unlike ``create_shot_event_data``'s own ``debug_print = false`` block (a
permanently-dead branch per this project's established "drop dead debug
println" precedent -- see ``mbb_ncaa_lineup_enrich.py``'s several
``del ev  # ponytail: only feeds a dead debug println`` notes), this
``println`` is NOT gated by any always-false flag -- it fires for real
whenever the self-correcting flip triggers. It has zero effect on the
function's return value (confirmed: the ``if`` block containing it is a
side-effect-only ``println``, no oracle assertion inspects stdout), so the
printed message reproduces the same INFORMATION (team/opponent/periods/
per-period counts) rather than the Scala case class's exact ``toString``
formatting (which has no natural Python equivalent worth inventing).

**Geometry: the ``PI/180``/``180/PI`` conversion constants are ported as the
Scala's own two-step chain (``pi/180.0`` then ``1.0/that``), NOT
:func:`math.degrees`/:func:`math.radians`.** CPython's ``math.degrees``
multiplies by a directly-computed ``180.0/pi``, a different division chain
than the Scala's ``1.0 / (Math.PI / 180.0)`` -- IEEE-754 division is not
perfectly reciprocal, so the two chains can differ in the last bit. The
oracle test pins exact literal ``lat``/``lon`` floats
(``40.75031148982409``/``-73.99301510956438``), so this module reproduces
the Scala's exact operation-by-operation chain via
:data:`_PI_OVER_180`/:data:`_180_OVER_PI` rather than the more idiomatic
stdlib call, to guarantee bit-identical output.

Apache-2.0 third-party port — see the ``NOTICE`` file at the repository root for the upstream copyright and full attribution.

**Landmine index (reachable scalar division).**
    1. ``phase1_shot_event_enrichment``'s geo synthesis divides by
       ``eff_radius = EARTH_RADIUS_M * cos(radians(shot_lat))``. This is
       ZERO only if ``shot_lat`` is exactly +-90 degrees; ``shot_lat`` is
       ``BASE_LAT`` (~40.75 degrees) plus an offset of
       ``degrees(shot_y_feet / EARTH_RADIUS_M)`` -- with ``shot_y_feet``
       bounded to plausible court coordinates (tens of feet at most) and
       ``EARTH_RADIUS_M`` = 6,371,000, the offset is on the order of
       ``1e-6`` degrees, so reaching +-90 degrees is not reachable through
       this parser's real input domain. Flagged here (not "none reachable")
       because the task's geometry-review explicitly calls for it, and
       because Python's ``float / 0.0`` **raises** ``ZeroDivisionError``
       where Scala/Java's IEEE-754 double division would instead silently
       yield ``Infinity`` -- a real behavioral divergence if this theoretical
       edge were ever hit, worth knowing about even though unreachable in
       practice.
    2. ``event_time_finder``'s ``sec / 60.0`` and ``csec / 6000.0`` divide by
       fixed literals, never zero -- no risk.
    3. ``ShotMapDimensions.ft_per_px_x``/``ft_per_px_y`` divide fixed
       literals at module-import time (``94.0/940.0``, ``50.0/500.0``) --
       computed once, never zero, no risk.
    Every other computation in this module is string/regex parsing, list
    indexing (guarded), or dict counting -- no further division sites exist.

Example::

    from pathlib import Path
    from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
    from sportsdataverse.mbb.mbb_ncaa_models import TeamId
    from sportsdataverse.mbb.mbb_ncaa_shot_parser import create_shot_event_data

    box_html = Path("tests/fixtures/ncaa/test_lineup.html").read_text(encoding="utf-8")
    box_lineup = get_box_lineup("test_p1.html", box_html, TeamId("TeamA"), format_version=1)
    shots = create_shot_event_data("test_p1.html", box_html, box_lineup)

See Also:
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R men's basketball companion package
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- R women's basketball companion package
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, replace
from itertools import takewhile
from typing import Callable, Optional, Union

from bs4 import BeautifulSoup
from bs4.element import Tag

from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError, build_sub_error, enrich_sub_error
from sportsdataverse.mbb.mbb_ncaa_html import jsoup_text, parse_html
from sportsdataverse.mbb.mbb_ncaa_models import (
    LineupEvent,
    LocationType,
    Score,
    ShotEvent,
    ShotGeo,
    ShotLocation,
)
from sportsdataverse.mbb.mbb_ncaa_names import TidyPlayerContext, build_tidy_player_context, tidy_player
from sportsdataverse.mbb.mbb_ncaa_stints import (
    build_player_code,
    duration_from_period,
    name_in_v0_box_format,
    parse_team_name,
    remove_html_encoding,
)
from sportsdataverse.mbb.mbb_ncaa_names import code_from_box
from sportsdataverse.mbb.mbb_ncaa_stints import sides_from_box

__all__ = [
    "ShotMapDimensions",
    "ShotEventBuilders",
    "v1_builders",
    "create_shot_event_data",
    "shot_js_to_html",
    "parse_shot_html",
    "build_base_event",
    "phase1_shot_event_enrichment",
    "get_ascending_time",
    "is_team_shooting_left_to_start",
    "is_women_game",
    "transform_shot_location",
]

#: Error-reporter location tag (``` `ncaa.parse_shotevent` ```,
#: ``ShotEventParser.scala:34``).
_LOCATION_PARSE_SHOTEVENT = "ncaa.parse_shotevent"


class ShotMapDimensions:
    """SVG shot-map pixel<->feet conversion constants, taken from the
    ``svg#court`` element (``ShotEventParser.ShotMapDimensions``,
    ``:568-582``). A plain class (not a dataclass) used purely as a
    namespace, mirroring the Scala ``object``'s "static singleton" role --
    field names are kept snake_case to match the Scala vals verbatim,
    letting the ported oracle tests reference e.g.
    ``ShotMapDimensions.court_length_x_px`` 1:1.
    """

    court_length_x_px: float = 940.0
    court_width_y_px: float = 500.0
    court_length_ft: float = 94.0
    court_width_ft: float = 50.0

    half_court_x_px: float = 0.5 * court_length_x_px

    ft_per_px_x: float = court_length_ft / court_length_x_px
    ft_per_px_y: float = court_width_ft / court_width_y_px

    goal_left_x_px: float = 50.0
    goal_y_px: float = 250.0


# ---------------------------------------------------------------------------
# v1_builders (``ShotEventParser.base_builders``/``v1_builders``, ``:37-172``)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ShotEventBuilders:
    """The HTML finder-function table (``ShotEventParser.base_builders``,
    ``:37-50``). v1-only -- SVG shot maps did not exist in the v0 page
    format, so there is exactly one instance (:data:`v1_builders`), unlike
    the v0/v1 pairs in every other 5e parser."""

    team_finder: Callable[[BeautifulSoup], list[str]]
    shot_event_finder: Callable[[BeautifulSoup], list[Tag]]
    script_extractor: Callable[[str], Optional[str]]
    title_extractor: Callable[[Tag], Optional[str]]
    event_period_finder: Callable[[Tag], Optional[int]]
    event_time_finder: Callable[[Tag], Optional[float]]
    event_player_finder: Callable[[Tag], Optional[str]]
    shot_location_finder: Callable[[Tag], Optional[tuple[float, float]]]
    event_score_finder: Callable[[Tag], Optional[Score]]
    shot_result_finder: Callable[[Tag], Optional[bool]]
    shot_taking_team_finder: Callable[[Tag], Optional[str]]


def _v1_team_finder(doc: BeautifulSoup) -> list[str]:
    """``table[align=center] img[alt]`` (``:53-56``)."""
    return [str(img.get("alt", "")) for img in doc.select("table[align=center] img[alt]")]


def _v1_shot_event_finder(doc: BeautifulSoup) -> list[Tag]:
    """``circle.shot`` (``:58-61``)."""
    return doc.select("circle.shot")


#: Raw ``<script>...</script>`` blocks, wrapper included (mirrors JSoup
#: ``outerHtml``). Extraction runs on the RAW page string, NOT the parsed
#: doc: JSoup treats script content as a DataNode and never decodes HTML
#: entities inside it, but bs4/lxml's entity handling inside ``<script>``
#: varies by libxml2 build (Linux/macOS decode ``&#39;`` -> ``'``, Windows
#: does not), which broke the single-quote-delimited ``addShot`` capture on
#: titles containing encoded apostrophes (e.g. ``De&#39;Shayne``). A raw-text
#: regex is platform-independent and byte-faithful to the JSoup semantics.
_SCRIPT_BLOCK_RE = re.compile(r"<script\b[^>]*>.*?</script\s*>", re.DOTALL | re.IGNORECASE)


def _v1_script_extractor(raw_html: str) -> Optional[str]:
    """Every ``<script>`` block (wrapper included, matching JSoup
    ``outerHtml``) whose first 128 chars contain ``"addShot("``, joined with
    ``"\\n"`` (``:63-73``). Operates on the raw HTML string -- see
    :data:`_SCRIPT_BLOCK_RE` for why the parsed doc must not be used here."""
    joined = "\n".join(m.group(0) for m in _SCRIPT_BLOCK_RE.finditer(raw_html) if "addShot(" in m.group(0)[:128])
    return joined if joined else None


def _v1_title_extractor(event: Tag) -> Optional[str]:
    """First ``<title>`` descendant's text (``:75-76``)."""
    titles = event.select("title")
    return jsoup_text(titles[0]) if titles else None


#: ``[0-9]+(st|nd|rd|th) HH:MM:SS...`` -- a FULL match (Scala's
#: ``Regex.unapplySeq`` requires ``Matcher.matches()``) (``:78-79``).
_PERIOD_RE = re.compile(r"([0-9]+)(?:st|nd|rd|th) [0-9]+:[0-9]+:[0-9]+.*")

#: ``...MM:SS[:CC] ...`` (``:86``).
_TIME_RE = re.compile(r".*?([0-9]+):([0-9]+)(?:[:]([0-9]+))? .*")

#: ``... made|missed by NAME(TEAM) NN-NN`` (``:125-126``).
_PLAYER_AND_TEAM_RE = re.compile(r".*?(?:made|missed) by *?(.*?) [0-9]+-[0-9]+.*")

#: ``... NN-NN`` at the end of the title (``:147``).
_SCORE_RE = re.compile(r".* ([0-9]+)[-]([0-9]+)$")

#: ``...: made|missed by...`` (``:155``).
_MADE_OR_MISSED_RE = re.compile(r".*?: (made|missed) by.*")


def _v1_event_period_finder(event: Tag) -> Optional[int]:
    """``:80-84``."""
    title = _v1_title_extractor(event)
    if title is None:
        return None
    m = _PERIOD_RE.fullmatch(title)
    return int(m.group(1)) if m is not None else None


def _v1_event_time_finder(event: Tag) -> Optional[float]:
    """``:86-96``. The third (centisecond-ish) group divides by ``6000.0``,
    verbatim -- not ``100.0``; ported exactly as the Scala computes it."""
    title = _v1_title_extractor(event)
    if title is None:
        return None
    m = _TIME_RE.fullmatch(title)
    if m is None:
        return None
    minutes = int(m.group(1))
    seconds = int(m.group(2))
    centis = int(m.group(3)) if m.group(3) is not None else 0
    return minutes + seconds / 60.0 + centis / 6000.0


def _resolve_team_name(name_and_team: str) -> Optional[tuple[str, str]]:
    """Splits ``"NAME(TEAM)"`` into ``(name, team)``, handling a team name
    that itself contains balanced parens (e.g. ``"St. Francis (PA)"``)
    (``ShotEventParser.resolve_team_name``, ``:98-122``).

    Scans ``name_and_team`` in reverse (right to left) tracking a bracket
    depth counter: the first ``)`` (if the string doesn't end in ``)``,
    there's no team at all) opens depth 1; matching nested parens increment/
    decrement; the ``(`` that returns depth to ``0`` closes the team name and
    every character before it is skipped (depth ``-1`` is the "done, ignore
    the rest" sentinel). Ported as a literal transliteration of the Scala
    ``foldLeft`` state machine -- not a regex, because regex cannot express
    "match the LAST balanced paren group" without look-around (unsupported
    by polars/Rust regex elsewhere in this project, and not attempted here
    either since Python's ``re`` has no simpler equivalent).

    Args:
        name_and_team: The captured ``"NAME(TEAM)"`` text.

    Returns:
        ``(name, team)`` with the trailing ``"(TEAM)"`` stripped from
        ``name``, or ``None`` if ``name_and_team`` doesn't end in a
        balanced-paren team name at all.
    """
    acc: list[str] = []
    depth = 0
    for char in reversed(name_and_team):
        if depth == -1:
            continue
        if char == ")" and depth == 0:
            depth = 1
        elif char == ")":
            acc.insert(0, char)
            depth += 1
        elif char == "(" and depth == 1:
            depth = -1
        elif char == "(":
            acc.insert(0, char)
            depth -= 1
        elif depth == 0:
            depth = -1
        else:
            acc.insert(0, char)
    team = "".join(acc)
    if not team:
        return None
    return (name_and_team[: len(name_and_team) - (len(team) + 2)], team)


def _v1_event_player_finder(event: Tag) -> Optional[str]:
    """``:124-137``."""
    title = _v1_title_extractor(event)
    if title is None:
        return None
    m = _PLAYER_AND_TEAM_RE.fullmatch(title)
    if m is None:
        return None
    resolved = _resolve_team_name(m.group(1))
    if resolved is None:
        return None
    name = name_in_v0_box_format(resolved[0].strip()).strip()
    return name if name else None


def _v1_shot_location_finder(event: Tag) -> Optional[tuple[float, float]]:
    """``cx``/``cy`` attributes, parsed as floats (``:139-145``)."""
    x_str, y_str = event.get("cx"), event.get("cy")
    if x_str is None or y_str is None:
        return None
    try:
        return (float(str(x_str)), float(str(y_str)))
    except ValueError:
        return None


def _v1_event_score_finder(event: Tag) -> Optional[Score]:
    """``:147-153``."""
    title = _v1_title_extractor(event)
    if title is None:
        return None
    m = _SCORE_RE.fullmatch(title)
    return Score(int(m.group(1)), int(m.group(2))) if m is not None else None


def _v1_shot_result_finder(event: Tag) -> Optional[bool]:
    """``:155-161``."""
    title = _v1_title_extractor(event)
    if title is None:
        return None
    m = _MADE_OR_MISSED_RE.fullmatch(title)
    return (m.group(1) == "made") if m is not None else None


def _v1_shot_taking_team_finder(event: Tag) -> Optional[str]:
    """``:163-171``."""
    title = _v1_title_extractor(event)
    if title is None:
        return None
    m = _PLAYER_AND_TEAM_RE.fullmatch(title)
    if m is None:
        return None
    resolved = _resolve_team_name(m.group(1))
    if resolved is None:
        return None
    team = resolved[1].strip()
    return team if team else None


#: The only builders table (v1-only page format, ``ShotEventParser
#: .v1_builders``, ``:52-172``).
v1_builders = ShotEventBuilders(
    team_finder=_v1_team_finder,
    shot_event_finder=_v1_shot_event_finder,
    script_extractor=_v1_script_extractor,
    title_extractor=_v1_title_extractor,
    event_period_finder=_v1_event_period_finder,
    event_time_finder=_v1_event_time_finder,
    event_player_finder=_v1_event_player_finder,
    shot_location_finder=_v1_shot_location_finder,
    event_score_finder=_v1_event_score_finder,
    shot_result_finder=_v1_shot_result_finder,
    shot_taking_team_finder=_v1_shot_taking_team_finder,
)


# ---------------------------------------------------------------------------
# create_shot_event_data -- the entry point (``:175-259``)
# ---------------------------------------------------------------------------


def _build_request_error(filename: str, exc: Exception) -> list[ParseError]:
    """Wraps an HTML-parse exception as a ``list[ParseError]``
    (``ParseUtils.build_request``'s ``Try(...)`` branch, mirrored from the
    other 5e parsers' identical inline pattern)."""
    return [
        ParseError(
            location=_LOCATION_PARSE_SHOTEVENT,
            id=f"[{filename}]" if filename else "",
            messages=[f"Exception=[{exc}]"],
        )
    ]


def create_shot_event_data(
    filename: str,
    in_html: str,
    box_lineup: LineupEvent,
) -> Union[list[ShotEvent], list[ParseError]]:
    """Parses a game page's SVG shot map into a list of :class:`~sportsdataverse
    .mbb.mbb_ncaa_models.ShotEvent` (``ShotEventParser.create_shot_event_data``,
    ``:175-259``).

    Args:
        filename: The source file name, used only for error reporting.
        in_html: The raw game-page HTML (containing the SVG shot map, either
            baked in as ``circle.shot`` elements or built client-side via an
            ``addShot(...)`` JS call -- see :func:`shot_js_to_html`).
        box_lineup: The team's box-score lineup event (supplies ``team``/
            ``year``/``location_type`` and the tidy-name lookup context).

    Returns:
        Every shot found, sorted chronologically and court-geometry
        enriched, or a ``list[ParseError]`` if the HTML couldn't be parsed,
        the team names couldn't be matched, no shot events were found (even
        after the JS fallback), or any one circle failed to parse (the first
        such failure's error(s) only -- Scala's ``.sequence`` over
        ``List[Either[...]]`` is fail-fast, not accumulating).

    Example:
        Quick start::

            from pathlib import Path
            from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
            from sportsdataverse.mbb.mbb_ncaa_models import TeamId
            from sportsdataverse.mbb.mbb_ncaa_shot_parser import create_shot_event_data

            box_html = Path("tests/fixtures/ncaa/test_lineup.html").read_text(encoding="utf-8")
            box_lineup = get_box_lineup("test_p1.html", box_html, TeamId("TeamA"), format_version=1)
            shots = create_shot_event_data("test_p1.html", box_html, box_lineup)
    """
    builders = v1_builders
    try:
        doc = parse_html(in_html)
    except Exception as exc:  # pragma: no cover - bs4/lxml is lenient; mirrors Scala's Try(request)
        return _build_request_error(filename, exc)

    home_hint, away_hint = sides_from_box(box_lineup)
    team_info = parse_team_name(
        builders.team_finder(doc), box_lineup.team.team, box_lineup.team.year, home_hint, away_hint
    )
    if isinstance(team_info, ParseError):
        return enrich_sub_error(_LOCATION_PARSE_SHOTEVENT, filename, team_info)
    _, _, target_team_first = team_info

    tidy_ctx = build_tidy_player_context(box_lineup)

    html_events = builders.shot_event_finder(doc)
    if not html_events:
        # The page is built client-side -- convert the JS addShot(...) calls
        # to HTML. The extractor takes the RAW html (not `doc`): entity
        # handling inside <script> is libxml2-build-dependent, JSoup's isn't.
        script = builders.script_extractor(in_html)
        html_events = shot_js_to_html(script) if script is not None else []
        if not html_events:
            return [build_sub_error(_LOCATION_PARSE_SHOTEVENT, error=f"No shot events found [{doc}]")]

    very_raw_events: list[tuple[int, ShotEvent]] = []
    for html_event in html_events:
        result = parse_shot_html(html_event, box_lineup, builders, tidy_ctx, target_team_first)
        if isinstance(result, list):  # list[ParseError] -- fail fast, matching Scala's .sequence
            return result
        very_raw_events.append(result)

    # (switch to correctly sorted ascending times)
    sorted_very_raw_events = sorted(very_raw_events, key=lambda ps: ps[0] * 1000 - ps[1].min)

    return phase1_shot_event_enrichment(sorted_very_raw_events)


def shot_js_to_html(js: str) -> list[Tag]:
    """Converts client-side ``addShot(...)`` JS calls into parseable
    ``circle.shot`` HTML, for pages where the shot map is built on the fly
    rather than baked into the initial HTML (``ShotEventParser
    .shot_js_to_html``, ``:266-283``). See the module docstring's "Scala
    idiom decision" note -- the Scala's ``builders``/``browser`` parameters
    are dropped here since the Scala body never actually uses them.

    Args:
        js: The concatenated ``<script>`` text containing one or more
            ``addShot(x, y, ..., 'title', ...)`` calls, one per line.

    Returns:
        The ``circle.shot`` elements reconstructed from every matching line
        (non-matching lines, e.g. the ``addShot`` function definition line
        itself, are silently skipped).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_shot_parser import shot_js_to_html
            js = "addShot(27.0, 77.0, 392, false, 1, 'title text', 'class', false);"
            circles = shot_js_to_html(js)
    """
    js_line_re = re.compile(r" *addShot[(]([^,]+), *([^,]+), *([^,]+), *([^,]+), *([^,]+), *'([^']+)',.*")
    circles_html = []
    for line in js.split("\n"):
        m = js_line_re.fullmatch(line)
        if m is None:
            continue
        cx = 0.01 * float(m.group(1)) * ShotMapDimensions.court_length_x_px
        cy = 0.01 * float(m.group(2)) * ShotMapDimensions.court_width_y_px
        title = m.group(6)
        # DELIBERATE DIVERGENCE from the Scala literal (:278), which emits a
        # malformed `<title>{title}<title/>` closing tag. JSoup's error
        # recovery happens to rebuild the intended per-circle structure from
        # that, but `<title>` is a rawtext element in HTML parsing and
        # whether `<title/>` terminates it is libxml2-BUILD-DEPENDENT
        # (Linux/macOS swallow every subsequent circle into the first title;
        # Windows recovers) -- this broke CI cross-platform. The synthesized
        # string is an internal intermediate (never oracle-asserted; the
        # resulting locations/titles are), so we emit well-formed
        # `</title>`, which parses identically everywhere and yields the
        # same downstream values JSoup produced.
        circles_html.append(f'<circle class="shot" cx="{cx}" cy="{cy}" r="5"><title>{title}</title></circle>')
    return _v1_shot_event_finder(parse_html("\n".join(circles_html)))


def build_base_event(box_lineup: LineupEvent) -> ShotEvent:
    """Fills in the fields a shot event can borrow straight from the
    box-score lineup, leaving the shot-specific fields as overridable
    placeholders (``ShotEventParser.build_base_event``, ``:379-410``).

    Args:
        box_lineup: The team's box-score lineup event.

    Returns:
        A :class:`~sportsdataverse.mbb.mbb_ncaa_models.ShotEvent` with
        ``date``/``location_type``/``team``/``opponent`` populated and every
        other field at its Scala-literal placeholder default (``player=None``,
        ``is_off=True``, ``lineup_id=None``, ``players=[]``, ``score=Score(0,
        0)``, ``min=0.0``, ``loc=ShotLocation(0.0, 0.0)``, ``geo=ShotGeo(0.0,
        0.0)``, ``dist=0.0``, ``pts=0``, ``value=0``, ``ast_by=None``,
        ``is_ast=None``, ``is_trans=None``, ``raw_event=None``) -- every
        caller immediately overrides the placeholders it cares about via
        :func:`dataclasses.replace`.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_shot_parser import build_base_event
            base = build_base_event(box_lineup)
    """
    return ShotEvent(
        player=None,
        date=box_lineup.date,
        location_type=box_lineup.location_type,
        team=box_lineup.team,
        opponent=box_lineup.opponent,
        is_off=True,
        lineup_id=None,
        players=[],
        score=Score(0, 0),
        min=0.0,
        loc=ShotLocation(0.0, 0.0),
        geo=ShotGeo(0.0, 0.0),
        dist=0.0,
        pts=0,
        value=0,
        ast_by=None,
        is_ast=None,
        is_trans=None,
        raw_event=None,
    )


def parse_shot_html(
    event: Tag,
    box_lineup: LineupEvent,
    builders: ShotEventBuilders,
    tidy_ctx: TidyPlayerContext,
    target_team_first: bool,
) -> Union[tuple[int, ShotEvent], list[ParseError]]:
    """An initial parse of one shot ``<circle>`` based solely on the element
    itself -- some fields cannot be filled in until the full collection of
    events provides context (``ShotEventParser.parse_shot_html``,
    ``:288-377``).

    Args:
        event: The ``circle.shot`` element to parse.
        box_lineup: The team's box-score lineup event.
        builders: The finder-function table (:data:`v1_builders`).
        tidy_ctx: The tidy-name lookup context (see :func:`~sportsdataverse
            .mbb.mbb_ncaa_names.build_tidy_player_context`).
        target_team_first: Whether the target team's title appeared first
            among :attr:`ShotEventBuilders.team_finder`'s results.

    Returns:
        ``(period, shot_event)`` on success, or a single-element
        ``list[ParseError]`` naming which of the 7 extracted fields
        (period/time/player/location/score/result/shooting-team, by index)
        came back ``None``.

    Raises:
        ValueError: If ``box_lineup.location_type`` is
            ``LocationType.SEMI_HOME``/``SEMI_AWAY`` -- landmine, ported for
            parity: the Scala ``match`` on ``Game.LocationType`` here only
            handles ``Home``/``Away``/``Neutral`` (``:344-351``) and would
            raise ``MatchError`` at runtime for either semi-* value too;
            neither language's parser is expected to see a semi-neutral game
            in this code path.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_shot_parser import parse_shot_html, v1_builders
            result = parse_shot_html(circle, box_lineup, v1_builders, tidy_ctx, target_team_first=True)
    """
    period = builders.event_period_finder(event)
    time = builders.event_time_finder(event)
    raw_player = builders.event_player_finder(event)
    location = builders.shot_location_finder(event)
    score = builders.event_score_finder(event)
    result = builders.shot_result_finder(event)
    raw_shot_taking_team = builders.shot_taking_team_finder(event)

    values: tuple[object, ...] = (period, time, raw_player, location, score, result, raw_shot_taking_team)
    if any(v is None for v in values):
        missing = ",".join(str(i) for i, v in enumerate(values) if v is None)
        return [
            build_sub_error(
                _LOCATION_PARSE_SHOTEVENT,
                error=f"Missing fields from shot: param_indices=[{missing}] in [{event}]",
            )
        ]
    assert period is not None and time is not None and raw_player is not None and location is not None
    assert score is not None and result is not None and raw_shot_taking_team is not None

    player_name = remove_html_encoding(raw_player)
    shot_taking_team = remove_html_encoding(raw_shot_taking_team)
    is_offensive = box_lineup.team.team.name == shot_taking_team

    if is_offensive:
        tidier_player_name, _ = tidy_player(player_name, tidy_ctx)
        player_code_id = code_from_box(tidier_player_name, box_lineup, box_lineup.team.team)
    else:
        # Still extract the opponent's player name (best-effort, to help
        # correlate with the play-by-play data later).
        player_code_id = build_player_code(player_name, None)

    if box_lineup.location_type == LocationType.HOME:
        oriented_score = score
    elif box_lineup.location_type == LocationType.AWAY:
        oriented_score = Score(score.allowed, score.scored)
    elif box_lineup.location_type == LocationType.NEUTRAL:
        oriented_score = score if target_team_first else Score(score.allowed, score.scored)
    else:
        raise ValueError(
            f"parse_shot_html: unhandled location_type {box_lineup.location_type!r} "
            "(ShotEventParser.scala:344-351 has no Semi* case either -- Scala MatchError parity)"
        )

    shot_event = replace(
        build_base_event(box_lineup),
        player=player_code_id,
        is_off=is_offensive,
        score=oriented_score,
        min=time,
        raw_event=builders.title_extractor(event),
        loc=ShotLocation(x=location[0], y=location[1]),
        pts=1 if result else 0,
    )
    return (period, shot_event)


# ---------------------------------------------------------------------------
# phase1_shot_event_enrichment -- the court-geometry pipeline (``:412-620``)
# ---------------------------------------------------------------------------

#: Shots farther than this (feet) from the basket are suspiciously long --
#: used by the self-correcting side-flip heuristic (``:419``).
_LONG_DISTANCE = 50.0

#: Synthetic geo base point + earth radius, for the lat/lon conversion
#: (``:473-477``). Not a real-world location.
_BASE_LAT = 40.750298
_BASE_LON = -73.993324
_EARTH_RADIUS_M = 6371000.0

#: Degree<->radian conversion, computed as the EXACT same two-step chain as
#: the Scala (``:475-476``) -- see the module docstring's geometry note on
#: why this isn't :func:`math.degrees`/:func:`math.radians`.
_PI_OVER_180 = math.pi / 180.0
_180_OVER_PI = 1.0 / _PI_OVER_180


def get_ascending_time(event: ShotEvent, period: int, is_women_game: bool) -> float:
    """Converts the descending in-period clock time to an ascending
    game-elapsed time (``ShotEventParser.get_ascending_time``, ``:531-537``).

    Args:
        event: The shot event (only :attr:`~sportsdataverse.mbb
            .mbb_ncaa_models.ShotEvent.min`, the raw descending clock
            minute, is read).
        period: The 1-indexed period the shot was taken in.
        is_women_game: Whether to use women's-quarters (10min) or men's-
            halves (20min, then 5min OTs) period lengths.

    Returns:
        The ascending game-elapsed time, in minutes.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_shot_parser import get_ascending_time
            get_ascending_time(shot_with_min_4, period=1, is_women_game=False)  # 16.0
    """
    return duration_from_period(period, is_women_game) - event.min


def is_team_shooting_left_to_start(sorted_very_raw_events: list[tuple[int, ShotEvent]]) -> tuple[bool, int]:
    """Infers which side of the SVG court the team under analysis shoots
    towards in the first period, from its own made/missed shot locations
    (``ShotEventParser.is_team_shooting_left_to_start``, ``:540-555``).

    Args:
        sorted_very_raw_events: The chronologically-sorted (period, shot)
            pairs, pre-geometry-transform.

    Returns:
        ``(team_shooting_left_in_first_period, first_period)`` -- the first
        element of ``sorted_very_raw_events``, if any, determines
        ``first_period``; the majority side (by count) of the team's own
        (``is_off``) shots within that period determines the direction.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_shot_parser import is_team_shooting_left_to_start
            is_team_shooting_left_to_start([(1, shot_a), (1, shot_b)])
    """
    first_period = sorted_very_raw_events[0][0] if sorted_very_raw_events else 1
    first_period_shots = [shot for _, shot in takewhile(lambda ps: ps[0] == first_period, sorted_very_raw_events)]
    offensive_shots = [shot for shot in first_period_shots if shot.is_off]
    shots_to_left = [shot for shot in offensive_shots if shot.loc.x < ShotMapDimensions.half_court_x_px]
    shots_to_right = [shot for shot in offensive_shots if not (shot.loc.x < ShotMapDimensions.half_court_x_px)]
    return (len(shots_to_left) > len(shots_to_right), first_period)


def is_women_game(sorted_very_raw_events: list[tuple[int, ShotEvent]]) -> bool:
    """Infers men's vs. women's game from timing evidence
    (``ShotEventParser.is_women_game``, ``:558-566``). **Shot-parser-specific
    variant** -- distinct from the play-by-play parser's own
    ``is_women_game`` (Task 5e.3), which uses PbP event timing instead of
    shot timing; the plan's recon flags both as "its OWN is_women_game
    variant" per module.

    Args:
        sorted_very_raw_events: The chronologically-sorted (period, shot)
            pairs.

    Returns:
        ``True`` if at least 4 periods were seen AND no shot was taken with
        more than 10 minutes showing on the (descending) clock in the very
        first event (women's quarters are 10 minutes; a shot at >10:00
        remaining could only happen in a longer men's period).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_shot_parser import is_women_game
            is_women_game([(1, shot), (2, shot), (3, shot), (4, shot)])  # True
    """
    num_periods = sorted_very_raw_events[-1][0] if sorted_very_raw_events else 2
    shot_taken_before_1st_quarter_starts = sorted_very_raw_events[0][1].min > 10.0 if sorted_very_raw_events else False
    return num_periods >= 4 and not shot_taken_before_1st_quarter_starts


def transform_shot_location(
    x: float,
    y: float,
    second_half_switch: bool,
    team_shooting_left_in_first_period: bool,
    is_offensive: bool,
) -> tuple[float, float, float, float]:
    """Transforms a raw SVG pixel location into feet from the basket, always
    oriented as if shooting towards the left goal (``ShotEventParser
    .transform_shot_location``, ``:588-620``).

    Args:
        x: Raw SVG ``cx`` pixel coordinate.
        y: Raw SVG ``cy`` pixel coordinate.
        second_half_switch: Whether this shot is in the "other" half of the
            game from :attr:`team_shooting_left_in_first_period` (each
            ``False`` factor below flips which side is treated as "left").
        team_shooting_left_in_first_period: Whether the team under analysis
            shot towards the left goal in the first period (see
            :func:`is_team_shooting_left_to_start`).
        is_offensive: Whether the team under analysis is shooting (an
            opponent shot flips the expected side again).

    Returns:
        ``(x, y, alt_x, alt_y)`` in feet -- the believed-correct location,
        then the alternative (mirror-image) location, both relative to the
        goal the shot is (believed to be) attacking.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_shot_parser import transform_shot_location
            transform_shot_location(310.2, 235, False, False, True)
    """
    factors = (team_shooting_left_in_first_period, not second_half_switch, is_offensive)
    product = 1
    for factor in factors:
        product *= 1 if factor else -1
    goal_is_to_left = product > 0

    alt_x = ShotMapDimensions.court_length_x_px - x
    alt_y = ShotMapDimensions.court_width_y_px - y
    if goal_is_to_left:
        trans_x, trans_y, alt_trans_x, alt_trans_y = x, y, alt_x, alt_y
    else:
        trans_x, trans_y, alt_trans_x, alt_trans_y = alt_x, alt_y, x, y

    return (
        (trans_x - ShotMapDimensions.goal_left_x_px) * ShotMapDimensions.ft_per_px_x,
        (ShotMapDimensions.goal_y_px - trans_y) * ShotMapDimensions.ft_per_px_y,
        (alt_trans_x - ShotMapDimensions.goal_left_x_px) * ShotMapDimensions.ft_per_px_x,
        (ShotMapDimensions.goal_y_px - alt_trans_y) * ShotMapDimensions.ft_per_px_y,
    )


def phase1_shot_event_enrichment(
    sorted_very_raw_events: list[tuple[int, ShotEvent]],
    second_half_override: Optional[set[int]] = None,
) -> list[ShotEvent]:
    """The court-geometry enrichment pass: ascending time, coordinate
    transform + geo synthesis, and the self-correcting side-flip re-run
    (``ShotEventParser.phase1_shot_event_enrichment``, ``:415-528``).

    For each shot: compute the ascending game time, decide (from
    :func:`is_team_shooting_left_to_start` + which half the period falls in)
    whether the shot's side needs flipping, run :func:`transform_shot_location`
    to get both the believed-correct and alternative (mirrored) locations,
    keep whichever is closer to the basket (a >1.2x distance advantage for
    the "alternative" wins, or ANY shot taken with <0.1 min left on the
    clock always keeps the original -- a half-court heave near the buzzer
    is plausible, so the tie-break favors trusting the raw geometry there),
    then synthesize a lat/lon.

    After all shots are processed, if any period had >=6 shots AND more than
    75% of them came back implausibly long-distance (>50ft), the whole pass
    re-runs ONCE with those periods' orientation flipped (the self-correcting
    part) -- ``second_half_override`` is ``None`` on the initial call and a
    non-``None`` set on the one allowed retry, preventing infinite recursion.

    Args:
        sorted_very_raw_events: The chronologically-sorted (period, shot)
            pairs from :func:`parse_shot_html`, pre-geometry-transform.
        second_half_override: The set of periods whose
            ``second_half_switch`` orientation should be inverted (the
            self-correction re-run's input); ``None`` on the first call.

    Returns:
        The fully court-geometry-enriched shots, in the same order as
        ``sorted_very_raw_events``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_shot_parser import phase1_shot_event_enrichment
            shots = phase1_shot_event_enrichment([(1, very_raw_shot)])
    """
    women_game = is_women_game(sorted_very_raw_events)
    team_shooting_left_in_first_period, first_period = is_team_shooting_left_to_start(sorted_very_raw_events)

    total_shots: dict[int, int] = {}
    long_shots: dict[int, int] = {}
    shots: list[ShotEvent] = []

    for period, shot in sorted_very_raw_events:
        ascending_time = get_ascending_time(shot, period, women_game)
        if women_game:
            second_half_switch = period > 2 and first_period <= 2
        else:
            second_half_switch = period > 1 and first_period <= 1

        flip_this_period = second_half_override is not None and period in second_half_override
        effective_switch = (not second_half_switch) if flip_this_period else second_half_switch

        x, y, alt_x, alt_y = transform_shot_location(
            shot.loc.x, shot.loc.y, effective_switch, team_shooting_left_in_first_period, shot.is_off
        )
        dist = math.sqrt(x * x + y * y)
        alt_dist = math.sqrt(alt_x * alt_x + alt_y * alt_y)

        if (dist < 1.2 * alt_dist) or (shot.min < 0.1):
            # (the 1.2x means if the 2 are close we trust the original more)
            trans_shot = replace(shot, loc=ShotLocation(x, y), dist=dist, min=ascending_time)
        else:
            trans_shot = replace(shot, loc=ShotLocation(alt_x, alt_y), dist=alt_dist, min=ascending_time)

        # Fake geo for the shot (feet -> a synthetic lat/lon offset).
        shot_lat = _BASE_LAT + (trans_shot.loc.y / _EARTH_RADIUS_M) * _180_OVER_PI
        eff_radius = _EARTH_RADIUS_M * math.cos(shot_lat * _PI_OVER_180)
        shot_lon = _BASE_LON + (trans_shot.loc.x / eff_radius) * _180_OVER_PI
        trans_shot_with_geo = replace(trans_shot, geo=ShotGeo(lat=shot_lat, lon=shot_lon))

        total_shots[period] = total_shots.get(period, 0) + 1
        if dist > _LONG_DISTANCE:
            long_shots[period] = long_shots.get(period, 0) + 1

        shots.append(trans_shot_with_geo)

    # (use "dist" to look for long shots -- systematically bad periods
    # where the court is flipped show up as mostly-implausible distances)
    problem_periods = {
        period
        for period, count in long_shots.items()
        if total_shots.get(period, 1) >= 6 and count > 0.75 * total_shots.get(period, 1)
    }

    if problem_periods:
        first_shot = sorted_very_raw_events[0][1] if sorted_very_raw_events else None
        team = first_shot.team.team if first_shot is not None else None
        oppo = first_shot.opponent.team if first_shot is not None else None
        print(
            f"[p1_s_e_e] [WARNING] [{team}]v[{oppo}] Flip court for periods: [{sorted(problem_periods)}] "
            f"because [total_shots={total_shots} long_shots={long_shots}]"
        )

    if problem_periods and second_half_override is None:
        return phase1_shot_event_enrichment(sorted_very_raw_events, problem_periods)
    return shots
