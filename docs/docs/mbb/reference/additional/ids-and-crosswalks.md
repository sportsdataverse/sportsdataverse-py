---
title: "MBB — additional Python functions — IDs and crosswalks"
sidebar_label: "IDs and crosswalks"
sidebar_position: 12
description: "MBB — additional Python functions — IDs and crosswalks — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — IDs and crosswalks

### FuzzyMatchError {#FuzzyMatchError}

`FuzzyMatchError(message: 'str') -> None`

A failed `fuzzy_box_match` resolution (Scala's `Left[String]`

half of `Either[String, String]` -- Python has no `Either`, so the
error is returned directly; check `isinstance(result, FuzzyMatchError)`,
matching the `parse_team_name` / `~sportsdataverse.mbb.mbb_ncaa_data_quality.ParseError`
convention already used in this port).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `message` | `str` |  | Human-readable description of why no name won. |

### NoSurnameMatch {#NoSurnameMatch}

`NoSurnameMatch(box_name: 'str', exact_first_name: 'Optional[str]', near_first_name: 'Optional[str]', err: 'str') -> None`

No candidate surname fragment scored well enough

(`NameFixer.NoSurnameMatch`, `:638-643`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_name` | `str` |  | The box-score name compared against. |
| `exact_first_name` | `Optional[str]` |  | A first-name fragment shared verbatim between candidate and box name, if any. |
| `near_first_name` | `Optional[str]` |  | A first-name fragment fuzzy-matching the box name's first name, if any (only computed when `exact_first_name` is absent). |
| `err` | `str` |  | Human-readable diagnostic (debug-only; see the module docstring's fuzzy-match-parity note for why its embedded score may not byte-match the upstream Java oracle). |

### StrongSurnameMatch {#StrongSurnameMatch}

`StrongSurnameMatch(box_name: 'str', score: 'int') -> None`

A surname fragment matched and the whole-name score cleared

`MIN_OVERALL_SCORE` (`NameFixer.StrongSurnameMatch`, `:646-647`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_name` | `str` |  | The box-score name compared against. |
| `score` | `int` |  | The whole-name similarity score. |

### TidyPlayerContext {#TidyPlayerContext}

`TidyPlayerContext(box_lineup: 'LineupEvent', all_players_map: 'dict[str, str]', alt_all_players_map: 'dict[str, list[str]]', resolution_cache: 'dict[str, str]' = <factory>) -> None`

Precomputed box-score lookup tables + resolution cache for

`tidy_player` (`LineupErrorAnalysisUtils.TidyPlayerContext`,
`:31-36`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_lineup` | `LineupEvent` |  | The box-score lineup event this context resolves names against. |
| `all_players_map` | `dict[str, str]` |  | Player code -> full name, for every player in `box_lineup.players`. |
| `alt_all_players_map` | `dict[str, list[str]]` |  | Truncated player code (see truncate_code_1` / truncate_code_2`) -> the list of full names sharing that truncation -- used when the exact code doesn't match but a unique truncated one does. |
| `resolution_cache` | `dict[str, str]` | `<factory>` | Memoizes prior `tidy_player` resolutions. See the module docstring's "Behavioral quirk" note -- this is read by the raw input name but written by the corrected name, faithfully reproducing the upstream asymmetry. |

### WeakSurnameMatch {#WeakSurnameMatch}

`WeakSurnameMatch(box_name: 'str', score: 'int', info: 'str') -> None`

A surname fragment matched, but the whole-name score fell short of

`MIN_OVERALL_SCORE` (`NameFixer.WeakSurnameMatch`, `:644-645`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_name` | `str` |  | The box-score name compared against. |
| `score` | `int` |  | The whole-name similarity score. |
| `info` | `str` |  | Human-readable diagnostic (debug-only; see the fuzzy-match- parity note). |

### box_aware_compare {#box_aware_compare}

`box_aware_compare(candidate_in: 'str', box_name_in: 'str') -> 'MatchResult'`

Score how well a single play-by-play candidate name fits a single

box-score name (`NameFixer.box_aware_compare`, `:658-766`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `candidate_in` | `str` |  | The raw play-by-play name fragment. |
| `box_name_in` | `str` |  | One box-score player's full name (`"Surname, First [Middle]"` format). |

**Returns**

A `StrongSurnameMatch` / `WeakSurnameMatch` / `NoSurnameMatch`, per the surname- and whole-name-score thresholds.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import box_aware_compare
box_aware_compare("Tuitele, Peanut", "Tuitele, Peanut")
# StrongSurnameMatch(box_name='Tuitele, Peanut', score=100)
```

### build_tidy_player_context {#build_tidy_player_context}

`build_tidy_player_context(box_lineup: 'LineupEvent') -> 'TidyPlayerContext'`

Build the alternative player-code lookup maps for a box-score lineup

(`LineupErrorAnalysisUtils.build_tidy_player_context`, `:59-73`).

Sometimes the play-by-play uses `SURNAME,INITIAL` instead of
`SURNAME,NAME`, or `SURNAME,NAME1` instead of `SURNAME,NAME1
NAME2` -- both collapse to the same *truncated* code, so grouping by
truncated code (and only keeping groups with exactly one distinct name)
lets `tidy_player` recover the box-score name unambiguously.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_lineup` | `LineupEvent` |  | The box-score lineup event to index. |

**Returns**

A fresh `TidyPlayerContext` (empty `resolution_cache`).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import build_tidy_player_context
ctx = build_tidy_player_context(box_lineup)
```

### code_from_box {#code_from_box}

`code_from_box(name: 'str', box_lineup: 'LineupEvent', team: 'Optional[TeamId]' = None) -> 'PlayerCodeId'`

Resolve a tidied player NAME to the box roster's own `PlayerCodeId`.

`~sportsdataverse.mbb.mbb_ncaa_stints.build_player_code` is a
faithful `ExtractorUtils.scala` port and keys a player as
`{first-two-letters}{Surname}`. When two teammates collide on that --
siblings, overwhelmingly --
`~sportsdataverse.mbb.mbb_ncaa_boxscore_parser.validate_box_score`
widens BOTH to full-name codes so the game is not thrown away.

Re-deriving a code from the tidied name after that point silently undoes
the widening: both Morris twins code back to `MaMorris`, one of them
wins the match, and the other DISAPPEARS from the lineup events. Kansas
2010 parsed 110 events with `MarcusMorris` present 18 times and
`MarkieffMorris` present ZERO times -- a game that looks healthy by
every count while a starter is missing.

So the roster is the authority. **Every PBP-side path that needs a code
for a name must call this, never** `build_player_code`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The tidied player name, as produced by `tidy_player`. |
| `box_lineup` | `LineupEvent` |  | The team's box-score `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent`, whose `players` carry the (possibly widened) codes. |
| `team` | `Optional[TeamId]` | `None` | Team context for the fallback `build_player_code` call, used only when `name` is not on the roster. |

**Returns**

The roster's `~sportsdataverse.mbb.mbb_ncaa_models.PlayerCodeId` when `name` is on it, else a freshly built one.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import code_from_box
code_from_box("Morris, Markieff", box_lineup, box_lineup.team.team)

# The distinction that matters

from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code
build_player_code("Morris, Markieff", team).code  # "MaMorris" -- collides
code_from_box("Morris, Markieff", box_lineup, team).code  # "MarkieffMorris"
```

### convert_from_digits {#convert_from_digits}

`convert_from_digits(name: 'str', player_numbers: 'list[PlayerCodeId]') -> 'Optional[str]'`

Resolve a jersey-number-only name to its box-score player

(`LineupErrorAnalysisUtils.convert_from_digits`, `:166-175`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The candidate name; only matches if every character is a digit (an empty string is vacuously all-digit, matching Scala's `forall` on an empty `String`). |
| `player_numbers` | `list[PlayerCodeId]` |  | Candidate `(code, id)` pairs -- typically `box_lineup.players_out`, since a number-only PbP mention almost always refers to a player who just left the game. |

**Returns**

The matching player's full name, or `None` if `name` isn't all-digit or no code matches.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import PlayerCodeId, PlayerId
from sportsdataverse.mbb.mbb_ncaa_names import convert_from_digits
codes = [PlayerCodeId(code="1000", id=PlayerId("name1"))]
convert_from_digits("1000", codes)  # "name1"
```

### convert_from_initials {#convert_from_initials}

`convert_from_initials(name: 'str', codes_to_names: 'dict[str, str]') -> 'Optional[str]'`

Resolve a 2-initial name (`"A B"` / `"B, A"`) to the single

box-score player whose code starts with those initials
(`LineupErrorAnalysisUtils.convert_from_initials`, `:147-164`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The candidate initials string. |
| `codes_to_names` | `dict[str, str]` |  | Player code -> full name (e.g. `TidyPlayerContext.all_players_map`). |

**Returns**

The single matching full name, or `None` if `name` isn't an initials shorthand, or if zero or multiple codes match.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import convert_from_initials
convert_from_initials("A B", {"AoBo": "name1"})  # "name1"
```

### display_name_to_roster_key {#display_name_to_roster_key}

`display_name_to_roster_key(name: 'Optional[str]') -> 'str'`

`"Ballisager Webb, Jermaine"` -> `"JERMAINE.BALLISAGER.WEBB"`.

Box-score and shot-chart pages render a player as `"Surname, First"`,
while `team_rosters` renders the same person as `FIRST.MIDDLE.LAST`
uppercase -- whitespace becomes dots, hyphens collapse, diacritics fold.
Joining the two needs one canonical direction, and this is it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `Optional[str]` |  | The display name (`"Surname, First"`), or `None`. |

**Returns**

The roster-style key, or `""` when the name cannot be split into at least a surname and a first name. An empty key never matches, which is the intended outcome -- an unresolved row beats a wrong join.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import display_name_to_roster_key

display_name_to_roster_key("Clark, Garry")            # "GARRY.CLARK"
display_name_to_roster_key("Wrightsell Jr., Latrell") # "LATRELL.WRIGHTSELL"
display_name_to_roster_key('"TJ" Madlock, Antonio')   # "ANTONIO.MADLOCK"
```

### fuzzy_box_match {#fuzzy_box_match}

`fuzzy_box_match(candidate: 'str', unassigned_box_names: 'list[str]', team_context: 'str') -> 'Union[str, FuzzyMatchError]'`

Pick the single unassigned box-score name a mis-spelled play-by-play

name most likely refers to (`NameFixer.fuzzy_box_match`, `:774-905`).

Resolution order: a single strong match wins outright; multiple strong
matches only resolve if there's a clear (>10-point) winner; failing
that, a single weak match wins; failing that, a first-name-only match
only wins if there are no other first-name matches (exact or fuzzy)
among the un-matched box names.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `candidate` | `str` |  | The raw play-by-play name. |
| `unassigned_box_names` | `list[str]` |  | Box-score full names not yet claimed by another resolution. |
| `team_context` | `str` |  | Debug-only context string (Scala used it to de-duplicate diagnostic prints; this port has no logging surface to de-duplicate, so the value is otherwise unused). |

**Returns**

The winning box-score name, or a `FuzzyMatchError` describing why no single name won.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import fuzzy_box_match
fuzzy_box_match(
    "sirena tuitele",
    ["Suitele, Sirena", "Tuitele, Peanut", "Guity, Amaya"],
    "team_context",
)
# "Suitele, Sirena"
```

### mbb_player_crosswalk {#mbb_player_crosswalk}

`mbb_player_crosswalk(season: 'Optional[int]' = None, min_confidence: 'float' = 0.92, *, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the MBB cross-source player crosswalk (ESPN / Fox).

One row per ESPN athlete per team. Fox is matched by normalized name
within each team block -- exact first (jersey-tiebroken, per hoopR), then
Jaro-Winkler at or above `min_confidence`. KenPom and Torvik publish no
per-player tables, so neither is joined.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent MBB season. |
| `min_confidence` | `float` | `0.92` | Jaro-Winkler floor for fuzzy matches (R default 0.92). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-team ESPN or Fox roster fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN athlete, 17 columns.

**Example**

```python
from sportsdataverse.mbb import mbb_player_crosswalk
df = mbb_player_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Tighten the fuzzy floor

strict = mbb_player_crosswalk(season=2026, min_confidence=0.97)

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "fuzzy_jw").head()
```

### mbb_schedule_crosswalk {#mbb_schedule_crosswalk}

`mbb_schedule_crosswalk(season: 'Optional[int]' = None, *, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the MBB cross-source schedule crosswalk (ESPN / Torvik).

One row per game. Dates reduce to the Eastern-Time game date before
joining and Torvik's unordered `team1`/`team2` join through a sorted
ESPN team-pair key. Torvik games whose teams cannot be resolved to ESPN
ids are dropped (the MBB variant differs from WBB here). `kp_game_id` is
a null placeholder -- the R builder's optional KenPom enrichment needs a
paid subscription and is not ported.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent MBB season. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-date ESPN scoreboard fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas) with `SCHEDULE_COLUMNS`.

**Example**

```python
from sportsdataverse.mbb import mbb_schedule_crosswalk
df = mbb_schedule_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "both").select("espn_game_id", "bart_muid").head()
```

### mbb_team_crosswalk {#mbb_team_crosswalk}

`mbb_team_crosswalk(season: 'Optional[int]' = None, *, fox: 'Optional[pl.DataFrame]' = None, bart: 'Optional[pl.DataFrame]' = None, kenpom: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the MBB cross-source team crosswalk (ESPN / Fox / Torvik / KenPom).

One row per ESPN team, keyed on `espn_team_id`. Fox joins on the
normalized mascot name via `FOX_DISPLAY_ALIAS`; Torvik and KenPom
each join on the normalized school name after `BART_ALIAS` /
`KP_ALIAS`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent MBB season. |
| `fox` | `Optional[DataFrame]` | `None` | Pre-fetched frame with `fox_team_id` / `fox_team_name` / `fox_section`. `None` fetches *season*'s conference standings live (`~sportsdataverse._crosswalk_basketball_sources.fox_season_teams`); Fox has none before 2017-18, so earlier seasons get null `fox_*`. Pass an empty frame to skip Fox. |
| `bart` | `Optional[DataFrame]` | `None` | Pre-fetched `torvik_ratings()` frame. `None` fetches live; Torvik starts in 2008, so earlier seasons get null `bart_*`. |
| `kenpom` | `Optional[DataFrame]` | `None` | KenPom teams frame with `Team` / `Conf`. `None` (the default) uses the KenPom team/conference directory bundled with sdv-py (hoopR's `teams_links`, seasons 2002-2026), filtered to *season*. A season the bundle does not carry gets null `kp_*` columns -- never another season's labels. Pass an empty frame to skip KenPom. No KenPom subscription or credential is involved: the bundled data is the public directory, not ratings. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Accepted for parity with the schedule and player crosswalks, which forward it; the team build has no per-item fetch loop to relax, so every source failure raises. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN team, with `TEAM_COLUMNS`.

**Example**

```python
from sportsdataverse.mbb import mbb_team_crosswalk
df = mbb_team_crosswalk(season=2026)
print(df.shape)

# Skip Fox

import polars as pl
df = mbb_team_crosswalk(season=2026, fox=pl.DataFrame())

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "espn_only").select("espn_display_name").head()
```

### name_is_initials {#name_is_initials}

`name_is_initials(name: 'str') -> 'Optional[tuple[str, str]]'`

Detect a 2-initial name shorthand, e.g. `"A B"` or `"B, A"`

(`ExtractorUtils.name_is_initials`, `ExtractorUtils.scala:94-102`).
Ported here (rather than into `mbb_ncaa_stints.py`) since
`convert_from_initials` was this module's original consumer;
promoted from private to public in Task 5e.1 for
`mbb_ncaa_roster_parser.py`'s `parse_roster` (a second consumer,
which only needs `.nonEmpty` -- whether a match exists at all -- to
reject initials-only roster rows).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The candidate initials string. |

**Returns**

`(p1, p2)` -- `p1` is the leading initial in a `"A B"`-style string, or the trailing initial in a `"B, A"`-style string; `None` if `name` doesn't fit either 3- or 4-character shape.

### ncaa_espn_team_crosswalk {#ncaa_espn_team_crosswalk}

`ncaa_espn_team_crosswalk(league: 'str' = 'mbb', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Season-keyed stats.ncaa.org -> ESPN team-id crosswalk.

One row per `(season, ncaa_team_id)`. Teams that could not be resolved to
an ESPN team are kept with a null `espn_team_id` and
`match_method="unmatched"` -- never dropped -- so the row count always
equals `ncaa_{league}_team_ids()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` | `'mbb'` | `"mbb"` (men's, 2009-10 onward) or `"wbb"` (women's). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

DataFrame with columns `season` (str, `"YYYY-YY"`), `ncaa_team_id` (Int64 -- the season-specific stats.ncaa.org id), `ncaa_team` / `ncaa_conference` (str), `conference_id` (str, nullable -- the SDV group id, e.g. `"mbb:big-ten"`), `espn_team_id` (str, nullable -- ESPN ids are strings throughout sdv-py), `espn_display_name` / `espn_location` / `espn_mascot` / `espn_abbreviation` / `espn_conference_name` / `espn_conference_id` (str, nullable), and `match_method` (str -- `"exact"`, `"dict"`, `"alias"` or `"unmatched"`). The three conference columns and `ncaa_conference` are per season: Maryland is ACC in 2013-14 and Big Ten in 2014-15.

| col_name | type | description |
|---|---|---|
| `season` | character | Season year. |
| `ncaa_team_id` | integer | stats.ncaa.org team id (Int64) for that season; stats.ncaa.org issues a new id every season, so the same school has a different id on each season row. |
| `ncaa_team` | character | School name as stats.ncaa.org writes it, in AP-style abbreviations (e.g. 'Alabama St.', 'A&M-Corpus Christi'). |
| `ncaa_conference` | character | stats.ncaa.org label of the team's conference that season (e.g. 'SEC', 'MWC'), taken from the groups tables' NCAA aliases for the season's conference_id. A conference with no NCAA alias gets its SDV abbreviation (men's Great West -> 'GWC'); a team the groups table has no row for keeps the bundled stats.ncaa.org team-list label. The label style can differ by league ('MWC' in men's, 'Mountain West' in women's through 2022-23). |
| `espn_team_id` | character | ESPN team id (canonical key). |
| `espn_display_name` | character | ESPN display name (school + mascot). |
| `espn_location` | character | ESPN school/location only. |
| `espn_mascot` | character | ESPN mascot/nickname. |
| `espn_abbreviation` | character | ESPN abbreviation. |
| `espn_conference_name` | character | Conference name for that season as the {mbb,wbb}_group_seasons table records it (e.g. 'Colonial Athletic Association' through 2022-23, 'Coastal Athletic Association' after). Null on the same rows as conference_id. |
| `espn_conference_id` | character | ESPN conference (group) id for that season as a string (e.g. '23' for the Southeastern Conference). ESPN group ids are sport-scoped (Summit League is 49 in men's, 47 in women's) and can change (men's Summit League was 15 before 2008). Null on the same rows as conference_id. |
| `match_method` | character | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `conference_id` | character | SportsDataverse conference (group) id for that season, prefixed by the league (e.g. 'mbb:big-ten'), from the {mbb,wbb}_team_group_seasons release table joined on espn_team_id and the season's ending year. Stable across renames and shared with the {mbb,wbb}_groups tables; null only when that table has no row for the team that season. |

**Example**

```python
from sportsdataverse.mbb import ncaa_espn_team_crosswalk
df = ncaa_espn_team_crosswalk()
print(df.shape)

# Women's crosswalk as pandas

wdf = ncaa_espn_team_crosswalk(league="wbb", return_as_pandas=True)

# Pipeline next step (one line)

df.filter(pl.col("season") == "2025-26").select("ncaa_team_id", "espn_team_id")
```

### ncaa_mbb_team_ids {#ncaa_mbb_team_ids}

`ncaa_mbb_team_ids(*, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Men's-basketball `(team, season) -> stats.ncaa.org id` crosswalk.

Port of bigballR's bundled `teamids` data asset (one row per team per
season, 2009-10 through 2025-26).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

DataFrame with columns `team` (str), `conference` (str), `id` (Int64 -- the season-specific stats.ncaa.org team id) and `season` (str, `"YYYY-YY"`).

**Example**

```python
from sportsdataverse.mbb import ncaa_mbb_team_ids
df = ncaa_mbb_team_ids()
print(df.shape)

# Pipeline next step (one line)

df.filter(pl.col("season") == "2025-26").head()
```

### refresh_ncaa_team_ids {#refresh_ncaa_team_ids}

`refresh_ncaa_team_ids(season: 'str', season_division_id: 'int', dates: 'Sequence[str]', *, league: 'str' = 'mbb', fetcher: "Optional['NcaaFetcher']" = None, prior_season: 'Optional[str]' = None, conference_overrides: 'Optional[Dict[str, str]]' = None, extra_teams: 'Optional[Iterable[Dict[str, object]]]' = None) -> 'pl.DataFrame'`

Extend the bundled crosswalk with a new season (update_team_ids recipe).

Port of wbigballR's `R/update_team_ids.R` maintainer scratch script:
scrape early-season scoreboard pages, harvest the `/teams/{id}` anchor
links, keep teams known from the prior season, carry each team's prior
conference forward, hand-patch realignments, and append the stamped rows
to the historical table.

Network path -- hits stats.ncaa.org via `~sportsdataverse.mbb.
mbb_ncaa_fetch.NcaaFetcher`. The result is returned (NOT written); a
maintainer overwrites `sportsdataverse/<league>/data/ncaa_teamids_
<league>.csv` with it after review.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season being added, e.g. `"2026-27"`. |
| `season_division_id` | `int` |  | The `season_divisions/{id}/scoreboards` id for that season + division (league-specific; see get_date_games' season table). |
| `dates` | `Sequence[str]` |  | Scoreboard dates to sweep, `"MM/DD/YYYY"` -- the recipe uses ~9 early-November dates so every D-I team appears at least once. |
| `league` | `str` | `'mbb'` | `"mbb"` or `"wbb"`. |
| `fetcher` | `Optional['NcaaFetcher']` | `None` | Injectable `NcaaFetcher`; defaults to a fresh one (which requires proxy/browser configuration at fetch time). |
| `prior_season` | `Optional[str]` | `None` | Season whose team list + conferences seed the join; defaults to the max season already in the bundled table. |
| `conference_overrides` | `Optional[Dict[str, str]]` | `None` | `{team: new_conference}` hand-patches for realignments (the recipe's `Conference[Team == p] <- ...` block). |
| `extra_teams` | `Optional[Iterable[Dict[str, object]]]` | `None` | Rows for brand-new programs the prior-season filter drops, e.g. `[{"team": "St. Thomas (MN)", "conference": "Summit League", "id": 529315}]` (`season` is stamped). |

**Returns**

The full refreshed crosswalk (historical rows + the new season), deduplicated and sorted by season/team.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher
from sportsdataverse.mbb.mbb_ncaa_team_ids import refresh_ncaa_team_ids
dates = [f"11/{d:02d}/2026" for d in range(9, 18)]
df = refresh_ncaa_team_ids("2026-27", 18823, dates,
                           fetcher=NcaaFetcher.with_browser())
df.write_csv("sportsdataverse/mbb/data/ncaa_teamids_mbb.csv")
```

### resolve_ncaa_team_id {#resolve_ncaa_team_id}

`resolve_ncaa_team_id(team: 'str', season: 'str', league: 'str' = 'mbb') -> 'Optional[int]'`

Resolve a school name + season to its stats.ncaa.org team id.

Exact `(team, season)` match first (bigballR semantics), then a
case-insensitive fallback.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `str` |  | School name as it appears in the crosswalk (`"Illinois"`, not `"Illinois Fighting Illini"`). |
| `season` | `str` |  | Season string, e.g. `"2025-26"`. |
| `league` | `str` | `'mbb'` | `"mbb"` or `"wbb"` -- which league's crosswalk to search. (Deliberate fix of wbigballR, which always searched the men's table.) |

**Returns**

The team id as `int`, or `None` when no row matches.

**Example**

```python
from sportsdataverse.mbb import resolve_ncaa_team_id
resolve_ncaa_team_id("Illinois", "2025-26")

# Women's league lookup

resolve_ncaa_team_id("South Carolina", "2024-25", league="wbb")
```

### tidy_player {#tidy_player}

`tidy_player(p_in: 'str', ctx: 'TidyPlayerContext') -> 'tuple[str, TidyPlayerContext]'`

Resolve a raw play-by-play name to its box-score full name, via an

ordered fallback chain (`LineupErrorAnalysisUtils.tidy_player`,
`:76-144`). Order is semantic -- ported as ordered first-non-`None`:

1. Cache hit (see the module docstring's asymmetric-cache note).
2. Exact box-score code match.
3. Unique truncated-code match (`TidyPlayerContext.alt_all_players_map`).
4. Double-barrel-surname strip retry (`"Smith-Jones"` -> `"Jones"`),
   recursing into this same function.
5. Initials (`convert_from_initials`).
6. Jersey number (`convert_from_digits`, against
   `ctx.box_lineup.players_out`).
7. Truncated-code + inserted-"j"-for-"junior" retry.
8. Fuzzy match (`fuzzy_box_match`) -- skipped (and normalized to
   `"Team"`) for the four team-stat-row sentinels.
9. Identity fallthrough (the input is returned unresolved; a later,
   out-of-scope validation pass is expected to reject it).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `p_in` | `str` |  | The raw play-by-play name. |
| `ctx` | `TidyPlayerContext` |  | The lookup context (see `build_tidy_player_context`). |

**Returns**

`(resolved_name, updated_ctx)` -- `updated_ctx` carries the new cache entry.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import build_tidy_player_context, tidy_player
ctx = build_tidy_player_context(box_lineup)
resolved_name, ctx = tidy_player("MITCHELL,M", ctx)
```
