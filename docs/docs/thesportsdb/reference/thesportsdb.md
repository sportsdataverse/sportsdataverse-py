---
title: THESPORTSDB — TheSportsDB API v1 (thesportsdb.com, free test key)
sidebar_label: TheSportsDB API v1 (thesportsdb.com, free test key)
description: "THESPORTSDB — TheSportsDB API v1 (thesportsdb.com, free test key) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# THESPORTSDB — TheSportsDB API v1 (thesportsdb.com, free test key)

`sportsdataverse.thesportsdb` — 12 endpoints.

## thesportsdb_event

One event.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/{key}/lookupevent.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/{key}/lookupevent.php?id=2267073](https://www.thesportsdb.com/api/v1/json/{key}/lookupevent.php?id=2267073)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  |  | `Y` | Required. Event id (idEvent from /eventsseason.php). |

### Returns {#thesportsdb_event-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_event` | character |  |
| `id_ap_ifootball` | character |  |
| `str_timestamp` | character |  |
| `str_event` | character |  |
| `str_event_alternate` | character |  |
| `str_filename` | character |  |
| `str_sport` | character |  |
| `id_league` | character |  |
| `str_league` | character |  |
| `str_league_badge` | character |  |
| `str_season` | character |  |
| `str_description_en` | character |  |
| `str_home_team` | character |  |
| `str_away_team` | character |  |
| `int_home_score` | character |  |
| `int_home_score_extra` | character |  |
| `int_away_score_extra` | character |  |
| `int_round` | character |  |
| `int_away_score` | character |  |
| `int_spectators` | character |  |
| `str_official` | character |  |
| `str_weather` | character |  |
| `date_event` | character |  |
| `date_event_local` | character |  |
| `str_time` | character |  |
| `str_time_local` | character |  |
| `str_group` | character |  |
| `id_home_team` | character |  |
| `str_home_team_badge` | character |  |
| `id_away_team` | character |  |
| `str_away_team_badge` | character |  |
| `int_score` | character |  |
| `int_score_votes` | character |  |
| `str_result` | character |  |
| `id_venue` | character |  |
| `str_venue` | character |  |
| `str_country` | character |  |
| `str_city` | character |  |
| `str_poster` | character |  |
| `str_square` | character |  |
| `str_fanart` | character |  |
| `str_thumb` | character |  |
| `str_banner` | character |  |
| `str_map` | character |  |
| `str_tweet1` | character |  |
| `str_video` | character |  |
| `str_status` | character |  |
| `str_postponed` | character |  |
| `str_locked` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#thesportsdb_event-example}

```python
thesportsdb_event(id='2267073')
```

_Last validated n/a._

## thesportsdb_league

One league.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/{key}/lookupleague.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/{key}/lookupleague.php?id=4328](https://www.thesportsdb.com/api/v1/json/{key}/lookupleague.php?id=4328)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  |  | `Y` | Required. League id (idLeague from /all_leagues.php). |

### Returns {#thesportsdb_league-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_league` | character |  |
| `id_ap_ifootball` | character |  |
| `id_ap_ifootballv3` | character |  |
| `str_sport` | character |  |
| `str_league` | character |  |
| `str_league_alternate` | character |  |
| `int_division` | character |  |
| `id_cup` | character |  |
| `str_current_season` | character |  |
| `int_formed_year` | character |  |
| `date_first_event` | character |  |
| `str_gender` | character |  |
| `str_country` | character |  |
| `str_website` | character |  |
| `str_facebook` | character |  |
| `str_instagram` | character |  |
| `str_twitter` | character |  |
| `str_youtube` | character |  |
| `str_rss` | character |  |
| `str_description_en` | character |  |
| `str_description_de` | character |  |
| `str_description_fr` | character |  |
| `str_description_it` | character |  |
| `str_description_cn` | character |  |
| `str_description_jp` | character |  |
| `str_description_ru` | character |  |
| `str_description_es` | character |  |
| `str_description_pt` | character |  |
| `str_description_se` | character |  |
| `str_description_nl` | character |  |
| `str_description_hu` | character |  |
| `str_description_no` | character |  |
| `str_description_pl` | character |  |
| `str_description_il` | character |  |
| `str_tv_rights` | character |  |
| `str_fanart1` | character |  |
| `str_fanart2` | character |  |
| `str_fanart3` | character |  |
| `str_fanart4` | character |  |
| `str_banner` | character |  |
| `str_badge` | character |  |
| `str_logo` | character |  |
| `str_poster` | character |  |
| `str_trophy` | character |  |
| `str_naming` | character |  |
| `str_complete` | character |  |
| `str_locked` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#thesportsdb_league-example}

```python
thesportsdb_league(id='4328')
```

_Last validated n/a._

## thesportsdb_league_next_events

Next 15 events of a league.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/{key}/eventsnextleague.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/{key}/eventsnextleague.php?id=4328](https://www.thesportsdb.com/api/v1/json/{key}/eventsnextleague.php?id=4328)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  |  | `Y` | Required. League id (idLeague from /all_leagues.php). |

### Returns {#thesportsdb_league_next_events-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_event` | character |  |
| `id_ap_ifootball` | character |  |
| `str_timestamp` | character |  |
| `str_event` | character |  |
| `str_event_alternate` | character |  |
| `str_filename` | character |  |
| `str_sport` | character |  |
| `id_league` | character |  |
| `str_league` | character |  |
| `str_league_badge` | character |  |
| `str_season` | character |  |
| `str_description_en` | character |  |
| `str_home_team` | character |  |
| `str_away_team` | character |  |
| `int_home_score` | character |  |
| `int_home_score_extra` | character |  |
| `int_away_score_extra` | character |  |
| `int_round` | character |  |
| `int_away_score` | character |  |
| `int_spectators` | character |  |
| `str_official` | character |  |
| `str_weather` | character |  |
| `date_event` | character |  |
| `date_event_local` | character |  |
| `str_time` | character |  |
| `str_time_local` | character |  |
| `str_group` | character |  |
| `id_home_team` | character |  |
| `str_home_team_badge` | character |  |
| `id_away_team` | character |  |
| `str_away_team_badge` | character |  |
| `int_score` | character |  |
| `int_score_votes` | character |  |
| `str_result` | character |  |
| `id_venue` | character |  |
| `str_venue` | character |  |
| `str_country` | character |  |
| `str_city` | character |  |
| `str_poster` | character |  |
| `str_square` | character |  |
| `str_fanart` | character |  |
| `str_thumb` | character |  |
| `str_banner` | character |  |
| `str_map` | character |  |
| `str_tweet1` | character |  |
| `str_video` | character |  |
| `str_status` | character |  |
| `str_postponed` | character |  |
| `str_locked` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#thesportsdb_league_next_events-example}

```python
thesportsdb_league_next_events(id='4328')
```

_Last validated n/a._

## thesportsdb_league_teams

Teams in a league (by league name).

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/{key}/search_all_teams.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/{key}/search_all_teams.php?l=English+Premier+League](https://www.thesportsdb.com/api/v1/json/{key}/search_all_teams.php?l=English+Premier+League)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `l` | `league_name` |  |  | `Y` | Required. League name as spelled in strLeague. |

### Returns {#thesportsdb_league_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_team` | character |  |
| `id_espn` | character |  |
| `id_ap_ifootball` | character |  |
| `int_loved` | character |  |
| `str_team` | character |  |
| `str_team_alternate` | character |  |
| `str_team_short` | character |  |
| `int_formed_year` | character |  |
| `str_sport` | character |  |
| `str_league` | character |  |
| `id_league` | character |  |
| `str_league2` | character |  |
| `id_league2` | character |  |
| `str_league3` | character |  |
| `id_league3` | character |  |
| `str_league4` | character |  |
| `id_league4` | character |  |
| `str_league5` | character |  |
| `id_league5` | character |  |
| `str_league6` | character |  |
| `id_league6` | character |  |
| `str_league7` | character |  |
| `id_league7` | character |  |
| `str_division` | character |  |
| `id_venue` | character |  |
| `str_stadium` | character |  |
| `str_keywords` | character |  |
| `str_rss` | character |  |
| `str_location` | character |  |
| `str_website` | character |  |
| `str_facebook` | character |  |
| `str_twitter` | character |  |
| `str_instagram` | character |  |
| `str_description_en` | character |  |
| `str_description_de` | character |  |
| `str_description_fr` | character |  |
| `str_description_cn` | character |  |
| `str_description_it` | character |  |
| `str_description_jp` | character |  |
| `str_description_ru` | character |  |
| `str_description_es` | character |  |
| `str_description_pt` | character |  |
| `str_description_se` | character |  |
| `str_description_nl` | character |  |
| `str_description_hu` | character |  |
| `str_description_no` | character |  |
| `str_description_il` | character |  |
| `str_description_pl` | character |  |
| `str_colour1` | character |  |
| `str_colour2` | character |  |
| `str_colour3` | character |  |
| `str_gender` | character |  |
| `str_country` | character |  |
| `str_badge` | character |  |
| `str_logo` | character |  |
| `str_fanart1` | character |  |
| `str_fanart2` | character |  |
| `str_fanart3` | character |  |
| `str_fanart4` | character |  |
| `str_banner` | character |  |
| `str_equipment` | character |  |
| `str_youtube` | character |  |
| `str_locked` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#thesportsdb_league_teams-example}

```python
thesportsdb_league_teams(league_name='English Premier League')
```

_Last validated n/a._

## thesportsdb_leagues

All leagues.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/{key}/all_leagues.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/{key}/all_leagues.php](https://www.thesportsdb.com/api/v1/json/{key}/all_leagues.php)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#thesportsdb_leagues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_league` | character |  |
| `str_league` | character |  |
| `str_sport` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#thesportsdb_leagues-example}

```python
thesportsdb_leagues()
```

_Last validated n/a._

## thesportsdb_player

One player.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/{key}/lookupplayer.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/{key}/lookupplayer.php?id=34145937](https://www.thesportsdb.com/api/v1/json/{key}/lookupplayer.php?id=34145937)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  |  | `Y` | Required. Player id (idPlayer from /lookup_all_players.php). |

### Returns {#thesportsdb_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_player` | character |  |
| `id_team` | character |  |
| `id_team2` | character |  |
| `id_team_national` | character |  |
| `id_ap_ifootball` | character |  |
| `id_player_manager` | character |  |
| `id_wikidata` | character |  |
| `id_transfer_mkt` | character |  |
| `id_espn` | character |  |
| `id_google` | character |  |
| `str_nationality` | character |  |
| `str_player` | character |  |
| `str_player_alternate` | character |  |
| `str_team` | character |  |
| `str_team2` | character |  |
| `str_sport` | character |  |
| `int_soccer_xml_team_id` | character |  |
| `date_born` | character |  |
| `date_died` | character |  |
| `str_number` | character |  |
| `date_signed` | character | Date the player signed a new contract (YYYY-MM-DD). |
| `str_signing` | character |  |
| `str_wage` | character |  |
| `str_outfitter` | character |  |
| `str_kit` | character |  |
| `str_agent` | character |  |
| `str_birth_location` | character |  |
| `str_death_location` | character |  |
| `str_ethnicity` | character |  |
| `str_status` | character |  |
| `str_description_en` | character |  |
| `str_description_de` | character |  |
| `str_description_fr` | character |  |
| `str_description_cn` | character |  |
| `str_description_it` | character |  |
| `str_description_jp` | character |  |
| `str_description_ru` | character |  |
| `str_description_es` | character |  |
| `str_description_pt` | character |  |
| `str_description_se` | character |  |
| `str_description_nl` | character |  |
| `str_description_hu` | character |  |
| `str_description_no` | character |  |
| `str_description_il` | character |  |
| `str_description_pl` | character |  |
| `str_gender` | character |  |
| `str_side` | character |  |
| `str_position` | character |  |
| `str_college` | character |  |
| `str_facebook` | character |  |
| `str_website` | character |  |
| `str_twitter` | character |  |
| `str_instagram` | character |  |
| `str_youtube` | character |  |
| `str_height` | character |  |
| `str_weight` | character |  |
| `int_loved` | character |  |
| `str_thumb` | character |  |
| `str_poster` | character |  |
| `str_cutout` | character |  |
| `str_cartoon` | character |  |
| `str_render` | character |  |
| `str_banner` | character |  |
| `str_fanart1` | character |  |
| `str_fanart2` | character |  |
| `str_fanart3` | character |  |
| `str_fanart4` | character |  |
| `str_creative_commons` | character |  |
| `str_creative_commons_attribution` | character |  |
| `str_locked` | character |  |
| `str_last_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#thesportsdb_player-example}

```python
thesportsdb_player(id='34145937')
```

_Last validated n/a._

## thesportsdb_player_search

Search players by name.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/{key}/searchplayers.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/{key}/searchplayers.php?p=Danny+Welbeck](https://www.thesportsdb.com/api/v1/json/{key}/searchplayers.php?p=Danny+Welbeck)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `p` | `player_name` |  |  | `Y` | Required. Player name (substring search). |

### Returns {#thesportsdb_player_search-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_player` | character |  |
| `id_team` | character |  |
| `str_player` | character |  |
| `str_team` | character |  |
| `str_sport` | character |  |
| `str_thumb` | character |  |
| `str_cutout` | character |  |
| `str_nationality` | character |  |
| `date_born` | character |  |
| `str_status` | character |  |
| `str_gender` | character |  |
| `str_position` | character |  |
| `relevance` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#thesportsdb_player_search-example}

```python
thesportsdb_player_search(player_name='Danny Welbeck')
```

_Last validated n/a._

## thesportsdb_season_events

Events of a league-season.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/{key}/eventsseason.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/{key}/eventsseason.php?id=4328&s=2025-2026](https://www.thesportsdb.com/api/v1/json/{key}/eventsseason.php?id=4328&s=2025-2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  |  | `Y` | Required. League id (idLeague from /all_leagues.php). |
| `s` | `season` |  |  | `Y` | Season label, e.g. 2025-2026; omitted, the API answers the current season. |

### Returns {#thesportsdb_season_events-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_event` | character |  |
| `str_event` | character |  |
| `str_event_alternate` | character |  |
| `str_season` | character |  |
| `id_league` | character |  |
| `str_league` | character |  |
| `str_league_badge` | character |  |
| `str_sport` | character |  |
| `str_home_team` | character |  |
| `str_away_team` | character |  |
| `id_home_team` | character |  |
| `id_away_team` | character |  |
| `int_round` | character |  |
| `int_home_score` | character |  |
| `int_away_score` | character |  |
| `str_timestamp` | character |  |
| `date_event` | character |  |
| `date_event_local` | character |  |
| `str_time` | character |  |
| `str_time_local` | character |  |
| `str_home_team_badge` | character |  |
| `str_away_team_badge` | character |  |
| `str_venue` | character |  |
| `str_country` | character |  |
| `str_thumb` | character |  |
| `str_poster` | character |  |
| `str_video` | character |  |
| `str_postponed` | character |  |
| `str_filename` | character |  |
| `str_status` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#thesportsdb_season_events-example}

```python
thesportsdb_season_events(id='4328', season='2025-2026')
```

_Last validated n/a._

## thesportsdb_sports

All sports.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/{key}/all_sports.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/{key}/all_sports.php](https://www.thesportsdb.com/api/v1/json/{key}/all_sports.php)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#thesportsdb_sports-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_sport` | character |  |
| `str_sport` | character |  |
| `str_format` | character |  |
| `str_sport_thumb` | character |  |
| `str_sport_thumb_bw` | character |  |
| `str_sport_icon_green` | character |  |
| `str_sport_description` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#thesportsdb_sports-example}

```python
thesportsdb_sports()
```

_Last validated n/a._

## thesportsdb_table

League table.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/{key}/lookuptable.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/{key}/lookuptable.php?l=4328&s=2025-2026](https://www.thesportsdb.com/api/v1/json/{key}/lookuptable.php?l=4328&s=2025-2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `l` | `league_id` |  |  | `Y` | Required. League id (idLeague from /all_leagues.php). |
| `s` | `season` |  |  | `Y` | Season label, e.g. 2025-2026; omitted, the API answers the current season. |

### Returns {#thesportsdb_table-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_standing` | character |  |
| `int_rank` | character |  |
| `id_team` | character |  |
| `str_team` | character |  |
| `str_badge` | character |  |
| `id_league` | character |  |
| `str_league` | character |  |
| `str_season` | character |  |
| `str_group` | character |  |
| `str_form` | character |  |
| `str_description` | character |  |
| `int_played` | character |  |
| `int_win` | character |  |
| `int_loss` | character |  |
| `int_draw` | character |  |
| `int_goals_for` | character |  |
| `int_goals_against` | character |  |
| `int_goal_difference` | character |  |
| `int_points` | character |  |
| `date_updated` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#thesportsdb_table-example}

```python
thesportsdb_table(league_id='4328', season='2025-2026')
```

_Last validated n/a._

## thesportsdb_team

One team.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/{key}/lookupteam.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/{key}/lookupteam.php?id=133604](https://www.thesportsdb.com/api/v1/json/{key}/lookupteam.php?id=133604)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  |  | `Y` | Required. Team id (idTeam from /search_all_teams.php). |

### Returns {#thesportsdb_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_team` | character |  |
| `id_espn` | character |  |
| `id_ap_ifootball` | character |  |
| `int_loved` | character |  |
| `str_team` | character |  |
| `str_team_alternate` | character |  |
| `str_team_short` | character |  |
| `int_formed_year` | character |  |
| `str_sport` | character |  |
| `str_league` | character |  |
| `id_league` | character |  |
| `str_league2` | character |  |
| `id_league2` | character |  |
| `str_league3` | character |  |
| `id_league3` | character |  |
| `str_league4` | character |  |
| `id_league4` | character |  |
| `str_league5` | character |  |
| `id_league5` | character |  |
| `str_league6` | character |  |
| `id_league6` | character |  |
| `str_league7` | character |  |
| `id_league7` | character |  |
| `str_division` | character |  |
| `id_venue` | character |  |
| `str_stadium` | character |  |
| `str_keywords` | character |  |
| `str_rss` | character |  |
| `str_location` | character |  |
| `str_website` | character |  |
| `str_facebook` | character |  |
| `str_twitter` | character |  |
| `str_instagram` | character |  |
| `str_description_en` | character |  |
| `str_description_de` | character |  |
| `str_description_fr` | character |  |
| `str_description_cn` | character |  |
| `str_description_it` | character |  |
| `str_description_jp` | character |  |
| `str_description_ru` | character |  |
| `str_description_es` | character |  |
| `str_description_pt` | character |  |
| `str_description_se` | character |  |
| `str_description_nl` | character |  |
| `str_description_hu` | character |  |
| `str_description_no` | character |  |
| `str_description_il` | character |  |
| `str_description_pl` | character |  |
| `str_colour1` | character |  |
| `str_colour2` | character |  |
| `str_colour3` | character |  |
| `str_gender` | character |  |
| `str_country` | character |  |
| `str_badge` | character |  |
| `str_logo` | character |  |
| `str_fanart1` | character |  |
| `str_fanart2` | character |  |
| `str_fanart3` | character |  |
| `str_fanart4` | character |  |
| `str_banner` | character |  |
| `str_equipment` | character |  |
| `str_youtube` | character |  |
| `str_locked` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#thesportsdb_team-example}

```python
thesportsdb_team(id='133604')
```

_Last validated n/a._

## thesportsdb_team_players

Players of a team.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/{key}/lookup_all_players.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/{key}/lookup_all_players.php?id=133604](https://www.thesportsdb.com/api/v1/json/{key}/lookup_all_players.php?id=133604)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  |  | `Y` | Required. Team id (idTeam from /search_all_teams.php). |

### Returns {#thesportsdb_team_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_player` | character |  |
| `id_team` | character |  |
| `id_team2` | character |  |
| `id_team_national` | character |  |
| `id_ap_ifootball` | character |  |
| `id_player_manager` | character |  |
| `id_wikidata` | character |  |
| `id_transfer_mkt` | character |  |
| `id_espn` | character |  |
| `id_google` | character |  |
| `str_nationality` | character |  |
| `str_player` | character |  |
| `str_player_alternate` | character |  |
| `str_team` | character |  |
| `str_team2` | character |  |
| `str_sport` | character |  |
| `int_soccer_xml_team_id` | character |  |
| `date_born` | character |  |
| `date_died` | character |  |
| `str_number` | character |  |
| `date_signed` | character | Date the player signed a new contract (YYYY-MM-DD). |
| `str_signing` | character |  |
| `str_wage` | character |  |
| `str_outfitter` | character |  |
| `str_kit` | character |  |
| `str_agent` | character |  |
| `str_birth_location` | character |  |
| `str_death_location` | character |  |
| `str_ethnicity` | character |  |
| `str_status` | character |  |
| `str_description_en` | character |  |
| `str_description_de` | character |  |
| `str_description_fr` | character |  |
| `str_description_cn` | character |  |
| `str_description_it` | character |  |
| `str_description_jp` | character |  |
| `str_description_ru` | character |  |
| `str_description_es` | character |  |
| `str_description_pt` | character |  |
| `str_description_se` | character |  |
| `str_description_nl` | character |  |
| `str_description_hu` | character |  |
| `str_description_no` | character |  |
| `str_description_il` | character |  |
| `str_description_pl` | character |  |
| `str_gender` | character |  |
| `str_side` | character |  |
| `str_position` | character |  |
| `str_college` | character |  |
| `str_facebook` | character |  |
| `str_website` | character |  |
| `str_twitter` | character |  |
| `str_instagram` | character |  |
| `str_youtube` | character |  |
| `str_height` | character |  |
| `str_weight` | character |  |
| `int_loved` | character |  |
| `str_thumb` | character |  |
| `str_poster` | character |  |
| `str_cutout` | character |  |
| `str_cartoon` | character |  |
| `str_render` | character |  |
| `str_banner` | character |  |
| `str_fanart1` | character |  |
| `str_fanart2` | character |  |
| `str_fanart3` | character |  |
| `str_fanart4` | character |  |
| `str_creative_commons` | character |  |
| `str_creative_commons_attribution` | character |  |
| `str_locked` | character |  |
| `str_last_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#thesportsdb_team_players-example}

```python
thesportsdb_team_players(id='133604')
```

_Last validated n/a._
