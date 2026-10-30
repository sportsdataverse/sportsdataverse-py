# IDs and joins

> How player, team and game ids are typed in sportsdataverse-py and how to join frames from different sources safely.

Player, athlete, team and game ids are join keys, and a join is only as correct as
the dtype agreement on its two sides. This page sets out which dtype each part of
the package gives an id, where the exceptions are, and how to join safely.

## The rule: Int64 at the parser boundary

The ESPN, NHL and HockeyTech parsers emit every `id` / `*_id` column as `Int64`.
`*_ids` columns hold lists and are not ids. All three families go through one cast,
`_codegen_runtime._cast_ids_int64`, which is conservative:

- A column is converted only when every non-null value survives the cast. A String
  column with a value that is not an integer is left as it came, and the parser
  warns `SchemaDriftWarning` naming the column, so nothing is silently nulled or
  corrupted.
- A float-origin id goes straight to `Int64`, never through text, so it can never
  come out as `"123.0"`.
- Whitespace around a String id is stripped, and an empty string is null.
- An empty frame is cast too, so a result with no rows joins like one with rows.

A few ids are text by design and stay String: ESPN's `now_id` (`"1-44686270"`), NHL
stats-rest `formatted_season_id` and the country `id`, and NFL.com gsis player ids
(`"00-0034796"`). With `return_as_pandas=True` the Int64 ids are pandas' nullable
`Int64`, or `int64[pyarrow]` for the Arrow-backed frames.

```python
from sportsdataverse.nba import espn_nba_game_rosters, espn_nba_scoreboard

games = espn_nba_scoreboard(dates="20240317")
roster = espn_nba_game_rosters(game_id=401585607)
print({c: str(games.schema[c]) for c in ("game_id", "home_id", "away_id")})
print({c: str(roster.schema[c]) for c in ("athlete_id", "team_id", "game_id")})
# {'game_id': 'Int64', 'home_id': 'Int64', 'away_id': 'Int64'}
# {'athlete_id': 'Int64', 'team_id': 'Int64', 'game_id': 'Int64'}
```

## The exceptions

### Release loaders keep their published dtypes

The `load_*` functions read parquet files published to GitHub releases, and they
return the dtypes those files were written with. Some of those files still carry
String ids while the live parsers now emit Int64:

| Loader | String id columns |
|---|---|
| `load_pwhl_schedules` | `game_id`, `home_team_id`, `away_team_id` |
| `load_pwhl_pbp` | `team_id`, `player_team_id`, `home_team_id`, `away_team_id`, `game_season_id` |
| PWHL box scores | `player_id` |
| `load_{mbb,nba,wbb,wnba}_rosters` | `athlete_id`, `position_id`, `status_id` |
| `load_{mbb,nba,wnba}_game_rosters` | `game_id` |

Joining one of those String columns to an Int64 column raises `SchemaError`. Cast
the release side first. An Int32 id (such as `team_id` in those rosters,
`load_pwhl_rosters` or `load_pwhl_xg_pbp`) joins an Int64 one as it is. Some loaders
already canonicalize their ids to Int64 at the loader boundary (the CFB loaders, for
example), so check the schema rather than assuming either way.

```python
import polars as pl
from sportsdataverse.pwhl import load_pwhl_schedules, pwhl_schedule

live = pwhl_schedule(season=2025)              # HockeyTech parser: Int64 ids
release = load_pwhl_schedules([2025])          # release file: String ids
print(live.schema["game_id"], release.schema["game_id"])   # Int64 String

release = release.with_columns(pl.col("game_id", "home_team_id", "away_team_id").cast(pl.Int64))
assert release.schema["game_id"] == live.schema["game_id"]
both = live.join(release.select("game_id"), on="game_id", how="inner")
```

### Name resolvers return raw ESPN strings

`find_team`, `find_athlete` and `find_event` return ESPN's own JSON record for the
match, a dict with camelCase keys, so `["id"]` is a string. Convert it with `int()`
before you compare it to, or join it against, a parser frame.

```python
from sportsdataverse import find_event, find_team

lakers = find_team("lakers", league="nba")
event = find_event(date="2024-03-17", league="nba", home="Orlando")
print(repr(lakers["id"]), repr(event["id"]))   # '13' '401585607'

team_id = int(lakers["id"])
game_id = int(event["id"])
```

### Soccer and cricket ESPN wrappers are still String

Not yet covered by the Int64 rule: the soccer leagues' `scoreboard`, `standings`,
`summary`, `teams_site` and `team_roster` wrappers, and the cricket leagues'
`scoreboard`, `standings` and `summary` wrappers. Codegen routes those to the soccer
and cricket parsers, which do not cast, so their ids are String. The same leagues'
other wrappers (`news`, `injuries`, ...) go through the common ESPN parsers and are
Int64.

```python
from sportsdataverse.soccer.epl import espn_epl_scoreboard

epl = espn_epl_scoreboard(dates="20241026")
print({c: str(epl.schema[c]) for c in ("event_id", "home_team_id", "away_team_id")})
# {'event_id': 'String', 'home_team_id': 'String', 'away_team_id': 'String'}
```

### Raw payloads are whatever the provider sent

A wrapper called with `raw=True` or `return_parsed=False` hands back the provider's
JSON, and some hand-written functions return dicts of lightly processed records. No
cast happens there. `espn_nba_pbp(game_id)["plays"]`, for example, is a list of
dicts whose `participants.0.athlete.id` arrives as a string.

## A worked join: shots to the game roster

This joins one NBA game's shooting plays to its game roster, so each shot carries
the shooter's name. The plays come from a raw payload (String ids), the roster from a
parser (Int64 ids), so the id is cast once at the boundary and the dtypes are
asserted before the join.

```python
import polars as pl
from sportsdataverse.nba import espn_nba_game_rosters, espn_nba_pbp

game_id = 401585607
plays = pl.from_dicts(espn_nba_pbp(game_id=game_id)["plays"], infer_schema_length=None)
roster = espn_nba_game_rosters(game_id=game_id)

shots = plays.filter(pl.col("shootingPlay") == True).select(
    "game_play_number",
    "text",
    "scoringPlay",
    pl.col("participants.0.athlete.id").cast(pl.Int64).alias("athlete_id"),
)

assert shots.schema["athlete_id"] == roster.schema["athlete_id"] == pl.Int64
shots = shots.join(
    roster.select("athlete_id", "athlete_display_name", "team_id"),
    on="athlete_id",
    how="left",
    validate="m:1",
)
assert shots["athlete_display_name"].null_count() == 0
print(shots.group_by("athlete_display_name").agg(pl.len().alias("shots")).sort("shots", descending=True).head(3))
```

`validate="m:1"` makes polars raise if the roster has two rows for one athlete, and
the null-count assertion catches a join that matched nothing, which is what a dtype
mismatch hidden by a cast to text looks like.

## Rules of thumb

- **Pick one dtype per id and cast at the boundary**, where the data enters your
  code, not in the middle of a pipeline.
- **Assert before you join**: `assert left.schema[key] == right.schema[key]`.
- **Never cast an id to text to make a join line up.** If you must have text, cast
  the integer (`pl.col("id").cast(pl.Int64).cast(pl.String)`), never a float, so you
  get `"123"` and not `"123.0"`; zero-padded source ids lose or gain leading zeros
  either way.
- **Stored frames**: if you persist parser output, keep the ids Int64 across every
  season. Concatenating an older String season with a new Int64 one through
  `pl.concat(how="diagonal_relaxed")` silently turns the column back into String. The
  [migration guide](migration-0-2-0.md#ids-are-int64-at-the-parser-boundary) has the
  producer checklist.
