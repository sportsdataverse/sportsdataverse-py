# sportsdataverse package

## Subpackages


* sportsdataverse.cfb package


    * Submodules


    * sportsdataverse.cfb.cfb_loaders module


    * sportsdataverse.cfb.cfb_pbp module


    * sportsdataverse.cfb.cfb_schedule module


    * sportsdataverse.cfb.cfb_teams module


    * sportsdataverse.cfb.model_vars module


    * Module contents


* sportsdataverse.mbb package


    * Submodules


    * sportsdataverse.mbb.mbb_game_rosters module


    * sportsdataverse.mbb.mbb_loaders module


    * sportsdataverse.mbb.mbb_pbp module


    * sportsdataverse.mbb.mbb_schedule module


    * sportsdataverse.mbb.mbb_teams module


    * Module contents


* sportsdataverse.mlb package


    * Submodules


    * sportsdataverse.mlb.mlb_loaders module


    * sportsdataverse.mlb.mlbam_games module


    * sportsdataverse.mlb.mlbam_players module


    * sportsdataverse.mlb.mlbam_reports module


    * sportsdataverse.mlb.mlbam_stats module


    * sportsdataverse.mlb.mlbam_teams module


    * sportsdataverse.mlb.retrosheet module


    * sportsdataverse.mlb.retrosplits module


    * Module contents


* sportsdataverse.nba package


    * Submodules


    * sportsdataverse.nba.nba_loaders module


    * sportsdataverse.nba.nba_pbp module


    * sportsdataverse.nba.nba_schedule module


    * sportsdataverse.nba.nba_teams module


    * Module contents


* sportsdataverse.nfl package


    * Submodules


    * sportsdataverse.nfl.model_vars module


    * sportsdataverse.nfl.nfl_games module


    * sportsdataverse.nfl.nfl_loaders module


    * sportsdataverse.nfl.nfl_pbp module


    * sportsdataverse.nfl.nfl_schedule module


    * sportsdataverse.nfl.nfl_teams module


    * Module contents


* sportsdataverse.nhl package


    * Submodules


    * sportsdataverse.nhl.nhl_api module


    * sportsdataverse.nhl.nhl_loaders module


    * sportsdataverse.nhl.nhl_pbp module


    * sportsdataverse.nhl.nhl_schedule module


    * sportsdataverse.nhl.nhl_teams module


    * Module contents


* sportsdataverse.wbb package


    * Submodules


    * sportsdataverse.wbb.wbb_loaders module


    * sportsdataverse.wbb.wbb_pbp module


    * sportsdataverse.wbb.wbb_schedule module


    * sportsdataverse.wbb.wbb_teams module


    * Module contents


* sportsdataverse.wnba package


    * Submodules


    * sportsdataverse.wnba.wnba_loaders module


    * sportsdataverse.wnba.wnba_pbp module


    * sportsdataverse.wnba.wnba_schedule module


    * sportsdataverse.wnba.wnba_teams module


    * Module contents


## Submodules

## sportsdataverse.config module

## sportsdataverse.dl_utils module


### class sportsdataverse.dl_utils.ESPNHTTP()
Bases: `object`


#### base_url( = None)

#### clean_contents(contents)

#### espn_response()
alias of `sportsdataverse.dl_utils.ESPNResponse`


#### headers( = None)

#### parameters( = None)

#### send_api_request(endpoint, parameters, referer=None, headers=None, timeout=None, raise_exception_on_error=False)

### class sportsdataverse.dl_utils.ESPNResponse(response, status_code, url)
Bases: `object`


#### \__init__(response, status_code, url)
Initialize self.  See help(type(self)) for accurate signature.


#### get_dict()

#### get_json()

#### get_response()

#### get_url()

#### valid_json()

### sportsdataverse.dl_utils.camelize(string, uppercase_first_letter=True)
Convert strings to CamelCase.

Examples:

```
>>> camelize("device_type")
'DeviceType'
>>> camelize("device_type", False)
'deviceType'
```

`camelize()` can be thought of as a inverse of `underscore()`,
although there are some cases where that does not hold:

```
>>> camelize(underscore("IOError"))
'IoError'
```


* **Parameters**

    **uppercase_first_letter** – if set to True `camelize()` converts
    strings to UpperCamelCase. If set to False `camelize()` produces
    lowerCamelCase. Defaults to True.



### sportsdataverse.dl_utils.download(url, params={}, num_retries=15)

### sportsdataverse.dl_utils.flatten_json_iterative(dictionary, sep='.', ind_start=0)
Flattening a nested json file


### sportsdataverse.dl_utils.key_check(obj, key, replacement=array([], dtype=float64))

### sportsdataverse.dl_utils.underscore(word)
Make an underscored, lowercase form from the expression in the string.

Example:

```
>>> underscore("DeviceType")
'device_type'
```

As a rule of thumb you can think of `underscore()` as the inverse of
`camelize()`, though there are cases where that does not hold:

```
>>> camelize(underscore("IOError"))
'IoError'
```

## sportsdataverse.errors module

Custom exceptions for sportsdataverse module


### exception sportsdataverse.errors.SeasonNotFoundError()
Bases: `Exception`


### sportsdataverse.errors.season_not_found_error(season, min_season)
## Module contents
