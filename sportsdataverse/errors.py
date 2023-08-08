"""
Custom exceptions for sportsdataverse module
"""


class SeasonNotFoundError(Exception):
    pass


def season_not_found_error(season, min_season):
    if int(season) >= int(min_season):
        return
    else:
        raise SeasonNotFoundError(f"Season {season} not found, season cannot be less than {min_season}")


class NoESPNDataError(Exception):
    pass


def no_espn_data(response):
    if response.status_code == 404:
        raise NoESPNDataError(f"NoESPNDataError: No response for {response.url}")
    elif response.json().get("code", None) == 404:
        raise NoESPNDataError(f"NoESPNDataError: No data found for {response.url}, response: {response.json()}")
    else:
        return response
