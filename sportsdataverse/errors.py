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
