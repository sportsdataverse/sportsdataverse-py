---
title: WNBA Calendar
sidebar_label: WNBA Calendar
---
### wehoop.wnba.wnba_calendar(season: int)
wnba_calendar - look up the WNBA calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing
    calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.
