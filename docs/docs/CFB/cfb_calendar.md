---
title: CFB Calendar
sidebar_label: CFB Calendar
sidebar_position: 7
---

### cfbfastR.cfb.cfb_calendar(season: int)

cfb_calendar - look up the college football calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.

