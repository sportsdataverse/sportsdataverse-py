---
title: Load WNBA Schedule
sidebar_label: Load WNBA Schedule
---


### wehoop.wnba.load_wnba_schedule(seasons: List[int])
Load WNBA schedule data

Example:

    wnba_df = wehoop.wnba.load_wnba_schedule(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    schedule for  the requested seasons.

Raises:

    ValueError: If season is less than 2002.
