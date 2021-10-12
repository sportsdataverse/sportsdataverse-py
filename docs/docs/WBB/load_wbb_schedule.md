---
title: Load WBB Schedule
sidebar_label: Load WBB Schedule
---


### wehoop.wbb.load_wbb_schedule(seasons: List[int])
Load women’s college basketball schedule data

Example:

    wbb_df = wehoop.wbb.load_wbb_schedule(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    schedule for  the requested seasons.

Raises:

    ValueError: If season is less than 2002.
