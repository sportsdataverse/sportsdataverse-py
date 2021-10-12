---
title: Load NBA PBP
sidebar_label: Load NBA PBP
---

### hoopR.nba.load_nba_pbp(seasons: List[int])
Load NBA play by play data going back to 2002

Example:

    nba_df = hoopR.nba.load_nba_pbp(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.
