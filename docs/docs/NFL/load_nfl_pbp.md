---
title: Load NFL PBP
sidebar_label: Load NFL PBP
---

### cfbfastR.nfl.load_nfl_pbp(seasons: List[int])
Load NFL play by play data going back to 1999

Example:

    nfl_df = cfbfastR.nfl.load_nfl_pbp(seasons=range(1999,2021))

Args:

    seasons (list): Used to define different seasons. 1999 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 1999.