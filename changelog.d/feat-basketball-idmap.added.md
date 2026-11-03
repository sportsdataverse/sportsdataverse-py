- **Basketball:** a private id map (`basketball.sources.idmap`) rewrites NBA / WNBA stats and cdn live ids
  and stats.ncaa.org game and team ids into ESPN ids and back, keeping unmatched ids as explicit rows;
  served by the Data API `/v1/{league}/idmap/{id}`. (#1064)
