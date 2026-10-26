- **FIBA game wrappers:** a FIBA game page asked under a `comp` that does not list the game raises `AssetFetchError`
  (wrong scope) when GDAP knows the game, and `NoDataError` when it does not. (#864)
- **Atrium game wrappers:** a game the site does not serve (an unknown fixture id, or another site's game) raises
  `NoDataError` instead of an echo `AssetFetchError`, and the minted Atrium game functions no longer refuse a game
  whose season the site's (incomplete) season list omits. (#864)
