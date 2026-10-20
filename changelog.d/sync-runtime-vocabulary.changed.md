- **Basketball / hockey runtimes:** in the 34 international basketball and European hockey getters a route's own data request's 204 / 205 returns `{}` (was `NoDataError` / `AssetFetchError`);
  a 204 on a lookup (token, session or key page, season list, finished-game or scope check) or on one page of a multi-request answer is `AssetFetchError`;
  a 400 is `ValueError` in EASL, KBL, KZS and VTB Infobasket; IBBA's blank 200 is `AssetFetchError`. (#TBD)
