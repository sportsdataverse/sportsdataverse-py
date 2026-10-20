- **Basketball / hockey runtimes:** a data request's 204 / 205 returns `{}` (zero rows) in the 34 international basketball and
  European hockey getters (ACB, BBL/DBBL raised `NoDataError`, the rest `AssetFetchError`; a token, session or season lookup's 204
  stays `AssetFetchError`); a 400 is `ValueError` in EASL, KBL, KZS, VTB Infobasket; IBBA's blank 200 is `AssetFetchError`. (#TBD)
