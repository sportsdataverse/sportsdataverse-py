- **Basketball / hockey runtimes:** a 204 / 205 returns `{}` (zero rows) from the 34 international basketball and European
  hockey getters (ACB and BBL/DBBL raised `NoDataError`, the rest `AssetFetchError`); an HTTP 400 is `ValueError` in EASL, KBL,
  KZS and VTB Infobasket (was `AssetFetchError`); IBBA's empty 200 is `AssetFetchError` (was `NoDataError`). (#TBD)
