- **Basketball / hockey runtimes:** in the 34 international basketball and European hockey getters a route's own data
  request's 204 / 205 returns `{}` (was `NoDataError` / `AssetFetchError`), and a 204 on a lookup (token, session, key
  page, season list, game or scope check) or mid-way through a multi-request answer is `AssetFetchError`. (#929)
- **Basketball runtimes:** a 400 is `ValueError` in EASL, KBL, KZS and VTB Infobasket, and IBBA's blank 200 is
  `AssetFetchError`. (#929)
