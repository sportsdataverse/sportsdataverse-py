- **stats.ncaa.org canary:** the canary's patchright and managed-CDP transports stop at an Akamai session denial
  (HTTP 403, under 1 KB) instead of spending every solve attempt on it, as the production fetcher has since #1039. (#1040)
