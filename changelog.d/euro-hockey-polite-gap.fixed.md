- **Hockey:** the European hockey throttle now leaves at least 1 s from the END of one response to the start of the next
  request to the same host, with one request in flight per host (it spaced request starts, so a 0.8 s answer let the
  next request go 0.2 s later). Every transport path records the finish, error answers and exceptions included. (#865)
