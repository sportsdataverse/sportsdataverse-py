import logging

import sportsdataverse as sdv

logging.basicConfig(level=logging.DEBUG, filename="wehoop_wnba_raw_logfile.txt")
logger = logging.getLogger(__name__)


@timer(number=10)
@record_mem_usage
def test_polars_cfb_schedule():
    sdv.cfb.espn_cfb_schedule()


# @timer(number=10)
# @record_mem_usage
# def test_pandas_cfb_schedule():
#     sdv.cfb.espn_cfb_schedule_pandas()


@timer(number=10)
@record_mem_usage
def test_polars_cfb_calendar_ondays():
    sdv.cfb.espn_cfb_calendar(season=2022, ondays=True)


# @timer(number=10)
# @record_mem_usage
# def test_pandas_cfb_calendar_ondays():
#     sdv.cfb.espn_cfb_calendar_pandas(season=2022, ondays=True)


@timer(number=10)
@record_mem_usage
def test_polars_cfb_calendar():
    sdv.cfb.espn_cfb_calendar(season=2022)


# @timer(number=10)
# @record_mem_usage
# def test_pandas_cfb_calendar():
#     sdv.cfb.espn_cfb_calendar_pandas(season=2022)
@timer(number=10)
@record_mem_usage
def test_polars_load_cfb_pbp():
    sdv.cfb.load_cfb_pbp(seasons=2021)


def main():
    test_polars_cfb_schedule()
    # test_pandas_cfb_schedule()
    test_polars_cfb_calendar()
    # test_pandas_cfb_calendar()
    test_polars_cfb_calendar_ondays()
    # test_pandas_cfb_calendar_ondays()
    test_polars_load_cfb_pbp()
    sdv.cfb.espn_cfb_schedule(dates=20241010, logger=logger)


if __name__ == "__main__":
    main()
