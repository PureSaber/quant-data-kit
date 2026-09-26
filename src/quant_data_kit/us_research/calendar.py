"""Regular US equity sessions; UTC instants retain DST and early closes."""

from functools import lru_cache
from typing import ClassVar

import pandas as pd
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, USColumbusDay, sunday_to_monday


class _AdditionalBankHolidays(AbstractHolidayCalendar):
    # Federal Reserve Banks do not move a Saturday Veterans Day to Friday.
    rules: ClassVar = [
        USColumbusDay,
        Holiday("Veterans Day", month=11, day=11, observance=sunday_to_monday),
    ]


@lru_cache(maxsize=32)
def _calendar(start: str, end: str):
    import exchange_calendars as xcals

    return xcals.get_calendar("XNYS", start=start, end=end)


def schedule(start: str, end: str) -> pd.DataFrame:
    start_ts, end_ts = pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize()
    if start_ts.tz is not None or end_ts.tz is not None or start_ts > end_ts:
        raise ValueError("session bounds must be ordered, timezone-naive dates")
    cal = _calendar(str(start_ts.date()), str((end_ts + pd.Timedelta(days=15)).date()))
    result = cal.schedule.loc[str(start_ts.date()) : str(end_ts.date()), ["open", "close"]].copy()
    result.index = pd.DatetimeIndex(result.index).tz_localize(None)
    result.index.name = "session"
    return result


@lru_cache(maxsize=4096)
def settlement_session(session: str) -> str:
    """Standard US equity cash settlement since 2018, not same-day resale rules."""
    day = pd.Timestamp(session).normalize()
    if day < pd.Timestamp("2018-01-01"):
        raise ValueError("US settlement model is supported from 2018 onward")
    end = day + pd.Timedelta(days=15)
    trading_days = schedule(str(day.date()), str(end.date())).index
    if not len(trading_days) or trading_days[0] != day:
        raise ValueError("trade date is not a regular US equity session")
    # Settlement has its own calendar: bank holidays prevent cash settlement,
    # while exchange-only ad-hoc closures (e.g. national mourning) need not.
    cal = _calendar(str(day.date()), str(end.date()))
    holidays = cal.regular_holidays.holidays(day, end).union(
        _AdditionalBankHolidays().holidays(day, end)
    )
    days = pd.bdate_range(day + pd.Timedelta(days=1), end).difference(holidays)
    lag = 1 if day >= pd.Timestamp("2024-05-28") else 2
    return str(days[lag - 1].date())
