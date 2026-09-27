"""Evidence-versioned purpose calendars, never implicit weekday fallbacks."""

from dataclasses import dataclass

import pandas as pd

from .common import day, utc

PURPOSES = {"trading", "settlement", "banking", "connect", "dealing", "confirmation"}


@dataclass(frozen=True)
class PurposeCalendar:
    calendar_id: str
    purpose: str
    version: str
    available_at: str
    valid_from: str
    valid_to: str
    open_days: tuple[str, ...]
    source: str
    evidence_kind: str = "historical_pit"

    def __post_init__(self):
        if (
            self.purpose not in PURPOSES
            or not self.calendar_id
            or not self.version
            or not self.source
        ):
            raise ValueError("calendar identity, purpose, version and source are required")
        if self.evidence_kind not in {"synthetic", "retrospective", "historical_pit"}:
            raise ValueError("unknown calendar evidence kind")
        utc(self.available_at)
        lo, hi = day(self.valid_from), day(self.valid_to)
        dates = tuple(day(x) for x in self.open_days)
        if lo > hi or tuple(sorted(set(dates))) != dates:
            raise ValueError("calendar bounds and unique ordered days required")
        if any(x < lo or x > hi for x in dates):
            raise ValueError("calendar open day outside declared coverage")
        object.__setattr__(self, "open_days", tuple(str(x.date()) for x in dates))

    def check(self, value, *, at, purpose=None):
        value = day(value)
        if purpose is not None and purpose != self.purpose:
            raise ValueError("calendar purpose mismatch")
        if utc(at) < utc(self.available_at):
            raise ValueError("calendar version was not yet known")
        if not day(self.valid_from) <= value <= day(self.valid_to):
            raise ValueError("calendar coverage exhausted")
        return value

    def is_open(self, value, *, at, purpose=None):
        value = self.check(value, at=at, purpose=purpose)
        return str(value.date()) in self.open_days

    def advance(self, value, lag, *, at, purpose=None):
        """lag=0 requires an open day; positive lag counts opens strictly AFTER value."""
        value = self.check(value, at=at, purpose=purpose)
        if type(lag) is not int or lag < 0:
            raise ValueError("lag must be a nonnegative integer")
        days = pd.DatetimeIndex(self.open_days)
        if lag == 0:
            if value not in days:
                raise ValueError("same-day settlement requires an open purpose day")
            return value
        future = days[days > value]
        if len(future) < lag:
            raise ValueError("calendar coverage exhausted before due date")
        return future[lag - 1]


class CalendarBook:
    def __init__(self, calendars):
        self.calendars = tuple(calendars)
        keys = [(x.calendar_id, x.purpose, utc(x.available_at)) for x in self.calendars]
        if len(keys) != len(set(keys)):
            raise ValueError("ambiguous calendar publication")

    def asof(self, calendar_id, purpose, at, on):
        candidates = [
            x
            for x in self.calendars
            if x.calendar_id == calendar_id
            and x.purpose == purpose
            and utc(x.available_at) <= utc(at)
            and day(x.valid_from) <= day(on) <= day(x.valid_to)
        ]
        if not candidates:
            raise ValueError("no visible calendar with requested purpose and coverage")
        return max(candidates, key=lambda x: utc(x.available_at))
