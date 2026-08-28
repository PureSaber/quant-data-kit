"""Deterministic market clock backed only by versioned UTC sessions."""

from __future__ import annotations

from collections.abc import Collection, Iterable
from datetime import date, datetime

from quant_data_kit.domain_v2 import SessionPhase, TradingSession
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.temporal_v2 import ensure_utc_datetime


class MarketClock:
    def __init__(self, calendar_id: str, sessions: Iterable[TradingSession]) -> None:
        if not isinstance(calendar_id, str) or not calendar_id.strip():
            raise ValidationError("calendar_id must be a non-empty string")
        ordered = tuple(sorted(sessions, key=lambda item: item.opens_at))
        if any(session.calendar_id != calendar_id for session in ordered):
            raise ValidationError("all sessions must belong to the market clock calendar")
        for previous, current in zip(ordered, ordered[1:]):
            if current.opens_at < previous.closes_at:
                raise ValidationError(
                    f"trading sessions overlap: {previous.session_id}, {current.session_id}"
                )
        self.calendar_id = calendar_id
        self.sessions = ordered

    def session_at(self, timestamp: datetime) -> TradingSession | None:
        moment = ensure_utc_datetime(timestamp, field="timestamp")
        return next(
            (
                session
                for session in self.sessions
                if session.opens_at <= moment < session.closes_at
            ),
            None,
        )

    def trading_day_at(self, timestamp: datetime) -> date | None:
        session = self.session_at(timestamp)
        return session.trading_day if session is not None else None

    def is_open(
        self,
        timestamp: datetime,
        *,
        phases: Collection[SessionPhase],
    ) -> bool:
        session = self.session_at(timestamp)
        return session is not None and session.phase in phases

    def next_open(
        self,
        timestamp: datetime,
        *,
        phases: Collection[SessionPhase],
    ) -> datetime | None:
        moment = ensure_utc_datetime(timestamp, field="timestamp")
        for session in self.sessions:
            if session.phase in phases and session.opens_at >= moment:
                return session.opens_at
        return None
