"""Unit tests for the per-side interval-join watermark clock."""

from datetime import datetime, timedelta, timezone
from typing import Optional

from bytewax.operators.interval_join import (
    _SideClockLogic,
    _SideClockState,
)
from bytewax.operators.windowing import UTC_MAX, UTC_MIN
from bytewax.testing import TimeTestingGetter

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
ZERO = timedelta(0)


def _clock(
    source: TimeTestingGetter,
    tolerance: timedelta = ZERO,
    idle_timeout: Optional[timedelta] = None,
) -> _SideClockLogic[datetime]:
    return _SideClockLogic(
        timestamp_getter=lambda x: x,
        out_of_order_tolerance=tolerance,
        idle_timeout=idle_timeout,
        now_getter=source.get,
        to_system_utc=lambda x: x,
    )


def test_watermark_starts_at_beginning_of_time() -> None:
    source = TimeTestingGetter(_T0)
    clock = _clock(source)
    assert clock.watermark() == UTC_MIN
    assert clock.on_notify() == UTC_MIN


def test_watermark_is_max_timestamp_minus_tolerance() -> None:
    source = TimeTestingGetter(_T0)
    clock = _clock(source, tolerance=timedelta(seconds=5))
    clock.before_batch()
    _, watermark = clock.on_item(_T0 + timedelta(seconds=10))
    assert watermark == _T0 + timedelta(seconds=5)


def test_watermark_advances_with_larger_timestamps() -> None:
    source = TimeTestingGetter(_T0)
    clock = _clock(source)
    clock.before_batch()
    clock.on_item(_T0 + timedelta(seconds=10))
    _, watermark = clock.on_item(_T0 + timedelta(seconds=20))
    assert watermark == _T0 + timedelta(seconds=20)


def test_watermark_does_not_regress_on_out_of_order_item() -> None:
    source = TimeTestingGetter(_T0)
    clock = _clock(source)
    clock.before_batch()
    clock.on_item(_T0 + timedelta(seconds=20))
    _, watermark = clock.on_item(_T0 + timedelta(seconds=5))
    # The older timestamp must not pull the watermark back.
    assert watermark == _T0 + timedelta(seconds=20)


def test_item_at_watermark_is_not_late() -> None:
    source = TimeTestingGetter(_T0)
    clock = _clock(source, tolerance=timedelta(seconds=5))
    clock.before_batch()
    _, watermark = clock.on_item(_T0 + timedelta(seconds=10))
    # An item exactly at the watermark is on time (late is strictly
    # behind), matching the join's strict comparison.
    assert not (_T0 + timedelta(seconds=5) < watermark)


def test_watermark_holds_while_active() -> None:
    source = TimeTestingGetter(_T0)
    clock = _clock(source)
    clock.before_batch()
    clock.on_item(_T0 + timedelta(seconds=10))
    # Without an idleness timeout, system time does not move the
    # watermark.
    source.advance(timedelta(minutes=5))
    assert clock.on_notify() == _T0 + timedelta(seconds=10)


def test_idle_watermark_creeps_with_system_time() -> None:
    source = TimeTestingGetter(_T0)
    clock = _clock(
        source,
        tolerance=timedelta(seconds=2),
        idle_timeout=timedelta(seconds=10),
    )
    clock.before_batch()
    clock.on_item(_T0 + timedelta(seconds=100))
    # The max event (event time 100) arrived at system time 0, so the
    # base is 98. It is still active immediately after the event.
    assert clock.watermark() == _T0 + timedelta(seconds=98)
    # Before the timeout, still active: the watermark holds.
    source.advance(timedelta(seconds=9))
    assert clock.on_notify() == _T0 + timedelta(seconds=98)
    # Once idle, the watermark base moves forward by the elapsed
    # system time since the max event was observed.
    source.advance(timedelta(seconds=1))
    assert clock.on_notify() == _T0 + timedelta(seconds=98 + 10)
    source.advance(timedelta(seconds=30))
    assert clock.on_notify() == _T0 + timedelta(seconds=98 + 40)


def test_idle_watermark_respects_event_system_offset() -> None:
    # Event time and system time need not be aligned; the idle creep
    # must stay in the event-time domain instead of stamping the raw
    # system time (minus tolerance) onto the watermark.
    source = TimeTestingGetter(_T0 + timedelta(hours=12))
    clock = _clock(source, idle_timeout=timedelta(seconds=10))
    clock.before_batch()
    clock.on_item(_T0 + timedelta(seconds=100))
    source.advance(timedelta(minutes=1))
    # A minute of system time elapsed since the max event was
    # observed; the watermark base advances by that same amount.
    assert clock.on_notify() == _T0 + timedelta(seconds=160)


def test_new_event_makes_idle_side_active_again_without_regression() -> None:
    source = TimeTestingGetter(_T0)
    clock = _clock(source, idle_timeout=timedelta(seconds=10))
    clock.before_batch()
    clock.on_item(_T0 + timedelta(seconds=100))
    source.advance(timedelta(seconds=60))
    idle_watermark = clock.on_notify()
    # Idle: base 100 plus the full 60s of elapsed system time.
    assert idle_watermark == _T0 + timedelta(seconds=160)

    # A new event arrives and marks the side active again; its
    # timestamp is behind the idle watermark, but the clock must still
    # never report a smaller watermark.
    clock.before_batch()
    _, watermark = clock.on_item(_T0 + timedelta(seconds=110))
    assert watermark >= idle_watermark
    assert watermark == _T0 + timedelta(seconds=160)
    # A later event advances it normally.
    clock.before_batch()
    _, watermark = clock.on_item(_T0 + timedelta(seconds=200))
    assert watermark == _T0 + timedelta(seconds=200)


def test_on_eof_drives_watermark_to_end_of_time() -> None:
    source = TimeTestingGetter(_T0)
    clock = _clock(source)
    clock.before_batch()
    clock.on_item(_T0)
    assert clock.on_eof() == UTC_MAX
    assert clock.on_notify() == UTC_MAX


def test_snapshot_round_trip_preserves_watermark() -> None:
    source = TimeTestingGetter(_T0)
    clock = _clock(source, tolerance=timedelta(seconds=5))
    clock.before_batch()
    clock.on_item(_T0 + timedelta(seconds=10))
    snapshot = clock.snapshot()

    # Rebuild from the snapshot with a later system clock.
    later = TimeTestingGetter(_T0 + timedelta(hours=1))
    restored = _SideClockLogic(
        lambda x: x,
        timedelta(seconds=5),
        None,
        later.get,
        lambda x: x,
        snapshot,
    )
    # The watermark picks up exactly where it left off; the later
    # system time cannot change it without new events, and it cannot
    # regress.
    assert restored.watermark() == _T0 + timedelta(seconds=5)
    restored.before_batch()
    _, watermark = restored.on_item(_T0 + timedelta(seconds=20))
    assert watermark == _T0 + timedelta(seconds=15)


def test_snapshot_is_deep_copied() -> None:
    source = TimeTestingGetter(_T0)
    clock = _clock(source)
    clock.before_batch()
    clock.on_item(_T0 + timedelta(seconds=10))
    snapshot = clock.snapshot()
    assert isinstance(snapshot, _SideClockState)
    # Mutating the snapshot must not affect the live clock.
    snapshot.watermark_base = UTC_MIN
    assert clock.watermark() == _T0 + timedelta(seconds=10)


def test_next_idle_system_time() -> None:
    source = TimeTestingGetter(_T0)
    clock = _clock(source, idle_timeout=timedelta(seconds=10))
    # Nothing seen yet: no idle deadline.
    assert clock.next_idle_system_time() is None
    clock.before_batch()
    clock.on_item(_T0 + timedelta(seconds=100))
    assert clock.next_idle_system_time() == _T0 + timedelta(seconds=10)
    # Once idle there is no further deadline to wait for.
    source.advance(timedelta(seconds=10))
    clock.on_notify()
    assert clock.next_idle_system_time() is None


def test_system_time_going_backwards_is_ignored() -> None:
    times = [_T0 + timedelta(seconds=30), _T0]

    def getter() -> datetime:
        return times[0]

    clock = _SideClockLogic(
        lambda x: x,
        ZERO,
        None,
        getter,
        lambda x: x,
    )
    clock.before_batch()
    clock.on_item(_T0 + timedelta(seconds=100))
    # Simulate the system clock jumping backwards; before_batch must
    # clamp it so internal state does not regress.
    times.reverse()
    clock.before_batch()
    assert clock.watermark() == _T0 + timedelta(seconds=100)
