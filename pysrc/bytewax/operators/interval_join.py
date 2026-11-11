"""Event-time interval join operator.

Join two keyed streams by an event-time interval instead of a fixed
window. A left record at timestamp `t_l` matches every right record of
the same key whose timestamp is within `[t_l + lower_bound, t_l +
upper_bound]`.

Each side maintains its own event-time watermark, advanced with a
configurable out-of-order tolerance and (optionally) an idleness
timeout. A left record is only emitted as unmatched (in a left outer
join) once the *right* watermark has passed the end of its matching
interval, so no valid match can ever be missed.

"""

import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import (
    Any,
    Callable,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
    cast,
)

from typing_extensions import TypeAlias, override

import bytewax.operators as op
from bytewax.dataflow import operator
from bytewax.operators import (
    KeyedStream,
    StatefulBatchLogic,
    _get_system_utc,
    _identity,
)
from bytewax.operators.windowing import UTC_MAX, UTC_MIN, ZERO_TD

U = TypeVar("U")
"""Type of values in the left (outer) stream."""

V = TypeVar("V")
"""Type of values in the right (inner) stream."""


# Index of the left and right side within the merged, side-labelled
# stream.
_LEFT_SIDE = 0
_RIGHT_SIDE = 1


@dataclass
class _SideClockState:
    """Snapshotable state of a single side's watermark clock.

    Only event-time bookkeeping is snapshot. The system time at which
    the last event was seen is derived from the configured system clock
    on resume and is not part of the persisted state, so a resume can
    never move the watermark backwards.

    """

    max_event_timestamp: datetime
    watermark_base: datetime
    # System time at which `max_event_timestamp` was observed. `None`
    # until the first event. Used to advance the watermark while idle.
    system_time_of_max_event: Optional[datetime]


@dataclass
class _SideClockLogic(Generic[V]):
    """Per-key, per-side event-time watermark clock.

    The watermark is the largest event timestamp seen on this side,
    minus the out-of-order tolerance. While the side is active that is
    the whole definition: the watermark holds until a later event
    arrives.

    If `idle_timeout` is set and no event has been seen on this side
    for that much system time, the side becomes *idle*. While idle the
    watermark creeps forward with system time (subtracting the
    tolerance), mirroring Flink's idle-partition semantics, so a silent
    side cannot hold back interval expiration forever. Any new event
    marks the side active again.

    The watermark is guaranteed to never regress, within an execution
    and across resumes.

    """

    timestamp_getter: Callable[[V], datetime]
    out_of_order_tolerance: timedelta
    idle_timeout: Optional[timedelta]
    now_getter: Callable[[], datetime]
    to_system_utc: Callable[[datetime], Optional[datetime]]

    state: _SideClockState = field(
        default_factory=lambda: _SideClockState(UTC_MIN, UTC_MIN, None)
    )
    # System time of the most recent event of any kind; drives the
    # idleness decision. Not snapshot (see `_SideClockState`).
    _last_event_system_time: Optional[datetime] = field(default=None, init=False)
    _system_now: datetime = field(init=False)
    _eof: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self._system_now = self.now_getter()

    # ----- helpers -------------------------------------------------

    def _is_idle(self, system_now: datetime) -> bool:
        if self.idle_timeout is None or self._last_event_system_time is None:
            return False
        return system_now - self._last_event_system_time >= self.idle_timeout

    def _idle_watermark(self, system_now: datetime) -> Optional[datetime]:
        """Watermark implied by elapsed system time while idle.

        Mirrors the advancement rule of Bytewax's event clock: the
        watermark base set by the largest event moves forward by the
        system time elapsed since that event was observed. This keeps
        the watermark in the event-time domain even when event and
        system time are offset.

        """
        if self.state.system_time_of_max_event is None:
            return None
        try:
            return self.state.watermark_base + (
                system_now - self.state.system_time_of_max_event
            )
        except OverflowError:
            return None

    # ----- runtime interface --------------------------------------

    def before_batch(self) -> None:
        system_now = self.now_getter()
        # Don't let now go backwards. This can happen due to NTP
        # synchronization, clock updates, or negative leap seconds.
        if system_now > self._system_now:
            self._system_now = system_now

    def on_item(self, value: V) -> Tuple[datetime, datetime]:
        """Register an event and return `(event timestamp, watermark)`.

        The returned watermark already incorporates this event and is
        guaranteed to be at least the previously observed watermark.

        """
        event_timestamp = self.timestamp_getter(value)
        prev_watermark = self.watermark()

        self._last_event_system_time = self._system_now
        if event_timestamp > self.state.max_event_timestamp:
            self.state.max_event_timestamp = event_timestamp
            try:
                self.state.watermark_base = (
                    event_timestamp - self.out_of_order_tolerance
                )
            except OverflowError:
                # The unadjusted timestamp is unrepresentable; keep the
                # previous base so the watermark does not regress.
                pass
            else:
                self.state.system_time_of_max_event = self._system_now

        watermark = max(prev_watermark, self.watermark())
        assert watermark >= prev_watermark
        return event_timestamp, watermark

    def on_notify(self) -> datetime:
        """Recompute the watermark when there is no new event."""
        self.before_batch()
        if self._eof:
            return UTC_MAX
        return self.watermark()

    def on_eof(self) -> datetime:
        # `on_eof` must not permanently mutate snapshot state, so only
        # remember this for the remainder of the current execution.
        self._eof = True
        return UTC_MAX

    def watermark(self) -> datetime:
        """Current watermark without mutating anything."""
        if self._eof:
            return UTC_MAX
        watermark = self.state.watermark_base
        if self._is_idle(self._system_now):
            idle_watermark = self._idle_watermark(self._system_now)
            if idle_watermark is not None and idle_watermark > watermark:
                watermark = idle_watermark
        return watermark

    def next_idle_system_time(self) -> Optional[datetime]:
        """System time when this side next becomes idle, if configured.

        Returns `None` if idleness is disabled, no event was ever seen,
        or the side is already idle.

        """
        if self.idle_timeout is None or self._last_event_system_time is None:
            return None
        if self._is_idle(self._system_now):
            return None
        return self._last_event_system_time + self.idle_timeout

    def to_system(self, timestamp: datetime) -> Optional[datetime]:
        return self.to_system_utc(timestamp)

    def snapshot(self) -> _SideClockState:
        return copy.deepcopy(self.state)


@dataclass
class _BufferedRecord(Generic[V]):
    """A record held until it can no longer take part in a match."""

    timestamp: datetime
    value: V
    # Whether this record has already produced every match it ever
    # will. Once `True` it only lingers until its retention deadline
    # passes, then is reclaimed.
    matched: bool = False


@dataclass(frozen=True)
class _IntervalJoinSnapshot(Generic[U, V]):
    """Full recovery state for one join key."""

    left_clock: _SideClockState
    right_clock: _SideClockState
    left_records: List[_BufferedRecord[U]]
    right_records: List[_BufferedRecord[V]]


# A value emitted on the matched output: the left and right values of
# one concrete pair.
_Match: TypeAlias = Tuple[U, V]
# A value emitted because a left record could not be matched: its value
# and `None` for the missing right side.
_UnmatchedLeft: TypeAlias = Tuple[U, None]
# A value emitted on the late output: which side (`0` for left, `1` for
# right) and the late value.
_Late: TypeAlias = Tuple[int, Any]
_IntervalJoinEvent: TypeAlias = Tuple[str, Any]


def _match_value(left: U, right: V) -> _IntervalJoinEvent:
    return ("M", (left, right))


def _unmatched_value(left: U) -> _IntervalJoinEvent:
    return ("U", (left, None))


def _late_value(side: int, value: Any) -> _IntervalJoinEvent:
    return ("L", (side, value))


@dataclass
class _IntervalJoinLogic(
    StatefulBatchLogic[
        Tuple[int, Any],
        _IntervalJoinEvent,
        _IntervalJoinSnapshot[U, V],
    ],
    Generic[U, V],
):
    """Core interval join logic for a single join key."""

    left_ts_getter: Callable[[U], datetime]
    right_ts_getter: Callable[[V], datetime]
    lower_bound: timedelta
    upper_bound: timedelta
    left_outer: bool
    left_clock: _SideClockLogic[U]
    right_clock: _SideClockLogic[V]

    left_records: List[_BufferedRecord[U]] = field(default_factory=list)
    right_records: List[_BufferedRecord[V]] = field(default_factory=list)

    _last_left_watermark: datetime = UTC_MIN
    _last_right_watermark: datetime = UTC_MIN

    # ----- interval predicates ------------------------------------

    def _in_interval(self, left_ts: datetime, right_ts: datetime) -> bool:
        return left_ts + self.lower_bound <= right_ts <= left_ts + self.upper_bound

    def _left_expired(self, left_ts: datetime, right_watermark: datetime) -> bool:
        """No future right event can still match this left record."""
        return right_watermark > left_ts + self.upper_bound

    def _right_expired(self, right_ts: datetime, left_watermark: datetime) -> bool:
        """No future left event can still match this right record."""
        return left_watermark > right_ts - self.lower_bound

    # ----- record handling ----------------------------------------

    def _on_left(
        self, value: U, timestamp: datetime, watermark: datetime
    ) -> List[_IntervalJoinEvent]:
        events: List[_IntervalJoinEvent] = []
        if timestamp < watermark:
            events.append(_late_value(_LEFT_SIDE, value))
            return events

        record = _BufferedRecord(timestamp, value)
        self.left_records.append(record)

        # Eagerly pair against every currently buffered right record.
        # Buffers only contain records that are still matchable, so
        # each `(left, right)` pair is considered exactly once over
        # the lifetime of the join and can never be duplicated.
        for right in self.right_records:
            if self._in_interval(timestamp, right.timestamp):
                events.append(_match_value(value, right.value))
                right.matched = True
                record.matched = True
        return events

    def _on_right(
        self, value: V, timestamp: datetime, watermark: datetime
    ) -> List[_IntervalJoinEvent]:
        events: List[_IntervalJoinEvent] = []
        if timestamp < watermark:
            events.append(_late_value(_RIGHT_SIDE, value))
            return events

        record = _BufferedRecord(timestamp, value)
        self.right_records.append(record)

        for left in self.left_records:
            if self._in_interval(left.timestamp, timestamp):
                events.append(_match_value(left.value, value))
                left.matched = True
                record.matched = True
        return events

    def _sweep(self) -> List[_IntervalJoinEvent]:
        """React to watermark progress: emit outer rows and reclaim.

        Must be called after both side watermarks have been refreshed
        for the current activation.

        """
        events: List[_IntervalJoinEvent] = []
        left_watermark = self.left_clock.watermark()
        right_watermark = self.right_clock.watermark()
        assert left_watermark >= self._last_left_watermark
        assert right_watermark >= self._last_right_watermark
        self._last_left_watermark = left_watermark
        self._last_right_watermark = right_watermark

        # Left records whose interval has fully passed: either emit an
        # outer row (if never matched) or simply reclaim them.
        retained_left: List[_BufferedRecord[U]] = []
        for left in self.left_records:
            if self._left_expired(left.timestamp, right_watermark):
                if self.left_outer and not left.matched:
                    events.append(_unmatched_value(left.value))
            else:
                retained_left.append(left)
        self.left_records = retained_left

        # Right records that no future left event could match anymore.
        self.right_records = [
            right
            for right in self.right_records
            if not self._right_expired(right.timestamp, left_watermark)
        ]
        return events

    # ----- StatefulBatchLogic interface ---------------------------

    @override
    def on_batch(
        self, values: List[Tuple[int, Any]]
    ) -> Tuple[List[_IntervalJoinEvent], bool]:
        self.left_clock.before_batch()
        self.right_clock.before_batch()

        events: List[_IntervalJoinEvent] = []
        for side, value in values:
            if side == _LEFT_SIDE:
                left_value = cast(U, value)
                timestamp, watermark = self.left_clock.on_item(left_value)
                events.extend(self._on_left(left_value, timestamp, watermark))
            else:
                right_value = cast(V, value)
                timestamp, watermark = self.right_clock.on_item(right_value)
                events.extend(self._on_right(right_value, timestamp, watermark))

        events.extend(self._sweep())
        return events, self._is_empty()

    @override
    def on_notify(self) -> Tuple[List[_IntervalJoinEvent], bool]:
        self.left_clock.on_notify()
        self.right_clock.on_notify()
        events = self._sweep()
        return events, self._is_empty()

    @override
    def on_eof(self) -> Tuple[List[_IntervalJoinEvent], bool]:
        # Drive both watermarks to the end of time so every retained
        # record is resolved and all state is reclaimed.
        self.left_clock.on_eof()
        self.right_clock.on_eof()
        events = self._sweep()
        assert self._is_empty()
        return events, StatefulBatchLogic.DISCARD

    @override
    def notify_at(self) -> Optional[datetime]:
        # The only thing that advances without an incoming event is an
        # idle-side watermark. Wake at the soonest idleness deadline
        # (translated to system time) so outer rows and state cleanup
        # are emitted promptly even on a quiet side.
        candidates: List[datetime] = []
        for clock in (self.left_clock, self.right_clock):
            idle_at = clock.next_idle_system_time()
            if idle_at is not None:
                system_at = clock.to_system(idle_at)
                if system_at is not None:
                    candidates.append(system_at)
        return min(candidates, default=None)

    def _is_empty(self) -> bool:
        # Never discard while a record could still be matched or an
        # outer row is owed. The clocks themselves are O(1) per key.
        return not self.left_records and not self.right_records

    @override
    def snapshot(self) -> _IntervalJoinSnapshot[U, V]:
        return _IntervalJoinSnapshot(
            self.left_clock.snapshot(),
            self.right_clock.snapshot(),
            copy.deepcopy(self.left_records),
            copy.deepcopy(self.right_records),
        )


@dataclass(frozen=True)
class IntervalJoinOut(Generic[U, V]):
    """Streams returned from the {py:obj}`interval_join` operator."""

    matched: KeyedStream[Tuple[U, V]]
    """Concrete matched pairs, as `(left_value, right_value)` 2-tuples.

    Every `(left record, right record)` pair whose timestamps satisfy
    the interval is emitted exactly once; there are no missed or
    duplicated pairs.

    """

    unmatched_left: KeyedStream[Tuple[U, None]]
    """Left records with no matching right record.

    A record appears here only in a left outer join, and only once the
    right-side watermark has passed the end of its matching interval,
    guaranteeing no later right event could have matched it. Values are
    `(left_value, None)` 2-tuples.

    """

    late: KeyedStream[Tuple[int, Any]]
    """Records that arrived behind their side's watermark.

    Values are `(side, value)` 2-tuples where `side` is `0` for the
    left stream and `1` for the right. Late records take no part in
    matching and are never silently dropped.

    """


def _unwrap(
    tag: str,
) -> Callable[[_IntervalJoinEvent], Optional[Any]]:
    def mapper(event: _IntervalJoinEvent) -> Optional[Any]:
        event_tag, value = event
        return value if event_tag == tag else None

    return mapper


@operator
def interval_join(
    step_id: str,
    left: KeyedStream[U],
    right: KeyedStream[V],
    /,
    *,
    left_ts_getter: Callable[[U], datetime],
    right_ts_getter: Callable[[V], datetime],
    lower_bound: timedelta = ZERO_TD,
    upper_bound: timedelta = ZERO_TD,
    left_outer: bool = True,
    left_out_of_order_tolerance: timedelta = ZERO_TD,
    right_out_of_order_tolerance: timedelta = ZERO_TD,
    left_idle_timeout: Optional[timedelta] = None,
    right_idle_timeout: Optional[timedelta] = None,
    now_getter: Callable[[], datetime] = _get_system_utc,
    to_system_utc: Callable[[datetime], Optional[datetime]] = _identity,
) -> IntervalJoinOut[U, V]:
    """Join two keyed streams on an event-time interval.

    A left record with event timestamp `t_l` matches every right
    record of the same key whose event timestamp `t_r` satisfies
    `t_l + lower_bound <= t_r <= t_l + upper_bound`. All such pairs are
    emitted on the {py:obj}`~IntervalJoinOut.matched` stream; this is
    an "all-to-all" join, so multiple records on either side produce
    the full set of matching combinations.

    Each side advances its own event-time watermark as the largest
    event timestamp seen minus that side's out-of-order tolerance. A
    record whose timestamp is behind its side's watermark when it
    arrives is considered late, is emitted on the
    {py:obj}`~IntervalJoinOut.late` stream, and never participates in
    matching.

    If an idleness timeout is given for a side, that side's watermark
    starts creeping forward with system time once no event has arrived
    for that duration, so an idle side cannot block interval
    expiration. A new event makes the side active again.

    With `left_outer=True` (the default), a left record that finds no
    match is emitted on {py:obj}`~IntervalJoinOut.unmatched_left` only
    after the *right* watermark has passed `t_l + upper_bound`. This is
    what makes the contract exact: an outer row cannot be emitted
    before every right record that could have matched has already had
    to arrive.

    In the advertising example below, a click matches a conversion if
    the conversion happens between 0 and 30 minutes after the click.

    ```{testcode}
    from datetime import datetime, timedelta, timezone
    from bytewax.dataflow import Dataflow
    import bytewax.operators as op
    from bytewax.operators.interval_join import interval_join
    from bytewax.testing import TestingSource, run_main

    align = datetime(2024, 1, 1, tzinfo=timezone.utc)

    def at(minute: int) -> datetime:
        return align + timedelta(minutes=minute)

    flow = Dataflow("ad_attribution")
    clicks = op.input(
        "clicks",
        flow,
        TestingSource([("user1", ("click", at(0))), ("user2", ("click", at(5)))]),
    )
    conversions = op.input(
        "conversions",
        flow,
        # The first conversion is 20 minutes after user1's click; the
        # second is 45 minutes after user2's click and so is too late.
        TestingSource(
            [
                ("user1", ("conv", at(20))),
                ("user2", ("conv", at(50))),
            ]
        ),
    )

    joined = interval_join(
        "attribute",
        clicks,
        conversions,
        left_ts_getter=lambda event: event[1],
        right_ts_getter=lambda event: event[1],
        lower_bound=timedelta(0),
        upper_bound=timedelta(minutes=30),
        # Freeze system time so watermarks move only on events.
        now_getter=lambda: align,
    )

    def show_pair(pair):
        (left_event, right_event) = pair
        return f"matched {left_event[0]} -> {right_event[0]}"

    matched = op.map_value("show", joined.matched, show_pair)
    op.inspect("matched", matched)

    def show_unmatched(row):
        left_event, _ = row
        return f"unmatched {left_event[0]}"

    unmatched = op.map_value("show_unmatched", joined.unmatched_left, show_unmatched)
    op.inspect("unmatched", unmatched)
    ```

    ```{testcode}
    :hide:

    run_main(flow)
    ```

    ```{testoutput}
    ad_attribution.matched: ('user1', 'matched click -> conv')
    ad_attribution.unmatched: ('user2', 'unmatched click')
    ```

    :arg step_id: Unique ID.

    :arg left: Left (outer) keyed stream.

    :arg right: Right (inner) keyed stream.

    :arg left_ts_getter: Called on each left value to get its aware
        UTC event timestamp.

    :arg right_ts_getter: Called on each right value to get its aware
        UTC event timestamp.

    :arg lower_bound: Inclusive lower bound of the matching interval,
        relative to a left record's timestamp. May be negative.
        Defaults to zero.

    :arg upper_bound: Inclusive upper bound of the matching interval,
        relative to a left record's timestamp. Must be greater than or
        equal to `lower_bound`. Defaults to zero.

    :arg left_outer: If `True`, emit unmatched left records on
        {py:obj}`~IntervalJoinOut.unmatched_left` once they can no
        longer match. If `False`, that stream is always empty.
        Defaults to `True`.

    :arg left_out_of_order_tolerance: How far behind the left
        watermark a left event may arrive and still be considered
        on-time. Defaults to zero.

    :arg right_out_of_order_tolerance: As above, for the right side.

    :arg left_idle_timeout: System-time gap without a left event after
        which the left side is considered idle and its watermark
        advances with system time. `None` disables idleness. Defaults
        to `None`.

    :arg right_idle_timeout: As above, for the right side.

    :arg now_getter: Return the current "system" timestamp used to
        advance idle watermarks. Defaults to the current system time
        in UTC.

    :arg to_system_utc: For a given event-time timestamp, return the
        UTC system time the runtime should wake at to run interval
        expiration. Returning `None` disables time-based wakeups, so
        expiration then depends on new items or EOF. Defaults to
        passing the timestamp through.

    :returns: The matched, unmatched-left, and late output streams.

    """
    if upper_bound < lower_bound:
        msg = (
            "`upper_bound` must be greater than or equal to "
            "`lower_bound`; got "
            f"lower_bound={lower_bound!r} and upper_bound={upper_bound!r}"
        )
        raise ValueError(msg)
    for name, tolerance in (
        ("left_out_of_order_tolerance", left_out_of_order_tolerance),
        ("right_out_of_order_tolerance", right_out_of_order_tolerance),
    ):
        if tolerance < ZERO_TD:
            msg = f"`{name}` must not be negative; got {tolerance!r}"
            raise ValueError(msg)
    for name, timeout in (
        ("left_idle_timeout", left_idle_timeout),
        ("right_idle_timeout", right_idle_timeout),
    ):
        if timeout is not None and timeout < ZERO_TD:
            msg = f"`{name}` must not be negative; got {timeout!r}"
            raise ValueError(msg)

    labelled = op._join_label_merge("label_sides", left, right)

    def shim_builder(
        resume_state: Optional[_IntervalJoinSnapshot[U, V]],
    ) -> _IntervalJoinLogic[U, V]:
        if resume_state is None:
            left_clock = _SideClockLogic(
                left_ts_getter,
                left_out_of_order_tolerance,
                left_idle_timeout,
                now_getter,
                to_system_utc,
            )
            right_clock = _SideClockLogic(
                right_ts_getter,
                right_out_of_order_tolerance,
                right_idle_timeout,
                now_getter,
                to_system_utc,
            )
            return _IntervalJoinLogic(
                left_ts_getter,
                right_ts_getter,
                lower_bound,
                upper_bound,
                left_outer,
                left_clock,
                right_clock,
            )

        left_clock = _SideClockLogic(
            left_ts_getter,
            left_out_of_order_tolerance,
            left_idle_timeout,
            now_getter,
            to_system_utc,
            resume_state.left_clock,
        )
        right_clock = _SideClockLogic(
            right_ts_getter,
            right_out_of_order_tolerance,
            right_idle_timeout,
            now_getter,
            to_system_utc,
            resume_state.right_clock,
        )
        logic = _IntervalJoinLogic(
            left_ts_getter,
            right_ts_getter,
            lower_bound,
            upper_bound,
            left_outer,
            left_clock,
            right_clock,
            resume_state.left_records,
            resume_state.right_records,
        )
        # Re-anchor the watermark invariants. On resume the persisted
        # bases give the same watermarks as before the snapshot, so
        # these can only hold them steady, never move them back.
        logic._last_left_watermark = left_clock.watermark()
        logic._last_right_watermark = right_clock.watermark()
        return logic

    events = op.stateful_batch("stateful_batch", labelled, shim_builder)

    matched = op.filter_map_value("unwrap_matched", events, _unwrap("M"))
    unmatched_left = op.filter_map_value("unwrap_unmatched", events, _unwrap("U"))
    late = op.filter_map_value("unwrap_late", events, _unwrap("L"))
    return IntervalJoinOut(matched, unmatched_left, late)
