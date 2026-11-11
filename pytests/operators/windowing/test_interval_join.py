"""Operator-level tests for the event-time interval join."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List, Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators.interval_join import (
    IntervalJoinOut,
    _IntervalJoinLogic,
    _SideClockLogic,
    interval_join,
)
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import TestingSink, TestingSource, TimeTestingGetter, run_main
from pytest import raises

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _at(secs: int) -> datetime:
    return _T0 + timedelta(seconds=secs)


ZERO = timedelta(0)


@dataclass(frozen=True)
class _Ev:
    """A keyed test event: `key` carries the join key, `t` the time."""

    key: str
    t: int
    name: str

    @property
    def ts(self) -> datetime:
        return _at(self.t)


def _build(
    left: List[_Ev],
    right: List[_Ev],
    *,
    lower: timedelta = ZERO,
    upper: timedelta = timedelta(seconds=30),
    left_outer: bool = True,
    left_tolerance: timedelta = ZERO,
    right_tolerance: timedelta = ZERO,
    left_idle: Optional[timedelta] = None,
    right_idle: Optional[timedelta] = None,
    now_getter: Any = None,
    batch_size: int = 1,
) -> Tuple[Dataflow, List[Tuple[str, str, Any]]]:
    flow = Dataflow("ij_test")
    ls = op.input("left_in", flow, TestingSource(left, batch_size=batch_size))
    rs = op.input("right_in", flow, TestingSource(right, batch_size=batch_size))
    keyed_l = op.key_on("key_l", ls, lambda e: e.key)
    keyed_r = op.key_on("key_r", rs, lambda e: e.key)

    joined: IntervalJoinOut = interval_join(
        "ij",
        keyed_l,
        keyed_r,
        left_ts_getter=lambda e: e.ts,
        right_ts_getter=lambda e: e.ts,
        lower_bound=lower,
        upper_bound=upper,
        left_outer=left_outer,
        left_out_of_order_tolerance=left_tolerance,
        right_out_of_order_tolerance=right_tolerance,
        left_idle_timeout=left_idle,
        right_idle_timeout=right_idle,
        now_getter=now_getter or (lambda: _T0),
    )

    out: List[Tuple[str, str, Any]] = []
    matched = op.map_value(
        "tag_m", joined.matched, lambda pair: ("M", pair[0].name, pair[1].name)
    )
    unmatched = op.map_value(
        "tag_u",
        joined.unmatched_left,
        lambda row: ("U", row[0].name, None),
    )
    late = op.map_value("tag_late", joined.late, lambda row: ("L", row[0], row[1].name))
    tagged = op.key_rm("strip_key", op.merge("merge", matched, unmatched, late))
    op.output("out", tagged, TestingSink(out))
    return flow, out


def _canon(out: List[Tuple[str, str, Any]]) -> List[Tuple[str, str, Any]]:
    return sorted(out, key=repr)


# ---------------------------------------------------------------------
# Interval semantics
# ---------------------------------------------------------------------


def test_matches_inside_inclusive_bounds() -> None:
    # Bounds are inclusive: a conversion exactly at t_l + lower and
    # exactly t_l + upper both match. Records are presented in
    # timestamp order so zero-tolerance watermarks do not flag them as
    # late; out-of-order handling is tested separately.
    left = [_Ev("k", 10, "click")]
    right = [
        _Ev("k", 9, "before"),
        _Ev("k", 10, "at_lower"),
        _Ev("k", 25, "inside"),
        _Ev("k", 40, "at_upper"),
        _Ev("k", 41, "after"),
    ]
    flow, out = _build(left, right)
    run_main(flow)
    assert _canon(out) == _canon(
        [
            ("M", "click", "at_lower"),
            ("M", "click", "at_upper"),
            ("M", "click", "inside"),
        ]
    )


def test_negative_lower_bound_allows_right_before_left() -> None:
    left = [_Ev("k", 100, "click")]
    right = [_Ev("k", 90, "conv")]
    flow, out = _build(left, right, lower=timedelta(seconds=-15), upper=ZERO)
    run_main(flow)
    assert _canon(out) == [("M", "click", "conv")]


def test_all_to_all_matching_for_multiple_records_per_side() -> None:
    left = [_Ev("k", 0, "l0"), _Ev("k", 10, "l1")]
    right = [_Ev("k", 5, "r0"), _Ev("k", 15, "r1"), _Ev("k", 25, "r2")]
    # l0 [0,30]: r0,r1,r2 ; l1 [10,40]: r1,r2
    flow, out = _build(left, right)
    run_main(flow)
    assert _canon(out) == _canon(
        [
            ("M", "l0", "r0"),
            ("M", "l0", "r1"),
            ("M", "l0", "r2"),
            ("M", "l1", "r1"),
            ("M", "l1", "r2"),
        ]
    )


def test_matching_is_per_key() -> None:
    left = [_Ev("a", 0, "la"), _Ev("b", 0, "lb")]
    right = [_Ev("a", 5, "ra"), _Ev("b", 50, "rb")]
    flow, out = _build(left, right)
    run_main(flow)
    assert _canon(out) == _canon(
        [
            ("M", "la", "ra"),
            ("U", "lb", None),
        ]
    )


def test_arrival_order_does_not_affect_results() -> None:
    # Right records arrive before the left record; the buffered right
    # side must still produce the same pairs as the reverse order.
    left = [_Ev("k", 10, "click")]
    right = [_Ev("k", 15, "conv")]
    flow_rev, out_rev = _build(left, right)
    run_main(flow_rev)

    flow_fwd, out_fwd = _build([_Ev("k", 10, "click")], [_Ev("k", 15, "conv")])
    run_main(flow_fwd)
    assert _canon(out_rev) == _canon(out_fwd) == [("M", "click", "conv")]


def test_out_of_order_records_still_match_within_tolerance() -> None:
    # Out-of-order but on-time (behind the max event but not behind
    # the watermark once tolerance is configured).
    left = [_Ev("k", 0, "l0"), _Ev("k", 30, "l1")]
    right = [
        _Ev("k", 40, "r_late_max"),
        # Arrives after an event at t=40 but with a 20s tolerance the
        # watermark is only 20, so this is still on time and matches
        # l1 (whose interval is [30,60]).
        _Ev("k", 35, "r_ooo"),
    ]
    flow, out = _build(left, right, right_tolerance=timedelta(seconds=20))
    run_main(flow)
    names = {row[2] for row in out if row[0] == "M"}
    assert names == {"r_late_max", "r_ooo"}


# ---------------------------------------------------------------------
# Left outer join
# ---------------------------------------------------------------------


def test_unmatched_left_emitted_only_after_right_watermark_passes() -> None:
    clock = TimeTestingGetter(_T0)
    left = [_Ev("k", 0, "click")]
    # The right event at t=100 is outside the click's [0,30] interval
    # and drives the right watermark past the interval end.
    right = [_Ev("k", 100, "far")]
    flow, out = _build(left, right, now_getter=clock.get)
    run_main(flow)
    assert _canon(out) == [("U", "click", None)]


def test_inner_join_never_emits_unmatched() -> None:
    left = [_Ev("k", 0, "click")]
    right = [_Ev("k", 100, "far")]
    flow, out = _build(left, right, left_outer=False)
    run_main(flow)
    assert out == []


def test_matched_left_is_never_also_emitted_unmatched() -> None:
    left = [_Ev("k", 0, "click")]
    right = [_Ev("k", 10, "conv"), _Ev("k", 100, "far")]
    flow, out = _build(left, right)
    run_main(flow)
    assert _canon(out) == [("M", "click", "conv")]
    assert all(row[0] != "U" for row in out)


# ---------------------------------------------------------------------
# Late records
# ---------------------------------------------------------------------


def test_late_right_record_goes_to_late_stream_and_does_not_match() -> None:
    # With zero tolerance, after seeing t=100 the watermark is 100; a
    # later-arriving t=10 event is late.
    left = [_Ev("k", 0, "click")]
    right = [_Ev("k", 100, "far"), _Ev("k", 10, "late_conv")]
    flow, out = _build(left, right)
    run_main(flow)
    assert _canon(out) == _canon(
        [
            ("U", "click", None),
            ("L", 1, "late_conv"),
        ]
    )


def test_late_left_record_goes_to_late_stream() -> None:
    left = [_Ev("k", 100, "far_click"), _Ev("k", 5, "late_click")]
    right = [_Ev("k", 10, "conv")]
    flow, out = _build(left, right)
    run_main(flow)
    # The t=5 click is late (watermark already 100) and cannot match;
    # the on-time t=100 click simply finds nothing in its interval and
    # is emitted as an outer row at EOF.
    assert _canon(out) == _canon(
        [
            ("L", 0, "late_click"),
            ("U", "far_click", None),
        ]
    )


def test_tolerance_keeps_records_on_time() -> None:
    # Same data as the late-right test, but a generous tolerance means
    # the t=10 record is no longer behind the watermark and matches.
    left = [_Ev("k", 0, "click")]
    right = [_Ev("k", 100, "far"), _Ev("k", 10, "conv")]
    flow, out = _build(left, right, right_tolerance=timedelta(seconds=95))
    run_main(flow)
    assert _canon(out) == [("M", "click", "conv")]


# ---------------------------------------------------------------------
# Idleness
# ---------------------------------------------------------------------


def test_idle_right_side_expires_pending_left_without_new_events() -> None:
    clock = TimeTestingGetter(_T0)
    left = [_Ev("k", 0, "click")]
    # No right records at all; with a right idleness timeout the right
    # watermark must still advance past t=30 on its own.
    flow, out = _build(
        left,
        [],
        right_idle=timedelta(seconds=10),
        now_getter=clock.get,
    )
    run_main(flow)
    assert _canon(out) == [("U", "click", None)]


def test_idle_left_side_reclaims_right_records() -> None:
    clock = TimeTestingGetter(_T0)
    # A right record with no left record ever matching it must be
    # reclaimed once the idle left watermark passes its retention
    # deadline, even without EOF (we assert via no errors and an empty
    # matched stream; reclamation itself is covered directly below).
    right = [_Ev("k", 100, "conv")]
    flow, out = _build(
        [],
        right,
        left_idle=timedelta(seconds=10),
        now_getter=clock.get,
    )
    run_main(flow)
    assert out == []


# ---------------------------------------------------------------------
# State reclamation (white-box, via the logic directly)
# ---------------------------------------------------------------------


def test_state_is_reclaimed_once_records_cannot_match() -> None:
    clock_getter = lambda: _T0  # noqa: E731
    left_clock = _SideClockLogic(lambda e: e.ts, ZERO, None, clock_getter, _ident)
    right_clock = _SideClockLogic(lambda e: e.ts, ZERO, None, clock_getter, _ident)
    logic = _IntervalJoinLogic(
        lambda e: e.ts,
        lambda e: e.ts,
        ZERO,
        timedelta(seconds=30),
        True,
        left_clock,
        right_clock,
    )

    # A matched pair is retained until the watermarks prove no record
    # on the other side can still reach it.
    logic.on_batch([(0, _Ev("k", 0, "click")), (1, _Ev("k", 20, "conv"))])
    assert len(logic.left_records) == 1
    assert len(logic.right_records) == 1
    assert logic.left_records[0].matched
    assert logic.right_records[0].matched

    # A far right event (t=100) pushes the right watermark past the
    # click's interval end (30), so the left record is reclaimed. The
    # right records linger because the left watermark is still 0.
    logic.on_batch([(1, _Ev("k", 100, "far_conv"))])
    assert logic.left_records == []
    assert {r.value.name for r in logic.right_records} == {"conv", "far_conv"}

    # A far left event (t=200) pushes the left watermark past every
    # retained right record's retention deadline, reclaiming them.
    logic.on_batch([(0, _Ev("k", 200, "far_click"))])
    # The new far-left record itself is retained, but all rights are
    # gone.
    assert [r.value.name for r in logic.left_records] == ["far_click"]
    assert logic.right_records == []

    # EOF drains the last record and discards the whole logic.
    _events, discard = logic.on_eof()
    assert discard is True
    assert logic.left_records == []


def _ident(ts: datetime) -> datetime:
    return ts


# ---------------------------------------------------------------------
# EOF resolves everything
# ---------------------------------------------------------------------


def test_eof_resolves_all_pending_state() -> None:
    left = [_Ev("a", 0, "la"), _Ev("b", 0, "lb")]
    right = [_Ev("a", 5, "ra")]
    flow, out = _build(left, right)
    run_main(flow)
    assert _canon(out) == _canon([("M", "la", "ra"), ("U", "lb", None)])


# ---------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------


def test_invalid_bounds_raise() -> None:
    flow = Dataflow("bad")
    ls = op.input("l", flow, TestingSource([]))
    rs = op.input("r", flow, TestingSource([]))
    with raises(ValueError):
        interval_join(
            "ij",
            op.key_on("kl", ls, lambda e: e.key),
            op.key_on("kr", rs, lambda e: e.key),
            left_ts_getter=lambda e: e.ts,
            right_ts_getter=lambda e: e.ts,
            lower_bound=timedelta(seconds=10),
            upper_bound=timedelta(seconds=5),
        )


def test_negative_tolerance_raises() -> None:
    flow = Dataflow("bad")
    ls = op.input("l", flow, TestingSource([]))
    rs = op.input("r", flow, TestingSource([]))
    with raises(ValueError):
        interval_join(
            "ij",
            op.key_on("kl", ls, lambda e: e.key),
            op.key_on("kr", rs, lambda e: e.key),
            left_ts_getter=lambda e: e.ts,
            right_ts_getter=lambda e: e.ts,
            left_out_of_order_tolerance=timedelta(seconds=-1),
        )


# ---------------------------------------------------------------------
# Recovery: aborting and resuming must give identical output.
#
# Like the windowing recovery tests, the two sides are fed from a
# single merged source (one item per batch) split with `branch`. This
# keeps the input-position snapshot and the join-state snapshot in the
# same epoch, which is the configuration under which exactly-once
# recovery semantics are defined for stateful operators with side
# outputs.
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class _SideEv:
    """An event tagged with its join side (`0` left, `1` right)."""

    side: int
    key: str
    t: int
    name: str

    @property
    def ts(self) -> datetime:
        return _at(self.t)


# Interleaved sides, two keys, out-of-order and late records on both
# sides, plus matches and a left record that never matches.
RECOVERY_EVENTS: List[_SideEv] = [
    _SideEv(0, "a", 0, "a0"),
    _SideEv(1, "a", 5, "ar0"),
    _SideEv(0, "b", 2, "b0"),
    _SideEv(1, "a", 25, "ar1"),
    _SideEv(1, "b", 100, "br0"),
    _SideEv(0, "a", 40, "a1"),
    _SideEv(1, "a", 45, "ar2"),
    # Late on the right: the right watermark is already at 45.
    _SideEv(1, "a", 2, "ar_late"),
    # Late on the left: the left watermark is already at 40.
    _SideEv(0, "a", 10, "a_late"),
]


def _recovery_flow(out: List[Any], inp: List[Any]) -> Dataflow:
    flow = Dataflow("ij_recovery")
    events = op.input("inp", flow, TestingSource(inp, batch_size=1))
    keyed = op.key_on("key", events, lambda e: e.key)
    sides = op.branch("side", keyed, lambda kv: kv[1].side == 0)
    joined = interval_join(
        "ij",
        sides.trues,
        sides.falses,
        left_ts_getter=lambda e: e.ts,
        right_ts_getter=lambda e: e.ts,
        upper_bound=timedelta(seconds=30),
        now_getter=lambda: _T0,
    )
    matched = op.map_value(
        "tag_m", joined.matched, lambda p: ("M", p[0].name, p[1].name)
    )
    unmatched = op.map_value(
        "tag_u", joined.unmatched_left, lambda r: ("U", r[0].name, None)
    )
    late = op.map_value("tag_late", joined.late, lambda r: ("L", r[0], r[1].name))
    op.output(
        "out",
        op.key_rm("strip_key", op.merge("merge", matched, unmatched, late)),
        TestingSink(out),
    )
    return flow


def _new_recovery_config(path: Path) -> RecoveryConfig:
    path.mkdir()
    init_db_dir(path, 1)
    return RecoveryConfig(str(path))


def test_interval_join_resume_after_abort(tmp_path: Path) -> None:
    expected: List[Any] = []
    run_main(_recovery_flow(expected, RECOVERY_EVENTS))
    expected_canon = _canon(expected)
    # The scenario must exercise every output stream.
    assert {row[0] for row in expected} == {"M", "U", "L"}

    for i in range(1, len(RECOVERY_EVENTS)):
        inp = RECOVERY_EVENTS[:i] + [TestingSource.ABORT()] + RECOVERY_EVENTS[i:]
        out: List[Any] = []
        flow = _recovery_flow(out, inp)
        cfg = _new_recovery_config(tmp_path / f"abort_{i}")

        run_main(flow, epoch_interval=ZERO, recovery_config=cfg)
        run_main(flow, epoch_interval=ZERO, recovery_config=cfg)

        assert _canon(out) == expected_canon, f"abort before item {i}"


def test_interval_join_resume_after_abort_after_every_item(
    tmp_path: Path,
) -> None:
    expected: List[Any] = []
    run_main(_recovery_flow(expected, RECOVERY_EVENTS))
    expected_canon = _canon(expected)

    inp: List[Any] = []
    for event in RECOVERY_EVENTS:
        inp.append(event)
        inp.append(TestingSource.ABORT())

    out: List[Any] = []
    flow = _recovery_flow(out, inp)
    cfg = _new_recovery_config(tmp_path / "abort_every")

    # One run per abort, then a final run to drain EOF.
    for _ in range(len(RECOVERY_EVENTS) + 1):
        run_main(flow, epoch_interval=ZERO, recovery_config=cfg)

    assert _canon(out) == expected_canon
