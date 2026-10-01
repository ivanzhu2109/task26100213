"""Tests for the cascading window aggregation engine."""

import random

import pytest

from cascade import Engine, Event, MaterializedRow, QueryDef

LOW = -(10**18)  # threshold that any non-empty group passes


def ev(ts, etype, src, value):
    return Event(timestamp=ts, event_type=etype, source_id=src, value=value)


def make_engine(defs):
    engine = Engine()
    engine.load_queries(defs)
    return engine


# ---------------------------------------------------------------------------
# Basic aggregation, windowing, threshold visibility
# ---------------------------------------------------------------------------

def test_raw_minute_metrics_and_threshold():
    engine = make_engine([
        QueryDef("m1", None, 60, ("source_id",), threshold=10),
    ])
    engine.ingest([
        ev(5, "click", "s1", 8),
        ev(10, "click", "s1", 8),
        ev(20, "click", "s2", 3),   # below threshold, hidden
    ])
    assert engine.read("m1") == [
        MaterializedRow(0, ("s1",), 2, 16, 8, 8),
    ]
    # Hidden group keeps accumulating and can reappear.
    engine.ingest([ev(30, "click", "s2", 9)])
    assert engine.read("m1") == [
        MaterializedRow(0, ("s1",), 2, 16, 8, 8),
        MaterializedRow(0, ("s2",), 2, 12, 3, 9),
    ]


def test_window_alignment_left_closed_right_open():
    engine = make_engine([
        QueryDef("m1", None, 60, (), threshold=LOW),
    ])
    engine.ingest([ev(0, "t", "s", 1), ev(59, "t", "s", 2), ev(60, "t", "s", 4)])
    assert engine.read("m1") == [
        MaterializedRow(0, (), 2, 3, 1, 2),
        MaterializedRow(60, (), 1, 4, 4, 4),
    ]


def test_no_grouping_and_empty_group_key():
    engine = make_engine([
        QueryDef("m1", None, 60, (), threshold=LOW),
    ])
    engine.ingest([ev(1, "a", "x", 7), ev(2, "b", "y", -3)])
    assert engine.read("m1") == [MaterializedRow(0, (), 2, 4, -3, 7)]


# ---------------------------------------------------------------------------
# Cascading: late event pushes upstream group out of the filter and back in
# ---------------------------------------------------------------------------

def test_threshold_exit_and_reappear_restores_downstream():
    engine = make_engine([
        QueryDef("m1", None, 60, ("source_id",), threshold=10),
        QueryDef("m5", "m1", 300, ("source_id",), threshold=LOW, value_field="sum"),
    ])
    engine.ingest([ev(5, "c", "s1", 8), ev(10, "c", "s1", 8)])
    assert engine.read("m1") == [MaterializedRow(0, ("s1",), 2, 16, 8, 8)]
    assert engine.read("m5") == [MaterializedRow(0, ("s1",), 1, 16, 16, 16)]

    # Late negative event: upstream sum drops below threshold, row disappears.
    engine.ingest([ev(20, "c", "s1", -10)])
    assert engine.read("m1") == []
    assert engine.read("m5") == []

    # Another late event brings the group back; downstream count/sum restore.
    engine.ingest([ev(30, "c", "s1", 7)])
    assert engine.read("m1") == [MaterializedRow(0, ("s1",), 4, 13, -10, 8)]
    assert engine.read("m5") == [MaterializedRow(0, ("s1",), 1, 13, 13, 13)]


def test_correction_replaces_instead_of_accumulating():
    engine = make_engine([
        QueryDef("m1", None, 60, ("event_type",), threshold=0),
        QueryDef("m5", "m1", 300, ("event_type",), threshold=LOW, value_field="count"),
    ])
    engine.ingest([ev(1, "a", "s", 5)])
    assert engine.read("m5") == [MaterializedRow(0, ("a",), 1, 1, 1, 1)]
    # Upstream count changes 1 -> 2; downstream must see 2, not 1 + 2.
    engine.ingest([ev(2, "a", "s", 6)])
    assert engine.read("m1") == [MaterializedRow(0, ("a",), 2, 11, 5, 6)]
    assert engine.read("m5") == [MaterializedRow(0, ("a",), 1, 2, 2, 2)]


# ---------------------------------------------------------------------------
# Multi-layer corrections: min replaced, duplicate max, last input removed
# ---------------------------------------------------------------------------

def build_three_layer():
    return make_engine([
        QueryDef("m1", None, 60, ("event_type",), threshold=5),
        QueryDef("m5", "m1", 300, (), threshold=LOW, value_field="min"),
        QueryDef("m15", "m5", 900, (), threshold=LOW, value_field="max"),
    ])


def test_unique_min_corrected_through_layers():
    engine = build_three_layer()
    engine.ingest([ev(1, "a", "s", 3), ev(2, "a", "s", 4)])
    assert engine.read("m5")[0].min == 3
    assert engine.read("m15")[0].max == 3
    # Late event replaces the unique minimum upstream.
    engine.ingest([ev(3, "a", "s", -1)])
    m5 = engine.read("m5")
    assert (m5[0].count, m5[0].sum, m5[0].min, m5[0].max) == (1, -1, -1, -1)
    m15 = engine.read("m15")
    assert (m15[0].count, m15[0].sum, m15[0].min, m15[0].max) == (1, -1, -1, -1)


def test_duplicate_max_survives_single_removal():
    engine = build_three_layer()
    engine.ingest([
        ev(1, "b", "s", 10),
        ev(2, "c", "s", 10),
    ])
    m5 = engine.read("m5")
    assert (m5[0].count, m5[0].max) == (2, 10)
    # Push group "b" below the threshold: one of the two max contributions
    # disappears, the max itself must remain 10.
    engine.ingest([ev(3, "b", "s", -6)])
    assert engine.read("m1") == [MaterializedRow(0, ("c",), 1, 10, 10, 10)]
    m5 = engine.read("m5")
    assert (m5[0].count, m5[0].sum, m5[0].min, m5[0].max) == (1, 10, 10, 10)


def test_last_input_disappears_leaves_no_residue():
    engine = build_three_layer()
    engine.ingest([ev(1, "a", "s", 6)])
    assert len(engine.read("m1")) == 1
    assert len(engine.read("m5")) == 1
    assert len(engine.read("m15")) == 1
    # The only upstream group drops below the threshold.
    engine.ingest([ev(2, "a", "s", -2)])
    assert engine.read("m1") == []
    assert engine.read("m5") == []
    assert engine.read("m15") == []


# ---------------------------------------------------------------------------
# Upstream rows bucketed by their window start; group subset projection
# ---------------------------------------------------------------------------

def test_upstream_rows_bucketed_by_window_start():
    engine = make_engine([
        QueryDef("m1", None, 60, (), threshold=LOW),
        QueryDef("m5", "m1", 300, (), threshold=LOW, value_field="sum"),
    ])
    # Minute windows 60 and 240 both fall into the 300-second window 0.
    engine.ingest([ev(119, "t", "s", 1), ev(200, "t", "s", 2), ev(300, "t", "s", 4)])
    assert engine.read("m5") == [
        MaterializedRow(0, (), 2, 3, 1, 2),
        MaterializedRow(300, (), 1, 4, 4, 4),
    ]


def test_group_subset_projection_merges_upstream_rows():
    engine = make_engine([
        QueryDef("m1", None, 60, ("event_type", "source_id"), threshold=LOW),
        QueryDef("m5", "m1", 60, ("source_id",), threshold=LOW, value_field="count"),
    ])
    engine.ingest([
        ev(1, "a", "s1", 5),
        ev(2, "b", "s1", 6),
        ev(3, "a", "s2", 7),
    ])
    # Two upstream rows for s1 (types a and b) merge into one downstream cell.
    assert engine.read("m5") == [
        MaterializedRow(0, ("s1",), 2, 2, 1, 1),
        MaterializedRow(0, ("s2",), 1, 1, 1, 1),
    ]


def test_shared_upstream_both_children_updated():
    engine = make_engine([
        QueryDef("m1", None, 60, ("event_type",), threshold=0),
        QueryDef("by_count", "m1", 300, ("event_type",), threshold=LOW, value_field="count"),
        QueryDef("by_sum", "m1", 300, ("event_type",), threshold=LOW, value_field="sum"),
    ])
    engine.ingest([ev(1, "a", "s", 5), ev(2, "a", "s", -2)])
    assert engine.read("by_count") == [MaterializedRow(0, ("a",), 1, 2, 2, 2)]
    assert engine.read("by_sum") == [MaterializedRow(0, ("a",), 1, 3, 3, 3)]
    # Upstream row drops out; both children must lose the contribution.
    engine.ingest([ev(3, "a", "s", -4)])
    assert engine.read("m1") == []
    assert engine.read("by_count") == []
    assert engine.read("by_sum") == []


# ---------------------------------------------------------------------------
# Definition validation
# ---------------------------------------------------------------------------

def test_rejects_invalid_definitions():
    with pytest.raises(ValueError):
        make_engine([QueryDef("q", None, 120, (), 0)])
    with pytest.raises(ValueError):
        make_engine([QueryDef("q", None, 60, ("nope",), 0)])
    with pytest.raises(ValueError):
        make_engine([QueryDef("q", "missing", 60, (), 0, "sum")])
    with pytest.raises(ValueError):
        make_engine([
            QueryDef("a", None, 300, (), 0),
            QueryDef("b", "a", 60, (), 0, "sum"),  # shorter than upstream
        ])


def test_rejects_group_superset():
    with pytest.raises(ValueError):
        make_engine([
            QueryDef("a", None, 60, ("event_type",), 0),
            QueryDef("b", "a", 60, ("event_type", "source_id"), 0, "sum"),
        ])


def test_rejects_cycle_and_duplicates_and_reload():
    with pytest.raises(ValueError):
        make_engine([
            QueryDef("a", "b", 60, (), 0, "sum"),
            QueryDef("b", "a", 60, (), 0, "sum"),
        ])
    with pytest.raises(ValueError):
        make_engine([
            QueryDef("a", None, 60, (), 0),
            QueryDef("a", None, 60, (), 0),
        ])
    engine = make_engine([QueryDef("a", None, 60, (), 0)])
    with pytest.raises(RuntimeError):
        engine.load_queries([QueryDef("b", None, 60, (), 0)])


def test_definition_order_is_irrelevant():
    engine = make_engine([
        QueryDef("m5", "m1", 300, (), LOW, "sum"),
        QueryDef("m1", None, 60, (), LOW),
    ])
    engine.ingest([ev(1, "t", "s", 5)])
    assert engine.read("m5") == [MaterializedRow(0, (), 1, 5, 5, 5)]


def test_unknown_query_read_raises():
    engine = make_engine([QueryDef("a", None, 60, (), 0)])
    with pytest.raises(KeyError):
        engine.read("nope")


# ---------------------------------------------------------------------------
# Randomized consistency against a from-scratch reference implementation
# ---------------------------------------------------------------------------

def reference_visible(defs, events):
    """Recompute every query's visible rows from all events, from scratch."""
    by_name = {d.name: d for d in defs}
    visible = {}

    def compute(name):
        if name in visible:
            return visible[name]
        d = by_name[name]
        inputs = []
        if d.source is None:
            for e in events:
                inputs.append((e.timestamp, {"event_type": e.event_type,
                                             "source_id": e.source_id}, e.value))
        else:
            for row in compute(d.source).values():
                fields = dict(zip(by_name[d.source].group_by, row.group))
                inputs.append((row.window_start, fields,
                               getattr(row, d.value_field)))
        cells = {}
        for ts, fields, value in inputs:
            ws = (ts // d.window_seconds) * d.window_seconds
            key = (ws, tuple(fields[f] for f in d.group_by))
            cells.setdefault(key, []).append(value)
        rows = {}
        for (ws, group), values in cells.items():
            row = MaterializedRow(ws, group, len(values), sum(values),
                                  min(values), max(values))
            if row.sum >= d.threshold:
                rows[(ws, group)] = row
        visible[name] = rows
        return rows

    for d in defs:
        compute(d.name)
    return {name: sorted(rows.values(), key=lambda r: (r.window_start, r.group))
            for name, rows in visible.items()}


def test_randomized_matches_from_scratch_recompute():
    rng = random.Random(20261001)
    defs = [
        QueryDef("m1", None, 60, ("event_type", "source_id"), threshold=rng.randint(-5, 15)),
        QueryDef("m5", "m1", 300, ("source_id",), threshold=rng.randint(-20, 40),
                 value_field="sum"),
        QueryDef("m15", "m5", 900, (), threshold=rng.randint(-50, 100),
                 value_field="max"),
        QueryDef("m1b", None, 60, ("event_type",), threshold=rng.randint(-5, 15)),
        QueryDef("m5b", "m1b", 300, ("event_type",), threshold=rng.randint(-20, 40),
                 value_field="count"),
    ]
    engine = make_engine(defs)
    all_events = []
    types = ["a", "b", "c"]
    sources = ["s1", "s2"]
    for _ in range(40):
        batch = [
            ev(rng.randint(0, 3000), rng.choice(types), rng.choice(sources),
               rng.randint(-15, 20))
            for _ in range(rng.randint(0, 12))
        ]
        engine.ingest(batch)
        all_events.extend(batch)
        expected = reference_visible(defs, all_events)
        for d in defs:
            assert engine.read(d.name) == expected[d.name], (
                f"query {d.name} diverged after {len(all_events)} events"
            )
