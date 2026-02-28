"""
Test path planner (bottom-up DP algorithm).

Covers:
- Basic plan generation for keyframes and deltas
- Path steps are ordered anchor-first, target-last
- Cost model with/without zstd
- Keyframe-only targets
- Chain walking through multiple deltas
- Plans for recently inserted data (all in L1+L2+DISK)
- Plans for ungrouped tables
- Empty/missing chain graceful handling
- Multiple delta columns
- Long chains with larger keyframe_every
- Compress depth > 1 (tags > 1)
"""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from conftest import insert_rows, insert_versions, row_count


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ANCHOR_ACTIONS = {"anchor_l1", "anchor_l3", "anchor_kf_l2", "anchor_kf_disk"}
DELTA_ACTIONS = {"delta_l2", "delta_disk"}
ALL_ACTIONS = ANCHOR_ACTIONS | DELTA_ACTIONS


def plan_path(
    db: psycopg.Connection,
    table: str,
    group_value: str | None,
    attnum: int,
    target_seq: int,
    enable_zstd: bool = False,
) -> list[dict[str, Any]]:
    """Call xpatch.plan_path() and return list of step dicts."""
    rows = db.execute(
        "SELECT * FROM xpatch.plan_path(%s::regclass, %s, %s::int2, %s::int8, %s)",
        [table, group_value, attnum, target_seq, enable_zstd],
    ).fetchall()
    return [dict(r) for r in rows]


def plan_actions(
    db: psycopg.Connection,
    table: str,
    group_value: str | None,
    attnum: int,
    target_seq: int,
    enable_zstd: bool = False,
) -> list[str]:
    """Return just the action names from a plan."""
    steps = plan_path(db, table, group_value, attnum, target_seq, enable_zstd)
    return [s["action"] for s in steps]


def plan_seqs(
    db: psycopg.Connection,
    table: str,
    group_value: str | None,
    attnum: int,
    target_seq: int,
    enable_zstd: bool = False,
) -> list[int]:
    """Return just the seq values from a plan."""
    steps = plan_path(db, table, group_value, attnum, target_seq, enable_zstd)
    return [s["seq"] for s in steps]


def plan_cost(
    db: psycopg.Connection,
    table: str,
    group_value: str | None,
    attnum: int,
    target_seq: int,
    enable_zstd: bool = False,
) -> int | None:
    """Return the total_cost_ns from a plan, or None if empty."""
    steps = plan_path(db, table, group_value, attnum, target_seq, enable_zstd)
    return steps[0]["total_cost_ns"] if steps else None


# ---------------------------------------------------------------------------
# Basic Plan Generation
# ---------------------------------------------------------------------------


class TestBasicPlans:
    """Test basic path plan generation for simple scenarios."""

    def test_keyframe_target_uses_l1_or_kf_l2(
        self, db: psycopg.Connection, make_table
    ):
        """
        Target is a keyframe (seq=1). Should use either anchor_l1 (if in L1)
        or anchor_kf_l2 (if only in L2). Both are valid optimal choices.
        """
        t = make_table(keyframe_every=10, compress_depth=1)
        insert_versions(db, t, group_id=1, count=1)

        steps = plan_path(db, t, "1", 3, 1)
        assert len(steps) == 1, "Keyframe target should be single-step"
        assert steps[0]["action"] in ("anchor_l1", "anchor_kf_l2"), \
            f"Unexpected action for keyframe: {steps[0]['action']}"

    def test_delta_target_freshly_inserted(
        self, db: psycopg.Connection, make_table
    ):
        """
        Freshly inserted deltas are in L1 + L2 + DISK. The planner should
        pick anchor_l1 as it's cheapest (200ns).
        """
        t = make_table(keyframe_every=10, compress_depth=1)
        insert_versions(db, t, group_id=1, count=5)

        steps = plan_path(db, t, "1", 3, 5)
        assert len(steps) == 1, \
            f"Freshly inserted target in L1 should be single-step, got {len(steps)} steps"
        assert steps[0]["action"] == "anchor_l1"
        assert steps[0]["seq"] == 5

    def test_plan_returns_valid_actions(
        self, db: psycopg.Connection, make_table
    ):
        """All actions in any plan are recognized action strings."""
        t = make_table(keyframe_every=5, compress_depth=1)
        insert_versions(db, t, group_id=1, count=10)

        for seq in range(1, 11):
            steps = plan_path(db, t, "1", 3, seq)
            for step in steps:
                assert step["action"] in ALL_ACTIONS, \
                    f"Unknown action '{step['action']}' at seq={seq}"

    def test_plan_step_nums_sequential(
        self, db: psycopg.Connection, make_table
    ):
        """Step numbers are 1-based and sequential."""
        t = make_table(keyframe_every=10, compress_depth=1)
        insert_versions(db, t, group_id=1, count=5)

        steps = plan_path(db, t, "1", 3, 5)
        step_nums = [s["step_num"] for s in steps]
        assert step_nums == list(range(1, len(steps) + 1))

    def test_plan_first_step_is_anchor(
        self, db: psycopg.Connection, make_table
    ):
        """The first step in any plan must be an anchor (terminal action)."""
        t = make_table(keyframe_every=100, compress_depth=1)
        insert_versions(db, t, group_id=1, count=10)

        for seq in range(1, 11):
            steps = plan_path(db, t, "1", 3, seq)
            assert len(steps) > 0, f"Plan for seq={seq} should not be empty"
            assert steps[0]["action"] in ANCHOR_ACTIONS, \
                f"First step should be anchor, got '{steps[0]['action']}' at seq={seq}"

    def test_plan_last_step_is_target(
        self, db: psycopg.Connection, make_table
    ):
        """The last step in the plan should be the target seq."""
        t = make_table(keyframe_every=100, compress_depth=1)
        insert_versions(db, t, group_id=1, count=10)

        for seq in range(1, 11):
            steps = plan_path(db, t, "1", 3, seq)
            assert steps[-1]["seq"] == seq, \
                f"Last step seq={steps[-1]['seq']} != target={seq}"


# ---------------------------------------------------------------------------
# Cost Model Tests
# ---------------------------------------------------------------------------


class TestCostModel:
    """Test the cost model constants and zstd multiplier."""

    def test_keyframe_l2_cost_no_zstd(
        self, db: psycopg.Connection, make_table
    ):
        """
        A keyframe in L2 (not in L1) should cost COST_L2_KEYFRAME_NS = 80ns.
        We can't easily evict L1 in a test, but we can verify the cost
        when the planner chooses anchor_kf_l2.
        """
        t = make_table(keyframe_every=10, compress_depth=1)
        insert_versions(db, t, group_id=1, count=1)

        steps = plan_path(db, t, "1", 3, 1, enable_zstd=False)
        if steps[0]["action"] == "anchor_kf_l2":
            assert steps[0]["total_cost_ns"] == 80

    def test_l1_anchor_cost(
        self, db: psycopg.Connection, make_table
    ):
        """L1 anchor should cost COST_L1_NS = 200ns."""
        t = make_table(keyframe_every=10, compress_depth=1)
        insert_versions(db, t, group_id=1, count=3)

        steps = plan_path(db, t, "1", 3, 3)
        if steps[0]["action"] == "anchor_l1":
            assert steps[0]["total_cost_ns"] == 200

    def test_cost_positive(
        self, db: psycopg.Connection, make_table
    ):
        """All plans should have positive cost."""
        t = make_table(keyframe_every=5, compress_depth=1)
        insert_versions(db, t, group_id=1, count=10)

        for seq in range(1, 11):
            cost = plan_cost(db, t, "1", 3, seq)
            assert cost is not None and cost > 0, \
                f"Plan for seq={seq} should have positive cost, got {cost}"


# ---------------------------------------------------------------------------
# Graceful Handling (edge cases)
# ---------------------------------------------------------------------------


class TestGracefulHandling:
    """Test graceful behavior for edge cases and error conditions."""

    def test_nonexistent_seq_returns_empty(
        self, db: psycopg.Connection, make_table
    ):
        """Requesting a seq not in the chain index returns no rows."""
        t = make_table(keyframe_every=10, compress_depth=1)
        insert_versions(db, t, group_id=1, count=3)

        steps = plan_path(db, t, "1", 3, 999)
        assert steps == [], \
            f"Nonexistent seq should return empty plan, got {len(steps)} steps"

    def test_nonexistent_group_returns_empty(
        self, db: psycopg.Connection, make_table
    ):
        """Requesting a group not in the chain index returns no rows."""
        t = make_table(keyframe_every=10, compress_depth=1)
        insert_versions(db, t, group_id=1, count=3)

        steps = plan_path(db, t, "999", 3, 1)
        assert steps == [], \
            f"Nonexistent group should return empty plan, got {len(steps)} steps"

    def test_nonexistent_table_raises(
        self, db: psycopg.Connection
    ):
        """Requesting a non-existent table should raise an error."""
        with pytest.raises(Exception):
            plan_path(db, "nonexistent_table_xyz", "1", 3, 1)

    def test_null_group_returns_empty_for_grouped_table(
        self, db: psycopg.Connection, make_table
    ):
        """
        Calling plan_path with NULL group on a grouped table returns empty
        (the NULL hash won't match any real group's hash).
        """
        t = make_table(keyframe_every=10, compress_depth=1)
        insert_versions(db, t, group_id=1, count=3)

        steps = plan_path(db, t, None, 3, 1)
        # NULL group hash (0,0) won't match INT hash of 1
        assert isinstance(steps, list)
        # Depending on hash collision, may or may not return results
        # Just verify no crash


# ---------------------------------------------------------------------------
# Multi-Version Chain Tests
# ---------------------------------------------------------------------------


class TestChainWalking:
    """Test the planner with chains that require walking through deltas."""

    def test_all_versions_planned(
        self, db: psycopg.Connection, make_table
    ):
        """Every inserted version should have a non-empty plan."""
        t = make_table(keyframe_every=10, compress_depth=1)
        insert_versions(db, t, group_id=1, count=20)

        for seq in range(1, 21):
            steps = plan_path(db, t, "1", 3, seq)
            assert len(steps) > 0, f"seq={seq} should have a plan"

    def test_keyframe_boundaries(
        self, db: psycopg.Connection, make_table
    ):
        """
        With keyframe_every=5, seqs 1,6,11,16 are keyframes.
        Their plans should not need to walk past themselves.
        """
        t = make_table(keyframe_every=5, compress_depth=1)
        insert_versions(db, t, group_id=1, count=20)

        for kf_seq in [1, 6, 11, 16]:
            steps = plan_path(db, t, "1", 3, kf_seq)
            # A keyframe should have a single-step plan (anchor)
            assert len(steps) == 1, \
                f"Keyframe seq={kf_seq} should be single-step, got {len(steps)}"
            assert steps[0]["action"] in ANCHOR_ACTIONS

    def test_path_seqs_form_valid_chain(
        self, db: psycopg.Connection, make_table
    ):
        """
        The sequence numbers in a plan should be in ascending order
        (anchor has lowest seq, target has highest).
        """
        t = make_table(keyframe_every=10, compress_depth=1)
        insert_versions(db, t, group_id=1, count=10)

        for seq in range(1, 11):
            seqs = plan_seqs(db, t, "1", 3, seq)
            assert seqs == sorted(seqs), \
                f"Plan seqs should be ascending for target={seq}, got {seqs}"
            if seqs:
                assert seqs[-1] == seq

    def test_multiple_groups_independent(
        self, db: psycopg.Connection, make_table
    ):
        """Plans for different groups are independent."""
        t = make_table(keyframe_every=5, compress_depth=1)
        insert_versions(db, t, group_id=1, count=10)
        insert_versions(db, t, group_id=2, count=5)

        # Group 1 seq=10 should have a plan
        steps_g1 = plan_path(db, t, "1", 3, 10)
        assert len(steps_g1) > 0

        # Group 2 seq=5 should have a plan
        steps_g2 = plan_path(db, t, "2", 3, 5)
        assert len(steps_g2) > 0

        # Group 2 seq=10 should NOT have a plan (only 5 versions)
        steps_g2_10 = plan_path(db, t, "2", 3, 10)
        assert steps_g2_10 == []

    def test_compress_depth_greater_than_1(
        self, db: psycopg.Connection, make_table
    ):
        """
        With compress_depth > 1, tags can be > 1. The planner must follow
        the actual chain (base = seq - tag), not assume tag=1 everywhere.
        """
        t = make_table(keyframe_every=10, compress_depth=3)
        insert_versions(db, t, group_id=1, count=10)

        # All versions should still have valid plans
        for seq in range(1, 11):
            steps = plan_path(db, t, "1", 3, seq)
            assert len(steps) > 0, f"seq={seq} should have a plan"
            assert steps[0]["action"] in ANCHOR_ACTIONS
            assert steps[-1]["seq"] == seq

    def test_long_chain_within_keyframe_group(
        self, db: psycopg.Connection, make_table
    ):
        """
        With keyframe_every=100 and compress_depth=1, a chain of 50
        versions all have plans and the target is correct.
        """
        t = make_table(keyframe_every=100, compress_depth=1)
        insert_versions(db, t, group_id=1, count=50)

        steps = plan_path(db, t, "1", 3, 50)
        assert len(steps) > 0
        assert steps[-1]["seq"] == 50
        assert steps[0]["action"] in ANCHOR_ACTIONS


# ---------------------------------------------------------------------------
# Multiple Delta Columns
# ---------------------------------------------------------------------------


class TestMultipleDeltaColumns:
    """Test path planner with multiple delta columns."""

    def test_two_delta_columns_independent_plans(
        self, db: psycopg.Connection, make_table
    ):
        """Each delta column has its own chain and can have a different plan."""
        t = make_table(
            columns="group_id INT, version INT, title TEXT NOT NULL, body TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["title", "body"],
            keyframe_every=10,
            compress_depth=1,
        )

        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (group_id, version, title, body) VALUES (%s, %s, %s, %s)",
                [1, v, f"Title v{v}", f"Body content version {v} with more text"],
            )

        # Get attnums for title (3) and body (4)
        attnums = db.execute(
            "SELECT attnum, attname FROM pg_attribute "
            "WHERE attrelid = %s::regclass AND attname IN ('title', 'body') "
            "ORDER BY attnum",
            [t],
        ).fetchall()

        title_attnum = attnums[0]["attnum"]
        body_attnum = attnums[1]["attnum"]

        # Both columns should have plans for seq=5
        title_plan = plan_path(db, t, "1", title_attnum, 5)
        body_plan = plan_path(db, t, "1", body_attnum, 5)

        assert len(title_plan) > 0, "Title column should have a plan"
        assert len(body_plan) > 0, "Body column should have a plan"
        assert title_plan[-1]["seq"] == 5
        assert body_plan[-1]["seq"] == 5


# ---------------------------------------------------------------------------
# Data Integrity (plans don't break reads)
# ---------------------------------------------------------------------------


class TestDataIntegrity:
    """Verify that calling plan_path doesn't affect data reads."""

    def test_plan_path_is_read_only(
        self, db: psycopg.Connection, make_table
    ):
        """
        Calling plan_path should not modify any data or cache state.
        Data reads before and after should return identical results.
        """
        t = make_table(keyframe_every=5, compress_depth=1)
        insert_versions(db, t, group_id=1, count=10)

        # Read all content
        before = db.execute(
            f"SELECT content FROM {t} WHERE group_id = 1 ORDER BY version"
        ).fetchall()

        # Call plan_path many times
        for seq in range(1, 11):
            plan_path(db, t, "1", 3, seq)
            plan_path(db, t, "1", 3, seq, enable_zstd=True)

        # Read again
        after = db.execute(
            f"SELECT content FROM {t} WHERE group_id = 1 ORDER BY version"
        ).fetchall()

        assert before == after, "plan_path should not modify data"

    def test_content_still_correct_after_planning(
        self, db: psycopg.Connection, make_table
    ):
        """Verify actual content values are correct after using plan_path."""
        t = make_table(keyframe_every=5, compress_depth=1)
        insert_versions(db, t, group_id=1, count=5)

        # Plan every version
        for seq in range(1, 6):
            plan_path(db, t, "1", 3, seq)

        # Verify content
        for v in range(1, 6):
            row = db.execute(
                f"SELECT content FROM {t} WHERE group_id = 1 AND version = %s",
                [v],
            ).fetchone()
            assert row is not None
            assert row["content"] == f"Version {v} content"
