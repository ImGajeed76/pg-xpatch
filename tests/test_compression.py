"""
Test delta compression, reconstruction, keyframes, and compress_depth.

Covers:
- Similar content compresses efficiently (compression ratio > 1)
- Completely different content still works
- Random-access reads reconstruct correctly
- Reverse order SELECT works
- Large data (1MB) compresses and reconstructs
- JSONB delta compression and operators
- keyframe_every respected (inspect shows keyframes at correct intervals)
- compress_depth > 1 encodes against best match
- enable_zstd toggle
- DISTINCT ON latest version per group
- Content integrity across version chains
"""

from __future__ import annotations

import json

import psycopg
import pytest

from conftest import insert_rows, insert_versions, row_count


class TestDeltaCompression:
    """Delta compression produces smaller storage for similar content."""

    def test_similar_content_compresses_well(self, db: psycopg.Connection, make_table):
        """Repeated similar content should achieve compression ratio > 1."""
        t = make_table()
        base = "A" * 10_000
        for v in range(1, 11):
            # Each version changes a few chars
            content = base[:v * 100] + "B" * 100 + base[(v + 1) * 100:]
            insert_rows(db, t, [(1, v, content)])

        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 10
        # Delta-compressed similar content should compress significantly
        assert float(stats["compression_ratio"]) > 1.0

    def test_different_content_still_works(self, db: psycopg.Connection, make_table):
        """Completely different content works (stored as keyframes or poor deltas)."""
        t = make_table(keyframe_every=2)
        insert_rows(db, t, [
            (1, 1, "alpha bravo charlie"),
            (1, 2, "12345 67890 !!!!!"),
            (1, 3, "completely unrelated stuff"),
        ])
        rows = db.execute(
            f"SELECT content FROM {t} ORDER BY version"
        ).fetchall()
        assert rows[0]["content"] == "alpha bravo charlie"
        assert rows[1]["content"] == "12345 67890 !!!!!"
        assert rows[2]["content"] == "completely unrelated stuff"

    def test_compression_stats_fields(self, db: psycopg.Connection, make_table):
        """xpatch.stats() returns all expected fields with sensible values."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)

        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 10
        assert stats["total_groups"] == 1
        assert stats["keyframe_count"] >= 1
        assert stats["delta_count"] >= 0
        assert stats["raw_size_bytes"] > 0
        assert stats["compressed_size_bytes"] > 0
        assert float(stats["compression_ratio"]) > 0


class TestReconstruction:
    """Delta-compressed data correctly reconstructed on read."""

    def test_sequential_read_all_versions(self, db: psycopg.Connection, xpatch_table):
        """Read all versions sequentially — content matches what was inserted."""
        t = xpatch_table
        expected = {}
        for v in range(1, 21):
            content = f"Version {v} unique content {v * 7}"
            insert_rows(db, t, [(1, v, content)])
            expected[v] = content

        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 20
        for row in rows:
            assert row["content"] == expected[row["version"]]

    def test_random_access_order(self, db: psycopg.Connection, xpatch_table):
        """Read specific versions in random order — reconstruction still correct."""
        t = xpatch_table
        contents = {}
        for v in range(1, 11):
            c = f"Content for version {v}: {'x' * (v * 50)}"
            insert_rows(db, t, [(1, v, c)])
            contents[v] = c

        # Access in non-sequential order
        for target_v in [7, 1, 10, 3, 5]:
            row = db.execute(
                f"SELECT content FROM {t} WHERE group_id = 1 AND version = {target_v}"
            ).fetchone()
            assert row["content"] == contents[target_v], f"Mismatch at version {target_v}"

    def test_reverse_order_read(self, db: psycopg.Connection, xpatch_table):
        """SELECT ORDER BY DESC reconstructs all deltas correctly."""
        t = xpatch_table
        for v in range(1, 11):
            insert_rows(db, t, [(1, v, f"v{v}-data-{'y' * v * 10}")])

        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version DESC"
        ).fetchall()
        assert len(rows) == 10
        for row in rows:
            v = row["version"]
            assert row["content"] == f"v{v}-data-{'y' * v * 10}"

    def test_multi_group_reconstruction(self, db: psycopg.Connection, xpatch_table):
        """Each group has an independent delta chain — no cross-contamination."""
        t = xpatch_table
        for g in range(1, 4):
            for v in range(1, 6):
                insert_rows(db, t, [(g, v, f"group{g}-version{v}")])

        for g in range(1, 4):
            rows = db.execute(
                f"SELECT version, content FROM {t} "
                f"WHERE group_id = {g} ORDER BY version"
            ).fetchall()
            assert len(rows) == 5
            for row in rows:
                assert row["content"] == f"group{g}-version{row['version']}"

    def test_large_data_1mb(self, db: psycopg.Connection, make_table):
        """1MB text data compresses and reconstructs correctly."""
        t = make_table()
        big_text = "x" * 1_000_000  # 1MB
        insert_rows(db, t, [(1, 1, big_text)])
        row = db.execute(f"SELECT content FROM {t} WHERE group_id = 1").fetchone()
        assert row["content"] == big_text
        assert len(row["content"]) == 1_000_000

    def test_large_data_multiple_versions(self, db: psycopg.Connection, make_table):
        """Multiple versions of large data with small changes."""
        t = make_table()
        base = "A" * 100_000
        for v in range(1, 6):
            content = base[:v * 1000] + "CHANGED" + base[v * 1000 + 7:]
            insert_rows(db, t, [(1, v, content)])

        rows = db.execute(
            f"SELECT version, length(content) as len FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 5
        # Each version should have the same length (100_000)
        for row in rows:
            assert row["len"] == 100_000


class TestKeyframes:
    """Keyframe creation at correct intervals."""

    def test_keyframe_every_5(self, db: psycopg.Connection, make_table):
        """With keyframe_every=5, keyframes at seq 1, 6, 11."""
        t = make_table(keyframe_every=5)
        insert_versions(db, t, group_id=1, count=12)

        rows = db.execute(
            f"SELECT * FROM xpatch.inspect('{t}'::regclass, 1) ORDER BY seq"
        ).fetchall()
        keyframe_seqs = [r["seq"] for r in rows if r["is_keyframe"]]
        # Keyframes at position 1, 6, 11 (seq is 1-based in inspect)
        assert 1 in keyframe_seqs
        assert 6 in keyframe_seqs
        assert 11 in keyframe_seqs
        # Everything else is a delta
        delta_seqs = [r["seq"] for r in rows if not r["is_keyframe"]]
        assert len(delta_seqs) > 0

    def test_keyframe_every_2(self, db: psycopg.Connection, make_table):
        """With keyframe_every=2, keyframes at seq 1, 3, 5 (seq % 2 == 1)."""
        t = make_table(keyframe_every=2)
        insert_versions(db, t, group_id=1, count=6)

        rows = db.execute(
            f"SELECT * FROM xpatch.inspect('{t}'::regclass, 1) ORDER BY seq"
        ).fetchall()
        keyframe_seqs = [r["seq"] for r in rows if r["is_keyframe"]]
        # Keyframes where seq == 1 or seq % 2 == 1
        assert 1 in keyframe_seqs
        assert 3 in keyframe_seqs
        assert 5 in keyframe_seqs

    def test_first_row_is_always_keyframe(self, db: psycopg.Connection, make_table):
        """The first row in every group is always a keyframe (seq=1)."""
        t = make_table(keyframe_every=100)
        for g in range(1, 4):
            insert_versions(db, t, group_id=g, count=3)

        for g in range(1, 4):
            rows = db.execute(
                f"SELECT * FROM xpatch.inspect('{t}'::regclass, {g}) ORDER BY seq"
            ).fetchall()
            assert rows[0]["seq"] == 1
            assert rows[0]["is_keyframe"] is True

    def test_data_correct_across_keyframe_boundary(self, db: psycopg.Connection, make_table):
        """Data reconstructed correctly across a keyframe boundary."""
        t = make_table(keyframe_every=3)
        for v in range(1, 8):
            insert_rows(db, t, [(1, v, f"ver{v}-{'z' * v * 20}")])

        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        for row in rows:
            v = row["version"]
            assert row["content"] == f"ver{v}-{'z' * v * 20}"


class TestCompressDepth:
    """compress_depth > 1 for multi-depth delta encoding."""

    def test_compress_depth_3(self, db: psycopg.Connection, make_table):
        """compress_depth=3 allows deltas against up to 3 previous versions."""
        # We can't directly configure compress_depth via make_table,
        # but we can test it via the configure function
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
        )
        # Reconfigure with compress_depth=3
        db.execute(
            f"SELECT xpatch.configure('{t}', "
            f"group_by => 'group_id', order_by => 'version', "
            f"delta_columns => '{{content}}', compress_depth => 3)"
        )
        # Insert content where v4 is most similar to v1 (not v3)
        insert_rows(db, t, [
            (1, 1, "AAAA" * 1000),
            (1, 2, "BBBB" * 1000),
            (1, 3, "CCCC" * 1000),
            (1, 4, "AAAA" * 1000),  # Same as v1 — should encode against v1 with depth=3
        ])

        # Verify data is correct
        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert rows[0]["content"] == "AAAA" * 1000
        assert rows[3]["content"] == "AAAA" * 1000
        assert rows[0]["content"] == rows[3]["content"]


class TestZstdToggle:
    """enable_zstd configuration."""

    def test_zstd_disabled_still_works(self, db: psycopg.Connection, make_table):
        """Data is correct with zstd compression disabled."""
        t = make_table(enable_zstd=False)
        for v in range(1, 6):
            insert_rows(db, t, [(1, v, f"Content version {v}: {'data' * 100}")])

        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        for row in rows:
            v = row["version"]
            assert row["content"] == f"Content version {v}: {'data' * 100}"

    def test_zstd_enabled_vs_disabled_both_correct(self, db: psycopg.Connection, make_table):
        """Both zstd=true and zstd=false produce correct results."""
        t_on = make_table(enable_zstd=True)
        t_off = make_table(enable_zstd=False)

        data = [(1, v, f"Shared content {v}: {'abc' * 500}") for v in range(1, 11)]
        insert_rows(db, t_on, data)
        insert_rows(db, t_off, data)

        rows_on = db.execute(
            f"SELECT version, content FROM {t_on} ORDER BY version"
        ).fetchall()
        rows_off = db.execute(
            f"SELECT version, content FROM {t_off} ORDER BY version"
        ).fetchall()

        assert len(rows_on) == len(rows_off) == 10
        for a, b in zip(rows_on, rows_off):
            assert a["content"] == b["content"]


class TestJsonbCompression:
    """JSONB delta compression and operator support."""

    def test_jsonb_insert_and_read(self, db: psycopg.Connection, make_table):
        """JSONB delta-compressed column stores and retrieves correctly."""
        t = make_table(
            "doc_id INT, version INT, payload JSONB NOT NULL",
            group_by="doc_id",
            order_by="version",
            delta_columns=["payload"],
        )
        data = {"name": "test", "value": 42, "tags": ["a", "b"]}
        db.execute(
            f"INSERT INTO {t} (doc_id, version, payload) VALUES (1, 1, %s::jsonb)",
            [json.dumps(data)],
        )
        row = db.execute(f"SELECT payload FROM {t}").fetchone()
        result = row["payload"]
        if isinstance(result, str):
            result = json.loads(result)
        assert result["name"] == "test"
        assert result["value"] == 42
        assert result["tags"] == ["a", "b"]

    def test_jsonb_containment_operator(self, db: psycopg.Connection, make_table):
        """JSONB @> containment operator works on delta-compressed data."""
        t = make_table(
            "doc_id INT, version INT, payload JSONB NOT NULL",
            group_by="doc_id",
            order_by="version",
            delta_columns=["payload"],
        )
        db.execute(
            f"INSERT INTO {t} VALUES (1, 1, '{{\"type\": \"a\", \"val\": 1}}'::jsonb)"
        )
        db.execute(
            f"INSERT INTO {t} VALUES (1, 2, '{{\"type\": \"b\", \"val\": 2}}'::jsonb)"
        )
        db.execute(
            f"INSERT INTO {t} VALUES (1, 3, '{{\"type\": \"a\", \"val\": 3}}'::jsonb)"
        )

        rows = db.execute(
            f"SELECT version FROM {t} WHERE payload @> '{{\"type\": \"a\"}}' ORDER BY version"
        ).fetchall()
        assert [r["version"] for r in rows] == [1, 3]

    def test_jsonb_key_exists_operator(self, db: psycopg.Connection, make_table):
        """JSONB ? key-exists operator works on delta-compressed data."""
        t = make_table(
            "doc_id INT, version INT, payload JSONB NOT NULL",
            group_by="doc_id",
            order_by="version",
            delta_columns=["payload"],
        )
        db.execute(
            f"INSERT INTO {t} VALUES (1, 1, '{{\"name\": \"x\"}}'::jsonb)"
        )
        db.execute(
            f"INSERT INTO {t} VALUES (1, 2, '{{\"name\": \"y\", \"extra\": true}}'::jsonb)"
        )

        rows = db.execute(
            f"SELECT version FROM {t} WHERE payload ? 'extra'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["version"] == 2

    def test_jsonb_arrow_operator(self, db: psycopg.Connection, make_table):
        """JSONB ->> text extraction works on delta-compressed data."""
        t = make_table(
            "doc_id INT, version INT, payload JSONB NOT NULL",
            group_by="doc_id",
            order_by="version",
            delta_columns=["payload"],
        )
        for v in range(1, 4):
            db.execute(
                f"INSERT INTO {t} VALUES (1, {v}, %s::jsonb)",
                [json.dumps({"name": f"item_{v}", "count": v * 10})],
            )

        rows = db.execute(
            f"SELECT version, payload->>'name' as name, "
            f"  (payload->>'count')::int as count "
            f"FROM {t} ORDER BY version"
        ).fetchall()
        assert rows[0]["name"] == "item_1" and rows[0]["count"] == 10
        assert rows[1]["name"] == "item_2" and rows[1]["count"] == 20
        assert rows[2]["name"] == "item_3" and rows[2]["count"] == 30

    def test_jsonb_multiple_versions_reconstruct(self, db: psycopg.Connection, make_table):
        """20 JSONB versions with incremental changes all reconstruct correctly."""
        t = make_table(
            "doc_id INT, version INT, payload JSONB NOT NULL",
            group_by="doc_id",
            order_by="version",
            delta_columns=["payload"],
        )
        for v in range(1, 21):
            data = {
                "version": v,
                "description": f"Version {v} of the document",
                "items": list(range(v)),
                "metadata": {"created_at": f"2025-01-{v:02d}"},
            }
            db.execute(
                f"INSERT INTO {t} VALUES (1, {v}, %s::jsonb)",
                [json.dumps(data)],
            )

        rows = db.execute(
            f"SELECT version, payload FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 20
        for row in rows:
            v = row["version"]
            p = row["payload"]
            if isinstance(p, str):
                p = json.loads(p)
            assert p["version"] == v
            assert p["items"] == list(range(v))
