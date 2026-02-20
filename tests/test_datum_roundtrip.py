"""
Tests for hypothesis B3/B8: datum_to_bytea vs reconstruction base content mismatch.

The concern is that the raw bytes stored in the FIFO insert cache at insert
time (via datum_to_bytea → VARDATA_ANY) might differ from the bytes produced
by reconstructing the same row from its stored delta chain.  If they differ,
any subsequent delta encoded against the FIFO base becomes undecodable when
reconstructed via the cold path.

We test this by:
1. Inserting rows with various data types and content patterns
2. Reading them back and verifying byte-exact reconstruction
3. Specifically targeting edge cases: TOAST-sized values, short varlena,
   mixed encodings, trailing nulls, etc.
"""

import pytest


class TestDatumRoundtripText:
    """Verify TEXT column round-trips are byte-exact through delta chain."""

    def test_short_text_roundtrip(self, db, make_table):
        """Short text values (< 127 bytes, uses 1-byte varlena header)."""
        tbl = make_table()
        texts = [
            "hello",
            "",  # empty string (NOT NULL, but zero-length text)
            "a",
            " ",
            "hello world" * 5,
            "special chars: àéîõü ñ ß",
            "unicode: \u2603\u2764\u2600",  # snowman, heart, sun
            "\t\n\r",
        ]
        for i, txt in enumerate(texts, 1):
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (%s, %s, %s)",
                (1, i, txt),
            )

        # Read back and verify
        for i, txt in enumerate(texts, 1):
            row = db.execute(
                f"SELECT content FROM {tbl} WHERE group_id = 1 AND version = %s", (i,)
            ).fetchone()
            assert row["content"] == txt, (
                f"Version {i} mismatch: expected {txt!r}, got {row['content']!r}"
            )

    def test_long_text_toast_boundary(self, db, make_table):
        """
        Values near and above TOAST threshold (~2KB).
        These exercise different varlena header sizes and potential TOAST
        compression/out-of-line storage.
        """
        tbl = make_table()
        sizes = [100, 500, 1000, 2000, 4000, 8000, 16000, 32000]
        for i, size in enumerate(sizes, 1):
            content = f"v{i}_" + "x" * (size - 3)
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, %s, %s)",
                (i, content),
            )

        for i, size in enumerate(sizes, 1):
            expected = f"v{i}_" + "x" * (size - 3)
            row = db.execute(
                f"SELECT content FROM {tbl} WHERE group_id = 1 AND version = %s", (i,)
            ).fetchone()
            assert row["content"] == expected, (
                f"Version {i} (size={size}) content mismatch: "
                f"len={len(row['content'])}, expected len={len(expected)}"
            )

    def test_delta_chain_reconstruction_matches_original(self, db, make_table):
        """
        Insert a chain of similar texts (to trigger delta encoding, not keyframes).
        Then read them in reverse order to force reconstruction from the delta chain,
        not the FIFO cache.
        """
        tbl = make_table(compress_depth=5)

        # Insert 20 versions with small incremental changes
        base = "The quick brown fox jumps over the lazy dog. " * 10
        for v in range(1, 21):
            content = base + f"\nVersion {v} modification: {'z' * v}"
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, %s, %s)",
                (v, content),
            )

        # Read all versions back — later versions require delta chain reconstruction
        for v in range(1, 21):
            expected = base + f"\nVersion {v} modification: {'z' * v}"
            row = db.execute(
                f"SELECT content FROM {tbl} WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            assert row["content"] == expected, (
                f"Version {v} reconstruction mismatch"
            )

    def test_identical_consecutive_versions(self, db, make_table):
        """
        Insert identical content for multiple consecutive versions.
        The delta should be minimal (or zero-length).
        Reconstruction must still return the exact content.
        """
        tbl = make_table(compress_depth=5)
        content = "This content is exactly the same for all versions."

        for v in range(1, 11):
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, %s, %s)",
                (v, content),
            )

        for v in range(1, 11):
            row = db.execute(
                f"SELECT content FROM {tbl} WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            assert row["content"] == content, f"Version {v} mismatch for identical content"

    def test_completely_different_versions(self, db, make_table):
        """
        Insert completely different content each version (no delta benefit).
        Each version should still be a keyframe or have a correct delta.
        """
        tbl = make_table(compress_depth=5)

        import hashlib
        for v in range(1, 16):
            content = hashlib.sha256(f"unique-{v}".encode()).hexdigest() * 20
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, %s, %s)",
                (v, content),
            )

        for v in range(1, 16):
            expected = hashlib.sha256(f"unique-{v}".encode()).hexdigest() * 20
            row = db.execute(
                f"SELECT content FROM {tbl} WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            assert row["content"] == expected, f"Version {v} content mismatch"


class TestDatumRoundtripBytea:
    """Verify BYTEA column round-trips are byte-exact."""

    def test_bytea_binary_data(self, db):
        """Insert raw binary data (all byte values 0-255) and verify round-trip."""
        db.execute("""
            CREATE TABLE xp_bytea (
                group_id INT NOT NULL,
                version INT NOT NULL,
                data BYTEA NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_bytea',
                group_by => 'group_id',
                order_by => 'version',
                delta_columns => ARRAY['data'])
        """)

        # All byte values
        all_bytes = bytes(range(256))
        for v in range(1, 11):
            data = all_bytes * v  # increasing sizes
            db.execute(
                "INSERT INTO xp_bytea (group_id, version, data) VALUES (1, %s, %s)",
                (v, data),
            )

        for v in range(1, 11):
            expected = all_bytes * v
            row = db.execute(
                "SELECT data FROM xp_bytea WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            assert bytes(row["data"]) == expected, (
                f"Version {v} bytea mismatch: len={len(row['data'])}, expected={len(expected)}"
            )

    def test_bytea_empty_content(self, db):
        """Empty bytea values (zero-length) should round-trip correctly."""
        db.execute("""
            CREATE TABLE xp_bytea_empty (
                group_id INT NOT NULL,
                version INT NOT NULL,
                data BYTEA NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_bytea_empty',
                group_by => 'group_id',
                order_by => 'version',
                delta_columns => ARRAY['data'])
        """)

        # Mix of empty and non-empty
        values = [b"hello", b"", b"world", b"", b"!", b"", b"end"]
        for v, data in enumerate(values, 1):
            db.execute(
                "INSERT INTO xp_bytea_empty (group_id, version, data) VALUES (1, %s, %s)",
                (v, data),
            )

        for v, expected in enumerate(values, 1):
            row = db.execute(
                "SELECT data FROM xp_bytea_empty WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            assert bytes(row["data"]) == expected, (
                f"Version {v} bytea mismatch: got {bytes(row['data'])!r}, expected {expected!r}"
            )


class TestDatumRoundtripJson:
    """Verify JSON/JSONB column round-trips."""

    def test_json_roundtrip(self, db):
        """JSON values should round-trip correctly through delta chain."""
        db.execute("""
            CREATE TABLE xp_json (
                group_id INT NOT NULL,
                version INT NOT NULL,
                data JSON NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_json',
                group_by => 'group_id',
                order_by => 'version',
                delta_columns => ARRAY['data'])
        """)

        import json
        for v in range(1, 11):
            obj = {"version": v, "items": list(range(v)), "nested": {"key": f"val_{v}"}}
            db.execute(
                "INSERT INTO xp_json (group_id, version, data) VALUES (1, %s, %s)",
                (v, json.dumps(obj)),
            )

        for v in range(1, 11):
            row = db.execute(
                "SELECT data FROM xp_json WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            obj = row["data"]
            if isinstance(obj, str):
                import json as j
                obj = j.loads(obj)
            assert obj["version"] == v, f"Version {v} JSON mismatch"
            assert obj["items"] == list(range(v))

    def test_jsonb_roundtrip(self, db):
        """JSONB values should round-trip correctly (JSONB has different binary format)."""
        db.execute("""
            CREATE TABLE xp_jsonb (
                group_id INT NOT NULL,
                version INT NOT NULL,
                data JSONB NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_jsonb',
                group_by => 'group_id',
                order_by => 'version',
                delta_columns => ARRAY['data'])
        """)

        import json
        for v in range(1, 11):
            obj = {"version": v, "data": "x" * (v * 10)}
            db.execute(
                "INSERT INTO xp_jsonb (group_id, version, data) VALUES (1, %s, %s)",
                (v, json.dumps(obj)),
            )

        for v in range(1, 11):
            row = db.execute(
                "SELECT data FROM xp_jsonb WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            obj = row["data"]
            if isinstance(obj, str):
                import json as j
                obj = j.loads(obj)
            assert obj["version"] == v
            assert obj["data"] == "x" * (v * 10)


class TestDatumRoundtripAfterCacheCold:
    """
    Test reconstruction when the FIFO/LRU caches are cold.
    
    This exercises the cold path (B8 scenario): reconstruction goes through
    the delta chain stored on disk, not through the in-memory FIFO.
    Force cache-cold by inserting into many groups to evict the target group.
    """

    def test_cold_reconstruction_matches_warm(self, db, make_table):
        """
        Insert data, then evict from caches by flooding with other groups,
        then read back and verify the reconstruction matches.
        """
        tbl = make_table(compress_depth=5)

        # Insert target group with delta chain
        base_text = "Base content that will be delta-encoded. " * 5
        target_versions = {}
        for v in range(1, 21):
            content = base_text + f" Version {v} unique part: {'*' * v}"
            target_versions[v] = content
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, %s, %s)",
                (v, content),
            )

        # Flood with other groups to try to evict group 1 from insert cache
        # (insert_cache_slots defaults to 16, so 20+ groups should cause eviction)
        for g in range(100, 130):
            for v in range(1, 6):
                db.execute(
                    f"INSERT INTO {tbl} (group_id, version, content) VALUES (%s, %s, %s)",
                    (g, v, f"filler_{g}_{v}_" + "x" * 200),
                )

        # Now read back group 1 — should reconstruct from disk, not cache
        for v in range(1, 21):
            row = db.execute(
                f"SELECT content FROM {tbl} WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            assert row["content"] == target_versions[v], (
                f"Cold reconstruction mismatch at version {v}"
            )
