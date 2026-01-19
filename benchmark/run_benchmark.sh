#!/bin/bash
#
# pg_xpatch Benchmark Script
#
# Measures insert throughput, read throughput, and compression ratios
# for various workloads.
#

set -e

DB_NAME="${DB_NAME:-postgres}"
PSQL="psql -d $DB_NAME -X -q"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================"
echo "pg_xpatch Benchmark Suite"
echo -e "========================================${NC}"

# Check if extension is available
$PSQL -c "DROP EXTENSION IF EXISTS pg_xpatch CASCADE;" 2>/dev/null || true
$PSQL -c "CREATE EXTENSION pg_xpatch;" || {
    echo -e "${RED}ERROR: pg_xpatch extension not available${NC}"
    exit 1
}

echo ""
echo -e "${GREEN}Extension loaded successfully${NC}"
echo ""

# Helper function to time a query
time_query() {
    local query="$1"
    local result
    result=$($PSQL -c "\\timing on" -c "$query" 2>&1 | grep "Time:" | awk '{print $2}')
    echo "$result"
}

# Benchmark 1: Document versioning (text content)
echo -e "${YELLOW}Benchmark 1: Document Versioning${NC}"
echo "  Simulates document editing with incremental changes"
echo ""

$PSQL -c "
    SET client_min_messages = warning;
    DROP TABLE IF EXISTS bench_documents CASCADE;
    CREATE TABLE bench_documents (
        doc_id INT,
        version INT,
        content TEXT,
        metadata JSONB
    ) USING xpatch;
    SELECT xpatch.configure('bench_documents', 
        group_by => 'doc_id', 
        order_by => 'version',
        delta_columns => ARRAY['content', 'metadata']::text[],
        keyframe_every => 100
    );
"

# Insert test: 100 documents, 50 versions each
echo -n "  Inserting 5,000 rows (100 docs × 50 versions)... "
INSERT_TIME=$($PSQL -c "\\timing on" -c "
    INSERT INTO bench_documents 
    SELECT 
        doc_id,
        version,
        'Document ' || doc_id || ' version ' || version || ': ' || 
        repeat('Lorem ipsum dolor sit amet, consectetur adipiscing elit. ', version),
        jsonb_build_object('author', 'user' || (doc_id % 10), 'timestamp', now())
    FROM 
        generate_series(1, 100) doc_id,
        generate_series(1, 50) version;
" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${INSERT_TIME} ms"

# Read test: full scan
echo -n "  Full table scan... "
READ_TIME=$($PSQL -c "\\timing on" -c "SELECT count(*) FROM bench_documents;" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${READ_TIME} ms"

# Read test: point query
echo -n "  Point query (single document, latest version)... "
POINT_TIME=$($PSQL -c "\\timing on" -c "
    SELECT * FROM bench_documents 
    WHERE doc_id = 50 AND version = 50;
" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${POINT_TIME} ms"

# Stats
echo "  Statistics:"
$PSQL -c "
    SELECT 
        total_rows,
        total_groups as documents,
        keyframe_count,
        delta_count,
        pg_size_pretty(raw_size_bytes::bigint) as raw_size,
        pg_size_pretty(compressed_size_bytes::bigint) as compressed_size,
        round(compression_ratio::numeric, 2) as ratio,
        round(avg_chain_length::numeric, 2) as avg_chain
    FROM xpatch_stats('bench_documents');
"

echo ""

# Benchmark 2: High-similarity content (code files)
echo -e "${YELLOW}Benchmark 2: Code File Versioning${NC}"
echo "  Simulates source code changes with small diffs"
echo ""

$PSQL -c "
    SET client_min_messages = warning;
    DROP TABLE IF EXISTS bench_code CASCADE;
    CREATE TABLE bench_code (
        file_id INT,
        commit_num INT,
        content TEXT
    ) USING xpatch;
    SELECT xpatch.configure('bench_code',
        group_by => 'file_id',
        order_by => 'commit_num',
        delta_columns => ARRAY['content']::text[],
        keyframe_every => 50,
        compress_depth => 3
    );
"

# Generate code-like content with small changes
echo -n "  Inserting 2,000 rows (20 files × 100 commits)... "
INSERT_TIME2=$($PSQL -c "\\timing on" -c "
    INSERT INTO bench_code
    SELECT 
        file_id,
        commit_num,
        '// File ' || file_id || E'\n' ||
        'function process_' || file_id || '(data) {' || E'\n' ||
        '    // Version ' || commit_num || E'\n' ||
        '    let result = [];' || E'\n' ||
        repeat('    result.push(transform(data[i]));' || E'\n', 10 + (commit_num % 5)) ||
        '    return result;' || E'\n' ||
        '}' || E'\n'
    FROM
        generate_series(1, 20) file_id,
        generate_series(1, 100) commit_num;
" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${INSERT_TIME2} ms"

# Read test
echo -n "  Full table scan... "
READ_TIME2=$($PSQL -c "\\timing on" -c "SELECT count(*) FROM bench_code;" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${READ_TIME2} ms"

# Stats
echo "  Statistics:"
$PSQL -c "
    SELECT 
        total_rows,
        total_groups as files,
        keyframe_count,
        delta_count,
        pg_size_pretty(raw_size_bytes::bigint) as raw_size,
        pg_size_pretty(compressed_size_bytes::bigint) as compressed_size,
        round(compression_ratio::numeric, 2) as ratio
    FROM xpatch_stats('bench_code');
"

echo ""

# Benchmark 3: JSON data versioning
echo -e "${YELLOW}Benchmark 3: JSON Configuration Versioning${NC}"
echo "  Simulates configuration changes in JSON format"
echo ""

$PSQL -c "
    SET client_min_messages = warning;
    DROP TABLE IF EXISTS bench_config CASCADE;
    CREATE TABLE bench_config (
        app_id INT,
        version INT,
        config TEXT
    ) USING xpatch;
    SELECT xpatch.configure('bench_config',
        group_by => 'app_id',
        order_by => 'version',
        delta_columns => ARRAY['config']::text[],
        keyframe_every => 20
    );
"

echo -n "  Inserting 1,000 rows (50 apps × 20 versions)... "
INSERT_TIME3=$($PSQL -c "\\timing on" -c "
    INSERT INTO bench_config
    SELECT 
        app_id,
        version,
        jsonb_build_object(
            'app_name', 'application_' || app_id,
            'version', version,
            'settings', jsonb_build_object(
                'max_connections', 100 + version,
                'timeout_ms', 5000 + (version * 100),
                'features', jsonb_build_array('feature_a', 'feature_b', 
                    CASE WHEN version > 10 THEN 'feature_c' ELSE NULL END)
            ),
            'servers', (
                SELECT jsonb_agg(jsonb_build_object(
                    'host', 'server' || s || '.example.com',
                    'port', 8080 + s
                ))
                FROM generate_series(1, 3 + (version % 3)) s
            )
        )::text
    FROM
        generate_series(1, 50) app_id,
        generate_series(1, 20) version;
" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${INSERT_TIME3} ms"

# Stats
echo "  Statistics:"
$PSQL -c "
    SELECT 
        total_rows,
        total_groups as apps,
        keyframe_count,
        delta_count,
        pg_size_pretty(raw_size_bytes::bigint) as raw_size,
        pg_size_pretty(compressed_size_bytes::bigint) as compressed_size,
        round(compression_ratio::numeric, 2) as ratio
    FROM xpatch_stats('bench_config');
"

echo ""

# Benchmark 4: Compare with regular heap table - INSERT overhead
echo -e "${YELLOW}Benchmark 4: INSERT Overhead Comparison${NC}"
echo "  Comparing INSERT performance: xpatch vs heap"
echo ""

$PSQL -c "
    SET client_min_messages = warning;
    DROP TABLE IF EXISTS bench_compare_xpatch CASCADE;
    DROP TABLE IF EXISTS bench_compare_heap CASCADE;
    
    CREATE TABLE bench_compare_xpatch (
        id INT,
        version INT,
        data TEXT
    ) USING xpatch;
    SELECT xpatch.configure('bench_compare_xpatch',
        group_by => 'id',
        order_by => 'version',
        delta_columns => ARRAY['data']::text[]
    );
    
    CREATE TABLE bench_compare_heap (
        id INT,
        version INT,
        data TEXT
    );
"

# Test with varying row counts
for ROWS in 1000 5000 10000; do
    echo "  Testing with $ROWS rows (100 groups × $((ROWS/100)) versions)..."
    
    $PSQL -c "TRUNCATE bench_compare_xpatch; TRUNCATE bench_compare_heap;" 2>/dev/null
    
    VERSIONS=$((ROWS/100))
    
    echo -n "    xpatch INSERT: "
    XPATCH_INSERT=$($PSQL -c "\\timing on" -c "
        INSERT INTO bench_compare_xpatch
        SELECT id, version, repeat('Version ' || version || ' data for item ' || id || '. ', 20)
        FROM generate_series(1, 100) id, generate_series(1, $VERSIONS) version;
    " 2>&1 | grep "Time:" | awk '{print $2}')
    echo "${XPATCH_INSERT} ms"
    
    echo -n "    heap INSERT:   "
    HEAP_INSERT=$($PSQL -c "\\timing on" -c "
        INSERT INTO bench_compare_heap
        SELECT id, version, repeat('Version ' || version || ' data for item ' || id || '. ', 20)
        FROM generate_series(1, 100) id, generate_series(1, $VERSIONS) version;
    " 2>&1 | grep "Time:" | awk '{print $2}')
    echo "${HEAP_INSERT} ms"
    
    # Calculate overhead using awk (bc may not be available)
    if [[ -n "$XPATCH_INSERT" && -n "$HEAP_INSERT" ]]; then
        OVERHEAD=$(awk "BEGIN {printf \"%.2f\", $XPATCH_INSERT / $HEAP_INSERT}" 2>/dev/null || echo "N/A")
        echo "    Overhead: ${OVERHEAD}x"
    fi
    echo ""
done

echo ""

# Benchmark 5: READ overhead comparison
echo -e "${YELLOW}Benchmark 5: READ Overhead Comparison${NC}"
echo "  Comparing SELECT performance: xpatch vs heap"
echo ""

# Use the 10k rows from previous test
echo "  Full table scan (10,000 rows):"
echo -n "    xpatch SELECT: "
XPATCH_READ=$($PSQL -c "\\timing on" -c "SELECT count(*) FROM bench_compare_xpatch;" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${XPATCH_READ} ms"

echo -n "    heap SELECT:   "
HEAP_READ=$($PSQL -c "\\timing on" -c "SELECT count(*) FROM bench_compare_heap;" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${HEAP_READ} ms"

if [[ -n "$XPATCH_READ" && -n "$HEAP_READ" ]]; then
    OVERHEAD=$(awk "BEGIN {printf \"%.2f\", $XPATCH_READ / $HEAP_READ}" 2>/dev/null || echo "N/A")
    echo "    Overhead: ${OVERHEAD}x"
fi

echo ""
echo "  Full table scan with data retrieval:"
echo -n "    xpatch SELECT: "
XPATCH_READ2=$($PSQL -c "\\timing on" -c "SELECT length(data) FROM bench_compare_xpatch;" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${XPATCH_READ2} ms"

echo -n "    heap SELECT:   "
HEAP_READ2=$($PSQL -c "\\timing on" -c "SELECT length(data) FROM bench_compare_heap;" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${HEAP_READ2} ms"

if [[ -n "$XPATCH_READ2" && -n "$HEAP_READ2" ]]; then
    OVERHEAD=$(awk "BEGIN {printf \"%.2f\", $XPATCH_READ2 / $HEAP_READ2}" 2>/dev/null || echo "N/A")
    echo "    Overhead: ${OVERHEAD}x"
fi

echo ""
echo "  Point query (single row by id and version):"

# Create indexes for fair comparison
$PSQL -c "
    CREATE INDEX IF NOT EXISTS bench_xpatch_idx ON bench_compare_xpatch(id, version);
    CREATE INDEX IF NOT EXISTS bench_heap_idx ON bench_compare_heap(id, version);
" 2>/dev/null

echo -n "    xpatch SELECT: "
XPATCH_POINT=$($PSQL -c "\\timing on" -c "SELECT * FROM bench_compare_xpatch WHERE id = 50 AND version = 50;" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${XPATCH_POINT} ms"

echo -n "    heap SELECT:   "
HEAP_POINT=$($PSQL -c "\\timing on" -c "SELECT * FROM bench_compare_heap WHERE id = 50 AND version = 50;" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${HEAP_POINT} ms"

echo ""
echo "  Range query (all versions for one group):"
echo -n "    xpatch SELECT: "
XPATCH_RANGE=$($PSQL -c "\\timing on" -c "SELECT * FROM bench_compare_xpatch WHERE id = 50 ORDER BY version;" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${XPATCH_RANGE} ms"

echo -n "    heap SELECT:   "
HEAP_RANGE=$($PSQL -c "\\timing on" -c "SELECT * FROM bench_compare_heap WHERE id = 50 ORDER BY version;" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${HEAP_RANGE} ms"

echo ""

# Benchmark 6: Storage comparison
echo -e "${YELLOW}Benchmark 6: Storage Size Comparison${NC}"
echo ""
echo "  Table sizes (10,000 rows):"
$PSQL -c "
    SELECT 
        'xpatch' as table_type,
        pg_size_pretty(pg_relation_size('bench_compare_xpatch')) as table_size,
        pg_size_pretty(pg_total_relation_size('bench_compare_xpatch')) as total_size
    UNION ALL
    SELECT 
        'heap' as table_type,
        pg_size_pretty(pg_relation_size('bench_compare_heap')) as table_size,
        pg_size_pretty(pg_total_relation_size('bench_compare_heap')) as total_size;
"

# Show xpatch compression stats
echo ""
echo "  xpatch compression statistics:"
$PSQL -c "
    SELECT 
        total_rows,
        keyframe_count,
        delta_count,
        pg_size_pretty(raw_size_bytes::bigint) as raw_size,
        pg_size_pretty(compressed_size_bytes::bigint) as compressed_size,
        round(compression_ratio::numeric, 2) as compression_ratio
    FROM xpatch_stats('bench_compare_xpatch');
"

echo ""

# Benchmark 7: Cache effectiveness
echo -e "${YELLOW}Benchmark 7: Cache Effectiveness${NC}"
echo "  Testing read performance with warm vs cold cache"
echo ""

# Clear cache by restarting (skip in container)
echo "  Cold cache read (first pass):"
echo -n "    xpatch SELECT: "
COLD_READ=$($PSQL -c "\\timing on" -c "SELECT length(data) FROM bench_compare_xpatch WHERE id <= 10;" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${COLD_READ} ms"

echo "  Warm cache read (second pass, same data):"
echo -n "    xpatch SELECT: "
WARM_READ=$($PSQL -c "\\timing on" -c "SELECT length(data) FROM bench_compare_xpatch WHERE id <= 10;" 2>&1 | grep "Time:" | awk '{print $2}')
echo "${WARM_READ} ms"

if [[ -n "$COLD_READ" && -n "$WARM_READ" ]]; then
    SPEEDUP=$(awk "BEGIN {printf \"%.2f\", $COLD_READ / $WARM_READ}" 2>/dev/null || echo "N/A")
    echo "    Cache speedup: ${SPEEDUP}x"
fi

echo ""

# Cache statistics
echo -e "${YELLOW}Cache Statistics${NC}"
$PSQL -c "SELECT * FROM xpatch_cache_stats();"

# Cleanup
echo ""
echo -e "${BLUE}Cleaning up...${NC}"
$PSQL -c "
    SET client_min_messages = warning;
    DROP TABLE IF EXISTS bench_documents CASCADE;
    DROP TABLE IF EXISTS bench_code CASCADE;
    DROP TABLE IF EXISTS bench_config CASCADE;
    DROP TABLE IF EXISTS bench_compare_xpatch CASCADE;
    DROP TABLE IF EXISTS bench_compare_heap CASCADE;
"

echo ""
echo -e "${GREEN}Benchmark complete!${NC}"
