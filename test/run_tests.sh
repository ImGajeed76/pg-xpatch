#!/bin/bash
# pg_xpatch test runner
# Runs all regression tests and compares output to expected results

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DOCKER_CONTAINER="pg-xpatch-dev"
TEST_DB="xpatch_test"
TEST_DIR="$(cd "$(dirname "$0")" && pwd)"
SQL_DIR="$TEST_DIR/sql"
EXPECTED_DIR="$TEST_DIR/expected"
RESULTS_DIR="$TEST_DIR/results"

# Test files in order
TESTS=(
    "00_setup"
    "01_basic"
    "02_compression"
    "03_reconstruction"
    "04_keyframes"
    "05_cache"
    "06_errors"
    "07_indexes"
    "08_parallel"
    "09_multi_delta"
    "10_no_group"
)

# Create results directory
mkdir -p "$RESULTS_DIR"

# Function to run a test
run_test() {
    local test_name=$1
    local sql_file="$SQL_DIR/${test_name}.sql"
    local expected_file="$EXPECTED_DIR/${test_name}.out"
    local result_file="$RESULTS_DIR/${test_name}.out"
    
    if [[ ! -f "$sql_file" ]]; then
        echo -e "${RED}SKIP${NC} $test_name - SQL file not found"
        return 1
    fi
    
    # Run the test
    docker exec -u postgres "$DOCKER_CONTAINER" psql -d "$TEST_DB" -f "/workspace/test/sql/${test_name}.sql" > "$result_file" 2>&1
    
    # Compare with expected if it exists
    if [[ -f "$expected_file" ]]; then
        if diff -q "$expected_file" "$result_file" > /dev/null 2>&1; then
            echo -e "${GREEN}PASS${NC} $test_name"
            return 0
        else
            echo -e "${RED}FAIL${NC} $test_name - output differs"
            echo "  Expected: $expected_file"
            echo "  Got:      $result_file"
            echo "  Diff:"
            diff "$expected_file" "$result_file" | head -20
            return 1
        fi
    else
        echo -e "${YELLOW}NEW${NC}  $test_name - no expected output (creating)"
        cp "$result_file" "$expected_file"
        return 0
    fi
}

# Function to setup test database
setup_db() {
    echo "Setting up test database..."
    docker exec -u postgres "$DOCKER_CONTAINER" dropdb --if-exists "$TEST_DB" 2>/dev/null || true
    docker exec -u postgres "$DOCKER_CONTAINER" createdb "$TEST_DB"
    echo "Test database '$TEST_DB' created."
}

# Function to generate expected outputs
generate_expected() {
    echo "Generating expected outputs..."
    mkdir -p "$EXPECTED_DIR"
    
    setup_db
    
    for test_name in "${TESTS[@]}"; do
        local sql_file="$SQL_DIR/${test_name}.sql"
        local expected_file="$EXPECTED_DIR/${test_name}.out"
        
        if [[ -f "$sql_file" ]]; then
            echo "  Generating $test_name..."
            docker exec -u postgres "$DOCKER_CONTAINER" psql -d "$TEST_DB" -f "/workspace/test/sql/${test_name}.sql" > "$expected_file" 2>&1
        fi
    done
    
    echo "Expected outputs generated in $EXPECTED_DIR"
}

# Main
case "${1:-run}" in
    run)
        echo "========================================"
        echo "pg_xpatch Regression Tests"
        echo "========================================"
        
        setup_db
        
        passed=0
        failed=0
        
        for test_name in "${TESTS[@]}"; do
            if run_test "$test_name"; then
                passed=$((passed + 1))
            else
                failed=$((failed + 1))
            fi
        done
        
        echo "========================================"
        echo -e "Results: ${GREEN}$passed passed${NC}, ${RED}$failed failed${NC}"
        echo "========================================"
        
        if [[ $failed -gt 0 ]]; then
            exit 1
        fi
        ;;
    
    generate)
        generate_expected
        ;;
    
    clean)
        echo "Cleaning test artifacts..."
        rm -rf "$RESULTS_DIR"
        docker exec -u postgres "$DOCKER_CONTAINER" dropdb --if-exists "$TEST_DB" 2>/dev/null || true
        echo "Done."
        ;;
    
    *)
        echo "Usage: $0 [run|generate|clean]"
        echo ""
        echo "Commands:"
        echo "  run       - Run all tests (default)"
        echo "  generate  - Generate expected outputs from current code"
        echo "  clean     - Remove test artifacts"
        exit 1
        ;;
esac
