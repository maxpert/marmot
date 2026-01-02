#!/bin/bash
# Marmot Correctness Verification Test Runner
#
# Usage:
#   ./run-correctness-test.sh              # Run basic correctness tests
#   ./run-correctness-test.sh --load       # Run with high-load simulation
#   ./run-correctness-test.sh --collect    # Only collect test data
#   ./run-correctness-test.sh --collect N  # Collect N posts (default: 10000)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[$(date '+%H:%M:%S')]${NC} $1"
}

# Check dependencies
check_deps() {
    if ! command -v bun &> /dev/null; then
        error "bun is not installed. Install with: curl -fsSL https://bun.sh/install | bash"
        exit 1
    fi

    if ! command -v go &> /dev/null; then
        error "go is not installed"
        exit 1
    fi
}

# Install npm dependencies
install_deps() {
    if [ ! -d "node_modules" ]; then
        log "Installing dependencies..."
        bun install
    fi
}

# Collect test data from Bluesky
collect_data() {
    local count=${1:-10000}
    log "Collecting $count posts from Bluesky Jetstream..."
    bun run collect-bluesky-posts.ts "$count"
}

# Run correctness tests
run_tests() {
    local args="$@"

    if [ ! -f "bluesky-posts.json" ]; then
        error "Test data not found. Run './run-correctness-test.sh --collect' first"
        exit 1
    fi

    log "Running correctness tests..."
    bun run correctness-test.ts $args
}

# Cleanup
cleanup() {
    log "Cleaning up..."
    pkill -f "marmot-v2.*correctness" 2>/dev/null || true
    rm -rf /tmp/marmot-correctness 2>/dev/null || true
}

# Parse arguments
main() {
    check_deps
    install_deps

    case "${1:-}" in
        --collect)
            collect_data "${2:-10000}"
            ;;
        --load)
            run_tests --load
            ;;
        --load-only)
            run_tests --load-only
            ;;
        --cleanup)
            cleanup
            ;;
        --help|-h)
            echo "Marmot Correctness Verification Test"
            echo ""
            echo "Usage:"
            echo "  $0                    Run basic correctness tests"
            echo "  $0 --load             Run with high-load simulation"
            echo "  $0 --load-only        Only run load test"
            echo "  $0 --collect [N]      Collect N posts (default: 10000)"
            echo "  $0 --cleanup          Clean up test data and processes"
            echo ""
            echo "First run '--collect' to download test data, then run tests."
            ;;
        *)
            run_tests
            ;;
    esac
}

trap cleanup EXIT
main "$@"
