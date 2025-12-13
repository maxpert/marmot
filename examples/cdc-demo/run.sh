#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

usage() {
    echo -e "${BOLD}Usage:${NC} $0 [up|down|logs]"
    echo ""
    echo "  up      Start Marmot + Redpanda + TODO app"
    echo "  down    Stop everything"
    echo "  logs    Show logs"
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker is not installed"
        exit 1
    fi
}

start_stack() {
    check_docker
    echo -e "${YELLOW}Building and starting CDC demo...${NC}"
    docker compose down -v 2>/dev/null || true
    docker compose up --build -d

    echo -e "${YELLOW}Waiting for services...${NC}"
    sleep 5

    echo ""
    echo -e "${GREEN}=== CDC TODO Demo Ready ===${NC}"
    echo ""
    echo -e "  ${BOLD}Web UI:${NC}  ${BLUE}http://localhost:3000${NC}"
    echo -e "  ${BOLD}MySQL:${NC}   localhost:3306 (user: root, no password)"
    echo ""
    echo -e "  Add/edit/delete TODOs and watch CDC events appear!"
    echo ""
    echo -e "  Logs: ${YELLOW}$0 logs${NC}"
    echo -e "  Stop: ${YELLOW}$0 down${NC}"
    echo ""
}

stop_stack() {
    check_docker
    echo "Stopping..."
    docker compose down -v
    echo "Done"
}

show_logs() {
    check_docker
    docker compose logs -f
}

case "${1:-up}" in
    up)     start_stack ;;
    down)   stop_stack ;;
    logs)   show_logs ;;
    *)      usage ;;
esac
