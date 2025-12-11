#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

print_banner() {
    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║${NC}     ${BOLD}Marmot v2.0 + WordPress Demo${NC}                             ${CYAN}║${NC}"
    echo -e "${CYAN}║${NC}     Distributed SQLite powering WordPress                    ${CYAN}║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

usage() {
    echo -e "${BOLD}Usage:${NC} $0 [command]"
    echo ""
    echo -e "${BOLD}Commands:${NC}"
    echo "  up        Start the WordPress + Marmot stack (default)"
    echo "  down      Stop and remove all containers and volumes"
    echo "  reset     Full reset: remove containers, volumes, and images"
    echo "  logs      Show logs from all containers"
    echo "  debug     Show WordPress PHP debug.log"
    echo "  debug-f   Tail WordPress PHP debug.log (follow)"
    echo "  status    Show container status"
    echo "  help      Show this help message"
    echo ""
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}Error: Docker is not installed or not in PATH${NC}"
        exit 1
    fi
    if ! docker info &> /dev/null; then
        echo -e "${RED}Error: Docker daemon is not running${NC}"
        exit 1
    fi
}

start_stack() {
    print_banner
    check_docker

    echo -e "${YELLOW}[1/4]${NC} Cleaning up any existing containers..."
    docker compose down -v 2>/dev/null || true

    echo -e "${YELLOW}[2/4]${NC} Building Marmot v2.0 image (this may take a few minutes on first run)..."
    docker compose build --quiet

    echo -e "${YELLOW}[3/4]${NC} Starting services..."
    docker compose up -d

    echo -e "${YELLOW}[4/4]${NC} Waiting for services to be ready..."

    # Wait for WordPress to be accessible
    max_attempts=30
    attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/ 2>/dev/null | grep -q "302\|200"; then
            break
        fi
        attempt=$((attempt + 1))
        sleep 2
        echo -n "."
    done
    echo ""

    if [ $attempt -eq $max_attempts ]; then
        echo -e "${RED}Timeout waiting for WordPress to start${NC}"
        echo -e "${YELLOW}Check logs with: $0 logs${NC}"
        exit 1
    fi

    print_success
}

stop_stack() {
    print_banner
    check_docker
    echo -e "${YELLOW}Stopping and removing all containers...${NC}"
    docker compose down -v
    echo -e "${GREEN}Stack stopped and cleaned up successfully${NC}"
}

reset_stack() {
    print_banner
    check_docker
    echo -e "${YELLOW}Full reset: removing containers, volumes, and local images...${NC}"
    docker compose down -v --rmi local 2>/dev/null || true
    echo -e "${GREEN}Full reset complete. Next 'up' will rebuild from scratch.${NC}"
}

show_logs() {
    check_docker
    docker compose logs -f
}

show_debug_log() {
    check_docker
    echo -e "${BOLD}WordPress PHP Debug Log:${NC}"
    docker exec wordpress cat /var/www/html/wp-content/debug.log 2>/dev/null || echo -e "${YELLOW}No debug.log yet (will appear after first PHP error)${NC}"
}

tail_debug_log() {
    check_docker
    echo -e "${BOLD}Tailing WordPress PHP Debug Log (Ctrl+C to stop):${NC}"
    docker exec wordpress tail -f /var/www/html/wp-content/debug.log 2>/dev/null || echo -e "${YELLOW}No debug.log yet${NC}"
}

show_status() {
    check_docker
    echo -e "${BOLD}Container Status:${NC}"
    docker compose ps
    echo ""
    echo -e "${BOLD}Marmot MySQL Connection Test:${NC}"
    if docker exec marmot-wordpress wget -q --spider http://localhost:8090/metrics 2>/dev/null; then
        echo -e "${GREEN}Marmot is healthy${NC}"
    else
        echo -e "${RED}Marmot health check failed${NC}"
    fi
}

print_success() {
    echo ""
    echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║${NC}  ${BOLD}Setup Complete!${NC}                                             ${GREEN}║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${BOLD}Services Running:${NC}"
    echo -e "  ${CYAN}Marmot v2.0${NC}  - Distributed SQLite with MySQL protocol"
    echo -e "  ${CYAN}WordPress${NC}    - Connected to Marmot as its database backend"
    echo ""
    echo -e "${BOLD}Access Points:${NC}"
    echo ""
    echo -e "  ${GREEN}WordPress:${NC}"
    echo -e "    Homepage:      ${BLUE}http://localhost:8080/${NC}"
    echo -e "    Installation:  ${BLUE}http://localhost:8080/wp-admin/install.php${NC}"
    echo -e "    Admin Panel:   ${BLUE}http://localhost:8080/wp-admin/${NC}"
    echo ""
    echo -e "  ${GREEN}Marmot MySQL Protocol:${NC}"
    echo -e "    Host: ${BOLD}127.0.0.1${NC}  Port: ${BOLD}3316${NC}  User: ${BOLD}root${NC}  (no password)"
    echo -e "    Connect: ${YELLOW}mysql -h 127.0.0.1 -P 3316 -u root${NC}"
    echo ""
    echo -e "${BOLD}Architecture:${NC}"
    echo -e "  ┌─────────────┐    ┌──────────────────┐"
    echo -e "  │  WordPress  │───>│  Marmot v2.0     │"
    echo -e "  │  (port 8080)│    │  MySQL: 3316     │"
    echo -e "  └─────────────┘    │  gRPC:  8090     │"
    echo -e "                     │  (internal only) │"
    echo -e "                     └──────────────────┘"
    echo ""
    echo -e "${BOLD}Next Steps:${NC}"
    echo -e "  1. Open ${BLUE}http://localhost:8080/${NC} in your browser"
    echo -e "  2. Complete the WordPress installation wizard"
    echo -e "  3. Your WordPress site is now powered by Marmot's distributed SQLite!"
    echo ""
    echo -e "${BOLD}Management Commands:${NC}"
    echo -e "  View logs:    ${YELLOW}$0 logs${NC}"
    echo -e "  Check status: ${YELLOW}$0 status${NC}"
    echo -e "  Stop stack:   ${YELLOW}$0 down${NC}"
    echo ""
}

# Main
case "${1:-up}" in
    up|start)
        start_stack
        ;;
    down|stop)
        stop_stack
        ;;
    reset)
        reset_stack
        ;;
    logs)
        show_logs
        ;;
    debug)
        show_debug_log
        ;;
    debug-f)
        tail_debug_log
        ;;
    status)
        show_status
        ;;
    help|-h|--help)
        print_banner
        usage
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        usage
        exit 1
        ;;
esac
