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
    echo -e "${CYAN}║${NC}     ${BOLD}Marmot v2.0 + WordPress Cluster Demo${NC}                    ${CYAN}║${NC}"
    echo -e "${CYAN}║${NC}     3-Node Distributed SQLite powering 3 WordPress instances ${CYAN}║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

usage() {
    echo -e "${BOLD}Usage:${NC} $0 [command]"
    echo ""
    echo -e "${BOLD}Commands:${NC}"
    echo "  up        Start the WordPress + Marmot cluster (default)"
    echo "  down      Stop and remove all containers and volumes"
    echo "  reset     Full reset: remove containers, volumes, and images"
    echo "  logs      Show logs from all containers"
    echo "  logs-m    Show logs from Marmot nodes only"
    echo "  logs-wp   Show logs from WordPress nodes only"
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
    docker compose build

    echo -e "${YELLOW}[3/4]${NC} Starting services..."
    docker compose up -d

    echo -e "${YELLOW}[4/4]${NC} Waiting for services to be ready..."

    # Wait for all WordPress instances to be accessible
    max_attempts=60
    attempt=0
    ready_count=0
    while [ $attempt -lt $max_attempts ] && [ $ready_count -lt 3 ]; do
        ready_count=0
        for port in 9101 9102 9103; do
            if curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/ 2>/dev/null | grep -q "302\|200"; then
                ready_count=$((ready_count + 1))
            fi
        done
        if [ $ready_count -lt 3 ]; then
            attempt=$((attempt + 1))
            sleep 2
            echo -n "."
        fi
    done
    echo ""

    if [ $ready_count -lt 3 ]; then
        echo -e "${YELLOW}Warning: Only $ready_count/3 WordPress instances ready${NC}"
        echo -e "${YELLOW}Check logs with: $0 logs${NC}"
    fi

    print_success
}

stop_stack() {
    print_banner
    check_docker
    echo -e "${YELLOW}Stopping and removing all containers...${NC}"
    docker compose down -v
    echo -e "${GREEN}Cluster stopped and cleaned up successfully${NC}"
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

show_logs_marmot() {
    check_docker
    docker compose logs -f marmot-1 marmot-2 marmot-3
}

show_logs_wordpress() {
    check_docker
    docker compose logs -f wordpress-1 wordpress-2 wordpress-3
}

show_status() {
    check_docker
    echo -e "${BOLD}Container Status:${NC}"
    docker compose ps
    echo ""
    echo -e "${BOLD}Marmot Cluster Health:${NC}"
    for i in 1 2 3; do
        port=$((9180 + i))
        if docker exec marmot-wp-$i wget -q --spider http://localhost:$port/metrics 2>/dev/null; then
            echo -e "  Node $i: ${GREEN}healthy${NC}"
        else
            echo -e "  Node $i: ${RED}unhealthy${NC}"
        fi
    done
}

print_success() {
    echo ""
    echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║${NC}  ${BOLD}Cluster Setup Complete!${NC}                                     ${GREEN}║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${BOLD}Services Running:${NC}"
    echo -e "  ${CYAN}Marmot Cluster${NC}  - 3 nodes with QUORUM write consistency"
    echo -e "  ${CYAN}WordPress${NC}       - 3 independent instances on different ports"
    echo ""
    echo -e "${BOLD}Port Allocation:${NC}"
    echo ""
    echo -e "  ${GREEN}Node 1:${NC} Marmot MySQL ${BOLD}9191${NC} | gRPC ${BOLD}9181${NC} | WordPress ${BLUE}http://localhost:9101${NC}"
    echo -e "  ${GREEN}Node 2:${NC} Marmot MySQL ${BOLD}9192${NC} | gRPC ${BOLD}9182${NC} | WordPress ${BLUE}http://localhost:9102${NC}"
    echo -e "  ${GREEN}Node 3:${NC} Marmot MySQL ${BOLD}9193${NC} | gRPC ${BOLD}9183${NC} | WordPress ${BLUE}http://localhost:9103${NC}"
    echo ""
    echo -e "${BOLD}Architecture:${NC}"
    echo -e "  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐"
    echo -e "  │ WordPress-1 │  │ WordPress-2 │  │ WordPress-3 │"
    echo -e "  │ (port 9101) │  │ (port 9102) │  │ (port 9103) │"
    echo -e "  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘"
    echo -e "         ▼                ▼                ▼"
    echo -e "  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐"
    echo -e "  │  Marmot-1   │  │  Marmot-2   │  │  Marmot-3   │"
    echo -e "  │ MySQL: 9191 │◄─┤ MySQL: 9192 │◄─┤ MySQL: 9193 │"
    echo -e "  │ gRPC:  9181 │  │ gRPC:  9182 │  │ gRPC:  9183 │"
    echo -e "  └─────────────┘  └─────────────┘  └─────────────┘"
    echo -e "         └──────────────┴──────────────┘"
    echo -e "            QUORUM Replication"
    echo ""
    echo -e "${BOLD}Quick Start:${NC}"
    echo -e "  1. Open ${BLUE}http://localhost:9101${NC} and complete WordPress installation"
    echo -e "  2. Open ${BLUE}http://localhost:9102${NC} or ${BLUE}http://localhost:9103${NC}"
    echo -e "  3. See your content replicated across all nodes!"
    echo ""
    echo -e "${BOLD}MySQL Access:${NC}"
    echo -e "  ${YELLOW}mysql -h 127.0.0.1 -P 9191 -u root${NC}  # Node 1"
    echo -e "  ${YELLOW}mysql -h 127.0.0.1 -P 9192 -u root${NC}  # Node 2"
    echo -e "  ${YELLOW}mysql -h 127.0.0.1 -P 9193 -u root${NC}  # Node 3"
    echo ""
    echo -e "${BOLD}Management Commands:${NC}"
    echo -e "  View all logs:     ${YELLOW}$0 logs${NC}"
    echo -e "  Marmot logs only:  ${YELLOW}$0 logs-m${NC}"
    echo -e "  WordPress logs:    ${YELLOW}$0 logs-wp${NC}"
    echo -e "  Check status:      ${YELLOW}$0 status${NC}"
    echo -e "  Stop cluster:      ${YELLOW}$0 down${NC}"
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
    logs-m|logs-marmot)
        show_logs_marmot
        ;;
    logs-wp|logs-wordpress)
        show_logs_wordpress
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
