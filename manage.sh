#!/bin/bash
# ============================================================
# Polymarket Streaming Dashboard - Management Script
# Snowpipe Streaming v2 High-Performance + React Dashboard
#
# Usage: ./manage.sh [command]
# ============================================================

set -e

APP_NAME="Polymarket Streaming Dashboard"
PORT=4000
PID_FILE=".server.pid"
STREAMER_PID_FILE=".streamer.pid"
VENV_DIR="venv"
LOG_DIR="logs"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

print_banner() {
    echo -e "${CYAN}"
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║     Polymarket Streaming Dashboard                         ║"
    echo "║     Snowpipe Streaming v2 High-Performance                 ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_usage() {
    echo "Usage: $0 [command]"
    echo ""
    echo "Dashboard Commands:"
    echo "  install       Install all dependencies (Python + Node.js)"
    echo "  setup         Full setup: install + Snowflake tables + validate"
    echo "  start         Start the React dashboard in background"
    echo "  stop          Stop the React dashboard"
    echo "  restart       Restart the React dashboard"
    echo "  status        Check status of all services"
    echo "  dev           Start dashboard in foreground (interactive)"
    echo "  build         Build React app for production"
    echo "  prod          Start production server"
    echo ""
    echo "Streaming Commands:"
    echo "  stream        Start Polymarket streaming (continuous, background)"
    echo "  stream-once   Run a single fetch-and-stream cycle"
    echo "  stream-stop   Stop the background streamer"
    echo "  stream-logs   Show streamer logs"
    echo ""
    echo "Data & Validation Commands:"
    echo "  test          Run all tests (Python + JS)"
    echo "  validate      Validate Snowflake connection and data"
    echo "  test-api      Test Polymarket API connectivity"
    echo "  test-auth     Test Snowflake authentication"
    echo "  check-data    Check data in Snowflake tables"
    echo ""
    echo "Maintenance Commands:"
    echo "  logs          Show dashboard server logs"
    echo "  clean         Remove build artifacts, node_modules, venv"
    echo "  help          Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 setup          # Full setup from scratch"
    echo "  $0 start          # Start React dashboard"
    echo "  $0 stream         # Start streaming Polymarket data"
    echo "  $0 status         # Check all services"
    echo "  $0 stream-once    # Single fetch cycle"
}

# ============================================================
# Utility Functions
# ============================================================

check_port() {
    lsof -i :$PORT >/dev/null 2>&1
}

get_pid() {
    if [ -f "$1" ]; then
        cat "$1"
    fi
}

ensure_log_dir() {
    mkdir -p "$LOG_DIR"
}

check_python() {
    if command -v python3 &>/dev/null; then
        echo "python3"
    elif command -v python &>/dev/null; then
        echo "python"
    else
        echo ""
    fi
}

check_node() {
    if command -v node &>/dev/null; then
        node --version
    else
        echo ""
    fi
}

# ============================================================
# Install Commands
# ============================================================

do_install() {
    echo -e "${CYAN}Installing all dependencies...${NC}"
    echo ""

    # Check prerequisites
    PYTHON_CMD=$(check_python)
    if [ -z "$PYTHON_CMD" ]; then
        echo -e "${RED}Python 3.9+ is required but not found${NC}"
        echo "  Install: brew install python3"
        exit 1
    fi
    echo -e "  Python:  ${GREEN}$($PYTHON_CMD --version)${NC}"

    NODE_VER=$(check_node)
    if [ -z "$NODE_VER" ]; then
        echo -e "${RED}Node.js 18+ is required but not found${NC}"
        echo "  Install: brew install node"
        exit 1
    fi
    echo -e "  Node.js: ${GREEN}${NODE_VER}${NC}"
    echo ""

    # Python virtual environment
    echo -e "${CYAN}Setting up Python virtual environment...${NC}"
    if [ ! -d "$VENV_DIR" ]; then
        $PYTHON_CMD -m venv "$VENV_DIR"
    fi
    source "$VENV_DIR/bin/activate"
    pip install --upgrade pip -q
    pip install -r requirements.txt -q
    echo -e "${GREEN}Python dependencies installed${NC}"
    echo ""

    # Node.js dependencies
    echo -e "${CYAN}Installing Node.js dependencies...${NC}"
    npm install
    echo ""

    ensure_log_dir

    echo -e "${GREEN}All dependencies installed${NC}"
}

do_setup() {
    echo -e "${CYAN}Running full setup...${NC}"
    echo ""

    # Install
    do_install
    echo ""

    # Check config
    if [ ! -f "snowflake_config.json" ]; then
        echo -e "${YELLOW}snowflake_config.json not found${NC}"
        echo "  Copy the example and edit with your credentials:"
        echo "  cp snowflake_config.example.json snowflake_config.json"
        echo ""
    fi

    if [ ! -f ".env.local" ] && [ ! -f ".env" ]; then
        echo -e "${YELLOW}No .env.local found for React app${NC}"
        echo "  Copy and edit: cp .env.example .env.local"
        echo ""
    fi

    # Test API
    do_test_api

    echo ""
    echo -e "${GREEN}Setup complete!${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Configure snowflake_config.json with your Snowflake credentials"
    echo "  2. Configure .env.local for the React dashboard"
    echo "  3. Run the Snowflake DDL: SETUP_SNOWFLAKE.sql"
    echo "  4. Start streaming: $0 stream"
    echo "  5. Start dashboard: $0 start"
}

# ============================================================
# Dashboard Commands
# ============================================================

do_start() {
    echo -e "${CYAN}Starting ${APP_NAME}...${NC}"
    echo ""

    if check_port; then
        echo -e "${YELLOW}Dashboard already running on port ${PORT}${NC}"
        do_status
        return 0
    fi

    ensure_log_dir
    nohup npm run dev > "$LOG_DIR/dashboard.log" 2>&1 &
    echo $! > "$PID_FILE"

    echo "  Starting server..."
    sleep 3

    if check_port; then
        echo -e "${GREEN}Dashboard started${NC}"
        echo ""
        echo -e "  URL:     ${CYAN}http://localhost:${PORT}${NC}"
        echo -e "  Logs:    $0 logs"
        echo -e "  Stop:    $0 stop"
    else
        echo -e "${RED}Failed to start dashboard${NC}"
        echo "  Check logs: $0 logs"
        return 1
    fi
}

do_stop() {
    echo -e "${CYAN}Stopping dashboard...${NC}"

    if ! check_port; then
        echo -e "${YELLOW}Dashboard is not running${NC}"
        rm -f "$PID_FILE"
        return 0
    fi

    PID=$(get_pid "$PID_FILE")
    if [ -n "$PID" ]; then
        kill $PID 2>/dev/null || true
        sleep 1
        if check_port; then
            kill -9 $PID 2>/dev/null || true
            sleep 1
        fi
    fi

    # Clean up remaining
    REMAINING=$(lsof -t -i :$PORT 2>/dev/null || echo "")
    if [ -n "$REMAINING" ]; then
        echo "$REMAINING" | xargs kill -9 2>/dev/null || true
    fi
    rm -f "$PID_FILE"

    echo -e "${GREEN}Dashboard stopped${NC}"
}

do_restart() {
    do_stop
    echo ""
    do_start
}

do_dev() {
    echo -e "${CYAN}Starting dashboard (foreground)...${NC}"
    echo -e "  Press ${YELLOW}Ctrl+C${NC} to stop"
    echo ""
    npm run dev
}

do_build() {
    echo -e "${CYAN}Building for production...${NC}"
    npm run build
    echo -e "${GREEN}Build complete. Run '$0 prod' to start.${NC}"
}

do_prod() {
    echo -e "${CYAN}Starting production server...${NC}"
    if [ ! -d ".next" ]; then
        echo -e "${YELLOW}No build found. Building first...${NC}"
        npm run build
    fi
    npm run start
}

# ============================================================
# Streaming Commands
# ============================================================

do_stream() {
    echo -e "${CYAN}Starting Polymarket streamer (background)...${NC}"
    echo ""

    PYTHON_CMD=$(check_python)
    if [ -z "$PYTHON_CMD" ]; then
        echo -e "${RED}Python not found${NC}"
        exit 1
    fi

    if [ -f "$STREAMER_PID_FILE" ]; then
        OLD_PID=$(cat "$STREAMER_PID_FILE")
        if kill -0 "$OLD_PID" 2>/dev/null; then
            echo -e "${YELLOW}Streamer already running (PID: ${OLD_PID})${NC}"
            return 0
        fi
    fi

    if [ ! -f "snowflake_config.json" ]; then
        echo -e "${RED}snowflake_config.json not found${NC}"
        echo "  cp snowflake_config.example.json snowflake_config.json"
        exit 1
    fi

    ensure_log_dir

    if [ -d "$VENV_DIR" ]; then
        source "$VENV_DIR/bin/activate"
    fi

    nohup $PYTHON_CMD main.py --interval 60 --pages 10 > "$LOG_DIR/streamer.log" 2>&1 &
    echo $! > "$STREAMER_PID_FILE"

    echo -e "${GREEN}Streamer started (PID: $(cat $STREAMER_PID_FILE))${NC}"
    echo -e "  Logs: $0 stream-logs"
    echo -e "  Stop: $0 stream-stop"
}

do_stream_once() {
    echo -e "${CYAN}Running single fetch-and-stream cycle...${NC}"
    echo ""

    PYTHON_CMD=$(check_python)
    if [ -d "$VENV_DIR" ]; then
        source "$VENV_DIR/bin/activate"
    fi

    $PYTHON_CMD main.py --once --pages 3
}

do_stream_stop() {
    echo -e "${CYAN}Stopping streamer...${NC}"

    if [ ! -f "$STREAMER_PID_FILE" ]; then
        echo -e "${YELLOW}No streamer PID file found${NC}"
        return 0
    fi

    PID=$(cat "$STREAMER_PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        sleep 2
        if kill -0 "$PID" 2>/dev/null; then
            kill -9 "$PID" 2>/dev/null || true
        fi
        echo -e "${GREEN}Streamer stopped${NC}"
    else
        echo -e "${YELLOW}Streamer was not running${NC}"
    fi
    rm -f "$STREAMER_PID_FILE"
}

do_stream_logs() {
    if [ -f "$LOG_DIR/streamer.log" ]; then
        echo -e "${CYAN}Streamer logs (last 50 lines):${NC}"
        echo ""
        tail -50 "$LOG_DIR/streamer.log"
    else
        echo -e "${YELLOW}No streamer log found. Start with: $0 stream${NC}"
    fi
}

# ============================================================
# Status
# ============================================================

do_status() {
    echo -e "${CYAN}Service Status${NC}"
    echo ""

    # Dashboard
    if check_port; then
        PID=$(get_pid "$PID_FILE")
        echo -e "  Dashboard:  ${GREEN}Running${NC} (PID: ${PID:-unknown}, port ${PORT})"
        echo -e "              http://localhost:${PORT}"
    else
        echo -e "  Dashboard:  ${RED}Stopped${NC}"
    fi

    # Streamer
    if [ -f "$STREAMER_PID_FILE" ]; then
        SPID=$(cat "$STREAMER_PID_FILE")
        if kill -0 "$SPID" 2>/dev/null; then
            echo -e "  Streamer:   ${GREEN}Running${NC} (PID: ${SPID})"
        else
            echo -e "  Streamer:   ${RED}Stopped${NC} (stale PID file)"
        fi
    else
        echo -e "  Streamer:   ${RED}Stopped${NC}"
    fi

    # Config files
    echo ""
    echo -e "${CYAN}Configuration${NC}"
    [ -f "snowflake_config.json" ] && echo -e "  snowflake_config.json: ${GREEN}Found${NC}" || echo -e "  snowflake_config.json: ${RED}Missing${NC}"
    [ -f ".env.local" ] && echo -e "  .env.local:            ${GREEN}Found${NC}" || echo -e "  .env.local:            ${YELLOW}Missing${NC}"
    [ -d "node_modules" ] && echo -e "  node_modules:          ${GREEN}Installed${NC}" || echo -e "  node_modules:          ${RED}Not installed${NC}"
    [ -d "$VENV_DIR" ] && echo -e "  Python venv:           ${GREEN}Created${NC}" || echo -e "  Python venv:           ${RED}Not created${NC}"
    echo ""
}

# ============================================================
# Test & Validation
# ============================================================

do_test() {
    echo -e "${CYAN}Running all tests...${NC}"
    echo ""

    PYTHON_CMD=$(check_python)
    if [ -d "$VENV_DIR" ]; then
        source "$VENV_DIR/bin/activate"
    fi

    # Python tests
    echo -e "${CYAN}Python tests:${NC}"
    if [ -f "test_polymarket.py" ]; then
        $PYTHON_CMD -m pytest test_polymarket.py -v 2>&1 || true
    fi
    if [ -f "validation.py" ]; then
        $PYTHON_CMD validation.py 2>&1 || true
    fi

    echo ""

    # Node tests
    echo -e "${CYAN}Node.js tests:${NC}"
    if [ -d "node_modules" ]; then
        npm test 2>&1 || true
    else
        echo -e "${YELLOW}Node modules not installed. Run: $0 install${NC}"
    fi
}

do_validate() {
    echo -e "${CYAN}Validating Snowflake connection and data...${NC}"
    echo ""

    PYTHON_CMD=$(check_python)
    if [ -d "$VENV_DIR" ]; then
        source "$VENV_DIR/bin/activate"
    fi

    if [ -f "validation.py" ]; then
        $PYTHON_CMD validation.py
    else
        echo -e "${RED}validation.py not found${NC}"
    fi
}

do_test_api() {
    echo -e "${CYAN}Testing Polymarket API...${NC}"

    RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "https://gamma-api.polymarket.com/markets?limit=1" 2>/dev/null)

    if [ "$RESPONSE" = "200" ]; then
        echo -e "  Polymarket API: ${GREEN}OK (HTTP 200)${NC}"

        # Fetch sample
        SAMPLE=$(curl -s "https://gamma-api.polymarket.com/markets?limit=1" 2>/dev/null)
        if echo "$SAMPLE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  Sample: {d[0].get(\"question\",\"N/A\")[:60]}')" 2>/dev/null; then
            echo -e "  ${GREEN}API is returning valid data${NC}"
        fi
    else
        echo -e "  Polymarket API: ${RED}FAILED (HTTP ${RESPONSE})${NC}"
    fi
}

do_test_auth() {
    echo -e "${CYAN}Testing Snowflake authentication...${NC}"

    PYTHON_CMD=$(check_python)
    if [ -d "$VENV_DIR" ]; then
        source "$VENV_DIR/bin/activate"
    fi

    $PYTHON_CMD -c "
from snowflake_jwt_auth import SnowflakeJWTAuth
import json
try:
    with open('snowflake_config.json') as f:
        config = json.load(f)
    auth = SnowflakeJWTAuth(config)
    token = auth.get_scoped_token()
    print(f'  Auth: OK (token length: {len(token)})')
except Exception as e:
    print(f'  Auth: FAILED - {e}')
"
}

do_check_data() {
    echo -e "${CYAN}Checking Snowflake data...${NC}"

    PYTHON_CMD=$(check_python)
    if [ -d "$VENV_DIR" ]; then
        source "$VENV_DIR/bin/activate"
    fi

    $PYTHON_CMD -c "
import json, requests
from snowpipe_streaming_client import SnowpipeStreamingClient

try:
    client = SnowpipeStreamingClient('snowflake_config.json')
    print('  Client initialized OK')
    print(f'  Database: {client.database}')
    print(f'  Schema:   {client.schema}')
    print(f'  Table:    {client.table}')
    print(f'  Pipe:     {client.pipe}')
except Exception as e:
    print(f'  FAILED: {e}')
" 2>&1
}

# ============================================================
# Maintenance
# ============================================================

do_logs() {
    if [ -f "$LOG_DIR/dashboard.log" ]; then
        echo -e "${CYAN}Dashboard logs (last 50 lines):${NC}"
        echo ""
        tail -50 "$LOG_DIR/dashboard.log"
    else
        echo -e "${YELLOW}No dashboard log found. Start with: $0 start${NC}"
    fi
}

do_clean() {
    echo -e "${CYAN}Cleaning build artifacts...${NC}"

    # Stop services
    if check_port; then
        do_stop
    fi
    do_stream_stop 2>/dev/null || true

    rm -rf .next node_modules "$VENV_DIR" "$LOG_DIR"
    rm -f "$PID_FILE" "$STREAMER_PID_FILE"
    rm -f polymarket_streaming.log
    rm -rf __pycache__ .pytest_cache

    echo -e "${GREEN}Cleaned. Run '$0 install' to reinstall.${NC}"
}

# ============================================================
# Main
# ============================================================

print_banner

case "${1:-help}" in
    install)          do_install ;;
    setup)            do_setup ;;
    start)            do_start ;;
    stop)             do_stop ;;
    restart)          do_restart ;;
    status)           do_status ;;
    dev)              do_dev ;;
    build)            do_build ;;
    prod|production)  do_prod ;;
    stream)           do_stream ;;
    stream-once)      do_stream_once ;;
    stream-stop)      do_stream_stop ;;
    stream-logs)      do_stream_logs ;;
    test)             do_test ;;
    validate)         do_validate ;;
    test-api)         do_test_api ;;
    test-auth)        do_test_auth ;;
    check-data)       do_check_data ;;
    logs)             do_logs ;;
    clean)            do_clean ;;
    help|--help|-h)   print_usage ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        echo ""
        print_usage
        exit 1
        ;;
esac
