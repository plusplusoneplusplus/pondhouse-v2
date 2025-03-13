#!/bin/bash

# Configuration
PROJECT_ROOT="$(git rev-parse --show-toplevel)"
BUILD_DIR="${PROJECT_ROOT}/build"
RUN_DIR="${PROJECT_ROOT}/.run"
GRPC_SERVER="${BUILD_DIR}/pond_server"
WEB_SERVER_DIR="${PROJECT_ROOT}/src/server/web"
GRPC_HOST="127.0.0.1"
GRPC_PORT="50051"
WEB_PORT="8000"
DB_NAME="pondhouse_test_db"
DB_PATH="${RUN_DIR}/${DB_NAME}"
LOG_FILE="${RUN_DIR}/pondhouse_servers.log"
GENERATED_PB_DIR="${RUN_DIR}/pb"

# Text formatting
BOLD="\033[1m"
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
RESET="\033[0m"

# Initialize variables
build_project=0
server_pids=()

# Function to print information
print_info() {
    echo -e "${GREEN}${BOLD}[INFO]${RESET} $1"
}

# Function to print warnings
print_warning() {
    echo -e "${YELLOW}${BOLD}[WARNING]${RESET} $1"
}

# Function to print errors
print_error() {
    echo -e "${RED}${BOLD}[ERROR]${RESET} $1"
}

# Function to clean up on exit
cleanup() {
    print_info "Shutting down servers..."
    
    for pid in "${server_pids[@]}"; do
        if ps -p $pid > /dev/null; then
            print_info "Killing process with PID: $pid"
            kill -15 $pid 2>/dev/null || kill -9 $pid 2>/dev/null
        fi
    done
    
    print_info "Cleanup complete. Exiting."
    exit 0
}

# Register the cleanup function on script exit
trap cleanup EXIT INT TERM

# Create run directory if it doesn't exist
mkdir -p "$RUN_DIR"
mkdir -p "$GENERATED_PB_DIR"

# Parse command-line options
while [[ $# -gt 0 ]]; do
    case $1 in
        --build)
            build_project=1
            shift
            ;;
        --grpc-port)
            GRPC_PORT="$2"
            shift 2
            ;;
        --web-port)
            WEB_PORT="$2"
            shift 2
            ;;
        --db-name)
            DB_NAME="$2"
            shift 2
            ;;
        --run-dir)
            RUN_DIR="$2"
            DB_PATH="${RUN_DIR}/${DB_NAME}"
            LOG_FILE="${RUN_DIR}/pondhouse_servers.log"
            GENERATED_PB_DIR="${RUN_DIR}/pb"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --build          Build the project before starting servers"
            echo "  --grpc-port PORT Set gRPC server port (default: 50051)"
            echo "  --web-port PORT  Set web server port (default: 8000)"
            echo "  --db-name NAME   Set database name (default: pondhouse_test_db)"
            echo "  --run-dir DIR    Set directory for generated files (default: PROJECT_ROOT/.run)"
            echo "  --help           Show this help message"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

# Print banner
echo -e "${GREEN}${BOLD}=======================================${RESET}"
echo -e "${GREEN}${BOLD}    PondHouse Development Servers${RESET}"
echo -e "${GREEN}${BOLD}=======================================${RESET}"
echo ""

# Build the project if requested
if [ $build_project -eq 1 ]; then
    print_info "Building project..."
    
    if [ ! -d "$BUILD_DIR" ]; then
        mkdir -p "$BUILD_DIR"
        cd "$BUILD_DIR"
        cmake ..
    fi
    
    cd "$BUILD_DIR"
    cmake --build .
    
    if [ $? -ne 0 ]; then
        print_error "Build failed. Aborting."
        exit 1
    fi
    
    print_info "Build completed successfully."
fi

# Check if GRPC server executable exists
if [ ! -f "$GRPC_SERVER" ]; then
    print_error "gRPC server executable not found at: $GRPC_SERVER"
    print_error "Please build the project first using --build option or manually."
    exit 1
fi

cd "$RUN_DIR"

# Configure core dumps BEFORE starting server
print_info "Configuring core dump settings..."
ulimit -c unlimited
mkdir -p "${RUN_DIR}/cores"
# Add fallback pattern if sudo fails
(sudo sysctl -w "kernel.core_pattern=${RUN_DIR}/cores/core.%e.%p.%t" >/dev/null 2>&1 || \
 sudo sysctl -w "kernel.core_pattern=core.%e.%p.%t" >/dev/null 2>&1 || true)

# Verify core dump configuration
echo -e "\nCurrent core dump settings:"
ulimit -a | grep 'core file size'
echo "Core pattern: $(cat /proc/sys/kernel/core_pattern 2>/dev/null)"
echo "Core dump directory: ${RUN_DIR}/cores"

print_info "Starting gRPC server on $GRPC_HOST:$GRPC_PORT..."
"$GRPC_SERVER" --address "$GRPC_HOST:$GRPC_PORT" --db_name "$DB_NAME" --db_path "$DB_PATH" > "$LOG_FILE" 2>&1 &
GRPC_PID=$!
server_pids+=($GRPC_PID)

# Check if gRPC server started successfully (give it a moment to start)
sleep 1
if ! ps -p $GRPC_PID > /dev/null; then
    print_error "gRPC server failed to start. Check $LOG_FILE for details."
    exit 1
fi

# Enable core dumps
print_info "Starting web server on port $WEB_PORT..."
cd "$WEB_SERVER_DIR"
print_info "Activate python venv"
source venv/bin/activate

# Ensure static directory exists
if [ ! -d "${WEB_SERVER_DIR}/static" ]; then
    print_info "Creating static files directory"
    mkdir -p "${WEB_SERVER_DIR}/static"
fi

# config the pb2 files
print_info "Configuring pb2 files"
python3 -m grpc_tools.protoc -I${PROJECT_ROOT}/src/proto --python_out=${GENERATED_PB_DIR} --grpc_python_out=${GENERATED_PB_DIR} ${PROJECT_ROOT}/src/proto/pond_service.proto

# Copy or symlink the generated files to the web directory
if [ -f "${GENERATED_PB_DIR}/pond_service_pb2.py" ] && [ -f "${GENERATED_PB_DIR}/pond_service_pb2_grpc.py" ]; then
    print_info "Copying generated protobuf files to web server directory"
    cp ${GENERATED_PB_DIR}/pond_service_pb2*.py ${WEB_SERVER_DIR}/
else
    print_error "Failed to generate protobuf files"
    exit 1
fi

print_info "Start web server"
GRPC_HOST="$GRPC_HOST" GRPC_PORT="$GRPC_PORT" WEB_PORT="$WEB_PORT" python3 main.py --port "$WEB_PORT" >> "$LOG_FILE" 2>&1 &
WEB_PID=$!
server_pids+=($WEB_PID)

# Check if web server started successfully
sleep 1
if ! ps -p $WEB_PID > /dev/null; then
    print_error "Web server failed to start. Check $LOG_FILE for details."
    # Kill gRPC server since we're exiting
    kill -15 $GRPC_PID 2>/dev/null || kill -9 $GRPC_PID 2>/dev/null
    exit 1
fi

# Print success message
echo ""
print_info "Servers started successfully!"
echo -e "  • gRPC Server: ${BOLD}$GRPC_HOST:$GRPC_PORT${RESET} (PID: $GRPC_PID)"
echo -e "  • Web Server:  ${BOLD}http://localhost:$WEB_PORT${RESET} (PID: $WEB_PID)"
echo -e "  • Database:    ${BOLD}$DB_NAME${RESET}"
echo -e "  • DB Path:     ${BOLD}$DB_PATH${RESET}"
echo -e "  • Run Dir:     ${BOLD}$RUN_DIR${RESET}"
echo -e "  • Log File:    ${BOLD}$LOG_FILE${RESET}"
echo ""
print_info "Press Ctrl+C to shut down both servers."
echo ""

# Keep the script running until Ctrl+C is pressed
while true; do
    # Check if both servers are still running
    if ! ps -p $GRPC_PID > /dev/null; then
        print_error "gRPC server (PID $GRPC_PID) has stopped unexpectedly. Check logs."
        break
    fi
    if ! ps -p $WEB_PID > /dev/null; then
        print_error "Web server (PID $WEB_PID) has stopped unexpectedly. Check logs."
        break
    fi
    sleep 2
done