#!/bin/bash
# gbx:docker:exec - Execute commands in geobrix-dev container

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"

# Default values
MODE=""
COMMAND=""
INTERACTIVE=false
LOG_FILE=""

# Help message
show_help() {
    cat << EOF
$(print_banner "🐳 GeoBrix: Docker Exec")

Execute commands or launch interactive shells in geobrix-dev container

USAGE:
    bash .cursor/commands/gbx-docker-exec.sh [MODE|COMMAND] [OPTIONS]

INTERACTIVE SHELL MODES:
    --spark              Launch Spark shell (spark-shell)
    --pyspark            Launch PySpark shell
    --python             Launch Python 3 shell
    --scala              Launch Scala REPL
    --bash               Launch interactive bash shell

COMMAND EXECUTION:
    <command>            Execute bash command and exit
    --command <cmd>      Execute bash command and exit (explicit)

OPTIONS:
    --interactive        Run command in interactive mode (keep TTY)
    --log <path>         Write output to log file (non-interactive only)
    --help               Display this help message

EXAMPLES:
    # Interactive shells
    bash .cursor/commands/gbx-docker-exec.sh --spark
    bash .cursor/commands/gbx-docker-exec.sh --pyspark
    bash .cursor/commands/gbx-docker-exec.sh --python
    bash .cursor/commands/gbx-docker-exec.sh --scala
    bash .cursor/commands/gbx-docker-exec.sh --bash

    # Execute commands
    bash .cursor/commands/gbx-docker-exec.sh "ls -la /root/geobrix"
    bash .cursor/commands/gbx-docker-exec.sh "mvn -version"
    bash .cursor/commands/gbx-docker-exec.sh --command "python3 --version"

    # Execute with logging
    bash .cursor/commands/gbx-docker-exec.sh "mvn test" --log maven-test.log

NOTES:
    - Requires geobrix-dev container to be running
    - Interactive shells use -it flag (TTY)
    - Command execution uses standard docker exec
    - Logging only available for non-interactive commands

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --spark)
            MODE="spark"
            shift
            ;;
        --pyspark)
            MODE="pyspark"
            shift
            ;;
        --python)
            MODE="python"
            shift
            ;;
        --scala)
            MODE="scala"
            shift
            ;;
        --bash)
            MODE="bash"
            shift
            ;;
        --command)
            COMMAND="$2"
            shift 2
            ;;
        --interactive)
            INTERACTIVE=true
            shift
            ;;
        --log)
            LOG_FILE=$(resolve_log_path "$2")
            shift 2
            ;;
        --help)
            show_help
            ;;
        *)
            # Treat as command if no mode set
            if [ -z "$MODE" ] && [ -z "$COMMAND" ]; then
                COMMAND="$1"
            else
                echo "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
            fi
            shift
            ;;
    esac
done

# Setup logging if requested (only for non-interactive)
if [ -n "$LOG_FILE" ] && [ -z "$MODE" ] && [ "$INTERACTIVE" = false ]; then
    setup_log "$LOG_FILE"
fi

# Print banner (skip for interactive modes to keep clean terminal)
if [ -z "$MODE" ]; then
    print_banner "🐳 GeoBrix: Docker Exec"
fi

# Check Docker
check_docker

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
    echo -e "${RED}❌ Container 'geobrix-dev' is not running${NC}"
    echo -e "${YELLOW}   Start with: gbx:docker:start${NC}"
    exit 1
fi

# Execute based on mode
if [ -n "$MODE" ]; then
    # Interactive shell modes
    case $MODE in
        spark)
            echo -e "${CYAN}🚀 Launching Spark shell...${NC}"
            echo -e "${YELLOW}   (Exit with Ctrl+D or :quit)${NC}"
            echo ""
            docker exec -it geobrix-dev spark-shell
            ;;
        pyspark)
            echo -e "${CYAN}🐍 Launching PySpark shell...${NC}"
            echo -e "${YELLOW}   (Exit with Ctrl+D or exit())${NC}"
            echo ""
            docker exec -it geobrix-dev pyspark
            ;;
        python)
            echo -e "${CYAN}🐍 Launching Python 3 shell...${NC}"
            echo -e "${YELLOW}   (Exit with Ctrl+D or exit())${NC}"
            echo ""
            docker exec -it geobrix-dev python3
            ;;
        scala)
            echo -e "${CYAN}⚡ Launching Scala REPL...${NC}"
            echo -e "${YELLOW}   (Exit with Ctrl+D or :quit)${NC}"
            echo ""
            docker exec -it geobrix-dev scala
            ;;
        bash)
            echo -e "${CYAN}💻 Launching bash shell...${NC}"
            echo -e "${YELLOW}   (Exit with Ctrl+D or exit)${NC}"
            echo ""
            docker exec -it geobrix-dev bash
            ;;
    esac
elif [ -n "$COMMAND" ]; then
    # Command execution
    if [ -z "$MODE" ]; then
        print_separator
        echo -e "${CYAN}📝 Executing command...${NC}"
        print_separator
    fi
    
    if [ "$INTERACTIVE" = true ]; then
        docker exec -it geobrix-dev bash -c "$COMMAND"
    else
        docker exec geobrix-dev bash -c "$COMMAND"
    fi
    
    EXIT_CODE=$?
    
    if [ -z "$MODE" ]; then
        print_separator
        if [ $EXIT_CODE -eq 0 ]; then
            echo -e "${GREEN}✅ Command completed successfully${NC}"
        else
            echo -e "${RED}❌ Command failed with exit code: $EXIT_CODE${NC}"
        fi
        print_separator
    fi
    
    exit $EXIT_CODE
else
    echo -e "${RED}❌ No mode or command specified${NC}"
    echo -e "${YELLOW}   Use --help for usage information${NC}"
    exit 1
fi
