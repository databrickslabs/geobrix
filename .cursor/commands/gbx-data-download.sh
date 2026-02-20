#!/bin/bash
# gbx:data:download - Download sample geospatial data

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📦 GeoBrix: Download Sample Data"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:data:download${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--bundle <type>${NC}    Which bundle to download (essential|complete|both)"
    echo -e "                     ${YELLOW}essential${NC}: ~355MB (GeoJSON, GeoTIFF, SRTM) [default]"
    echo -e "                     ${YELLOW}complete${NC}:  ~795MB (+ Shapefiles, GPKG, FileGDB, GRIB2)"
    echo -e "                     ${YELLOW}both${NC}:      Download both bundles"
    echo -e "  ${GREEN}--force${NC}            Re-download even if files exist"
    echo -e "  ${GREEN}--log <path>${NC}       Write output to log file"
    echo -e "  ${GREEN}--help${NC}             Show this help"
    echo ""
    echo -e "${CYAN}Log Path Behavior:${NC}"
    echo -e "  ${YELLOW}filename.log${NC}       → test-logs/filename.log"
    echo -e "  ${YELLOW}subdir/file.log${NC}    → test-logs/subdir/file.log"
    echo -e "  ${YELLOW}/abs/path/file.log${NC} → /abs/path/file.log"
    echo ""
    echo -e "${CYAN}Output Directory:${NC}"
    echo -e "  ${YELLOW}sample-data/Volumes/main/default/geobrix_samples/geobrix-examples/${NC}"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:data:download${NC}"
    echo -e "  ${YELLOW}gbx:data:download --bundle complete${NC}"
    echo -e "  ${YELLOW}gbx:data:download --bundle both --log data-download.log${NC}"
    echo -e "  ${YELLOW}gbx:data:download --force${NC}"
    echo ""
}

# Parse arguments
BUNDLE="essential"
FORCE_FLAG=""
LOG_PATH=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --bundle)
            BUNDLE="$2"
            if [[ ! "$BUNDLE" =~ ^(essential|complete|both)$ ]]; then
                echo -e "${RED}❌ Invalid bundle type: $BUNDLE${NC}"
                echo "Must be: essential, complete, or both"
                exit 1
            fi
            shift 2
            ;;
        --force)
            FORCE_FLAG="--force"
            shift
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            echo ""
            show_help
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"

show_banner "📦 GeoBrix: Download Sample Data"
check_docker
setup_log_file "$LOG_PATH"

if [ -n "$FORCE_FLAG" ]; then
    echo -e "${YELLOW}⚠️  Force mode enabled - will re-download existing files${NC}"
fi

EXIT_CODE=0

if [[ "$BUNDLE" == "essential" || "$BUNDLE" == "both" ]]; then
    echo -e "${CYAN}📥 Downloading Essential Bundle (~355MB)...${NC}"
    show_separator
    echo ""
    
    docker exec geobrix-dev python3 /root/geobrix/sample-data/download-essential-bundle.py $FORCE_FLAG
    ESSENTIAL_EXIT=$?
    EXIT_CODE=$((EXIT_CODE + ESSENTIAL_EXIT))
    
    echo ""
    show_separator
    if [ $ESSENTIAL_EXIT -eq 0 ]; then
        echo -e "${GREEN}✅ Essential bundle downloaded!${NC}"
    else
        echo -e "${RED}❌ Essential bundle failed (exit code: $ESSENTIAL_EXIT)${NC}"
    fi
    show_separator
    echo ""
fi

if [[ "$BUNDLE" == "complete" || "$BUNDLE" == "both" ]]; then
    echo -e "${CYAN}📥 Downloading Complete Bundle (~795MB)...${NC}"
    show_separator
    echo ""
    
    docker exec geobrix-dev python3 /root/geobrix/sample-data/download-complete-bundle.py $FORCE_FLAG
    COMPLETE_EXIT=$?
    EXIT_CODE=$((EXIT_CODE + COMPLETE_EXIT))
    
    echo ""
    show_separator
    if [ $COMPLETE_EXIT -eq 0 ]; then
        echo -e "${GREEN}✅ Complete bundle downloaded!${NC}"
    else
        echo -e "${RED}❌ Complete bundle failed (exit code: $COMPLETE_EXIT)${NC}"
    fi
    show_separator
    echo ""
fi

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}🎉 Sample data ready!${NC}"
    echo ""
    echo -e "${CYAN}📂 Location:${NC}"
    echo -e "  ${YELLOW}sample-data/Volumes/main/default/geobrix_samples/geobrix-examples/${NC}"
    echo ""
    echo -e "${CYAN}📊 What's inside:${NC}"
    if [[ "$BUNDLE" == "essential" || "$BUNDLE" == "both" ]]; then
        echo -e "  ✅ GeoJSON files (NYC boroughs, neighborhoods, etc.)"
        echo -e "  ✅ GeoTIFF rasters (Sentinel-2 imagery)"
        echo -e "  ✅ SRTM elevation data"
    fi
    if [[ "$BUNDLE" == "complete" || "$BUNDLE" == "both" ]]; then
        echo -e "  ✅ Zipped Shapefiles (.shp.zip)"
        echo -e "  ✅ GeoPackage (.gpkg)"
        echo -e "  ✅ FileGDB (.gdb.zip)"
        echo -e "  ✅ GRIB2 weather data"
    fi
fi

if [ -n "$LOG_PATH" ]; then
    echo ""
    echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
