#!/bin/bash

# List of migrations in "OLD_NAME:NEW_NAME" format
MIGRATIONS=(
    "geobrix-base:geobrix-checkpoint:base"
    "geobrix-system-deps:geobrix-checkpoint:system-deps"
    "geobrix-hadoop-builder:geobrix-checkpoint:hadoop-builder"
    "geobrix-gdal-builder:geobrix-checkpoint:gdal-builder"
    "geobrix-pdal-builder:geobrix-checkpoint:pdal-builder"
    "geobrix-final:geobrix-checkpoint:final"
)

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${CYAN}Migrating Geobrix Image Tags (macOS Compatibility Mode)...${NC}"

for item in "${MIGRATIONS[@]}"; do
    # Split the string by the colon
    OLD_NAME=$(echo $item | cut -d':' -f1)
    NEW_NAME=$(echo $item | cut -d':' -f2-4) # Handles multiple colons in the new name

    # Check if the old image actually exists
    if [ "$(docker images -q "$OLD_NAME" 2> /dev/null)" ]; then
        echo -e "Found ${YELLOW}$OLD_NAME${NC}. Tagging as ${GREEN}$NEW_NAME${NC}..."
        docker tag "$OLD_NAME" "$NEW_NAME"
    else
        echo -e "Skipping $OLD_NAME (not found)."
    fi
done

echo -e "${GREEN}Migration Complete!${NC}"