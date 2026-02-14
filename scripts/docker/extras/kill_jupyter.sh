#!/bin/bash
echo "--- Cleaning up Geobrix Environment ---"

# 1. Kill any hanging python kernel processes
pkill -9 -f ipykernel_launcher || echo "No hanging kernels found."

# 2. Remove Jupyter runtime files (where connection JSONs live)
rm -rf /tmp/jupyter_runtime/*
rm -rf /root/.local/share/jupyter/runtime/*

# 3. Clean up shared memory (prevents ZMQ deadlocks)
find /dev/shm -user root -delete 2>/dev/null || true

echo "Cleanup complete."