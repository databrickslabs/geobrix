#!/bin/bash

# Start haveged in the background
service haveged start

# Execute the command passed to the entrypoint (which is /bin/bash by default)
# The 'exec' command ensures that /bin/bash becomes the main process (PID 1)
exec "$@"