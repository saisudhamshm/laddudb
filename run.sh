#!/bin/sh
#
# Use this script to run your program LOCALLY.
#
# This script compiles and runs a Go-based Redis implementation.

set -e # Exit early if any commands fail

# Compile the Go program
#
# - Edit this to change how your program compiles locally
(
  cd "$(dirname "$0")" # Ensure compile steps are run within the repository directory
  go build -o /tmp/redis-go app/*.go
)

# Run the compiled program
#
# - Edit this to change how your program runs locally
exec /tmp/redis-go "$@"
