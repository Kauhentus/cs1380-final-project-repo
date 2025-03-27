#!/usr/bin/env bash

# Check if we are inside a git repository, otherwise exit
if ! git rev-parse --is-inside-work-tree &> /dev/null; then
    echo "[pretest] not insde a git repository, make sure your project is under git version control" >&2
    exit 1
fi

TOP_LEVEL=$(git rev-parse --show-toplevel)
cd "$TOP_LEVEL" || exit 1

# Check if node_modules exist in the current directory
INSTALLATION_OK=0

if [ ! -d "node_modules" ]; then
    echo "[pretest] please run 'npm install' in the root directory of your project" >&2
    INSTALLATION_OK=1
fi

if [ ! -d "non-distribution/node_modules" ]; then
    echo "[pretest] please run 'npm install' in the 'non-distribution' folder" >&2
    INSTALLATION_OK=1
fi

exit $INSTALLATION_OK
