#!/usr/bin/env bash

# Default values for options
RUN_SCENARIOS=false
RUN_EXTRA_CREDIT=false
RUN_NORMAL_TESTS=false
INCLUDE_NON_DISTRIBUTION=false

# Function to show usage
show_usage() {
    cat << EOF
Usage: npm test -- [options | pattern]

Pattern:
  A pattern to match the test files. This can be a substring of the test file name.
  If no pattern is provided, tests will be run based on the options.

Options:
  -s, --scenarios         Run the scenario tests (*.scenario.js)
  -ec, --extra-credit     Run the extra credit tests (*.extra.test.js)
  -t, --tests             Run the normal tests (*.test.js)
  -nd, --non-distribution Run the non-distribution tests
  -h, --help              Show this help message and exit

By default:
  - Only regular tests (*.test.js) run if no options are passed.
  - Tests in "non-distribution" are excluded unless -nd is specified.
EOF
}

PATTERN=""

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--scenarios)
            RUN_SCENARIOS=true
            shift
            ;;
        -ec|--extra-credit)
            RUN_EXTRA_CREDIT=true
            shift
            ;;
        -t|--tests)
            RUN_NORMAL_TESTS=true
            shift
            ;;
        -nd|--non-distribution)
            INCLUDE_NON_DISTRIBUTION=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            PATTERN="$1"
            shift
            ;;
    esac
done

# Construct the JEST command
JEST_COMMAND="jest --maxWorkers=1"
JEST_COMMAND_FLAGS=""

# Add test matching logic
if $RUN_SCENARIOS; then
    JEST_COMMAND_FLAGS+=" --testMatch \"**/*.scenario.js\""
fi

if $RUN_EXTRA_CREDIT; then
    JEST_COMMAND_FLAGS+=" --testMatch \"**/*.extra.test.js\""
fi

if $RUN_NORMAL_TESTS || (! $RUN_SCENARIOS && ! $RUN_EXTRA_CREDIT && ! $RUN_NORMAL_TESTS); then
    JEST_COMMAND_FLAGS+=" --testMatch \"**/*.test.js\""
fi

# Exclude "non-distribution" tests by default unless -nd is specified
if ! $INCLUDE_NON_DISTRIBUTION; then
    JEST_COMMAND_FLAGS+=" --testPathIgnorePatterns \"non-distribution\""
fi

if ! $RUN_EXTRA_CREDIT; then
    JEST_COMMAND_FLAGS+=" --testPathIgnorePatterns \"extra\""
fi

# Run the constructed jest command with the rest of the arguments
# shellcheck disable=SC2294
if [ -n "$PATTERN" ]; then
    eval "$JEST_COMMAND --testMatch \"**/*$PATTERN*\""
    exit $?
else
    eval "$JEST_COMMAND $JEST_COMMAND_FLAGS"
    exit $?
fi

TOP_LEVEL=$(git rev-parse --show-toplevel)

# Check if the student is using the reference implementation (useLibrary in package.json is true)
if [ "$(jq -r '.useLibrary' "$TOP_LEVEL/package.json")" = "true" ]; then
    echo "[test] WARNING: You are using the reference implementation. Make sure to set useLibrary to false in package.json to use your own implementation."
    exit 1
fi
