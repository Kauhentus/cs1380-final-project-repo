#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
DS_FOLDER=${DS_FOLDER:-ds}

DIFF=${DIFF:-diff}

INPUT_FILE=$T_FOLDER/$DS_FOLDER/d14.txt
EXPECTED_OUTPUT_FILE=$T_FOLDER/$DS_FOLDER/d15.txt

# This file contains edge cases such as empty lines, duplicate terms, and special characters
# to ensure the output is consistent with what is defined in combine.sh 

# Compare the actual output with the expected output
if $DIFF <(cat "$INPUT_FILE" | c/combine.sh) <(cat "$EXPECTED_OUTPUT_FILE") >&2;
then
    echo "Test Passed: The actual output matches the expected output."
    exit 0
else
    echo "Test Failed: The actual output differs from the expected output."
    exit 1
fi