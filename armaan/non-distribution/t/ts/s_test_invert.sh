#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
DS_FOLDER=${DS_FOLDER:-ds}

DIFF=${DIFF:-diff}

URL="patankar.com"
INPUT_FILE=$T_FOLDER/$DS_FOLDER/d8.txt
EXPECTED_OUTPUT_FILE=$T_FOLDER/$DS_FOLDER/d9.txt

# The input file contains different variations of the same phrase with different spacing and tabs
# to ensure the output is consistent with what is defined in invert.sh 

# Compare the actual output with the expected output
if $DIFF <(cat "$INPUT_FILE" | c/invert.sh $URL) <(cat "$EXPECTED_OUTPUT_FILE") >&2;
then
    echo "Test Passed: The actual output matches the expected output."
    exit 0
else
    echo "Test Failed: The actual output differs from the expected output."
    exit 1
fi