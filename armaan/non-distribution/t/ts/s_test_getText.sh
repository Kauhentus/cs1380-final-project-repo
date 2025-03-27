#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
DS_FOLDER=${DS_FOLDER:-ds}

DIFF=${DIFF:-diff}

INPUT_FILE=$T_FOLDER/$DS_FOLDER/d12.txt
EXPECTED_OUTPUT_FILE=$T_FOLDER/$DS_FOLDER/d13.txt

if $DIFF <(cat "$INPUT_FILE" | c/getText.js) <(cat "$EXPECTED_OUTPUT_FILE") >&2;
then
    echo "Test Passed: The actual output matches the expected output."
    exit 0
else
    echo "Test Failed: The actual output differs from the expected output."
    exit 1
fi
