#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
DS_FOLDER=${DS_FOLDER:-ds}

DIFF=${DIFF:-diff}

# The test case here is that the input file contains different non-ASCII characters
# such as emojis and other special symbols to ensure that the process script properly
# handles them

if $DIFF <(cat "$T_FOLDER"/"$DS_FOLDER"/d4.txt | c/process.sh | sort) <(sort "$T_FOLDER"/"$DS_FOLDER"/d5.txt) >&2;
then
    echo "$0 success: texts are identical"
    exit 0
else
    echo "$0 failure: texts are not identical"
    exit 1
fi

