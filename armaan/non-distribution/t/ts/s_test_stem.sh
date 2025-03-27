#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
DS_FOLDER=${DS_FOLDER:-ds}

DIFF=${DIFF:-diff}


if $DIFF <(cat "$T_FOLDER"/"$DS_FOLDER"/d1.txt | c/stem.js | sort) <(sort "$T_FOLDER"/"$DS_FOLDER"/d2.txt) >&2;
then
    echo "$0 success: stemmed words are identical"
    exit 0
else
    echo "$0 failure: stemmed words are not identical"
    exit 1
fi
