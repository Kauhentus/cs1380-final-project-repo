#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
DS_FOLDER=${DS_FOLDER:-ds}

DIFF=${DIFF:-diff}

term="quantum"

cat "$T_FOLDER"/"$DS_FOLDER"/global-index.txt > d/global-index.txt


if $DIFF <(./query.js "$term") <(cat "$T_FOLDER"/"$DS_FOLDER"/d3.txt) >&2;
then
    echo "$0 success: search results are identical"
    exit 0
else
    echo "$0 failure: search results are not identical"
    exit 1
fi