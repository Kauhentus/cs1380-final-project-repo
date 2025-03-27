#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
DS_FOLDER=${DS_FOLDER:-ds}

DIFF=${DIFF:-diff}
DIFF_PERCENT=${DIFF_PERCENT:-0}

cat /dev/null > d/global-index.txt

# The special test case here is that I am actually trying to induce a failure and make sure that
# the merge.js script properly handles errors gracefully

local_index="$T_FOLDER"/"$DS_FOLDER"/local-index.txt
global_indices=("$T_FOLDER"/"$DS_FOLDER"/global-index{1..2}.txt)


for global_index in "${global_indices[@]}"
do
    cat "$local_index" | c/merge.js "$global_index" 2>> "$T_FOLDER"/"$DS_FOLDER"/d6.txt
done


if DIFF_PERCENT=$DIFF_PERCENT t/gi-diff.js <(cat "$T_FOLDER"/"$DS_FOLDER"/d6.txt) <(cat "$T_FOLDER"/"$DS_FOLDER"/d7.txt) >&2;
then
    echo "$0 success: global indexes are identical"
    exit 0
else
    echo "$0 failure: global indexes are not identical"
    exit 1
fi
