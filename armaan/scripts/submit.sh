#!/usr/bin/env bash

log() 
{
    if [ "$2" = "-n" ]; then
	printf "[submit] %s" "$1"
    else
	printf "[submit] %s\n" "$1"
    fi
}

[[ ! -x "$(command -v zip)" ]] && log "zip not found, please install it" && exit 1
[[ ! -x "$(command -v git)" ]] && log "git not found, please install it" && exit 1

# Check if we are in a git repository
git ls-files > /dev/null || exit 1

TOP_LEVEL=$(git rev-parse --show-toplevel)
cd "$TOP_LEVEL" || exit 1

TARGET_FOLDER="submission"
SUBMISSION_FILE="submission.zip"

log "creating submission..."

[ -f $SUBMISSION_FILE ] && rm -f $SUBMISSION_FILE
[ -d $TARGET_FOLDER ] && rm -rf $TARGET_FOLDER
mkdir -p "$TARGET_FOLDER"

git ls-files | while IFS='' read -r file
do 
    mkdir -p $TARGET_FOLDER/"$(dirname "$file")"
    cp "$file" $TARGET_FOLDER/"$(dirname "$file")"
done

log "copied files to submission folder"

cd "$TARGET_FOLDER" && zip -r "$TOP_LEVEL"/"$SUBMISSION_FILE" . || exit 1
cd "$TOP_LEVEL" || exit 1

log "created submission: $SUBMISSION_FILE"

[[ -d $TARGET_FOLDER ]] && rm -rf $TARGET_FOLDER
[[ -f "$SUBMISSION_FILE" ]] || exit 1

log "you can now upload $SUBMISSION_FILE to the autograder!"
