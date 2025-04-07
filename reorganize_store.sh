#!/bin/bash

# Script to reorganize store directory structure
# Will move all files in each hash directory into a tfidf subdirectory

# Navigate to the store directory
cd /home/ec2-user/cs1380-final-project-repo/store

echo "Starting file reorganization..."

# For each hash directory in the store
for dir in */ ; do
  # Remove trailing slash
  dir=${dir%/}
  
  echo "Processing directory: $dir"
  
  # Create tfidf directory if it doesn't exist
  mkdir -p "$dir/tfidf"
  
  # Count files to be moved
  file_count=$(find "$dir" -maxdepth 1 -type f | wc -l)
  echo "Found $file_count files to move in $dir"
  
  # Move all files directly under the hash directory to the tfidf directory
  # excluding subdirectories
  find "$dir" -maxdepth 1 -type f -exec mv {} "$dir/tfidf/" \;
  
  # Verify files were moved
  moved_count=$(find "$dir/tfidf" -type f | wc -l)
  echo "Moved $moved_count files to $dir/tfidf/"
done

echo "File reorganization complete."