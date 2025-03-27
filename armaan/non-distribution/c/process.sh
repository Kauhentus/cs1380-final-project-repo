#!/bin/bash

# Convert input to a stream of non-stopword terms
# Usage: ./process.sh < input > output

# Convert each line to one word per line, **remove non-letter characters**,
# make lowercase, convert to ASCII; then remove stopwords (inside d/stopwords.txt)
# Commands that will be useful: tr, iconv, grep

# The first command takes the complement of the set of A-Za-z characters and squashes them into newlines
tr -cs "[:upper:][:lower:]" '\n' |
# The second command converts all uppercase letters to lowercase
 tr "[:upper:]" "[:lower:]" |
 # The third command converts the input to ASCII
  iconv -f utf-8 -t ascii//TRANSLIT |
  # The fourth command removes stopwords
   grep -v -w -f d/stopwords.txt |
   grep -v '^$'


