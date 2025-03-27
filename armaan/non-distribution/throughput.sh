#!/bin/bash
#
# throughput.sh

# start tracking time
SCRIPT_START_TIME=$(date +%s.%N)

# 2) We'll keep separate accumulators and counters:
TOTAL_CRAWL_TIME=0         # Sum of crawl durations for each URL
TOTAL_INDEX_TIME=0         # Sum of index durations for each URL
COUNT=0            # How many URLs crawled

cd "$(dirname "$0")" || exit 1

while read -r url; do

  # If 'stop' is encountered, break out of loop.
  if [[ "$url" == "stop" ]]; then
    echo "[throughput] Detected 'stop' command. Exiting."
    break
  fi

  echo "[throughput] Crawling $url" >&2

  # 3) Time the crawler
  CRAWL_START_TIME=$(date +%s.%N)
  ./crawl.sh "$url" > d/content.txt
  CRAWL_END_TIME=$(date +%s.%N)

  # 4) Calculate how long the crawler took for this URL
  CRAWL_ELAPSED=$(echo "$CRAWL_END_TIME - $CRAWL_START_TIME" | bc)
  # 5) Accumulate total crawl time
  TOTAL_CRAWL_TIME=$(echo "$TOTAL_CRAWL_TIME + $CRAWL_ELAPSED" | bc)
  # Increase the count of crawled URLs
  COUNT=$((COUNT + 1))

  echo "[throughput] Indexing $url" >&2

  # 6) Time the indexer
  INDEX_START_TIME=$(date +%s.%N)
  ./index.sh d/content.txt "$url"
  INDEX_END_TIME=$(date +%s.%N)

  # 7) Calculate how long the indexer took for this URL
  INDEX_ELAPSED=$(echo "$INDEX_END_TIME - $INDEX_START_TIME" | bc)
  # 8) Accumulate total index time
  TOTAL_INDEX_TIME=$(echo "$TOTAL_INDEX_TIME + $INDEX_ELAPSED" | bc)


  # 9) Stop if we've visited all known URLs
  if [[ "$(wc -l < d/visited.txt)" -ge "$(wc -l < d/urls.txt)" ]]; then
    echo "[throughput] All known URLs have been visited. Exiting."
    break
  fi

done < <(tail -f d/urls.txt)

# 10) Final time for the entire script
SCRIPT_END_TIME=$(date +%s.%N)
TOTAL_ELAPSED=$(echo "$SCRIPT_END_TIME - $SCRIPT_START_TIME" | bc)

echo "Total runtime: $TOTAL_ELAPSED seconds"
echo

# 11) Show stats for the crawler
echo "Crawler Stats:"
echo "  - total URLs crawled: $COUNT"
if (( $(echo "$TOTAL_CRAWL_TIME > 0" | bc -l) )); then
  CRAWL_THROUGHPUT=$(echo "scale=2; $COUNT / $TOTAL_CRAWL_TIME" | bc)
  echo "  - Total crawl time: ${TOTAL_CRAWL_TIME}s"
  echo "  - Crawl throughput: $CRAWL_THROUGHPUT URLs/s"
else
  echo "failed"
fi

# 12) Show stats for the indexer
echo "Indexer Stats:"
echo "  - URLs indexed: $COUNT"
if (( $(echo "$TOTAL_INDEX_TIME > 0" | bc -l) )); then
  INDEX_THROUGHPUT=$(echo "scale=2; $COUNT / $TOTAL_INDEX_TIME" | bc)
  echo "  - Total index time: ${TOTAL_INDEX_TIME}s"
  echo "  - Index throughput: $INDEX_THROUGHPUT URLs/s"
else
  echo "failed"
fi