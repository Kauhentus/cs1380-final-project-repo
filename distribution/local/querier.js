const fs = require("fs");
const path = require("path");
const fsp = require("fs/promises");

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};

let metrics = {
  totalQueryTime: 0,
  queriesProcessed: 0,
  resultsReturned: 0,
  failedQueries: 0,
  emptyResultQueries: 0,
  totalRangeQueryTime: 0,
  rangeQueriesProcessed: 0,
  peakMemoryUsage: 0,
  avgResponseTime: 0,
  processing_times: [],
  current_time: Date.now(),
  time_since_previous: 0,
};

function initialize(callback) {
  callback = callback || cb;
  fs.appendFileSync(
    global.logging_path,
    `QUERIER INITIALIZING... ${new Date()}\n`
  );

  const crawlerDir = path.join("crawler-files");
  const metricsDir = path.join(crawlerDir, "metrics");
  if (!fs.existsSync(crawlerDir)) fs.mkdirSync(crawlerDir, { recursive: true });
  if (!fs.existsSync(metricsDir)) fs.mkdirSync(metricsDir, { recursive: true });
  const metrics_file_path = path.join(
    metricsDir,
    `metrics-querier-${global.nodeConfig.port}.json`
  );

  if (fs.existsSync(metrics_file_path)) {
    const old_metrics = JSON.parse(
      fs.readFileSync(metrics_file_path).toString()
    );
    metrics = old_metrics.at(-1);
  } else {
    fs.writeFileSync(metrics_file_path, JSON.stringify([metrics], null, 2));
  }

  metricsInterval = setInterval(async () => {
    metrics.time_since_previous = Date.now() - metrics.current_time;
    metrics.current_time = Date.now();

    const memUsage = process.memoryUsage();
    metrics.peakMemoryUsage = Math.max(
      metrics.peakMemoryUsage,
      Math.round(memUsage.heapUsed / 1024 / 1024)
    );

    if (metrics.processing_times.length > 0) {
      const sum = metrics.processing_times.reduce((a, b) => a + b, 0);
      metrics.avgResponseTime = sum / metrics.processing_times.length;
    }

    const old_metrics = JSON.parse(
      fs.readFileSync(metrics_file_path).toString()
    );
    old_metrics.push(metrics);
    await fsp.writeFile(
      metrics_file_path,
      JSON.stringify(old_metrics, null, 2)
    );

    metrics.processing_times = [];
  }, 60000);

  callback();
}

function query_one(queryConfiguration, callback) {
  const fs = require("fs");
  callback = callback || cb;
  const { terms, prefix, totalDocCount } = queryConfiguration;
  const query = terms.join(" ");

  fs.appendFileSync(
    global.logging_path,
    `QUERIER QUERYING... ${query}:  ${terms}\n`
  );

  function calculateIDF(df, totalDocuments) {
    return Math.log((totalDocuments + 1) / (df + 1)) + 1;
  }

  function combineScores(docScores, termScores) {
    for (const [docId, score] of Object.entries(termScores)) {
      if (docScores[docId]) {
        docScores[docId].score += 2.5 * score;
        docScores[docId].matchedTerms += 1;
      } else {
        docScores[docId] = {
          score: score,
          matchedTerms: 1,
        };
      }
    }
    return docScores;
  }

  const bulkReadConfig = {
    key: `prefix-${prefix}.json`,
    gid: "indexer_group",
  };
  const queryStartTime = Date.now();
  console.log(`Starting query for: ${query} at ${queryStartTime}`);

  distribution.local.store.read_bulk(bulkReadConfig, (error, prefixData) => {
    const queryTime = Date.now() - queryStartTime;
    metrics.totalQueryTime += queryTime;
    metrics.queriesProcessed += 1;
    metrics.processing_times.push(queryTime);
    if (error) {
      metrics.failedQueries += 1;
      fs.appendFileSync(
        global.logging_path,
        `QUERIER ERROR: ${error} (${queryTime}ms)\n`
      );
      return callback(error);
    }

    try {
      const parsedBulk = JSON.parse(prefixData);

      let docScores = {};
      let termDeets = {};
      const docTermMetaData = {};

      for (const term of terms) {
        if (!parsedBulk[term]) {
          continue;
        }

        const termData = parsedBulk[term];
        const df = termData.df;
        const idf = calculateIDF(df, totalDocCount);

        termDeets[term] = {
          df: df,
          idf: idf,
          documents: Object.keys(termData.postings).length,
        };
        const termScores = {};

        for (const [docId, posting] of Object.entries(termData.postings)) {
          let score = posting.tf * idf;

          if (posting.ranking) {
            score *= posting.ranking.taxonomyBoost || 1.0;
            score *= posting.ranking.binomialBoost || 1.0;
            score *= posting.ranking.positionBoost || 1.0;
          }

          termScores[docId] = score;
          docTermMetaData[docId] = {
            taxonomyLevel: posting ? posting.taxonomyLevel : null,
            isBinomial: posting ? posting.isBinomial : null,
            pageInfo: posting ? posting.pageInfo : null,
          };
        }

        docScores = combineScores(docScores, termScores);
      }

      for (const [docId, data] of Object.entries(docScores)) {
        if (data.matchedTerms > 1) {
          data.score *= Math.pow(2.0, data.matchedTerms - 1);

          const matchRatio = data.matchedTerms / terms.length;
          if (matchRatio > 0.5) {
            data.score *= 1 + matchRatio;
          }
        }
      }

      const results = Object.entries(docScores).map(([docId, data]) => {
        return {
          docId: docId,
          score: data.score,
          matchedTerms: data.matchedTerms,
          matchRatio: data.matchedTerms / terms.length,
          termDetails: docTermMetaData[docId] || {},
        };
      });

      results.sort((a, b) => {
        if (b.matchedTerms !== a.matchedTerms) {
          return b.matchedTerms - a.matchedTerms;
        }
        return b.score - a.score;
      });

      const topResults = results.slice(0, 100); // TODO: maybe configurable limit

      const response = {
        prefix: prefix,
        queryTerms: terms,
        totalMatches: results.length,
        termStatistics: termDeets,
        results: topResults,
      };

      metrics.resultsReturned += topResults.length;

      if (results.length === 0) {
        metrics.emptyResultQueries += 1;
      }

      fs.appendFileSync(
        global.logging_path,
        `QUERIER SUCCESS: ${results.length} results (${queryTime}ms)\n`
      );

      callback(null, response);
    } catch (error) {
      metrics.failedQueries += 1;
      fs.appendFileSync(
        global.logging_path,
        `QUERIER ERROR PROCESSING: ${error} (${queryTime}ms)\n`
      );
      console.error(`Error processing term details: ${error}`);
      callback(error);
    }
  });
}

function query_range(query, depth, visited, options, callback) {
  const rangeQueryStartTime = Date.now();

  query = query.trim().toLowerCase();
  const return_tree = options.return_tree || false;

  function getChosenNode(key, nids, nodes) {
    const kid = distribution.util.id.getID(key);
    const chosenNID = distribution.util.id.naiveHash(kid, nids);
    const chosenNode = nodes.find(
      (nc) => distribution.util.id.getNID(nc) === chosenNID
    );
    return chosenNode;
  }

  const this_nid = distribution.util.id.getNID(global.nodeConfig);
  const prefix = query.slice(0, 2);
  const prefix_file = `./store/${this_nid}/indexer_ranged_group/${prefix}.json`;
  metrics.rangeQueriesProcessed += 1;

  try {
    const data = fs
      .readFileSync(prefix_file, "utf8")
      .split("\n")
      .filter((line) => line.trim() !== "")
      .map((line) => line.split(" => "))
      .filter((line_parts) => line_parts[0] === query);

    const results = return_tree ? {} : [];

    (async () => {
      const potential_new_queries = data.map((line_parts) => line_parts[1]);
      const new_results = potential_new_queries.filter((query) =>
        query.includes("[SPECIES]")
      );

      distribution.local.groups.get("indexer_ranged_group", async (e, v) => {
        const new_queries = potential_new_queries
          .filter((query) => !query.includes("[SPECIES]"))
          .filter((query) => !visited.includes(query));
        new_queries.map((query) => visited.push(query));

        const new_query_results = await Promise.all(
          new_queries.map(
            (query) =>
              new Promise((resolve, reject) => {
                const nodes = Object.values(v);
                const nids = nodes.map((node) =>
                  distribution.util.id.getNID(node)
                );
                const chosen_node = getChosenNode(query, nids, nodes);

                distribution.local.comm.send(
                  [query, depth + 1, visited, options],
                  {
                    service: "querier",
                    method: "query_range",
                    node: chosen_node,
                  },
                  (err, val) => {
                    if (typeof val === "object") {
                      resolve(val);
                    } else {
                      console.error(err);
                      resolve(return_tree ? {} : []);
                    }
                  }
                );
              })
          )
        );

        if (return_tree) {
          results.name = query;
          results.children = new_query_results;
          new_results.map((query) =>
            results.children.push({ name: query, is_species: true })
          );
        } else {
          results.push(...new_results);
          results.push(...new_query_results.flat());
        }

        callback(null, results);
      });
    })();
  } catch (e) {
    metrics.failedQueries += 1;
    const rangeQueryTime = Date.now() - rangeQueryStartTime;
    metrics.totalRangeQueryTime += rangeQueryTime;

    fs.appendFileSync(
      global.logging_path,
      `RANGE QUERY ERROR: ${query}, ${error} (${rangeQueryTime}ms)\n`
    );

    callback(error, return_tree ? {} : []);
  }
}

function get_stats(callback) {
  callback = callback || cb;

  // Calculate derived metrics
  const successfulQueries = metrics.queriesProcessed - metrics.failedQueries;
  const avgResultsPerQuery =
    successfulQueries > 0 ? metrics.resultsReturned / successfulQueries : 0;
  const avgQueryTime =
    metrics.queriesProcessed > 0
      ? metrics.totalQueryTime / metrics.queriesProcessed
      : 0;
  const avgRangeQueryTime =
    metrics.rangeQueriesProcessed > 0
      ? metrics.totalRangeQueryTime / metrics.rangeQueriesProcessed
      : 0;

  // Get current memory usage
  const memUsage = process.memoryUsage();
  const currentHeapUsed = Math.round(memUsage.heapUsed / 1024 / 1024);

  // Update peak memory usage if needed
  if (currentHeapUsed > metrics.peakMemoryUsage) {
    metrics.peakMemoryUsage = currentHeapUsed;
  }

  // Return current stats
  const stats = {
    queriesProcessed: metrics.queriesProcessed,
    rangeQueriesProcessed: metrics.rangeQueriesProcessed,
    totalQueries: metrics.queriesProcessed + metrics.rangeQueriesProcessed,
    failedQueries: metrics.failedQueries,
    emptyResultQueries: metrics.emptyResultQueries,
    resultsReturned: metrics.resultsReturned,

    performance: {
      avgQueryTime: avgQueryTime,
      avgRangeQueryTime: avgRangeQueryTime,
      avgResultsPerQuery: avgResultsPerQuery,
      peakMemoryUsage: metrics.peakMemoryUsage,
      currentMemoryUsage: currentHeapUsed,
    },

    // Return raw metrics for complete stats aggregation
    metrics: metrics,
  };

  console.log("Current Stats:", stats);

  callback(null, stats);
}

module.exports = {
  initialize,
  query_one,
  query_range,
  get_stats,
};
