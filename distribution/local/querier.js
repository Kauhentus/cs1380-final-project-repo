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

metrics = {
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

  queryGrowthData: {
    queries: {},
    timestamp: Date.now(),
  },
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

  metrics = {
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

  try {
    if (fs.existsSync(metrics_file_path)) {
      const metricsContent = fs.readFileSync(metrics_file_path, "utf8");
      if (metricsContent) {
        const old_metrics = JSON.parse(metricsContent);
        if (Array.isArray(old_metrics) && old_metrics.length > 0) {
          metrics = old_metrics[old_metrics.length - 1]; // get the last one to update most recent metrics
          // console.log(
          //   `Loaded existing metrics from ${metrics_file_path}:`,
          //   metrics
          // );
        } else {
          // console.log(
          //   `Found metrics file but no valid metrics data, using defaults`
          // );
        }
      }
    } else {
      // console.log(
      //   `No metrics file found at ${metrics_file_path}, creating new one`
      // );
      fs.writeFileSync(metrics_file_path, JSON.stringify([metrics], null, 2));
    }
  } catch (error) {
    console.error(`Error loading metrics: ${error.message}`);
  }

  fs.appendFileSync(
    global.logging_path,
    `QUERIER METRICS INITIALIZED: ${JSON.stringify(metrics)}\n`
  );

  metricsInterval = setInterval(async () => {
    try {
      metrics.time_since_previous = Date.now() - metrics.current_time;
      metrics.current_time = Date.now();

      // Josh metrics collection copy
      if (metrics.processing_times.length > 0) {
        const sum = metrics.processing_times.reduce((a, b) => a + b, 0);
        metrics.avgResponseTime = sum / metrics.processing_times.length;
      }

      let old_metrics = [];
      try {
        if (fs.existsSync(metrics_file_path)) {
          const content = fs.readFileSync(metrics_file_path, "utf8");
          if (content) {
            old_metrics = JSON.parse(content);
            if (!Array.isArray(old_metrics)) {
              old_metrics = [];
            }
          }
        }
      } catch (readError) {
        console.error(`Error reading metrics file: ${readError.message}`);
        old_metrics = [];
      }

      old_metrics.push({ ...metrics });

      // console.log(
      //   `Saving metrics: queriesProcessed=${metrics.queriesProcessed}, totalQueryTime=${metrics.totalQueryTime}ms`
      // );

      await fsp.writeFile(
        metrics_file_path,
        JSON.stringify(old_metrics, null, 2)
      );

      metrics.processing_times = [];
    } catch (saveError) {
      console.error(`Error saving metrics: ${saveError.message}`);
    }
  }, 60000);

  callback(null, {
    status: "success",
    message: "Querier service initialized",
    metrics: metrics,
  });
}

function query_one(queryConfiguration, callback) {
  const fs = require("fs");
  callback = callback || cb;

  fs.appendFileSync(
    global.logging_path,
    `QUERIER RECEIVED QUERY: ${JSON.stringify(queryConfiguration)}\n`
  );

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
  // console.log(`Starting query for: ${query} at ${queryStartTime}`);

  distribution.local.store.read_bulk(bulkReadConfig, (error, prefixData) => {
    const queryTime = Date.now() - queryStartTime;

    metrics.totalQueryTime += queryTime;
    metrics.queriesProcessed += 1;
    metrics.processing_times.push(queryTime);

    fs.appendFileSync(
      global.logging_path,
      `QUERIER METRICS UPDATED: queriesProcessed=${metrics.queriesProcessed}, totalQueryTime=${metrics.totalQueryTime}ms\n`
    );

    if (error) {
      metrics.failedQueries += 1;
      fs.appendFileSync(
        global.logging_path,
        `QUERIER INFO: No data found for prefix '${prefix}' (${queryTime}ms)\n`
      );
      return callback(null, {
        prefix: prefix,
        queryTerms: terms,
        totalMatches: 0,
        termStatistics: {},
        results: [],
      });
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

      for (const [__, data] of Object.entries(docScores)) {
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

      const totalResultCount = results.length;
      const topResults = results.slice(0, 100); // TODO: maybe configurable limit

      const response = {
        prefix: prefix,
        queryTerms: terms,
        totalMatches: totalResultCount,
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

function log_query_growth(data, callback) {
  callback = callback || cb;

  try {
    const { timestamp, query, count } = data;

    const logDir = path.join("crawler-files", "metrics");
    const logFile = path.join(
      logDir,
      `query_growth_${global.nodeConfig.port}.log`
    );

    if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });

    if (!metrics.queryGrowthData) {
      metrics.queryGrowthData = {
        queries: {},
        timestamp: Date.now(),
      };
    }

    const currentStoredData = metrics.queryGrowthData.queries[query];
    const currentCount = currentStoredData ? currentStoredData.count : 0;

    if (count > currentCount) {
      const logLine = `${new Date(timestamp).toISOString()},${query},${count},${
        global.nodeConfig.port
      },${currentCount}\n`;

      if (!fs.existsSync(logFile)) {
        fs.writeFileSync(
          logFile,
          "timestamp,query,count,node,previous_count\n"
        );
      }

      fs.appendFileSync(logFile, logLine);

      metrics.queryGrowthData.queries[query] = {
        timestamp,
        count,
      };
      metrics.queryGrowthData.timestamp = timestamp;

      fs.appendFileSync(
        global.logging_path,
        `QUERY GROWTH UPDATE: ${query}, count increased from ${currentCount} to ${count}\n`
      );
    } else {
      fs.appendFileSync(
        global.logging_path,
        `QUERY GROWTH IGNORED: ${query}, current count ${count} <= previous ${currentCount}\n`
      );
    }

    callback(null, true);
  } catch (error) {
    console.error("Error in log_query_growth:", error);
    fs.appendFileSync(
      global.logging_path,
      `ERROR IN QUERY GROWTH LOG: ${error}\n`
    );
    callback(error);
  }
}

function get_stats(callback) {
  callback = callback || cb;

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
    },

    metrics: metrics,

    queryGrowthData: metrics.queryGrowthData,
  };

  callback(null, stats);
}

module.exports = {
  initialize,
  query_one,
  query_range,
  log_query_growth,
  get_stats,
};
