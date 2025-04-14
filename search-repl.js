const readline = require("readline");
const distribution = require("./config.js");
const fs = require("fs");
const id = distribution.util.id;

const num_nodes = 4;
const nodes = [];
const nids = [];
const crawler_group = {};
const crawler_group_config = { gid: "crawler_group", hash: id.naiveHash };
const indexer_group = {};
const indexer_group_config = { gid: "indexer_group", hash: id.naiveHash };
const indexer_ranged_group = {};
const indexer_ranged_group_config = {
  gid: "indexer_ranged_group",
  hash: id.naiveHash,
};
const querier_group = {};
const querier_group_config = { gid: "querier_group", hash: id.naiveHash };
let isInRecoveryMode = false;
let lastOperationTime = 0;
const OPERATION_COOLDOWN = 2000; // 2 seconds

const log_and_append = (string) => {
  console.log(string);
  fs.appendFileSync("log.txt", string + "\n");
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Setup nodes
for (let i = 0; i < num_nodes; i++) {
  nodes.push({ ip: "127.0.0.1", port: 7110 + i });
  nids.push(id.getNID(nodes[i]));

  const sid = id.getSID(nodes[i]);
  crawler_group[sid] = nodes[i];
  indexer_group[sid] = nodes[i];
  indexer_ranged_group[sid] = nodes[i];
  querier_group[sid] = nodes[i];
}

function isEmptyObject(obj) {
  return obj && typeof obj === "object" && Object.keys(obj).length === 0;
}

distribution.node.start(async (server) => {
  // Utility function to get the target node for a link
  const get_nx = (link) => {
    // console.log(link);
    return nodes[parseInt(id.getID(link).slice(0, 8), 16) % num_nodes];
  };

  // System metrics trackers
  let startTime = Date.now();

  // Initialize your distributed system
  const spawn_nx = (nx) =>
    new Promise((resolve) => {
      distribution.local.status.spawn(nx, (e, v) => {
        resolve(e, v);
      });
    });

  const init_group = (group, config) =>
    new Promise((resolve) => {
      distribution.local.groups.put(config, group, (e, v) => {
        distribution[config.gid].groups.put(config, group, (e, v) => {
          resolve();
        });
      });
    });

  const run_remote = (group_name, remote, args = []) =>
    new Promise((resolve) => {
      distribution[group_name].comm.send(args, remote, (e, v) => {
        resolve();
      });
    });

  let spinnerInterval;
  const startSpinner = (message, indent = 0) => {
    const spinChars = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
    let i = 0;
    // ?? \r puts back the beginning of the line
    process.stdout.write(`\r${" ".repeat(indent)}${message} ${spinChars[0]}`);
    spinnerInterval = setInterval(() => {
      i = (i + 1) % spinChars.length;
      process.stdout.write(`\r${" ".repeat(indent)}${message} ${spinChars[i]}`);
    }, 100);
    // TODO: should I call stop spinner in here?
  };

  const stopSpinner = () => {
    clearInterval(spinnerInterval);
    process.stdout.write("\r\x1b[K"); // Clear the line
  };

  console.log("Initializing distributed search engine...");
  startSpinner("Starting nodes");
  for (let i = 0; i < num_nodes; i++) await spawn_nx(nodes[i]);
  stopSpinner();
  console.log("\x1b[32m✓\x1b[0m Nodes started");

  startSpinner("Initializing groups");
  await init_group(crawler_group, crawler_group_config);
  await init_group(indexer_group, indexer_group_config);
  await init_group(indexer_ranged_group, indexer_ranged_group_config);
  await init_group(querier_group, querier_group_config);
  stopSpinner();
  console.log("\x1b[32m✓\x1b[0m Groups initialized");

  startSpinner("Initializing services");
  // Now we need to initlaize all of the services
  await run_remote("crawler_group", {
    gid: "local",
    service: "crawler",
    method: "initialize",
  });
  await run_remote("indexer_group", {
    gid: "local",
    service: "indexer",
    method: "initialize",
  });
  await run_remote("indexer_ranged_group", {
    gid: "local",
    service: "indexer_ranged",
    method: "initialize",
  });
  await run_remote("querier_group", {
    gid: "local",
    service: "querier",
    method: "initialize",
  });
  stopSpinner();
  console.log("\x1b[32m✓\x1b[0m Services initialized");
  console.log("\x1b[32m✓\x1b[0m Distributed search engine is ready!\n");

  startSpinner("Adding initial seed link");
  // TODO: idk if u agree here, but i started with all categories so we cover all for the initial seed links
  await Promise.all([
    new Promise((resolve, reject) => {
      const link = "/wiki/Cnidaria";
      // console.log(`Seeding link: ${link} to node ${get_nx(link).port}`);
      const remote = {
        node: get_nx(link),
        gid: "local",
        service: "crawler",
        method: "add_link_to_crawl",
      };
      distribution.local.comm.send([link], remote, (e, v) => {
        const remote = {
          node: get_nx(link),
          gid: "local",
          service: "crawler",
          method: "save_maps_to_disk",
        };
        distribution.local.comm.send([], remote, (e, v) => {
          resolve();
        });
      });
    }),
    new Promise((resolve, reject) => {
      const link = "/wiki/Plant";
      // console.log(`Seeding link: ${link} to node ${get_nx(link).port}`);
      const remote = {
        node: get_nx(link),
        gid: "local",
        service: "crawler",
        method: "add_link_to_crawl",
      };
      distribution.local.comm.send([link], remote, (e, v) => {
        const remote = {
          node: get_nx(link),
          gid: "local",
          service: "crawler",
          method: "save_maps_to_disk",
        };
        distribution.local.comm.send([], remote, (e, v) => {
          resolve();
        });
      });
    }),
    new Promise((resolve, reject) => {
      const link = "/Lepidoptera";
      // console.log(`Seeding link: ${link} to node ${get_nx(link).port}`);
      const remote = {
        node: get_nx(link),
        gid: "local",
        service: "crawler",
        method: "add_link_to_crawl",
      };
      distribution.local.comm.send([link], remote, (e, v) => {
        const remote = {
          node: get_nx(link),
          gid: "local",
          service: "crawler",
          method: "save_maps_to_disk",
        };
        distribution.local.comm.send([], remote, (e, v) => {
          resolve();
        });
      });
    }),
  ]);
  stopSpinner();
  console.log(
    "\x1b[32m✓\x1b[0m Added initial seed links for \x1b[32mPlants\x1b[0m, \x1b[34mSealife\x1b[0m, and \x1b[35mButterflies\x1b[0m!"
  );

  const headerLine = (text) => "=".repeat(text.length + 4);
  const formatTime = (ms) => {
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(2)}s`;
  };

  async function addLinkToCrawl(link) {
    // TODO: or should we write a function to fix the user input
    if (!link.startsWith("/wiki/")) {
      console.log("\x1b[31mError: Link must start with '/wiki/'\x1b[0m");
      return;
    }

    return new Promise((resolve) => {
      startSpinner(`Adding link ${link} to crawler`);

      distribution.local.comm.send(
        [link],
        {
          node: get_nx(link),
          gid: "local",
          service: "crawler",
          method: "add_link_to_crawl",
        },
        (err, result) => {
          stopSpinner();

          if (err) {
            console.log(`\x1b[31mError adding link: ${err}\x1b[0m`);
          } else if (result && result.status === "skipped") {
            console.log(
              `\x1b[33mLink ${link} was skipped: ${result.reason}\x1b[0m`
            );
          } else {
            console.log(
              `\x1b[32mSuccessfully added ${link} to crawler queue\x1b[0m`
            );
          }

          resolve();
        }
      );
    });
  }

  function logDescriptionTitle(result) {
    return new Promise((detailResolve) => {
      distribution.crawler_group.store.get(result.docId, (err, data) => {
        if (err || !data) {
          return detailResolve();
        }

        const title = data.title || data.binomial_name || "Unknown Title";

        console.log(headerLine(`DETAILED INFORMATION: ${title}`));
        console.log(`| DETAILED INFORMATION: ${title} |`);
        console.log(headerLine(`DETAILED INFORMATION: ${title}`));

        if (data.binomial_name) {
          console.log(`\nBinomial name: \x1b[1m${data.binomial_name}\x1b[0m`);
        }

        if (data.hierarchy && data.hierarchy.length > 0) {
          console.log("\nTaxonomic Classification:");
          data.hierarchy.forEach((entry) => {
            if (Array.isArray(entry) && entry.length === 2) {
              console.log(`  ${entry[0]}: ${entry[1]}`);
            }
          });
        }

        if (data.description) {
          console.log("\nDescription:");
          console.log(data.description);
        }

        detailResolve();
      });
    });
  }

  // Query execution function
  async function executeQuery(queryString) {
    console.log("\nExecuting query...");
    const startTime = Date.now();

    return new Promise((resolve) => {
      console.log(
        `Sending query "${queryString}" to querier_group at ${new Date().toISOString()}`
      );

      distribution.querier_group.querier.query_one(
        queryString,
        {},
        async (e, v) => {
          const queryTime = Date.now() - startTime;

          if (e) {
            console.error("\x1b[31mQuery failed:\x1b[0m", e);
            return resolve();
          }

          console.log(headerLine("QUERY RESULTS"));
          console.log(`| ${v.query} |`);
          console.log(headerLine("QUERY RESULTS"));
          console.log(`Terms searched: ${v.terms.join(", ")}`);
          console.log(
            `Total results: ${v.totalResults} (query took ${formatTime(
              queryTime
            )})\n`
          );

          if (!v.topResults || v.topResults.length === 0) {
            console.log("No results found for this query.");
            return resolve();
          }

          console.log(headerLine("TOP RESULTS"));
          console.log(`| TOP RESULTS |`);
          console.log(headerLine("TOP RESULTS"));

          v.topResults.slice(0, 5).forEach((result, index) => {
            const pageInfo = result.termDetails?.pageInfo || {};
            const title =
              pageInfo.binomialName || result.docId.replace("/wiki/", "");

            console.log(`${index + 1}. \x1b[1m${title.toUpperCase()}\x1b[0m`);
            console.log(`   URL: https://www.wikipedia.org${result.docId}`);
            console.log(
              `   Score: ${result.score.toFixed(4)} (matched ${
                result.matchedTerms
              }/${v.terms.length} terms)`
            );

            if (pageInfo.kingdom)
              console.log(`   Kingdom: ${pageInfo.kingdom}`);
            if (pageInfo.family) console.log(`   Family: ${pageInfo.family}`);

            if (result.termDetails?.taxonomyLevel) {
              console.log(
                `   \x1b[32m* BOOSTED! Taxonomy match: ${result.termDetails.taxonomyLevel}\x1b[0m`
              );
            }

            if (result.termDetails?.isBinomial) {
              console.log(
                `   \x1b[32m* BOOSTED! Term appears in binomial name\x1b[0m`
              );
            }

            console.log("");
          });

          // TODO: Put this in a clickable, interactive component using a library
          // TODO: Decide if I want to keep the logging of both the title and the description since we
          // TODO: are now printing this for all of the results (want to find a balance between information and simplicity)

          if (v.topResults.length > 0) {
            const topResult = v.topResults[0];
            try {
              await logDescriptionTitle(topResult);
            } catch (detailError) {
              console.error(
                "Error processing detailed information:",
                detailError
              );
            }
          }

          resolve();
        }
      );
    });
  }

  async function executeRangeQuery(taxonomyTerm, options = {}) {
    console.log(`\nExploring taxonomy tree for: ${taxonomyTerm}`);
    const startTime = Date.now();

    // Default options
    const defaultOptions = {
      collapseSpecies: false, // Whether to collapse species nodes
      maxDepth: 10, // Maximum depth to display
    };

    const finalOptions = { ...defaultOptions, ...options };

    return new Promise((resolve) => {
      distribution.querier_group.querier.query_range(
        taxonomyTerm,
        { return_tree: true },
        async (err, results) => {
          if (err) {
            console.error("\x1b[31mError exploring taxonomy:\x1b[0m", err);
            return resolve();
          }

          if (!results || !results.name) {
            console.log(`No taxonomy information found for "${taxonomyTerm}"`);
            return resolve();
          }

          // Print taxonomy tree statistics
          const queryTime = Date.now() - startTime;

          // Track counts for reporting
          let speciesCount = 0;
          let taxaCount = 0;

          // Recursive printing function
          const printTree = (
            node,
            depth = 0,
            isLastChild = true,
            prefix = ""
          ) => {
            if (depth > finalOptions.maxDepth) return;

            const isSpecies = node.is_species;
            const hasChildren = node.children && node.children.length > 0;
            const numSpeciesChildren = hasChildren
              ? node.children.filter((child) => child.is_species).length
              : 0;

            // Format node name
            let nodeName = isSpecies
              ? node.name.replace("[SPECIES] /wiki/", "")
              : node.name;

            if (finalOptions.collapseSpecies && numSpeciesChildren > 0) {
              nodeName += ` (${numSpeciesChildren} species)`;
            }

            // Count nodes by type
            if (isSpecies) {
              speciesCount++;
              nodeName = "\x1b[32m*" + nodeName + "\x1b[0m"; // Green for species
            } else {
              taxaCount++;
              nodeName = "\x1b[33m" + nodeName + "\x1b[0m"; // Yellow for taxa
            }

            // Print the current node
            if (depth === 0) {
              console.log(" ".repeat(depth * 2) + nodeName);
            } else {
              const branch = isLastChild ? "└─" : "├─";
              console.log(prefix + branch + nodeName);
            }

            // Prepare prefix for children
            const newPrefix = prefix + (isLastChild ? "   " : "│  ");

            // Print children
            if (node.children) {
              // Sort children: taxa first, then species, both alphabetically
              node.children.sort((a, b) => {
                if (a.is_species && !b.is_species) return 1;
                if (!a.is_species && b.is_species) return -1;
                return a.name.localeCompare(b.name);
              });

              // Print each child
              node.children.forEach((child, i) => {
                if (finalOptions.collapseSpecies && child.is_species) return;

                const isLast =
                  i === node.children.length - 1 ||
                  (finalOptions.collapseSpecies &&
                    i ===
                      node.children.filter((c) => !c.is_species).length - 1);

                printTree(child, depth + 1, isLast, newPrefix);
              });
            }
          };

          // Print the tree
          console.log("\n\x1b[1mTaxonomy Tree:\x1b[0m");
          printTree(results);

          // Print summary
          console.log(
            `\nFound \x1b[33m${taxaCount} taxa\x1b[0m and \x1b[32m${speciesCount} species\x1b[0m in ${queryTime}ms`
          );

          // Warn if hitting max depth
          if (taxaCount + speciesCount >= 100) {
            console.log(
              "\n\x1b[33mNote: Large taxonomy tree detected. You can refine your search with a more specific term.\x1b[0m"
            );
          }

          resolve();
        }
      );
    });
  }

  // Display help text
  function displayHelp() {
    console.log("\n\x1b[1mAvailable Commands:\x1b[0m");
    console.log(
      "  \x1b[36m<search query>\x1b[0m           - Search for terms in the index"
    );
    console.log(
      "  \x1b[36mcrawl /wiki/PAGE\x1b[0m         - Add a Wikipedia page to the crawler queue"
    );
    console.log(
      "  \x1b[36mtree TAXONOMY\x1b[0m            - Explore taxonomic hierarchy as a tree"
    );
    console.log(
      "  \x1b[36mstats\x1b[0m                    - Display system statistics"
    );
    console.log(
      "  \x1b[36msave\x1b[0m                     - Force save crawler and indexer state to disk"
    );
    console.log(
      "  \x1b[36mhelp\x1b[0m                     - Display this help message"
    );
    console.log(
      "  \x1b[36mexit\x1b[0m or \x1b[36mquit\x1b[0m             - Exit the REPL"
    );

    console.log("\n\x1b[1mSearch Tips:\x1b[0m");
    console.log("  - Try combining multiple terms for better results");
    console.log(
      "  - Terms found in taxonomy classification get higher relevance"
    );

    console.log("\n\x1b[1mTaxonomy Tree Options:\x1b[0m");
    console.log(
      "  - \x1b[36mtree plantae\x1b[0m                  - Display the plantae taxonomy tree"
    );
    console.log(
      "  - \x1b[36mtree cnidaria --collapse\x1b[0m      - Display tree with species collapsed"
    );
    console.log(
      "  - \x1b[36mtree lepidoptera --depth=3\x1b[0m    - Limit tree depth to 3 levels"
    );
  }

  const saveToDisk = async (indent = 0) => {
    startSpinner("Saving system state to disk", indent);

    try {
      await Promise.all([
        new Promise((resolve) => {
          distribution.crawler_group.comm.send(
            [],
            { gid: "local", service: "crawler", method: "save_maps_to_disk" },
            () => resolve()
          );
        }),
        new Promise((resolve) => {
          distribution.indexer_group.comm.send(
            [],
            { gid: "local", service: "indexer", method: "save_maps_to_disk" },
            () => resolve()
          );
        }),
      ]);

      stopSpinner();
      console.log(
        "    ".repeat(indent),
        "\x1b[32m✓\x1b[0m System state saved successfully"
      );
    } catch (error) {
      stopSpinner();
      console.error(
        "    ".repeat(indent),
        "\x1b[31mError saving system state:\x1b[0m",
        error
      );
    }
  };

  const stopNode = (node) => {
    new Promise((resolve) => {
      distribution.local.comm.send(
        [],
        { service: "status", method: "stop", node: node },
        () => resolve()
      );
    });
  };

  async function aggregateStats() {
    const aggregatedStats = {
      crawling: {
        docsInQueue: 0,
        totalCrawlTime: 0,
        pagesProcessed: 0,
        targetsHit: 0,
        errors: 0,
        throughput: 0,
      },
      indexing: {
        docsInQueue: 0,
        totalIndexTime: 0,
        documentsIndexed: 0,
        totalTermsProcessed: 0,
        totalPrefixesProcessed: 0,
        batchesSent: 0,
        errors: 0,
        throughput: 0,
      },
      rangeIndex: {
        docsInQueue: 0,
        totalIndexTime: 0,
        documentsIndexed: 0,
        errors: 0,
        throughput: 0,
      },
      querying: {
        queriesProcessed: 0,
        rangeQueriesProcessed: 0,
        totalQueries: 0,
        failedQueries: 0,
        emptyResultQueries: 0,
        resultsReturned: 0,
        avgQueryTime: 0,
        avgRangeQueryTime: 0,
        avgResultsPerQuery: 0,
        peakMemoryUsage: 0,
      },
    };
    return new Promise((resolve, reject) => {
      console.log(`\n Fetching stats from all services...`);
      distribution.crawler_group.crawler.get_stats((e, v1) => {
        // console.log("Crawler stats received");
        distribution.indexer_group.indexer.get_stats((e, v2) => {
          // console.log("Indexer stats received");
          distribution.indexer_ranged_group.indexer_ranged.get_stats(
            (e3, v3) => {
              // console.log("Ranged indexer stats received");
              // console.log("Requesting querier stats from all nodes...");
              distribution.querier_group.querier.get_stats((e4, v4) => {
                // console.log("Querier stats received:", v4);
                Object.keys(v1).map((key) => {
                  aggregatedStats.crawling.docsInQueue +=
                    v1[key].links_to_crawl;
                  const nodeMetrics = v1[key].metrics.crawling;
                  if (nodeMetrics) {
                    aggregatedStats.crawling.totalCrawlTime +=
                      nodeMetrics.totalCrawlTime || 0;
                    aggregatedStats.crawling.pagesProcessed +=
                      nodeMetrics.pagesProcessed || 0;
                    aggregatedStats.crawling.targetsHit +=
                      nodeMetrics.targetsHit || 0;
                    aggregatedStats.crawling.throughput +=
                      nodeMetrics.pagesProcessed /
                        (nodeMetrics.totalCrawlTime / 1000) || 0;
                  }
                });

                Object.keys(v2).map((key) => {
                  aggregatedStats.indexing.docsInQueue +=
                    v2[key].links_to_index;
                  const nodeMetrics = v2[key].metrics;
                  if (nodeMetrics) {
                    aggregatedStats.indexing.totalIndexTime +=
                      nodeMetrics.totalIndexTime || 0;
                    aggregatedStats.indexing.documentsIndexed +=
                      nodeMetrics.documentsIndexed || 0;
                    aggregatedStats.indexing.totalTermsProcessed +=
                      nodeMetrics.totalTermsProcessed || 0;
                    aggregatedStats.indexing.totalPrefixesProcessed = Math.min(
                      (nodeMetrics.totalPrefixesProcessed || 0) +
                        aggregatedStats.indexing.totalPrefixesProcessed,
                      6160
                    );
                    aggregatedStats.indexing.throughput +=
                      nodeMetrics.documentsIndexed /
                        (nodeMetrics.totalIndexTime / 1000) || 0;
                  }
                });

                Object.keys(v3).map((key) => {
                  aggregatedStats.rangeIndex.docsInQueue =
                    v3[key].links_to_range_index;
                  const nodeMetrics = v3[key].metrics;
                  if (nodeMetrics) {
                    aggregatedStats.rangeIndex.totalIndexTime +=
                      nodeMetrics.totalIndexTime || 0;
                    aggregatedStats.rangeIndex.documentsIndexed +=
                      nodeMetrics.documentsIndexed || 0;
                    aggregatedStats.rangeIndex.throughput +=
                      nodeMetrics.documentsIndexed /
                        (nodeMetrics.totalIndexTime / 1000) || 0;
                  }
                });
                if (v4) {
                  Object.keys(v4).forEach((key) => {
                    if (v4[key] && v4[key].queriesProcessed !== undefined) {
                      aggregatedStats.querying.queriesProcessed +=
                        v4[key].queriesProcessed || 0;
                      aggregatedStats.querying.rangeQueriesProcessed +=
                        v4[key].rangeQueriesProcessed || 0;
                      aggregatedStats.querying.failedQueries +=
                        v4[key].failedQueries || 0;
                      aggregatedStats.querying.emptyResultQueries +=
                        v4[key].emptyResultQueries || 0;
                      aggregatedStats.querying.resultsReturned +=
                        v4[key].resultsReturned || 0;

                      if (
                        v4[key].performance &&
                        v4[key].performance.peakMemoryUsage
                      ) {
                        aggregatedStats.querying.peakMemoryUsage = Math.max(
                          aggregatedStats.querying.peakMemoryUsage,
                          v4[key].performance.peakMemoryUsage
                        );
                      }

                      if (v4[key].metrics) {
                        const m = v4[key].metrics;
                        // console.log(`Node ${key} detailed metrics:`, m);

                        // Track query times by type for calculating averages
                        if (m.queriesProcessed > 0) {
                          aggregatedStats.querying.avgQueryTime +=
                            m.totalQueryTime / m.queriesProcessed;
                        }

                        if (m.rangeQueriesProcessed > 0) {
                          aggregatedStats.querying.avgRangeQueryTime +=
                            m.totalRangeQueryTime / m.rangeQueriesProcessed;
                        }
                      }
                    } else {
                      console.warn(`Node ${key} has invalid metrics:`, v4[key]);
                    }
                  });

                  aggregatedStats.querying.totalQueries =
                    aggregatedStats.querying.queriesProcessed +
                    aggregatedStats.querying.rangeQueriesProcessed;

                  const successfulQueries =
                    aggregatedStats.querying.queriesProcessed -
                    aggregatedStats.querying.failedQueries;

                  if (successfulQueries > 0) {
                    aggregatedStats.querying.avgResultsPerQuery =
                      aggregatedStats.querying.resultsReturned /
                      successfulQueries;
                  }

                  const nodeCount = Object.keys(v4).length;
                  if (nodeCount > 0) {
                    aggregatedStats.querying.avgQueryTime /= nodeCount;
                    aggregatedStats.querying.avgRangeQueryTime /= nodeCount;
                  }
                } else {
                  console.error("Error retrieving querier stats:", e);
                }

                // console.log("Final aggregated stats:", aggregatedStats);
                resolve(aggregatedStats);
              });
            }
          );
        });
      });
    });
  }

  const updatePrompt = () => {
    if (isInRecoveryMode) {
      rl.setPrompt("\x1b[33msearch(recovery)>\x1b[0m ");
    } else {
      rl.setPrompt("\x1b[36msearch>\x1b[0m ");
    }
    rl.prompt();
  };

  const main_metric_loop = () => {
    // console.log("PAUSING CORE SERVICES...\n");
    isInRecoveryMode = true;

    const t1 = Date.now();
    console.log(
      "\n\x1b[33mSystem is now in recovery mode. REPL remains available.\x1b[0m"
    );
    updatePrompt();

    new Promise((resolve) => {
      distribution.crawler_group.crawler.set_service_state(true, (e, v) => {
        distribution.indexer_group.indexer.set_service_state(true, (e, v) => {
          distribution.indexer_ranged_group.indexer_ranged.set_service_state(
            true,
            (e, v) => {
              resolve();
            }
          );
        });
      });
    }).then(() => {
      const t2 = Date.now();
      // log_and_append(`RECOVERY TIME FOR CORE SERVICES: ${t2 - t1}ms`);

      setTimeout(() => {
        // console.log("RESUMING CORE SERVICES...\n");
        // const t5 = Date.now();

        new Promise((resolve) => {
          distribution.crawler_group.crawler.set_service_state(
            false,
            (e, v) => {
              distribution.indexer_group.indexer.set_service_state(
                false,
                (e, v) => {
                  distribution.indexer_ranged_group.indexer_ranged.set_service_state(
                    false,
                    (e, v) => {
                      resolve();
                    }
                  );
                }
              );
            }
          );
        }).then(() => {
          // console.log(`  (RESUMED CORE SERVICES IN ${t6 - t5}ms)`);
          console.log(
            "\n\x1b[32mRecovery mode ended. System resumed normal operations.\x1b[0m"
          );
          isInRecoveryMode = false;
          updatePrompt();
        });
      }, 9000);
    });

    setTimeout(() => main_metric_loop(), 120000);
  };

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: "\x1b[36msearch>\x1b[0m ",
  });

  console.log(
    "\n\x1b[1;35m===== TAXI🚕 Distributed Search Engine REPL =====\x1b[0m"
  );
  console.log("Background crawling and indexing has been enabled!");
  console.log("Type 'help' to see available commands");
  rl.prompt();

  rl.on("line", async (line) => {
    const input = line.trim();
    const parts = input.split(" ");
    const command = parts[0].toLowerCase();

    if (input === "") {
      updatePrompt();
      return;
    }

    if (
      isInRecoveryMode &&
      command !== "help" &&
      command !== "exit" &&
      command !== "quit"
    ) {
      console.log(
        "\x1b[33mNote: System is currently in recovery mode. Some operations might be limited.\x1b[0m"
      );
    }

    if (command === "exit" || command === "quit") {
      console.log("Shutting down...");
      rl.close();

      startSpinner("Stopping nodes and saving state");

      try {
        await saveToDisk(1);

        for (let i = 0; i < num_nodes; i++) {
          await stopNode(nodes[i]);
        }
      } catch (e) {
        console.error(`Failed during shutdown:`, e);
      }

      stopSpinner();
      console.log("\x1b[32m✓\x1b[0m System shutdown complete");

      server.close();
      process.exit(0);
    } else if (command === "help") {
      displayHelp();
    } else if (command === "stats") {
      try {
        startSpinner("Collecting system statistics");
        await new Promise((resolve, reject) => {
          distribution.crawler_group.crawler.set_service_state(true, (e, v) => {
            distribution.indexer_group.indexer.set_service_state(
              true,
              (e, v) => {
                distribution.indexer_ranged_group.indexer_ranged.set_service_state(
                  true,
                  (e, v) => {
                    resolve();
                  }
                );
              }
            );
          });
        });

        const systemStats = await aggregateStats();
        stopSpinner();

        // Helper functions for consistent formatting
        const formatNumber = (num) => {
          return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
        };

        const createProgressBar = (value, max, length = 20) => {
          const percentage = Math.min(Math.max(value / max, 0), 1);
          const filledLength = Math.round(length * percentage);
          const emptyLength = length - filledLength;

          const filledPart = "█".repeat(filledLength);
          const emptyPart = "░".repeat(emptyLength);

          return `${filledPart}${emptyPart} ${(percentage * 100).toFixed(0)}%`;
        };

        // Color constants
        const RESET = "\x1b[0m";
        const BOLD = "\x1b[1m";
        const DIM = "\x1b[2m";
        const ITALIC = "\x1b[3m";
        const UNDERLINE = "\x1b[4m";

        const RED = "\x1b[31m";
        const GREEN = "\x1b[32m";
        const YELLOW = "\x1b[33m";
        const BLUE = "\x1b[34m";
        const MAGENTA = "\x1b[35m";
        const CYAN = "\x1b[36m";
        const WHITE = "\x1b[37m";

        const BG_MAGENTA = "\x1b[45m";

        // Header styling function
        const header = (text) => {
          console.log(
            `\n${BOLD}${CYAN}┌─ ${text} ${"─".repeat(
              40 - text.length
            )}┐${RESET}`
          );
        };

        // Begin displaying statistics
        const runtime = formatTime(Date.now() - startTime);

        console.log("\n");
        console.log(
          `${BG_MAGENTA}${WHITE}${BOLD} TAXI🚕 DISTRIBUTED SEARCH ENGINE - SYSTEM STATISTICS ${RESET}`
        );
        console.log(
          `${DIM}Runtime: ${runtime} | Generated at: ${new Date().toLocaleTimeString()}${RESET}`
        );

        // System summary
        header("SYSTEM SUMMARY");

        const crawlOps = systemStats.crawling.pagesProcessed || 0;
        const indexOps = systemStats.indexing.documentsIndexed || 0;
        const queryOps = systemStats.querying.totalQueries || 0;

        console.log(
          `${CYAN}┌─────────────────────────────────────────────┐${RESET}`
        );
        console.log(
          `${CYAN}│${RESET} Crawl Operations:     ${YELLOW}${formatNumber(
            crawlOps
          ).padStart(8)}${RESET}              ${CYAN}│${RESET}`
        );
        console.log(
          `${CYAN}│${RESET} Index Operations:     ${GREEN}${formatNumber(
            indexOps
          ).padStart(8)}${RESET}              ${CYAN}│${RESET}`
        );
        console.log(
          `${CYAN}│${RESET} Query Operations:     ${MAGENTA}${formatNumber(
            queryOps
          ).padStart(8)}${RESET}              ${CYAN}│${RESET}`
        );
        console.log(
          `${CYAN}└─────────────────────────────────────────────┘${RESET}`
        );

        // Crawler Statistics
        header("CRAWLER STATISTICS");

        const crawlerStats = systemStats.crawling;
        const totalPagesCrawled = crawlerStats.pagesProcessed || 0;
        const totalLinksQueued = crawlerStats.docsInQueue || 0;
        const avgCrawlTime = formatTime(
          crawlerStats.totalCrawlTime / (totalPagesCrawled || 1)
        );

        console.log(
          `${YELLOW}┌─────────────────────────────────────────────┐${RESET}`
        );
        console.log(
          `${YELLOW}│${RESET} Pages Crawled:      ${BOLD}${formatNumber(
            totalPagesCrawled
          ).padStart(8)}${RESET}                ${YELLOW}│${RESET}`
        );
        console.log(
          `${YELLOW}│${RESET} Links in Queue:     ${BOLD}${formatNumber(
            totalLinksQueued
          ).padStart(8)}${RESET}                ${YELLOW}│${RESET}`
        );
        console.log(
          `${YELLOW}│${RESET} Average Crawl Time: ${BOLD}${avgCrawlTime
            .slice(0, 5)
            .padStart(8)} ms${RESET}             ${YELLOW}│${RESET}`
        );

        if (crawlerStats.throughput > 0) {
          console.log(
            `${YELLOW}│${RESET} Crawl Throughput:   ${BOLD}${crawlerStats.throughput
              .toFixed(2)
              .padStart(8)}${RESET} pages/sec      ${YELLOW}│${RESET}`
          );
        }

        console.log(
          `${YELLOW}│${RESET} Queue Progress:   ${createProgressBar(
            totalPagesCrawled,
            totalPagesCrawled + totalLinksQueued,
            20
          )}  ${YELLOW}│${RESET}`
        );
        console.log(
          `${YELLOW}└─────────────────────────────────────────────┘${RESET}`
        );

        // Indexer Statistics
        if (systemStats.indexing) {
          header("INDEXER STATISTICS");

          const indexingStats = systemStats.indexing;
          const totalDocsIndexed = indexingStats.documentsIndexed || 0;
          const totalLinksQueued = indexingStats.docsInQueue || 0;
          const totalTermsProcessed = indexingStats.totalTermsProcessed || 0;
          const avgIndexTime = formatTime(
            indexingStats.totalIndexTime / (totalDocsIndexed || 1)
          );

          console.log(
            `${GREEN}┌─────────────────────────────────────────────┐${RESET}`
          );
          console.log(
            `${GREEN}│${RESET} Documents Indexed:  ${BOLD}${formatNumber(
              totalDocsIndexed
            ).padStart(8)}${RESET}                ${GREEN}│${RESET}`
          );
          console.log(
            `${GREEN}│${RESET} Links in Queue:     ${BOLD}${formatNumber(
              totalLinksQueued
            ).padStart(8)}${RESET}                ${GREEN}│${RESET}`
          );
          console.log(
            `${GREEN}│${RESET} Terms Processed:    ${BOLD}${formatNumber(
              totalTermsProcessed
            ).padStart(8)}${RESET}                ${GREEN}│${RESET}`
          );
          console.log(
            `${GREEN}│${RESET} Average Index Time: ${BOLD}${avgIndexTime
              .slice(0, 5)
              .padStart(8)} ms ${RESET}            ${GREEN}│${RESET}`
          );

          if (indexingStats.throughput > 0) {
            console.log(
              `${GREEN}│${RESET} Index Throughput:   ${BOLD}${indexingStats.throughput
                .toFixed(2)
                .padStart(8)}${RESET} docs/sec       ${GREEN}│${RESET}`
            );
          }

          console.log(
            `${GREEN}│${RESET} Queue Progress:   ${createProgressBar(
              totalDocsIndexed,
              totalDocsIndexed + totalLinksQueued,
              20
            )} ${GREEN}│${RESET}`
          );
          console.log(
            `${GREEN}└─────────────────────────────────────────────┘${RESET}`
          );
        }

        // Range Indexer Statistics
        if (systemStats.rangeIndex) {
          header("RANGE INDEXER STATISTICS");

          const rangeIndexStats = systemStats.rangeIndex;
          const totalDocsIndexed = rangeIndexStats.documentsIndexed || 0;
          const totalLinksQueued = rangeIndexStats.docsInQueue || 0;
          const avgIndexTime = formatTime(
            rangeIndexStats.totalIndexTime / (totalDocsIndexed || 1)
          );

          console.log(
            `${BLUE}┌─────────────────────────────────────────────┐${RESET}`
          );
          console.log(
            `${BLUE}│${RESET} Documents Indexed:  ${BOLD}${formatNumber(
              totalDocsIndexed
            ).padStart(8)}${RESET}                ${BLUE}│${RESET}`
          );
          console.log(
            `${BLUE}│${RESET} Links in Queue:     ${BOLD}${formatNumber(
              totalLinksQueued
            ).padStart(8)}${RESET}                ${BLUE}│${RESET}`
          );
          console.log(
            `${BLUE}│${RESET} Average Index Time: ${BOLD}${avgIndexTime
              .slice(0, 5)
              .padStart(8)} ms ${RESET}            ${BLUE}│${RESET}`
          );

          if (rangeIndexStats.throughput > 0) {
            console.log(
              `${BLUE}│${RESET} Throughput:         ${BOLD}${rangeIndexStats.throughput
                .toFixed(2)
                .padStart(8)}${RESET} docs/sec       ${BLUE}│${RESET}`
            );
          }

          console.log(
            `${BLUE}│${RESET} Queue Progress:  ${createProgressBar(
              totalDocsIndexed,
              totalDocsIndexed + totalLinksQueued,
              20
            )}   ${BLUE}│${RESET}`
          );
          console.log(
            `${BLUE}└─────────────────────────────────────────────┘${RESET}`
          );
        }

        // Querier Statistics - Simplified as requested
        if (systemStats.querying) {
          header("QUERIER STATISTICS");

          const queryStats = systemStats.querying;
          const termQueries = queryStats.queriesProcessed || 0;
          const taxonomyQueries = queryStats.rangeQueriesProcessed || 0;
          const avgQueryTime = formatTime(queryStats.avgQueryTime || 0);
          const avgRangeQueryTime = formatTime(
            queryStats.avgRangeQueryTime || 0
          );
          const totalResults = queryStats.resultsReturned || 0;
          const avgResultsPerQuery = queryStats.avgResultsPerQuery || 0;

          console.log(
            `${MAGENTA}┌─────────────────────────────────────────────┐${RESET}`
          );
          console.log(
            `${MAGENTA}│${RESET} Term Queries:       ${BOLD}${formatNumber(
              termQueries
            ).padStart(8)}${RESET}                ${MAGENTA}│${RESET}`
          );
          console.log(
            `${MAGENTA}│${RESET} Taxonomy Queries:   ${BOLD}${formatNumber(
              taxonomyQueries
            ).padStart(8)}${RESET}                ${MAGENTA}│${RESET}`
          );
          console.log(
            `${MAGENTA}│${RESET} Avg Query Time:     ${BOLD}${avgQueryTime.padStart(
              8
            )}${RESET}                ${MAGENTA}│${RESET}`
          );
          console.log(
            `${MAGENTA}│${RESET} Avg Taxonomy Time:  ${BOLD}${avgRangeQueryTime.padStart(
              8
            )}${RESET}                ${MAGENTA}│${RESET}`
          );
          console.log(
            `${MAGENTA}│${RESET} Total Results:      ${BOLD}${formatNumber(
              totalResults
            ).padStart(8)}${RESET}                ${MAGENTA}│${RESET}`
          );
          console.log(
            `${MAGENTA}│${RESET} Avg Results/Query:  ${BOLD}${avgResultsPerQuery
              .toFixed(2)
              .padStart(8)}${RESET}                ${MAGENTA}│${RESET}`
          );
          console.log(
            `${MAGENTA}└─────────────────────────────────────────────┘${RESET}`
          );
        }

        // Footer
        console.log(
          `\n${BG_MAGENTA}${WHITE}${BOLD} END OF STATISTICS REPORT ${RESET}`
        );

        // Resume services
        await new Promise((resolve, reject) => {
          distribution.crawler_group.crawler.set_service_state(
            false,
            (e, v) => {
              distribution.indexer_group.indexer.set_service_state(
                false,
                (e, v) => {
                  distribution.indexer_ranged_group.indexer_ranged.set_service_state(
                    false,
                    (e, v) => {
                      resolve();
                    }
                  );
                }
              );
            }
          );
        });
      } catch (error) {
        stopSpinner();
        console.error(
          "\x1b[31mError retrieving system statistics:\x1b[0m",
          error
        );
      }
    } else if (command === "save") {
      // Force save state to disk
      startSpinner("Saving system state to disk");

      try {
        await Promise.all([
          new Promise((resolve, reject) => {
            const remote = {
              gid: "local",
              service: "crawler",
              method: "save_maps_to_disk",
            };
            distribution.indexer_group.comm.send([], remote, (e, v) => {
              resolve();
            });
          }),
          new Promise((resolve, reject) => {
            const remote = {
              gid: "local",
              service: "indexer",
              method: "save_maps_to_disk",
            };
            distribution.indexer_group.comm.send([], remote, (e, v) => {
              resolve();
            });
          }),
        ]);

        stopSpinner();
        console.log("\x1b[32m✓\x1b[0m System state saved successfully");
      } catch (error) {
        stopSpinner();
        console.error("\x1b[31mError saving system state:\x1b[0m", error);
      }
    } else if (command === "crawl") {
      // Add a link to crawl
      if (parts.length < 2) {
        console.log(
          "\x1b[31mError: Missing link. Usage: crawl /wiki/PAGE\x1b[0m"
        );
      } else {
        const link = parts.slice(1).join(" ");
        await addLinkToCrawl(link);
      }
    } else if (command === "tree" || command === "taxonomy") {
      if (parts.length < 2) {
        console.log(
          "\x1b[31mError: Missing taxonomy term. Usage: tree TAXONOMY_TERM [options]\x1b[0m"
        );
        console.log("Example: tree plantae");
        console.log("Example: tree cnidaria --collapse");
      } else {
        const taxonomyTerm = parts[1].toLowerCase();
        const options = {
          collapseSpecies: parts.includes("--collapse") || parts.includes("-c"),
          maxDepth: 10,
        };

        const depthFlag = parts.find(
          (part) => part.startsWith("--depth=") || part.startsWith("-d=")
        );
        if (depthFlag) {
          const depthValue = depthFlag.split("=")[1];
          options.maxDepth = parseInt(depthValue) || 10;
        }

        await executeRangeQuery(taxonomyTerm, options);
      }
    } else if (input) {
      await executeQuery(input);
    }

    updatePrompt();
  }).on("close", () => {
    console.log("Exiting REPL. Goodbye!");
    // TODO: Should I add a loop here to close the nodes
    process.exit(0);
  });

  console.log("\x1b[33mStarting background crawling and indexing...\x1b[0m\n");

  distribution.crawler_group.crawler.start_crawl((e, v) => {});
  distribution.indexer_group.indexer.start_index((e, v) => {});
  distribution.indexer_ranged_group.indexer_ranged.start_index((e, v) => {});
  setTimeout(() => main_metric_loop(), 3000);

  setInterval(async () => {
    try {
      await Promise.all([
        new Promise((resolve, reject) => {
          const remote = {
            gid: "local",
            service: "crawler",
            method: "save_maps_to_disk",
          };
          distribution.crawler_group.comm.send([], remote, (e, v) => {
            resolve();
          });
        }),
        new Promise((resolve, reject) => {
          const remote = {
            gid: "local",
            service: "indexer",
            method: "save_maps_to_disk",
          };
          distribution.indexer_group.comm.send([], remote, (e, v) => {
            resolve();
          });
        }),
        new Promise((resolve, reject) => {
          const remote = {
            gid: "local",
            service: "indexer_ranged",
            method: "save_maps_to_disk",
          };
          distribution.indexer_ranged_group.comm.send([], remote, (e, v) => {
            resolve();
          });
        }),
      ]);

      console.log("\x1b[2m[System] State saved automatically\x1b[0m");
    } catch (error) {
      console.error(
        "\x1b[31m[System] Error during automatic state saving:\x1b[0m",
        error
      );
    }
  }, 120000);
});
