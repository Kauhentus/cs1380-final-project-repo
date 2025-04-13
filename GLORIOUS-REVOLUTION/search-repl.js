const readline = require("readline");
const distribution = require("./config.js");
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
    console.log(link);
    nodes[parseInt(id.getID(link).slice(0, 8), 16) % num_nodes];
  };

  // System metrics trackers
  let crawlCount = 0;
  let indexCount = 0;
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

  // Add initial seed link
  const seedLink = (link) => {
    new Promise((resolve) => {
      distribution.local.comm.send(
        [link],
        {
          node: get_nx(link),
          gid: "local",
          service: "crawler",
          method: "add_link_to_crawl",
        },
        () => resolve()
      );
    });
  };
  startSpinner("Adding initial seed link");
  //   await new Promise((resolve) => {
  //     const link = "/wiki/Cnidaria";
  //     distribution.local.comm.send(
  //       [link],
  //       {
  //         node: get_nx(link),
  //         gid: "local",
  //         service: "crawler",
  //         method: "add_link_to_crawl",
  //       },
  //       () => resolve()
  //     );
  //   });
  await seedLink("/wiki/Cnidaria");
  await seedLink("/wiki/Plant");
  await seedLink("/wiki/Lepidoptera");
  stopSpinner();
  console.log(
    "\x1b[32m✓\x1b[0m Added initial seed links for \x1b[32mPlants\x1b[0m, \x1b[34mSealife\x1b[0m, and \x1b[35mButterflies\x1b[0m!"
  );

  // Utility functions
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

  // Query execution function
  async function executeQuery(queryString) {
    console.log("\nExecuting query...");
    const startTime = Date.now();

    return new Promise((resolve) => {
      distribution.querier_group.querier.query_one(
        queryString,
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
            logDescriptionTitle(result);
            console.log("");
          });

          // TODO: Put this in a clickable, interactive component using a library
          // TODO: Decide if I want to keep the logging of both the title and the description since we
          // TODO: are now printing this for all of the results (want to find a balance between information and simplicity)
          const logDescriptionTitle = (result) => {
            new Promise((detailResolve) => {
              distribution.crawler_group.store.get(
                result.docId,
                (err, data) => {
                  if (err || !data) {
                    return detailResolve();
                  }

                  const title =
                    data.title || data.binomial_name || "Unknown Title";

                  console.log(headerLine(`DETAILED INFORMATION: ${title}`));
                  console.log(`| DETAILED INFORMATION: ${title} |`);
                  console.log(headerLine(`DETAILED INFORMATION: ${title}`));

                  if (data.binomial_name) {
                    console.log(
                      `\nBinomial name: \x1b[1m${data.binomial_name}\x1b[0m`
                    );
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
                }
              );
            });
          };

          // if (v.topResults.length > 0) {
          //   const topResult = v.topResults[0];
          //   try {
          //     await new Promise((detailResolve) => {
          //       distribution.crawler_group.store.get(
          //         topResult.docId,
          //         (err, data) => {
          //           if (err || !data) {
          //             return detailResolve();
          //           }

          //           const title =
          //             data.title || data.binomial_name || "Unknown Title";

          //           console.log(headerLine(`DETAILED INFORMATION: ${title}`));
          //           console.log(`| DETAILED INFORMATION: ${title} |`);
          //           console.log(headerLine(`DETAILED INFORMATION: ${title}`));

          //           if (data.binomial_name) {
          //             console.log(
          //               `\nBinomial name: \x1b[1m${data.binomial_name}\x1b[0m`
          //             );
          //           }

          //           if (data.hierarchy && data.hierarchy.length > 0) {
          //             console.log("\nTaxonomic Classification:");
          //             data.hierarchy.forEach((entry) => {
          //               if (Array.isArray(entry) && entry.length === 2) {
          //                 console.log(`  ${entry[0]}: ${entry[1]}`);
          //               }
          //             });
          //           }

          //           if (data.description) {
          //             console.log("\nDescription:");
          //             console.log(data.description);
          //           }

          //           detailResolve();
          //         }
          //       );
          //     });
          //   } catch (detailError) {
          //     console.error(
          //       "Error processing detailed information:",
          //       detailError
          //     );
          //   }
          // }

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
    console.log(
      "  - The system is constantly crawling and indexing in the background"
    );
    console.log("  - Check stats periodically to see growth of the index");
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

  // Setup the REPL interface
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: "\x1b[36msearch>\x1b[0m ",
  });

  console.log("\n\x1b[1;35m===== Distributed Search Engine REPL =====\x1b[0m");
  console.log("Background crawling and indexing has been enabled!");
  console.log("Type 'help' to see available commands");
  rl.prompt();

  rl.on("line", async (line) => {
    const input = line.trim();
    const parts = input.split(" ");
    const command = parts[0].toLowerCase();

    if (input === "") {
      rl.prompt();
      return;
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
      // Display system statistics
      try {
        startSpinner("Collecting system statistics");

        const [crawlerStats, indexerStats] = await Promise.all([
          new Promise((resolve) => {
            distribution.crawler_group.crawler.get_stats(null, (err, stats) => {
              if (err) {
                console.error("Error getting crawler stats:", err);
                resolve({});
              } else {
                resolve(stats);
              }
            });
          }),
          new Promise((resolve) => {
            if (distribution.indexer_group.indexer.get_stats) {
              distribution.indexer_group.indexer.get_stats(
                null,
                (err, stats) => {
                  if (err) {
                    console.error("Error getting indexer stats:", err);
                    resolve({});
                  } else {
                    resolve(stats);
                  }
                }
              );
            } else {
              resolve({});
            }
          }),
        ]);

        stopSpinner();

        const runtime = formatTime(Date.now() - startTime);
        console.log(
          `\n\x1b[1;35m===== System Statistics (Runtime: ${runtime}) =====\x1b[0m`
        );

        // Background processing stats
        console.log(`\nBackground Processing:`);
        console.log(`  Crawl operations: ${crawlCount}`);
        console.log(`  Index operations: ${indexCount}`);

        // Crawler stats
        console.log("\nCrawler Statistics:");
        let totalPagesCrawled = 0;
        let totalLinksQueued = 0;

        for (const nodeId in crawlerStats) {
          if (crawlerStats[nodeId]) {
            totalPagesCrawled += crawlerStats[nodeId].crawled_links || 0;
            totalLinksQueued += crawlerStats[nodeId].links_to_crawl || 0;
          }
        }

        console.log(`  Pages crawled: ${totalPagesCrawled}`);
        console.log(`  Links in queue: ${totalLinksQueued}`);

        // Per-node crawler details if available
        const nodeDetails = [];
        for (const nodeId in crawlerStats) {
          if (crawlerStats[nodeId]) {
            const nodeInfo = crawlerStats[nodeId];
            nodeDetails.push({
              id: nodeId,
              crawled: nodeInfo.crawled_links || 0,
              queued: nodeInfo.links_to_crawl || 0,
            });
          }
        }

        if (nodeDetails.length > 0) {
          console.log("\n  Crawler Nodes:");
          nodeDetails.forEach((node) => {
            console.log(
              `    Node ${node.id}: ${node.crawled} crawled, ${node.queued} queued`
            );
          });
        }

        // Indexer stats if available
        if (indexerStats && Object.keys(indexerStats).length > 0) {
          console.log("\nIndexer Statistics:");
          let totalDocsIndexed = 0;
          let totalTermsProcessed = 0;

          for (const nodeId in indexerStats) {
            if (indexerStats[nodeId] && indexerStats[nodeId].metrics) {
              totalDocsIndexed +=
                indexerStats[nodeId].metrics.documentsIndexed || 0;
              totalTermsProcessed +=
                indexerStats[nodeId].metrics.totalTermsProcessed || 0;
            }
          }

          console.log(`  Documents indexed: ${totalDocsIndexed}`);
          if (totalTermsProcessed > 0) {
            console.log(`  Terms processed: ${totalTermsProcessed}`);
          }

          // Display per-node indexer details if available
          const indexerNodeDetails = [];
          for (const nodeId in indexerStats) {
            if (indexerStats[nodeId] && indexerStats[nodeId].metrics) {
              const nodeMetrics = indexerStats[nodeId].metrics;
              indexerNodeDetails.push({
                id: nodeId,
                docs: nodeMetrics.documentsIndexed || 0,
                terms: nodeMetrics.totalTermsProcessed || 0,
              });
            }
          }

          if (indexerNodeDetails.length > 0) {
            console.log("\n  Indexer Nodes:");
            indexerNodeDetails.forEach((node) => {
              console.log(
                `    Node ${node.id}: ${node.docs} documents, ${node.terms} terms`
              );
            });
          }
        }

        // Add system performance metrics if available
        console.log(`\nSystem Performance:`);
        const uptime = formatTime(Date.now() - startTime);
        console.log(`  Uptime: ${uptime}`);

        if (crawlCount > 0 && indexCount > 0) {
          const crawlRate = (
            crawlCount /
            ((Date.now() - startTime) / 1000)
          ).toFixed(2);
          const indexRate = (
            indexCount /
            ((Date.now() - startTime) / 1000)
          ).toFixed(2);
          console.log(`  Crawl rate: ${crawlRate} pages/second`);
          console.log(`  Index rate: ${indexRate} documents/second`);
        }
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
    } else if (input) {
      // Execute the search query
      await executeQuery(input);
    }

    rl.prompt();
  }).on("close", () => {
    console.log("Exiting REPL. Goodbye!");
    process.exit(0);
  });

  // Start background crawling and indexing
  console.log("\x1b[33mStarting background crawling and indexing...\x1b[0m");

  // Background crawling process
  (async function crawlLoop() {
    try {
      await new Promise((resolve) => {
        distribution.crawler_group.comm.send(
          [],
          { gid: "local", service: "crawler", method: "crawl_one" },
          (e, v) => {
            if (e && !isEmptyObject(e)) {
              console.error("Crawl error:", e);
            } else if (
              v &&
              Object.values(v).some((r) => r && r.status === "success")
            ) {
              crawlCount++;
            }
            resolve();
          }
        );
      });

      // Throttle to prevent overloading
      await new Promise((resolve) => setTimeout(resolve, 200));
      crawlLoop();
    } catch (err) {
      console.error("Crawl loop error:", err);
      setTimeout(crawlLoop, 5000); // Retry after 5 seconds on error
    }
  })();

  // Background indexing process
  (async function indexLoop() {
    try {
      await new Promise((resolve) => {
        distribution.indexer_group.comm.send(
          [],
          { gid: "local", service: "indexer", method: "index_one" },
          (e, v) => {
            if (e && !isEmptyObject(e)) {
              console.error("Index error:", e);
            } else if (
              v &&
              Object.values(v).some((data) => data && data.status !== "skipped")
            ) {
              indexCount++;
            }
            resolve();
          }
        );
      });

      // Throttle to prevent overloading
      await new Promise((resolve) => setTimeout(resolve, 500));
      indexLoop();
    } catch (err) {
      console.error("Index loop error:", err);
      setTimeout(indexLoop, 5000); // Retry after 5 seconds on error
    }
  })();

  // Periodic state saving (every 2 minutes)
  setInterval(async () => {
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

      console.log("\x1b[2m[System] State saved automatically\x1b[0m");
    } catch (error) {
      console.error(
        "\x1b[31m[System] Error during automatic state saving:\x1b[0m",
        error
      );
    }
  }, 120000); // Every 2 minutes
});
