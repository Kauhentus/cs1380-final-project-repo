const readline = require("readline");
const distribution = require("./config.js");
const fs = require("fs");
const id = distribution.util.id;

// ANSI color codes (consolidated)
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
const BG_MAGENTA = "\x1b[42m";
const DEFAULT = RESET;
const COLOR_OF_THE_BOX = GREEN;
const BOX_WIDTH = 78;

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

// const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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

function stripAnsi(str) {
  return str.replace(/\x1B\[\d+m/g, "");
}

function cleanText(text) {
  if (!text) return "";
  return text.replace(/\n/g, " ").replace(/\s+/g, " ").trim();
}

function createPrintLine(text) {
  const visibleText = stripAnsi(text);
  const textLength = [...visibleText].length;

  // console.log(COLOR);

  const fixedLength = textLength + 7;
  const totalEquals = 78 - fixedLength;
  const equalsPerSide = Math.floor(totalEquals / 2);

  const leftEquals = "=".repeat(equalsPerSide);

  const rightEquals = "=".repeat(equalsPerSide + (totalEquals % 2));

  return `\n${BOLD}${MAGENTA}${leftEquals} ${text}  ${rightEquals}${DEFAULT}`;
}

distribution.node.start(async (server) => {
  const get_nx = (link) => {
    return nodes[parseInt(id.getID(link).slice(0, 8), 16) % num_nodes];
  };

  let startTime = Date.now();

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
    process.stdout.write(`\r${" ".repeat(indent)}${message} ${spinChars[0]}`);
    spinnerInterval = setInterval(() => {
      i = (i + 1) % spinChars.length;
      process.stdout.write(`\r${" ".repeat(indent)}${message} ${spinChars[i]}`);
    }, 100);
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
  console.log("\x1b[32m✓\x1b[0m Distributed search engine is ready!");

  startSpinner("Adding initial seed link");
  await Promise.all([
    new Promise((resolve, reject) => {
      const link = "/wiki/Cnidaria";
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
      const link = "/wiki/Lepidoptera";
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

  const formatTime = (ms) => {
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(2)}s`;
  };

  async function addLinkToCrawl(link) {
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

  function formatLine(content, indent = 0) {
    content = cleanText(content);

    const indentStr = " ".repeat(indent);
    const fullContent = indentStr + content;

    const visibleLength = stripAnsi(fullContent).length;
    const padding = Math.max(0, BOX_WIDTH - 4 - visibleLength);

    return `${COLOR_OF_THE_BOX}│ ${DEFAULT}${fullContent}${" ".repeat(
      padding
    )}${COLOR_OF_THE_BOX} │${DEFAULT}`;
  }

  function splitTextToLines(text, maxWidth) {
    if (!text) return [];

    text = cleanText(text);

    const words = text.split(" ");
    const lines = [];
    let currentLine = "";

    for (const word of words) {
      const testLine = currentLine ? `${currentLine} ${word}` : word;
      if (stripAnsi(testLine).length <= maxWidth) {
        currentLine = testLine;
      } else {
        lines.push(currentLine);
        currentLine = word;
      }
    }

    if (currentLine) {
      lines.push(currentLine);
    }

    return lines;
  }

  function formatResult(
    result,
    rank,
    termCount,
    showDetails,
    extraData = null
  ) {
    const pageInfo = result.termDetails?.pageInfo || {};
    const title = cleanText(
      pageInfo.binomialName || result.docId.replace("/wiki/", "")
    );
    const url = `https://www.wikipedia.org${result.docId}`;

    const textToDisplay = [];

    textToDisplay.push(
      `${COLOR_OF_THE_BOX}┌${"─".repeat(BOX_WIDTH - 2)}┐${DEFAULT}`
    );
    textToDisplay.push(
      formatLine(`${BOLD}${rank}. ${title.toUpperCase()}${DEFAULT}`)
    );
    textToDisplay.push(
      `${COLOR_OF_THE_BOX}├${"─".repeat(BOX_WIDTH - 2)}┤${DEFAULT}`
    );

    textToDisplay.push(formatLine(`URL: ${UNDERLINE}${url}${DEFAULT}`));
    textToDisplay.push(
      formatLine(
        `Score: ${YELLOW}${result.score.toFixed(4)}${DEFAULT} (matched ${
          result.matchedTerms
        }/${termCount} terms)`
      )
    );

    if (pageInfo.kingdom) {
      textToDisplay.push(formatLine(`Kingdom: ${cleanText(pageInfo.kingdom)}`));
    }

    if (pageInfo.family) {
      textToDisplay.push(formatLine(`Family: ${cleanText(pageInfo.family)}`));
    }

    const boostFlag =
      result.termDetails?.taxonomyLevel || result.termDetails?.isBinomial;
    if (boostFlag) {
      textToDisplay.push(
        `${COLOR_OF_THE_BOX}├${"─".repeat(BOX_WIDTH - 2)}┤${DEFAULT}`
      );
      textToDisplay.push(formatLine(`${BOLD}Relevance Boosters:${DEFAULT}`));

      if (result.termDetails?.taxonomyLevel) {
        textToDisplay.push(
          formatLine(
            `${GREEN}✓${DEFAULT} Taxonomy match: ${cleanText(
              result.termDetails.taxonomyLevel
            )}`,
            2
          )
        );
      }

      if (result.termDetails?.isBinomial) {
        textToDisplay.push(
          formatLine(`${GREEN}✓${DEFAULT} Term appears in binomial name`, 2)
        );
      }
    }

    if (showDetails && extraData) {
      if (extraData.description) {
        textToDisplay.push(
          `${COLOR_OF_THE_BOX}├${"─".repeat(BOX_WIDTH - 2)}┤${DEFAULT}`
        );
        textToDisplay.push(formatLine(`${BOLD}Description:${DEFAULT}`));

        const descLines = splitTextToLines(
          extraData.description,
          BOX_WIDTH - 6
        );
        const maxLines = 5;

        for (let i = 0; i < Math.min(descLines.length, maxLines); i++) {
          textToDisplay.push(formatLine(descLines[i]));
        }

        if (descLines.length > maxLines) {
          textToDisplay.push(formatLine("..."));
        }
      }

      if (extraData.hierarchy && extraData.hierarchy.length > 0) {
        textToDisplay.push(
          `${COLOR_OF_THE_BOX}├${"─".repeat(BOX_WIDTH - 2)}┤${DEFAULT}`
        );
        textToDisplay.push(
          formatLine(`${BOLD}Taxonomic Classification:${DEFAULT}`)
        );

        for (const entry of extraData.hierarchy) {
          if (Array.isArray(entry) && entry.length === 2) {
            const key = cleanText(entry[0]);
            const value = cleanText(entry[1]);
            textToDisplay.push(formatLine(`${key}: ${value}`, 2));
          }
        }
      }
    }

    textToDisplay.push(
      `${COLOR_OF_THE_BOX}└${"─".repeat(BOX_WIDTH - 2)}┘${DEFAULT}`
    );

    return textToDisplay.join("\n");
  }

  function fetchDocumentData(docId) {
    return new Promise((resolve, reject) => {
      distribution.crawler_group.store.get(docId, (err, data) => {
        if (err) {
          reject(err);
        } else {
          resolve(data || null);
        }
      });
    });
  }

  function parseQuery(queryString) {
    const parts = queryString.split(" ");
    const hasDetailFlag = parts.includes("-d");

    const actualQuery = parts.filter((part) => part !== "-d").join(" ");

    return {
      query: actualQuery,
      showDetails: hasDetailFlag,
    };
  }

  async function executeQuery(queryString) {
    // console.log(`\n${BOLD}Executing query...${DEFAULT}`);
    // console.log(MAGENTA);

    const startTime = Date.now();

    startSpinner(`${BOLD}Executing query for ${queryString}.${DEFAULT}`);

    const { query, showDetails } = parseQuery(queryString);

    return new Promise((resolve) => {
      // console.log(`Sending query "${query}" at ${new Date().toISOString()}`);

      distribution.querier_group.querier.query_one(query, {}, async (e, v) => {
        const queryTime = Date.now() - startTime;

        stopSpinner();

        if (e) {
          console.log(`\n${BOLD}Error:${DEFAULT} Query failed: ${e}`);
          return resolve();
        }

        // console.log(`\n${BOLD}${CYAN}SEARCH RESULTS: "${v.query}"${DEFAULT}`);
        console.log(
          createPrintLine(`🚕 "${v.query.toUpperCase()}" RESULTS 🚕`),
          MAGENTA
        );
        console.log(`${CYAN}Terms: ${v.terms.join(", ")}${DEFAULT}`);
        console.log(
          `${CYAN}Found ${v.totalResults} results in ${formatTime(
            queryTime
          )}${DEFAULT}`
        );
        if (showDetails) {
          console.log(`${CYAN}Detailed view enabled (-d flag)${DEFAULT}\n`);
        } else {
          console.log(
            `${CYAN}Showing details for top result only (use -d flag for all details)${DEFAULT}\n`
          );
        }

        if (!v.topResults || v.topResults.length === 0) {
          console.log(
            `\n${RED}${BOLD}No results found for this query.${DEFAULT}`
          );
          return resolve();
        }

        const maxResults = Math.min(v.topResults.length, 5);
        const metaDataPromises = [];

        for (let i = 0; i < maxResults; i++) {
          if (i === 0 || showDetails) {
            metaDataPromises.push(fetchDocumentData(v.topResults[i].docId));
          } else {
            metaDataPromises.push(Promise.resolve(null));
          }
        }

        try {
          const metaData = await Promise.all(metaDataPromises);

          for (let i = 0; i < maxResults; i++) {
            const moorDeets = i === 0 || showDetails;
            console.log(
              formatResult(
                v.topResults[i],
                i + 1,
                v.terms.length,
                moorDeets,
                metaData[i]
              )
            );

            if (i < maxResults - 1) console.log();
          }
        } catch (err) {
          console.error("Error fetching result data:", err);
          for (let i = 0; i < maxResults; i++) {
            console.log(
              formatResult(v.topResults[i], i + 1, v.terms.length, false, null)
            );
            if (i < maxResults - 1) console.log();
          }
        }

        resolve();
      });
    });
  }

  async function executeRangeQuery(taxonomyTerm, options = {}) {
    console.log(`\nExploring taxonomy tree for: ${taxonomyTerm}`);
    const startTime = Date.now();

    const defaultOptions = {
      collapseSpecies: false,
      maxDepth: 10,
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

          const queryTime = Date.now() - startTime;

          let speciesCount = 0;
          let taxaCount = 0;

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

            let nodeName = isSpecies
              ? node.name.replace("[SPECIES] /wiki/", "")
              : node.name;

            if (finalOptions.collapseSpecies && numSpeciesChildren > 0) {
              nodeName += ` (${numSpeciesChildren} species)`;
            }

            if (isSpecies) {
              speciesCount++;
              nodeName = "\x1b[32m*" + nodeName + "\x1b[0m";
            } else {
              taxaCount++;
              nodeName = "\x1b[33m" + nodeName + "\x1b[0m";
            }

            if (depth === 0) {
              console.log(" ".repeat(depth * 2) + nodeName);
            } else {
              const branch = isLastChild ? "└─" : "├─";
              console.log(prefix + branch + nodeName);
            }

            const newPrefix = prefix + (isLastChild ? "   " : "│  ");

            if (node.children) {
              node.children.sort((a, b) => {
                if (a.is_species && !b.is_species) return 1;
                if (!a.is_species && b.is_species) return -1;
                return a.name.localeCompare(b.name);
              });

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

          console.log("\n\x1b[1mTaxonomy Tree:\x1b[0m");
          printTree(results);

          console.log(
            `\nFound \x1b[33m${taxaCount} taxa\x1b[0m and \x1b[32m${speciesCount} species\x1b[0m in ${queryTime}ms`
          );

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

    console.log("\n\x1b[1mSearch Options:\x1b[0m");
    console.log(
      "  - \x1b[36m <query> -d\x1b[0m                  - Display the detailed results with hierarchy"
    );
    console.log("\n\x1b[1mSearch Tips:\x1b[0m");
    console.log("  - Try combining multiple terms for better results");
    console.log(
      "  - Terms found in taxonomy classification get higher relevance"
    );
    console.log(
      "  - Multi-term queries are ranked on the number of terms matched and their relevance"
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
        new Promise((resolve) => {
          distribution.indexer_ranged_group.comm.send(
            [],
            {
              gid: "local",
              service: "indexer_ranged",
              method: "save_maps_to_disk",
            },
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
        distribution.indexer_group.indexer.get_stats((e, v2) => {
          distribution.indexer_ranged_group.indexer_ranged.get_stats(
            (e3, v3) => {
              distribution.querier_group.querier.get_stats((e4, v4) => {
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
      rl.setPrompt("\x1b[33m 🚕search>\x1b[0m ");
    } else {
      rl.setPrompt("\x1b[36m 🚕search>\x1b[0m ");
    }
    rl.prompt();
  };

  const main_metric_loop = () => {
    isInRecoveryMode = true;

    console.log(
      "\n\x1b[2m[System] Background processes are now in recovery mode. REPL functionality remains available.\x1b[0m"
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
      setTimeout(() => {
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
          console.log(
            "\n\x1b[2m[System] Recovery mode ended. System resumed normal operations.\x1b[0m\n"
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
    prompt: "\x1b[36m🚕search>\x1b[0m ",
  });

  console.log(
    "\n\x1b[1;35m===== TAXI🚕 Distributed Search Engine REPL =====\x1b[0m"
  );
  console.log("Background crawling and indexing has been enabled!");
  console.log("Type 'help' to see available commands");
  console.log("\x1b[33mStarting background crawling and indexing...\x1b[0m\n");

  distribution.crawler_group.crawler.start_crawl((e, v) => {});
  distribution.indexer_group.indexer.start_index((e, v) => {});
  distribution.indexer_ranged_group.indexer_ranged.start_index((e, v) => {});
  setTimeout(() => main_metric_loop(), 3000);

  setInterval(async () => {
    try {
      await Promise.all([
        new Promise((resolve) => {
          const remote = {
            gid: "local",
            service: "crawler",
            method: "save_maps_to_disk",
          };
          distribution.crawler_group.comm.send([], remote, (e, v) => {
            resolve();
          });
        }),
        new Promise((resolve) => {
          const remote = {
            gid: "local",
            service: "indexer",
            method: "save_maps_to_disk",
          };
          distribution.indexer_group.comm.send([], remote, (e, v) => {
            resolve();
          });
        }),
        new Promise((resolve) => {
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

      console.log(`\n\x1b[2m[System] State saved automatically\x1b[0m`);
    } catch (error) {
      console.error(
        "\n\x1b[31m[System] Error during automatic state saving:\x1b[0m",
        error
      );
    }
  }, 120000);
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
        "\n\x1b[2m[System] Currently in recovery mode. Crawling and indexing operations are limited.\x1b[0m"
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
        await new Promise((resolve) => {
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

        const header = (text) => {
          console.log(
            `\n${BOLD}${CYAN}┌─ ${text} ${"─".repeat(
              40 - text.length
            )}┐${RESET}`
          );
        };

        const runtime = formatTime(Date.now() - startTime);

        console.log("\n");
        console.log(
          `${BG_MAGENTA}${WHITE}${BOLD} TAXI🚕 DISTRIBUTED SEARCH ENGINE - SYSTEM STATISTICS ${RESET}`
        );
        console.log(
          `${DIM}Runtime: ${runtime} | Generated at: ${new Date().toLocaleTimeString()}${RESET}`
        );

        header("SYSTEM SUMMARY");

        const crawlOps = systemStats.crawling.pagesProcessed || 0;
        const indexOps = systemStats.indexing.documentsIndexed || 0;
        const queryOps = systemStats.querying.queriesProcessed || 0;
        const rangeQueryOps = systemStats.querying.rangeQueriesProcessed || 0;

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
          `${CYAN}│${RESET} Range Query Ops:      ${BLUE}${formatNumber(
            rangeQueryOps
          ).padStart(8)}${RESET}              ${CYAN}│${RESET}`
        );
        console.log(
          `${CYAN}└─────────────────────────────────────────────┘${RESET}`
        );

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
            ).padStart(8)}${RESET}               ${GREEN}│${RESET}`
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
            )}  ${BLUE}│${RESET}`
          );
          console.log(
            `${BLUE}└─────────────────────────────────────────────┘${RESET}`
          );
        }

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
            `${MAGENTA}│${RESET} Avg Query Time:     ${BOLD}${avgQueryTime
              .slice(0, 5)
              .padStart(8)}${RESET}                ${MAGENTA}│${RESET}`
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

        console.log(
          `\n${BG_MAGENTA}${WHITE}${BOLD} END OF STATISTICS REPORT ${RESET}`
        );

        await new Promise((resolve) => {
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
      startSpinner("Saving system state to disk");

      try {
        await Promise.all([
          new Promise((resolve) => {
            const remote = {
              gid: "local",
              service: "crawler",
              method: "save_maps_to_disk",
            };
            distribution.crawler_group.comm.send([], remote, (e, v) => {
              resolve();
            });
          }),
          new Promise((resolve) => {
            const remote = {
              gid: "local",
              service: "indexer",
              method: "save_maps_to_disk",
            };
            distribution.indexer_group.comm.send([], remote, (e, v) => {
              resolve();
            });
          }),
          new Promise((resolve) => {
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

        stopSpinner();
        console.log("\x1b[32m✓\x1b[0m System state saved successfully");
      } catch (error) {
        stopSpinner();
        console.error("\x1b[31mError saving system state:\x1b[0m", error);
      }
    } else if (command === "crawl") {
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
    process.exit(0);
  });
});
