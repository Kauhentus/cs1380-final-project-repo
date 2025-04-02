// distributed-indexer.js
const distribution = require("../config.js");
const id = distribution.util.id;
const fs = require("fs");
const path = require("path");
const LZ = require("lz-string");

// TODO: We now want to make a call to the group to tell it to perform the MapReduce
// TODO: job to calculate TF-IDF scores after all documents have been processed.

// TODO: However, we do not want the Map

// TODO: Mapper needs to filter first, get document length, then word count,
// TODO: emit link, TF-Score,
// Configuration
const numNodes = 8; // Same as your crawler
const totalDocuments = 100000; // Hard-coded as requested

// Create nodes for the index group (using different ports from crawler)
const nodes = [];
const nids = [];
const indexGroup = {};

for (let i = 0; i < numNodes; i++) {
  nodes.push({ ip: "127.0.0.1", port: 7110 + i });
  nids.push(id.getNID(nodes[i]));
  indexGroup[id.getSID(nodes[i])] = nodes[i];
}

distribution.node.start(async (server) => {
  const spawn_nx = (nx) =>
    new Promise((resolve) =>
      distribution.local.status.spawn(nx, (e, v) => resolve())
    );

  const stop_nx = (nx) =>
    new Promise((resolve) =>
      distribution.local.comm.send(
        [],
        { service: "status", method: "stop", node: nx },
        (e, v) => resolve()
      )
    );

  // Function to determine which node should handle a term
  const get_nx = (term) =>
    nodes[parseInt(id.getID(term).slice(0, 8), 16) % numNodes];

  // Indexer service
  const indexerService = {
    initialize: (cb) => {
      console.log(
        `[Node ${global.nodeConfig.port}] Initializing indexer service`
      );

      // Create directories for logs and index storage
      if (!fs.existsSync(`./indexer-files`))
        fs.mkdirSync(`./indexer-files`, { recursive: true });
      if (!fs.existsSync(`./indexer-files/logs`))
        fs.mkdirSync(`./indexer-files/logs`, { recursive: true });
      if (!fs.existsSync(`./indexer-files/index`))
        fs.mkdirSync(`./indexer-files/index`, { recursive: true });

      const logPath = `./indexer-files/logs/log-${global.nodeConfig.port}.txt`;
      fs.writeFileSync(
        logPath,
        `Indexer initialized at ${new Date().toISOString()}\n`
      );

      // Initialize in-memory maps for terms and document processing
      const termMap = new Map();
      const processedDocuments = new Map();
      const nodeInfo = { crawlerNodes: [], documentPaths: [] };

      distribution.local.mem.put(termMap, "term_map", (e, v) => {
        distribution.local.mem.put(
          processedDocuments,
          "processed_documents",
          (e, v) => {
            distribution.local.mem.put(nodeInfo, "node_info", (e, v) => {
              cb(null, { success: true });
            });
          }
        );
      });
    },

    // Method to find all crawler node directories
    getDocuments: (cb) => {},

    // Method to process a batch of documents
    processDocumentBatch: (batchInfo, cb) => {
      const { startIdx, count } = batchInfo;
      console.log(
        `[Node ${global.nodeConfig.port}] Processing document batch from ${startIdx}, count ${count}`
      );

      distribution.local.mem.get("node_info", (e1, nodeInfo) => {
        distribution.local.mem.get(
          "processed_documents",
          (e2, processedDocs) => {
            if (startIdx >= nodeInfo.documentPaths.length) {
              return cb(null, { error: "Invalid batch start index" });
            }

            const endIdx = Math.min(
              startIdx + count,
              nodeInfo.documentPaths.length
            );
            const batchPaths = nodeInfo.documentPaths.slice(startIdx, endIdx);

            let processed = 0;
            let successful = 0;
            const errors = [];

            // Process each document in the batch
            const processNextDocument = (idx) => {
              if (idx >= batchPaths.length) {
                return cb(null, {
                  total: batchPaths.length,
                  successful,
                  errors: errors.length,
                  hasMore: endIdx < nodeInfo.documentPaths.length,
                });
              }

              const docPath = batchPaths[idx];
              const docId = docPath.file;

              // Skip if already processed
              if (processedDocs.has(docId)) {
                process.nextTick(() => processNextDocument(idx + 1));
                return;
              }

              try {
                // Read the file
                const fileContent = fs.readFileSync(docPath.path, "utf8");

                try {
                  // Parse the JSON
                  const jsonData = JSON.parse(fileContent);

                  if (jsonData.value && typeof jsonData.value === "string") {
                    // Decompress with LZ-string
                    const decompressed = LZ.decompressFromBase64(
                      jsonData.value
                    );

                    // Parse the decompressed JSON
                    const docObj = JSON.parse(decompressed);

                    // Get the article words
                    const words = docObj.article_words || [];
                    const url = docObj.url || docId;

                    if (words.length > 0) {
                      // Process this document's words
                      const termFreqs = {};
                      words.forEach((term) => {
                        // Skip very short terms and numbers
                        if (!term || term.length <= 2 || /^\d+$/.test(term))
                          return;
                        termFreqs[term] = (termFreqs[term] || 0) + 1;
                      });

                      // Calculate term frequencies
                      const docLength = words.length;

                      // Send terms to their respective nodes
                      const termsSent = [];

                      for (const term of Object.keys(termFreqs)) {
                        const tf = termFreqs[term] / docLength;
                        const termNode = get_nx(term);

                        // Group terms by node to reduce communication
                        const idx = termsSent.findIndex(
                          (item) =>
                            item.node.ip === termNode.ip &&
                            item.node.port === termNode.port
                        );

                        if (idx === -1) {
                          termsSent.push({
                            node: termNode,
                            terms: [{ term, docId: url, tf }],
                          });
                        } else {
                          termsSent[idx].terms.push({ term, docId: url, tf });
                        }
                      }

                      // Send terms to their respective nodes
                      const promises = termsSent.map((item) => {
                        return new Promise((resolve) => {
                          const remote = {
                            node: item.node,
                            gid: "local",
                            service: "indexer",
                            method: "addTerms",
                          };

                          distribution.local.comm.send(
                            [item.terms],
                            remote,
                            (e, v) => {
                              resolve();
                            }
                          );
                        });
                      });

                      Promise.all(promises)
                        .then(() => {
                          // Mark this document as processed
                          processedDocs.set(docId, true);
                          successful++;

                          // Process next document
                          processNextDocument(idx + 1);
                        })
                        .catch((err) => {
                          console.error(
                            `Error distributing terms for document ${docId}:`,
                            err
                          );
                          errors.push({ docId, error: err.message });

                          // Process next document
                          processNextDocument(idx + 1);
                        });
                    } else {
                      // Empty document, mark as processed
                      processedDocs.set(docId, true);

                      // Process next document
                      processNextDocument(idx + 1);
                    }
                  } else {
                    // No LZ-compressed value, mark as processed
                    processedDocs.set(docId, true);
                    errors.push({ docId, error: "No compressed value found" });

                    // Process next document
                    processNextDocument(idx + 1);
                  }
                } catch (e) {
                  console.error(`Error processing document ${docId}:`, e);
                  errors.push({ docId, error: e.message });

                  // Process next document
                  processNextDocument(idx + 1);
                }
              } catch (e) {
                console.error(`Error reading file ${docPath.path}:`, e);
                errors.push({ docId, error: e.message });

                // Process next document
                processNextDocument(idx + 1);
              }
            };

            // Start processing the batch
            processNextDocument(0);
          }
        );
      });
    },

    // Method to add multiple terms to the index
    addTerms: (terms, cb) => {
      console.log(
        `[Node ${global.nodeConfig.port}] Adding ${terms.length} terms to index`
      );

      distribution.local.mem.get("term_map", (e, termMap) => {
        terms.forEach(({ term, docId, tf }) => {
          if (!termMap.has(term)) {
            termMap.set(term, { docs: {}, df: 0 });
          }

          const termEntry = termMap.get(term);
          if (!termEntry.docs[docId]) {
            termEntry.docs[docId] = tf;
            termEntry.df += 1;
          }
        });

        // Log the results
        const logPath = `./indexer-files/logs/log-${global.nodeConfig.port}.txt`;
        fs.appendFileSync(logPath, `Added ${terms.length} terms to index\n`);

        cb(null, { success: true });
      });
    },

    // Method to calculate TF-IDF scores and save the index
    calculateTFIDF: (cb) => {
      console.log(`[Node ${global.nodeConfig.port}] Calculating TF-IDF scores`);

      distribution.local.mem.get("term_map", (e, termMap) => {
        try {
          // Convert the map to an object for storage
          const indexData = {};
          let termCount = 0;

          termMap.forEach((data, term) => {
            const { docs, df } = data;
            const idf = Math.log(totalDocuments / Math.max(1, df));

            // Calculate TF-IDF for each document
            const docScores = {};
            for (const docId in docs) {
              docScores[docId] = docs[docId] * idf;
            }

            indexData[term] = {
              idf: idf,
              docScores: docScores,
            };

            termCount++;
          });

          // Save the index part
          const nodeId = global.nodeConfig.port;
          const indexPath = `./indexer-files/index/index-part-${nodeId}.json`;

          fs.writeFileSync(indexPath, JSON.stringify(indexData));

          // Log the results
          const logPath = `./indexer-files/logs/log-${global.nodeConfig.port}.txt`;
          fs.appendFileSync(
            logPath,
            `Calculated TF-IDF scores for ${termCount} terms\n`
          );

          cb(null, { termCount });
        } catch (error) {
          console.error(`Error calculating TF-IDF:`, error);
          cb(null, { error: error.message });
        }
      });
    },

    // Method to get statistics about the index
    getStats: (cb) => {
      console.log(`[Node ${global.nodeConfig.port}] Getting index statistics`);

      distribution.local.mem.get("term_map", (e1, termMap) => {
        distribution.local.mem.get(
          "processed_documents",
          (e2, processedDocs) => {
            distribution.local.mem.get("node_info", (e3, nodeInfo) => {
              const stats = {
                termCount: termMap.size,
                processedDocuments: processedDocs.size,
                totalDocumentPaths: nodeInfo.documentPaths.length,
                crawlerNodeCount: nodeInfo.crawlerNodes.length,
              };

              cb(null, stats);
            });
          }
        );
      });
    },
  };

  // Setup the indexing cluster
  const setupCluster = (cb) => {
    console.log("SETTING UP INDEXING CLUSTER...");

    const indexConfig = { gid: "search-index" };
    distribution.local.groups.put(indexConfig, indexGroup, (e, v) => {
      distribution["search-index"].groups.put(
        indexConfig,
        indexGroup,
        (e, v) => {
          distribution["search-index"].routes.put(
            indexerService,
            "indexer",
            (e, v) => {
              const remote = {
                gid: "local",
                service: "indexer",
                method: "initialize",
              };

              distribution["search-index"].comm.send([], remote, (e, v) => {
                console.log("Indexer service initialized on all nodes");

                // Find crawler nodes
                const findNodesRemote = {
                  gid: "local",
                  service: "indexer",
                  method: "findCrawlerNodes",
                };

                distribution["search-index"].comm.send(
                  [],
                  findNodesRemote,
                  (e, findResults) => {
                    let totalNodes = 0;

                    Object.values(findResults).forEach((result) => {
                      if (result && !result.error) {
                        totalNodes += result.nodeCount || 0;
                      }
                    });

                    console.log(
                      `Discovered ${totalNodes} crawler node directories`
                    );

                    // Scan each crawler node for documents
                    const scanCrawlerNodes = async () => {
                      console.log("Scanning crawler nodes for documents...");

                      for (let i = 0; i < totalNodes; i++) {
                        await new Promise((resolve) => {
                          const scanNodeRemote = {
                            gid: "local",
                            service: "indexer",
                            method: "scanCrawlerNode",
                          };

                          distribution["search-index"].comm.send(
                            [i],
                            scanNodeRemote,
                            (e, scanResults) => {
                              let totalDocs = 0;

                              Object.values(scanResults).forEach((result) => {
                                if (result && !result.error) {
                                  totalDocs += result.count || 0;
                                }
                              });

                              console.log(
                                `Scanned crawler node ${
                                  i + 1
                                }/${totalNodes}, found ${totalDocs} documents`
                              );
                              resolve();
                            }
                          );
                        });
                      }

                      console.log("Completed scanning all crawler nodes");
                      cb();
                    };

                    scanCrawlerNodes();
                  }
                );
              });
            }
          );
        }
      );
    });
  };

  // Run the indexing task
  const runIndexingTask = async (cb) => {
    console.log("STARTING INDEXING TASK...");

    // Process documents in batches
    const processBatch = async (startIdx, batchSize) => {
      return new Promise((resolve) => {
        const remote = {
          gid: "local",
          service: "indexer",
          method: "processDocumentBatch",
        };

        distribution["search-index"].comm.send(
          [{ startIdx, count: batchSize }],
          remote,
          (e, results) => {
            if (e) {
              console.error("Error processing batch:", e);
              resolve({ hasMore: false });
              return;
            }

            let totalProcessed = 0;
            let totalSuccessful = 0;
            let hasMore = false;

            Object.values(results).forEach((result) => {
              if (result && !result.error) {
                totalProcessed += result.total || 0;
                totalSuccessful += result.successful || 0;

                if (result.hasMore) {
                  hasMore = true;
                }
              }
            });

            console.log(
              `Processed batch: ${totalSuccessful}/${totalProcessed} documents successful`
            );
            resolve({ hasMore });
          }
        );
      });
    };

    // Process documents in batches of 100
    let startIdx = 0;
    const batchSize = 50; // Smaller batch size for stability
    let hasMore = true;
    let batchNum = 1;

    while (hasMore) {
      console.log(`Processing batch ${batchNum} (start index: ${startIdx})`);
      const result = await processBatch(startIdx, batchSize);
      hasMore = result.hasMore;
      startIdx += batchSize;
      batchNum++;

      // Add a small delay to prevent overwhelming the system
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    // After all documents are processed, calculate TF-IDF
    console.log("All documents processed, calculating TF-IDF scores...");

    await new Promise((resolve) => {
      const remote = {
        gid: "local",
        service: "indexer",
        method: "calculateTFIDF",
      };

      distribution["search-index"].comm.send([], remote, (e, results) => {
        let totalTerms = 0;

        Object.values(results).forEach((result) => {
          if (result && !result.error) {
            totalTerms += result.termCount || 0;
          }
        });

        console.log(`TF-IDF calculation completed for ${totalTerms} terms`);
        resolve();
      });
    });

    // Get final statistics
    await new Promise((resolve) => {
      const remote = {
        gid: "local",
        service: "indexer",
        method: "getStats",
      };

      distribution["search-index"].comm.send([], remote, (e, stats) => {
        let totalTerms = 0;
        let totalDocs = 0;

        Object.values(stats).forEach((nodeStat) => {
          if (nodeStat) {
            totalTerms += nodeStat.termCount || 0;
            totalDocs += nodeStat.processedDocuments || 0;
          }
        });

        console.log(`Final indexing statistics:`);
        console.log(`- Total terms indexed: ${totalTerms}`);
        console.log(`- Total documents processed: ${totalDocs}`);

        resolve();
      });
    });

    cb();
  };

  // Main execution
  try {
    // Spawn nodes
    console.log(`Spawning ${numNodes} indexer nodes...`);
    for (let i = 0; i < numNodes; i++) {
      await spawn_nx(nodes[i]);
      console.log(`Spawned node ${i + 1}/${numNodes}`);
    }

    // Set up cluster and run indexing
    setupCluster(() => {
      runIndexingTask(() => {
        console.log("INDEXING COMPLETED");

        // Create a simple summary of the index
        try {
          const indexDir = "./indexer-files/index";
          const indexFiles = fs.readdirSync(indexDir);

          // Count terms in each index part
          let totalTerms = 0;
          let termSamples = [];

          indexFiles.forEach((file) => {
            const indexPath = path.join(indexDir, file);
            const indexData = JSON.parse(fs.readFileSync(indexPath, "utf8"));

            const termCount = Object.keys(indexData).length;
            totalTerms += termCount;

            // Get a few sample terms
            const terms = Object.keys(indexData).slice(0, 5);
            termSamples = termSamples.concat(terms);
          });

          console.log(`Index summary:`);
          console.log(`- Total index parts: ${indexFiles.length}`);
          console.log(`- Total terms in index: ${totalTerms}`);
          console.log(`- Sample terms: ${termSamples.slice(0, 10).join(", ")}`);
        } catch (error) {
          console.error("Error creating index summary:", error);
        }

        // Shutdown
        Promise.all(nodes.map((node) => stop_nx(node))).then(() => {
          server.close();
          console.log("All nodes stopped, indexer terminated.");
        });
      });
    });
  } catch (error) {
    console.error("Error in indexing process:", error);
    server.close();
  }
});
