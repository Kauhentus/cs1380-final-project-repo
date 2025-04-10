const distribution = require("./config.js");
const id = distribution.util.id;

const crawl_nodes = [
  { ip: "127.0.0.1", port: 8001 },
  { ip: "127.0.0.1", port: 8002 },
  { ip: "127.0.0.1", port: 8003 },
];

const index_nodes = [
  { ip: "127.0.0.1", port: 8004 },
  { ip: "127.0.0.1", port: 8005 },
  { ip: "127.0.0.1", port: 8006 },
  { ip: "127.0.0.1", port: 8007 },
  { ip: "127.0.0.1", port: 8008 },
];
const nodes = [...crawl_nodes, ...index_nodes];

let documentsIndexedSinceLastPause = 0;
let isPaused = false;
let previousIndexedCount = 0;
let recoveryPauseCount = 0;

function isEmptyObject(obj) {
  return obj && typeof obj === "object" && Object.keys(obj).length === 0;
}

const spawnNode = (nodeConfig) => {
  return new Promise((resolve, reject) => {
    distribution.local.status.spawn(nodeConfig, (err, val) => {
      if (err) {
        console.error(
          `Error spawning node ${nodeConfig.ip}:${nodeConfig.port}:`,
          err
        );
        reject(err);
      } else {
        console.log(`Node spawned at ${nodeConfig.ip}:${nodeConfig.port}`);
        resolve(val);
      }
    });
  });
};

const stopNode = (nodeConfig) => {
  return new Promise((resolve, reject) => {
    distribution.local.comm.send(
      [],
      {
        service: "status",
        method: "stop",
        node: nodeConfig,
      },
      (err, val) => {
        if (err) {
          console.error(
            `Error stopping node ${nodeConfig.ip}:${nodeConfig.port}:`,
            err
          );
          reject(err);
        } else {
          console.log(`Node stopped at ${nodeConfig.ip}:${nodeConfig.port}`);
          resolve(val);
        }
      }
    );
  });
};

const createGroup = async (groupName, nodes) => {
  const groupConfig = { gid: groupName };
  const group = {};

  nodes.forEach((node) => {
    group[id.getSID(node)] = node;
  });

  // Create the group locally
  await new Promise((resolve, reject) => {
    distribution.local.groups.put(groupConfig, group, (err, val) => {
      if (err) {
        console.error(`Error creating local ${groupName} group:`, err);
        reject(err);
      } else {
        console.log(`Local ${groupName} group created successfully`);
        resolve(val);
      }
    });
  });

  // Create the group in the distributed system
  await new Promise((resolve, reject) => {
    // Check if the group service exists
    if (!distribution[groupName]) {
      console.error(`Error: ${groupName} group not initialized properly`);
      reject(new Error(`${groupName} group not initialized`));
      return;
    }

    distribution[groupName].groups.put(groupConfig, group, (err, val) => {
      if (err && !isEmptyObject(err)) {
        console.error(`Error creating distributed ${groupName} group:`, err);
        reject(err);
      } else {
        console.log(`Distributed ${groupName} group created successfully`);
        resolve(val);
      }
    });
  });

  return { groupConfig, group };
};

const createGroupOnGroup = async (originGroupName, groupName, nodes) => {
  const groupConfig = { gid: groupName };
  const group = {};

  nodes.forEach((node) => {
    group[id.getSID(node)] = node;
  });

  await new Promise((resolve, reject) => {
    // Check if the group service exists
    if (!distribution[originGroupName]) {
      console.error(`Error: ${originGroupName} group not initialized properly`);
      reject(new Error(`${originGroupName} group not initialized`));
      return;
    }

    distribution[originGroupName].groups.put(groupConfig, group, (err, val) => {
      if (err && !isEmptyObject(err)) {
        console.error(`Error creating distributed ${groupName} group:`, err);
        reject(err);
      } else {
        console.log(`Distributed ${groupName} group created successfully`);
        resolve(val);
      }
    });
  });
};

const getCrawlerStats = () => {
  return new Promise((resolve, reject) => {
    distribution.taxonomy.crawler.get_stats((err, crawlerStats) => {
      if (err && !isEmptyObject(err)) {
        console.error("Error getting crawler crawlerStats:", err);
        reject(err);
      } else {
        resolve(crawlerStats);
      }
    });
  });
};

const getIndexerStats = () => {
  return new Promise((resolve, reject) => {
    distribution.index.indexer.get_stats((err, indexerStats) => {
      if (err && !isEmptyObject(err)) {
        console.error("Error getting indexer indexerStats:", err);
        reject(err);
      } else {
        resolve(indexerStats);
      }
    });
  });
};

const aggregateMetrics = (crawlerStats, indexerStats) => {
  const aggregated = {
    crawling: {
      totalPagesProcessed: 0,
      totalBytesDownloaded: 0,
      avgProcessingTime: 0,
      totalTermsExtracted: 0,
      totalBytesTransferred: 0,
    },
    indexing: {
      totalDocumentsIndexed: 0,
      avgIndexTime: 0,
    },
    links: {
      totalToCrawl: 0,
      totalCrawled: 0,
      totalTargetsFound: 0,
    },
  };

  let totalCrawlTime = 0;
  let totalIndexTime = 0;

  for (const nodeId in crawlerStats) {
    if (!crawlerStats[nodeId]) continue;

    aggregated.links.totalToCrawl += crawlerStats[nodeId].links_to_crawl || 0;
    aggregated.links.totalCrawled += crawlerStats[nodeId].crawled_links || 0;

    if (crawlerStats[nodeId].metrics) {
      const crawlerMetrics = crawlerStats[nodeId].metrics;

      if (crawlerMetrics.crawling) {
        aggregated.crawling.totalPagesProcessed +=
          crawlerMetrics.crawling.pagesProcessed || 0;
        aggregated.crawling.totalBytesDownloaded +=
          crawlerMetrics.crawling.bytesDownloaded || 0;
        aggregated.links.totalTargetsFound +=
          crawlerMetrics.crawling.targetsHit || 0;
        totalCrawlTime += crawlerMetrics.crawling.totalCrawlTime || 0;
        aggregated.crawling.totalBytesTransferred +=
          crawlerMetrics.crawling.bytesTransferred || 0;
        aggregated.crawling.totalTermsExtracted +=
          crawlerMetrics.crawling.termsExtracted || 0;
      }
    }
  }

  for (const nodeId in indexerStats) {
    if (!indexerStats[nodeId]) continue;
    if (indexerStats[nodeId].metrics) {
      const indexerMetrics = indexerStats[nodeId].metrics;
      if (indexerMetrics) {
        aggregated.indexing.totalDocumentsIndexed +=
          indexerMetrics.documentsIndexed || 0;
        totalIndexTime += indexerMetrics.totalIndexTime || 0;
      }
    }
  }
  if (aggregated.crawling.totalPagesProcessed > 0) {
    aggregated.crawling.avgProcessingTime =
      totalCrawlTime / aggregated.crawling.totalPagesProcessed;
  }
  if (aggregated.indexing.totalDocumentsIndexed > 0) {
    aggregated.indexing.avgIndexTime =
      totalIndexTime / aggregated.indexing.totalDocumentsIndexed;
  }
  return aggregated;
};

// !! HEAVY DEBUG
async function requestSystemCleanup() {
  console.log("Initiating system cleanup...");

  try {
    console.log("Performing direct node memory cleanup...");
    let taxonomyCleanupSuccesses = 0;
    for (const node of crawl_nodes) {
      try {
        await new Promise((resolve) => {
          const nodeId = distribution.util.id.getSID(node);
          const config = {
            service: "mem",
            method: "clear",
            node: node,
          };

          console.log(
            `Cleaning taxonomy node ${nodeId} (${node.ip}:${node.port})...`
          );
          distribution.local.comm.send(
            [{ gid: "taxonomy" }],
            config,
            (err, result) => {
              if (
                err &&
                typeof err === "object" &&
                Object.keys(err).length > 0
              ) {
                console.warn(
                  `Warning: Failed to clear memory on taxonomy node ${nodeId}`
                );
              } else {
                console.log(`Memory cleared on taxonomy node ${nodeId}`);
                taxonomyCleanupSuccesses++;
              }
              resolve();
            }
          );
        });
      } catch (e) {
        console.error(`Error contacting taxonomy node:`, e);
      }
    }

    let indexCleanupSuccesses = 0;
    for (const node of index_nodes) {
      try {
        await new Promise((resolve) => {
          const nodeId = distribution.util.id.getSID(node);
          const config = {
            service: "mem",
            method: "clear",
            node: node,
          };

          console.log(
            `Cleaning index node ${nodeId} (${node.ip}:${node.port})...`
          );
          distribution.local.comm.send(
            [{ gid: "index" }],
            config,
            (err, result) => {
              if (
                err &&
                typeof err === "object" &&
                Object.keys(err).length > 0
              ) {
                console.warn(
                  `Warning: Failed to clear memory on index node ${nodeId}`
                );
              } else {
                console.log(`Memory cleared on index node ${nodeId}`);
                indexCleanupSuccesses++;
              }
              resolve();
            }
          );
        });
      } catch (e) {
        console.error(`Error contacting index node:`, e);
      }
    }

    console.log(
      `Memory cleanup completed: ${taxonomyCleanupSuccesses}/${crawl_nodes.length} taxonomy nodes and ${indexCleanupSuccesses}/${index_nodes.length} index nodes successfully cleaned`
    );

    if (global.gc) {
      console.log("Forcing garbage collection on main node...");
      global.gc();
      console.log("Garbage collection completed on main node");
    }

    await new Promise((resolve) => {
      distribution.taxonomy.crawler.save_maps_to_disk((err, result) => {
        if (err && !isEmptyObject(err)) {
          console.warn("Warning: Failed to save crawler data:", err);
        } else {
          console.log("Crawler data saved to disk during pause");
        }
        resolve();
      });
    });

    console.log("Checking node health during pause...");
    const nodeHealth = await checkNodeHealth();
    console.log(
      `Node health check: ${nodeHealth.healthy} healthy, ${nodeHealth.unhealthy} unhealthy`
    );

    return true;
  } catch (error) {
    console.error("Error during system cleanup:", error);
    return false;
  }
}

// !! HEAVY DEBUG
async function checkNodeHealth() {
  const healthStatus = {
    healthy: 0,
    unhealthy: 0,
    details: {},
    potentialLeaks: [],
  };

  healthStatus.currentSnapshot = {};

  try {
    await new Promise((resolve) => {
      distribution.taxonomy.status.get("memory", (err, memoryMap) => {
        if (err && !isEmptyObject(err)) {
          console.warn("Could not check taxonomy node memory:", err);
        } else {
          for (const nodeId in memoryMap) {
            const memory = memoryMap[nodeId];
            const heapUsed = memory.heapUsed;
            const heapTotal = memory.heapTotal;
            const usageRatio = heapUsed / heapTotal;

            const isHealthy = usageRatio < 0.85;

            if (isHealthy) {
              healthStatus.healthy++;
            } else {
              healthStatus.unhealthy++;
              if (usageRatio > 0.9) {
                healthStatus.potentialLeaks.push({
                  nodeId,
                  type: "taxonomy",
                  memoryUsage: `${(heapUsed / 1024 / 1024).toFixed(2)}MB/${(
                    heapTotal /
                    1024 /
                    1024
                  ).toFixed(2)}MB (${(usageRatio * 100).toFixed(1)}%)`,
                });
              }
            }

            healthStatus.details[nodeId] = {
              type: "taxonomy",
              heapUsed: `${(heapUsed / 1024 / 1024).toFixed(2)}MB`,
              heapTotal: `${(heapTotal / 1024 / 1024).toFixed(2)}MB`,
              usageRatio: `${(usageRatio * 100).toFixed(1)}%`,
              healthy: isHealthy,
            };
            healthStatus.currentSnapshot[nodeId] = { heapUsed, heapTotal };
          }
        }
        resolve();
      });
    });

    await new Promise((resolve) => {
      distribution.index.status.get("memory", (err, memoryMap) => {
        if (err && !isEmptyObject(err)) {
          console.warn("Could not check index node memory:", err);
        } else {
          for (const nodeId in memoryMap) {
            const memory = memoryMap[nodeId];
            const heapUsed = memory.heapUsed;
            const heapTotal = memory.heapTotal;
            const usageRatio = heapUsed / heapTotal;

            const isHealthy = usageRatio < 0.85;

            if (isHealthy) {
              healthStatus.healthy++;
            } else {
              healthStatus.unhealthy++;

              if (usageRatio > 0.9) {
                healthStatus.potentialLeaks.push({
                  nodeId,
                  type: "index",
                  memoryUsage: `${(heapUsed / 1024 / 1024).toFixed(2)}MB/${(
                    heapTotal /
                    1024 /
                    1024
                  ).toFixed(2)}MB (${(usageRatio * 100).toFixed(1)}%)`,
                });
              }
            }

            healthStatus.details[nodeId] = {
              type: "index",
              heapUsed: `${(heapUsed / 1024 / 1024).toFixed(2)}MB`,
              heapTotal: `${(heapTotal / 1024 / 1024).toFixed(2)}MB`,
              usageRatio: `${(usageRatio * 100).toFixed(1)}%`,
              healthy: isHealthy,
            };

            healthStatus.currentSnapshot[nodeId] = { heapUsed, heapTotal };
          }
        }
        resolve();
      });
    });

    if (healthStatus.potentialLeaks.length > 0) {
      console.warn("⚠️ POTENTIAL MEMORY OVERLOAD DETECTED ⚠️ :"); // Love GPT use of emojis lol
      healthStatus.potentialLeaks.forEach((leak) => {
        console.warn(
          `  - ${leak.type.toUpperCase()} node ${leak.nodeId}: ${
            leak.memoryUsage
          }`
        );
      });
      console.warn("Consider restarting these nodes if performance degrades");
    }

    return healthStatus;
  } catch (error) {
    console.error("Error checking node health:", error);
    return healthStatus;
  }
}

distribution.node.start(async (server) => {
  console.log("Starting crawler-indexer test with", nodes.length, "nodes");
  let metricsInterval = null;

  try {
    console.log("Spawning test nodes...");
    for (const node of nodes) {
      await spawnNode(node);
    }

    console.log("Creating taxonomy group...");
    const { groupConfig: taxonomyConfig, group: taxonomyGroup } =
      await createGroup("taxonomy", crawl_nodes);

    console.log("Creating index group...");
    const { groupConfig: indexConfig, group: indexGroup } = await createGroup(
      "index",
      index_nodes
    );

    console.log("Creating index group on taxonomy group...");
    await createGroupOnGroup("taxonomy", "index", index_nodes);

    console.log("Creating taxonomy group on index group...");
    await createGroupOnGroup("index", "taxonomy", crawl_nodes);

    // Step 5: Initialize the crawler service
    console.log("Initializing crawler service...");
    await new Promise((resolve, reject) => {
      distribution.taxonomy.crawler.initialize((err, result) => {
        if (err) {
          console.error("Error initializing crawler:", err);
          reject(err);
        } else {
          console.log("Crawler initialized successfully");
          resolve(result);
        }
      });
    });

    console.log("Adding starting links for crawling...");
    // TODO: Josh do we think these are good starting places?
    const seedLinks = [
      "/wiki/Cnidaria", // Sea life
      "/wiki/Plantae", // Plants
      "/wiki/Fungi", // Fungi
      "/wiki/Lepidoptera", // Butterflies
    ];

    for (const link of seedLinks) {
      await new Promise((resolve) => {
        distribution.taxonomy.crawler.add_link_to_crawl(link, (err, result) => {
          if (err) {
            console.error(`Error adding seed link ${link}:`, err);
          } else {
            console.log(`Seed link ${link} added successfully`);
          }
          resolve();
        });
      });
    }

    console.log("\n=== Starting Crawler-Indexer System ===");
    console.log("SIGINTTTYYY to stop the test");

    const crawlInterval = setInterval(() => {
      if (isPaused) {
        return;
      }

      distribution.taxonomy.crawler.crawl_one((err, result) => {
        if (err && !isEmptyObject(err)) {
          console.error("Error during crawl iteration:", err);
        } else if (result && Object.keys(result).length > 0) {
          // At least one node performed a successful crawl
          const successCount = Object.values(result).filter(
            (r) => r && r.status === "success"
          ).length;

          if (successCount > 0) {
            console.log(
              `Crawl iteration completed: ${successCount} pages processed`
            );
          }
        }
      });
    }, 2000);

    const saveInterval = setInterval(() => {
      distribution.taxonomy.crawler.save_maps_to_disk((err, result) => {
        if (err && !isEmptyObject(err)) {
          console.error("Error saving crawler data:", err);
        } else {
          console.log("Crawler data saved to disk");
        }
      });
    }, 30000); // 3 minutes i think...

    metricsInterval = setInterval(async () => {
      try {
        const crawlerStats = await getCrawlerStats();
        const indexerStats = await getIndexerStats();
        const metrics = aggregateMetrics(crawlerStats, indexerStats);

        const currentIndexedCount = metrics.indexing.totalDocumentsIndexed;
        const newlyIndexedDocs = currentIndexedCount - previousIndexedCount;
        documentsIndexedSinceLastPause += newlyIndexedDocs;
        previousIndexedCount = currentIndexedCount;

        if (documentsIndexedSinceLastPause >= 10 && !isPaused) {
          recoveryPauseCount++;
          isPaused = true;
          documentsIndexedSinceLastPause = 0;

          console.log(
            "\n=== SYSTEM RECOVERY PAUSE #" +
              recoveryPauseCount +
              " INITIATED ==="
          );
          console.log(`Time: ${new Date().toISOString()}`);
          console.log(`Indexed documents before pause: ${currentIndexedCount}`);
          console.log(
            "Pausing operations for 60 seconds to allow system recovery"
          );

          requestSystemCleanup()
            .then((success) => {
              console.log(
                `System cleanup ${
                  success
                    ? "completed successfully"
                    : "completed with some soul searching issues issues"
                }`
              );

              setTimeout(() => {
                isPaused = false;
                console.log("\n=== SYSTEM RECOVERY COMPLETE ===");
                console.log(
                  `Recovery pause #${recoveryPauseCount} completed at ${new Date().toISOString()}`
                );
                console.log("Resuming normal operation");

                if (recoveryPauseCount > 5) {
                  console.log(
                    "Multiple recovery pauses needed !! NOTE TO SELF: consider increasing pause frequency or reducing batch sizes"
                  );
                }
              }, 60000); // 1 minute pause
            })
            .catch((error) => {
              console.error("Error during system recovery:", error);

              setTimeout(() => {
                isPaused = false;
                console.log("\n=== SYSTEM RECOVERY COMPLETE (WITH ERRORS) ===");
                console.log(
                  `Recovery pause #${recoveryPauseCount} completed at ${new Date().toISOString()}`
                );
                console.log("Resuming normal operation despite cleanup errors");
              }, 60000);
            });
        }

        console.log("\n=== System Metrics ===");
        console.log(`Time: ${new Date().toISOString()}`);
        console.log(`Links in queue: ${metrics.links.totalToCrawl}`);
        console.log(`Links crawled: ${metrics.links.totalCrawled}`);
        console.log(`Target pages found: ${metrics.links.totalTargetsFound}`);
        console.log(`Pages processed: ${metrics.crawling.totalPagesProcessed}`);
        console.log(
          `Documents indexed: ${metrics.indexing.totalDocumentsIndexed}`
        );
        console.log(`Terms extracted: ${metrics.crawling.totalTermsExtracted}`);
        console.log(
          `Data downloaded: ${(
            metrics.crawling.totalBytesDownloaded /
            1024 /
            1024
          ).toFixed(2)}MB`
        );
        console.log(
          `Avg processing time: ${metrics.crawling.avgProcessingTime.toFixed(
            2
          )}ms`
        );
        console.log(
          `Avg indexing time: ${metrics.indexing.avgIndexTime.toFixed(2)}ms`
        );
        if (isPaused) {
          console.log("STATUS: System in recovery pause");
        }
        console.log("======================\n");
      } catch (err) {
        console.error("Error getting metrics:", err);
      }
    }, 10000);

    process.on("SIGINT", async () => {
      console.log("\nShutting down test...");
      clearInterval(crawlInterval);
      clearInterval(saveInterval);
      clearInterval(metricsInterval);

      await new Promise((resolve) => {
        distribution.taxonomy.crawler.cleanup((err, result) => {
          if (err) {
            console.error("Error cleaning up crawler:", err);
          } else {
            console.log("Crawler cleaned up successfully");
          }
          resolve();
        });
      });

      try {
        const crawlerStats = await getCrawlerStats();
        const indexerStats = await getIndexerStats();
        const metrics = aggregateMetrics(crawlerStats, indexerStats);

        console.log("\n=== Final Metrics ===");
        console.log(`Links crawled: ${metrics.links.totalCrawled}`);
        console.log(`Target pages found: ${metrics.links.totalTargetsFound}`);
        console.log(`Pages processed: ${metrics.crawling.totalPagesProcessed}`);
        console.log(
          `Documents indexed: ${metrics.indexing.totalDocumentsIndexed}`
        );
        console.log(`Terms extracted: ${metrics.crawling.totalTermsExtracted}`);
        console.log(
          `Data downloaded: ${(
            metrics.crawling.totalBytesDownloaded /
            1024 /
            1024
          ).toFixed(2)}MB`
        );
        console.log(
          `Avg processing time: ${metrics.crawling.avgProcessingTime.toFixed(
            2
          )}ms`
        );
        console.log(
          `Avg indexing time: ${metrics.indexing.avgIndexTime.toFixed(2)}ms`
        );
        console.log("======================\n");
      } catch (err) {
        console.error("Error getting final metrics:", err);
      }

      for (const node of nodes) {
        await stopNode(node);
      }

      server.close(() => {
        console.log("Server closed");
        process.exit(0);
      });
    });
  } catch (error) {
    console.error("Test failed:", error);

    clearInterval(metricsInterval);

    for (const node of nodes) {
      try {
        await stopNode(node);
      } catch (err) {
        console.error(`Error stopping node ${node.ip}:${node.port}:`, err);
      }
    }

    server.close(() => {
      console.log("Server closed due to error");
      process.exit(1);
    });
  }
});
