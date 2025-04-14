const distribution = require("./config.js");
const id = distribution.util.id;
const fs = require("fs");
const os = require("os");
const path = require("path");

// Set up nodes for distributed processing
const num_nodes = 4;
const nodes = [
  { ip: "3.225.196.65", port: 8000 },
  { ip: "34.231.129.22", port: 8000 },
  { ip: "35.168.108.255", port: 8000 },
  { ip: "54.145.177.180", port: 8000 },
];
const nids = [];
const testGroup = {};
const indexGroup = {};

// Define separate groups for different data types
const tfidfConfig = { gid: "tfidf" }; // For document data
const indexConfig = { gid: "index" }; // For term data

function isEmptyObject(obj) {
  return obj && typeof obj === "object" && Object.keys(obj).length === 0;
}

for (let i = 0; i < num_nodes; i++) {
  const nodeConfig = nodes[i];
  nodes.push(nodeConfig);
  nids.push(id.getNID(nodeConfig));

  // Add node to both groups
  const sid = id.getSID(nodeConfig);
  testGroup[sid] = nodeConfig;
  indexGroup[sid] = nodeConfig;
}

// Configuration
const CONFIG = {
  // Processing config
  BATCH_SIZE: 10, // Number of keys each node processes per batch
  MAX_EMPTY_BATCHES: 4, // Stop after this many consecutive empty batches
  PROCESS_CHUNK_SIZE: 10000, // Chunk size for processing terms
  SAVE_REFERENCE_COPY: true, // Whether to save a reference copy of the full index

  // Index structure config
  USE_DISTRIBUTED_STORE: true, // Whether to use the distributed key-value store
  STORE_DURING_PROCESSING: false, // Store terms immediately during processing or after

  // Group configuration
  TERM_GROUP: "index", // Group for storing terms
  DOC_GROUP: "tfidf", // Group for storing documents

  // Key prefixes
  INDEX_PREFIX: "term:", // Prefix for index keys in the distributed store
  DOC_PREFIX: "doc:", // Prefix for document keys in the distributed store

  // Output directories
  RESULTS_DIR: "./tfidf-results", // Directory for results
  BATCHES_DIR: "./tfidf-results/batches",

  // Inverted index config
  MAX_DOCS_PER_TERM: 500, // Maximum number of documents to store per term (reduced from 1000)
  MAX_TERMS_PER_DOC: 50, // Maximum number of top terms to store per document (reduced from 100)

  // Search optimization config
  CREATE_SHARDS: true, // Whether to create alphabet-based shards
  SHARD_SIZE: 2, // Number of letters per shard

  // Recovery config
  CHECKPOINT_INTERVAL: 10, // Save checkpoint every N batches
  RESUME_ENABLED: true, // Enable resuming from a checkpoint

  // Performance monitoring
  PERF_LOG_INTERVAL: 5, // Log performance stats every N batches
  MEMORY_MONITORING: true, // Monitor memory usage

  // Error handling
  MAX_RETRIES: 3, // Maximum number of retries for failed operations
  RETRY_DELAY: 1000, // Delay in ms between retries
};

// Performance tracking
const PERF = {
  startTime: Date.now(),
  batchTimes: [],
  throughput: [],
  memoryUsage: [],
  errors: 0,
  lastCheckpoint: 0,
};

// Main function to run the TF-IDF calculation
distribution.node.start(async (server) => {
  PERF.startTime = Date.now();
  console.log("SETTING UP OPTIMIZED TF-IDF TEST NODE...");

  // Helper function to spawn a node
  const spawn_node = (node) =>
    new Promise((resolve, reject) =>
      distribution.local.status.spawn(node, (e, v) => {
        console.log(
          `Spawned node at ${node.ip}:${
            node.port
          } ${distribution.util.id.getNID(node)} with result:`,
          e ? e : v
        );
        resolve(e, v);
      })
    );

  // Helper function to stop a node
  const stop_node = (node) =>
    new Promise((resolve, reject) =>
      distribution.local.comm.send(
        [],
        { service: "status", method: "stop", node: node },
        (e, v) => resolve(e, v)
      )
    );

  // Start the nodes
  for (let i = 0; i < nodes.length; i++) {
    const node = nodes[i];
    try {
      await spawn_node(node);
      console.log(`Node started at ${node.ip}:${node.port}`);
    } catch (e) {
      console.error(`Failed to start node at ${node.ip}:${node.port}`, e);
      finish();
      return;
    }
  }

  // Set up the TFIDF group
  distribution.local.groups.put(tfidfConfig, testGroup, (e, v) => {
    if (e && !isEmptyObject(e)) {
      console.error("Error setting up TFIDF group:", e);
      finish();
      return;
    }

    console.log("TFIDF group set up successfully");

    // Set up the INDEX group
    distribution.local.groups.put(indexConfig, indexGroup, (e, v) => {
      if (e && !isEmptyObject(e)) {
        console.error("Error setting up INDEX group:", e);
        finish();
        return;
      }

      console.log("INDEX group set up successfully");

      // Set up the group in TFIDF service
      console.log(
        "Setting up TFIDF service group with the following configuration:",
        tfidfConfig,
        testGroup
      );
      distribution.tfidf.groups.put(tfidfConfig, testGroup, (e, v) => {
        if (e && !isEmptyObject(e)) {
          console.error("Error setting up TFIDF service group:", e);
          finish();
          return;
        }

        console.log(
          "TFIDF service group set up successfully, starting TF-IDF calculation..."
        );

        // Get total documents count to use in final calculation
        distribution.tfidf.store.get({ key: null }, async (err, allKeys) => {
          if (err && !isEmptyObject(err)) {
            console.error("Error getting document keys:", err);
            finish();
            return;
          }

          // Set total documents count
          const totalDocuments = allKeys.length;
          console.log(`Total documents to process: ${totalDocuments}`);

          if (totalDocuments === 0) {
            console.log("No documents found to process!");
            finish();
            return;
          }

          // Configure batch processing
          const BATCH_SIZE = CONFIG.BATCH_SIZE;
          const ESTIMATED_TOTAL_BATCHES = Math.ceil(
            totalDocuments / (BATCH_SIZE * num_nodes)
          );
          CONFIG.ESTIMATED_TOTAL_BATCHES = ESTIMATED_TOTAL_BATCHES; // Store for progress estimation

          console.log(
            `Processing with ${num_nodes} nodes, each processing ${BATCH_SIZE} keys per batch`
          );
          console.log(
            `Estimated total batches: ${ESTIMATED_TOTAL_BATCHES} (may vary based on key distribution)`
          );

          // Finish and shutdown
          finish();
        });
      });
    });
  });

  // Cleanup function
  const finish = async () => {
    console.log("SHUTTING DOWN...");
    for (const node of nodes) {
      await stop_node(node);
    }
    server.close();
  };
});
