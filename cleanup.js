/**
 * Node Cleanup Script
 *
 * This script gracefully terminates distribution nodes.
 * It can be configured to stop any list of nodes specified in the configuration.
 */

const distribution = require("./config.js");
const fs = require("fs");
const path = require("path");

// Configuration options
const CONFIG = {
  // Set to true to load nodes from a JSON file
  useConfigFile: false,
  // Path to optional config file (JSON format)
  configFilePath: "./node_config.json",
  // Default timeout per node (in milliseconds)
  nodeStopTimeout: 5000,
  // Default delay between stopping nodes (in milliseconds)
  stopDelay: 100,
  // Show verbose logging
  verbose: true,
};

// Default node configurations if not using a config file
const DEFAULT_CONFIGS = {
  inNodes: [
    { ip: "127.0.0.1", port: 7110 },
    { ip: "127.0.0.1", port: 7111 },
    { ip: "127.0.0.1", port: 7112 },
    { ip: "127.0.0.1", port: 7113 },

    { ip: "127.0.0.1", port: 7114 },
    { ip: "127.0.0.1", port: 7115 },
    { ip: "127.0.0.1", port: 7116 },
    { ip: "127.0.0.1", port: 7117 },
  ],
  outNodes: [
    // { ip: "127.0.0.1", port: 8003 },
    // { ip: "127.0.0.1", port: 8004 },
    // { ip: "127.0.0.1", port: 8005 },
    // { ip: "127.0.0.1", port: 8006 },
    // { ip: "127.0.0.1", port: 8007 },
    // { ip: "127.0.0.1", port: 8008 },
    // { ip: "127.0.0.1", port: 8116 },
    // { ip: "127.0.0.1", port: 8117 }
  ],
  // Add any additional node configurations here
  additionalNodes: [
    // { ip: "127.0.0.1", port: 7110 },
    // { ip: "127.0.0.1", port: 7111 },
    // { ip: "127.0.0.1", port: 7116 },
    // { ip: "127.0.0.1", port: 7117 }
  ],
};

/**
 * Helper function to stop a single node with timeout
 * @param {Object} node - Node configuration object with ip and port
 * @returns {Promise} - Resolves when node is stopped or rejects on error/timeout
 */
function stopNode(node) {
  return new Promise((resolve, reject) => {
    const nodeId = `${node.ip}:${node.port}`;
    const timeoutId = setTimeout(() => {
      reject(new Error(`Timeout while stopping node ${nodeId}`));
    }, CONFIG.nodeStopTimeout);

    const remote = {
      service: "status",
      method: "stop",
      node: node,
    };

    if (CONFIG.verbose) {
      console.log(`Attempting to stop node at ${nodeId}...`);
    }

    distribution.local.comm.send([], remote, (error, result) => {
      clearTimeout(timeoutId);

      if (error) {
        console.error(`Failed to stop node at ${nodeId}: ${error}`);
        reject(error);
      } else {
        console.log(`Successfully stopped node at ${nodeId}`);
        resolve(result);
      }
    });
  });
}

/**
 * Stop a list of nodes with delay between each stop attempt
 * @param {Array} nodes - Array of node configuration objects
 * @returns {Promise} - Resolves when all nodes are processed
 */
async function stopNodes(nodes) {
  console.log(`Starting to stop ${nodes.length} nodes...`);

  const results = {
    success: 0,
    failed: 0,
    nodes: [],
  };

  for (const node of nodes) {
    try {
      await stopNode(node);
      results.success++;
      results.nodes.push({ node, status: "success" });
    } catch (error) {
      results.failed++;
      results.nodes.push({ node, status: "failed", error: error.message });
    }

    // Add a small delay between stopping nodes to avoid overwhelming the system
    await new Promise((resolve) => setTimeout(resolve, CONFIG.stopDelay));
  }

  return results;
}

/**
 * Main cleanup function
 */
async function cleanup() {
  console.log("=== Node Cleanup Script ===");
  let nodeConfig;

  // Load configuration from file if enabled
  if (CONFIG.useConfigFile) {
    try {
      if (fs.existsSync(CONFIG.configFilePath)) {
        const configContent = fs.readFileSync(CONFIG.configFilePath, "utf8");
        nodeConfig = JSON.parse(configContent);
        console.log(`Loaded node configuration from ${CONFIG.configFilePath}`);
      } else {
        console.warn(
          `Config file ${CONFIG.configFilePath} not found, using default config`
        );
        nodeConfig = DEFAULT_CONFIGS;
      }
    } catch (error) {
      console.error(`Error loading config file: ${error.message}`);
      console.log("Falling back to default configuration");
      nodeConfig = DEFAULT_CONFIGS;
    }
  } else {
    nodeConfig = DEFAULT_CONFIGS;
  }

  // Combine all nodes into a single array
  const allNodes = [
    ...(nodeConfig.inNodes || []),
    ...(nodeConfig.outNodes || []),
    ...(nodeConfig.additionalNodes || []),
  ];

  // Remove duplicate nodes (based on IP and port)
  const uniqueNodes = [];
  const nodeMap = new Map();

  for (const node of allNodes) {
    const key = `${node.ip}:${node.port}`;
    if (!nodeMap.has(key)) {
      nodeMap.set(key, true);
      uniqueNodes.push(node);
    }
  }

  console.log(`Prepared to stop ${uniqueNodes.length} unique nodes`);

  try {
    // Stop all nodes
    const results = await stopNodes(uniqueNodes);

    // Display final summary
    console.log("\n=== Cleanup Summary ===");
    console.log(`Total nodes processed: ${uniqueNodes.length}`);
    console.log(`Successfully stopped: ${results.success}`);
    console.log(`Failed to stop: ${results.failed}`);

    if (results.failed > 0 && CONFIG.verbose) {
      console.log("\nFailed nodes:");
      results.nodes
        .filter((item) => item.status === "failed")
        .forEach((item) => {
          console.log(
            `- ${item.node.ip}:${item.node.port} - Error: ${item.error}`
          );
        });
    }

    console.log("\nCleanup complete!");
  } catch (error) {
    console.error(`Critical error during cleanup: ${error.message}`);
  } finally {
    // Always exit when done
    process.exit(0);
  }
}

// Create a node_config.json template file if it doesn't exist
function createConfigTemplate() {
  if (!CONFIG.useConfigFile) {
    return;
  }

  if (!fs.existsSync(CONFIG.configFilePath)) {
    try {
      fs.writeFileSync(
        CONFIG.configFilePath,
        JSON.stringify(DEFAULT_CONFIGS, null, 2),
        "utf8"
      );
      console.log(`Created template config file at ${CONFIG.configFilePath}`);
    } catch (error) {
      console.error(`Failed to create config template: ${error.message}`);
    }
  }
}

// Handle process termination
process.on("SIGINT", () => {
  console.log("\nCleanup interrupted. Exiting...");
  process.exit(1);
});

// Run the script
createConfigTemplate();
cleanup();
