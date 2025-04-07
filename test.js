const distribution = require("./config.js");
const id = distribution.util.id;

const inNodes = [
  { ip: "127.0.0.1", port: 7112 },
  { ip: "127.0.0.1", port: 7113 },
  { ip: "127.0.0.1", port: 7114 },
  { ip: "127.0.0.1", port: 7115 },
];

const outNodes = [
  { ip: "127.0.0.1", port: 7112 },
  { ip: "127.0.0.1", port: 7113 },
  { ip: "127.0.0.1", port: 7114 },
  { ip: "127.0.0.1", port: 7115 },
  { ip: "127.0.0.1", port: 7116 },
  { ip: "127.0.0.1", port: 7117 },
  { ip: "127.0.0.1", port: 7118 },
  { ip: "127.0.0.1", port: 7119 },
];

console.log("Starting test script...");

for (let i = 0; i < outNodes.length; i++) {
  const nodeConfig = outNodes[i];
  const nid = id.getNID(nodeConfig);
  console.log(
    `Testing input node ${i + 1} with NID: ${nid} and port: ${nodeConfig.port}`
  );
}

// // Test the connectivity to each node
// async function testNodes() {
//   for (let i = 0; i < nodes.length; i++) {
//     const nodeConfig = nodes[i];
//     const nid = id.getNID(nodeConfig);
//     console.log(
//       `Testing node ${i + 1} with NID: ${nid} and  port: ${nodeConfig.ip}`
//     );
//   }
// }

// async function runTests() {
//   console.log("Running connectivity tests...");
//   await testNodes();
//   console.log("All nodes tested successfully.");
// }

// // Execute the tests
// runTests()
//   .then(() => {
//     console.log("Test script completed successfully.");
//   })
//   .catch((error) => {
//     console.error("Error running test script:", error);
//   });
