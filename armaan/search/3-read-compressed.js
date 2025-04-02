const LZ = require("lz-string");
const fs = require("fs");
const distribution = require("../config.js");
// const data = fs
//   .readFileSync(
//     "store/c65b0147f5b849e3906c4667dd83b19dc3f684e63d9614bc9fbfd9404266e0d6/tfidf/-wiki--C3-97-Aegilotriticum-erebunii"
//   )
//   .toString();
// const decompressed = LZ.decompressFromBase64(JSON.parse(data).value);
// console.log(data, decompressed);

distribution.node.start(async (server) => {
  console.log("Node started for testing read-compressed functionality.");

  let node = { ip: "127.0.0.1", port: 7110 };

  distribution.local.status.spawn(node, (e, v) => {
    let testGroup = {};
    testGroup[distribution.util.id.getSID(node)] = node;
    let testConfig = { gid: "tfidf" };

    if (e) {
      console.error("Error spawning node:", e);
      server.stop(() => {
        console.log("Server stopped due to spawn error.");
      });
      return;
    }
    console.log(
      "Spawned node at",
      `${node.ip}:${node.port}`,
      "with result:",
      v
    );
    // Set up the group for testing
    distribution.local.groups.put(testConfig, testGroup, (e, v) => {
      let messageConfig = {
        service: "store",
        method: "get",
        node: node,
      };
      let message = [
        {
          gid: "tfidf", // Group ID where documents are stored
          key: null, // Get all keys in the group
        },
      ];
      distribution.local.comm.send(message, messageConfig, (err, result) => {
        if (err) {
          console.error("Error retrieving data from store:", err);
          server.close(() => {
            console.log("Server stopped due to store retrieval error.");
          });
          return;
        }
        if (!result) {
          console.error("No result found in the store.");
          server.stop(() => {
            console.log("Server stopped due to no result found.");
          });
          return;
        }

        let keys = result.slice(0, 12);

        console.log(
          "Successfully retrieved keys from store:",
          keys,
          Array.isArray(keys) ? keys.length : 0
        );

        let pending = keys.length;

        Array.from(keys).forEach((key) => {
          distribution.tfidf.store.get(key, (e, v) => {
            if (e) {
              console.error(`Error retrieving key ${key} from store:`, e);
              pending--;
              if (pending === 0) {
                server.close(() => {
                  console.log("Server stopped after retrieval errors.");
                });
              }
              return;
            }
            // const decompressed = LZ.decompressFromBase64(v);

            // distribution.tfidf.store.put(decompressed, key, (e, v) => {
            //   if (e) {
            //     console.error(
            //       `Error putting decompressed data back to store for key ${key}:`,
            //       e
            //     );
            //   } else {
            //     console.log(
            //       `Successfully put decompressed data back to store for key ${key}`
            //     );
            //   }
            // });

            pending--;
            if (pending === 0) {
              console.log(
                `Successfully retrieved and decompressed data for key ${key}:`,
                typeof v,
                v
              );
              console.log(
                "All TF-IDF retrievals completed, shutting down server."
              );

              // All done, close the server
              server.close(() => {
                console.log("Server stopped after all retrievals.");
              });
            }
          });
        });
      });
    });
  });

  // You can add any additional logic here if needed
  // For example, you might want to perform some operations using the decompressed data
  // distribution.local.comm.send(
  //   [],
  //   { service: "status", method: "stop", node: node },
  //   (e, v) => {
  //     if (e) {
  //       console.error("Failed to stop the node:", e);
  //     } else {
  //       console.log("Node stopped successfully.");
  //     }
  //   }
  // );
});
