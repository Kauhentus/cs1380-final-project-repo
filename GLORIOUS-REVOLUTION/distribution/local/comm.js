/** @typedef {import("../types").Callback} Callback */
/** @typedef {import("../types").Node} Node */

const { appendFileSync } = require("fs");
const http = require("http");

/**
 * @typedef {Object} Target
 * @property {string} service
 * @property {string} method
 * @property {Node} node
 */

/**
 * @param {Array} message
 * @param {Target} remote
 * @param {Callback} [callback]
 * @return {void}
 */
function send(message, remote, callback, retries = 3, backoff = 500) {
  const serialize = distribution.util.serialize;
  const deserialize = distribution.util.deserialize;

  let data;
  try {
    data = serialize(message);
  } catch (error) {
    callback(error);
    return;
  }

  // Validate parameters
  if (!remote.node) {
    callback(new Error("no node"));
    return;
  }
  if (!remote.node.ip) {
    callback(new Error("no node ip"));
    return;
  }
  if (!remote.node.port) {
    callback(new Error("no node port"));
    return;
  }
  if (!remote.service) {
    callback(new Error("no service"));
    return;
  }
  if (!remote.method) {
    callback(new Error("no method"));
    return;
  }

  const has_gid = "gid" in remote;

  const options = {
    method: "PUT",
    hostname: remote.node.ip,
    port: remote.node.port,
    path: `/${has_gid ? remote.gid : "local"}/${remote.service}/${
      remote.method
    }`,
    headers: {
      "Content-Type": "application/json",
      "Content-Length": Buffer.byteLength(data),
    },
  };

  // use timeout to let first comm request finish before the next comm request
  setTimeout(() => {
    const req = http.request(options, (res) => {
      let response_data = "";
      res.on("error", (e) => {
        callback(e);
      });

      res.on("data", (chunk) => {
        response_data += chunk;
      });

      res.on("end", () => {
        try {
          if (res.statusCode !== 200) {
            callback(
              new Error(
                `errored in comm with status code ${res.statusCode} and message ${res.statusMessage}`
              )
            );
            return;
          }
          const result = deserialize(response_data);
          if (typeof result === "object" && "e" in result && "v" in result) {
            // distributed callback
            callback(result.e, result.v);
          } else {
            // non-distributed callback
            callback(null, result);
          }
        } catch (error) {
          callback(new Error(`ERR: ${error.message} and ${response_data}`));
        }
      });
    });

    req.on("error", (e) => {
      if (
        retries > 0 &&
        (e.code === "ECONNRESET" || e.code === "ECONNREFUSED")
      ) {
        if (retries === 1)
          console.log(
            `Connection error (${e.code}), retrying in ${backoff}ms. Retries left: ${retries} ${remote.service} ${remote.method} ${remote.node.port}`
          );
        setTimeout(() => {
          send(message, remote, callback, retries - 1, backoff * 2);
        }, backoff);
        return;
      }

      // If we've exhausted normal retries and still have a connection issue,
      // attempt to restart the node and try one last time
      if (e.code === "ECONNREFUSED" || e.code === "ECONNRESET") {
        console.log(
          `Node ${remote.node.ip}:${remote.node.port} appears to be down. Attempting to restart it...`
        );

        // Try to spawn the node
        distribution.local.status.spawn(
          remote.node,
          (spawnErr, spawnResult) => {
            if (spawnErr) {
              console.error(
                `Failed to restart node ${remote.node.ip}:${remote.node.port}:`,
                spawnErr
              );
              callback(
                new Error(
                  `Failed to connect to node and restart attempt failed: ${e.message}`
                )
              );
              return;
            }

            console.log(
              `Successfully restarted node ${remote.node.ip}:${remote.node.port}, waiting for it to initialize...`
            );

            // Wait a bit for the node to initialize fully before retrying
            setTimeout(() => {
              console.log(
                `Retrying request to restarted node ${remote.node.ip}:${remote.node.port}...`
              );

              // Make one final attempt with the newly spawned node
              send(
                message,
                remote,
                (finalErr, finalResult) => {
                  if (finalErr) {
                    console.error(
                      `Request to restarted node still failed:`,
                      finalErr
                    );
                    callback(
                      new Error(
                        `Failed to connect even after node restart: ${finalErr.message}`
                      )
                    );
                  } else {
                    console.log(`Request to restarted node succeeded!`);
                    callback(null, finalResult);
                  }
                },
                0,
                0
              ); // No more retries on this final attempt
            }, 2000); // 2 second delay to allow node to initialize
          }
        );
      } else {
        // For errors other than connection issues
        callback(new Error(e.message));
      }
    });

    req.write(data);
    req.end();
  });
}

module.exports = { send };
