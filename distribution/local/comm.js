/** @typedef {import("../types").Callback} Callback */
/** @typedef {import("../types").Node} Node */

const { appendFileSync } = require("fs");
const http = require("http");
const { exec } = require("child_process");

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
function killPortProcess(port) {
  return new Promise((resolve) => {
    const command = `lsof -ti:${port} | xargs kill -9 || true`;

    console.log(`Attempting to free port ${port}...`);
    exec(command, (error) => {
      if (error) {
        console.log(`Warning: Could not free port ${port}: ${error.message}`);
      } else {
        console.log(`Successfully freed port ${port}`);
      }
      setTimeout(resolve, 1000);
    });
  });
}

const circuitBreakers = {};

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

  const nodeId = `${remote.node.ip}:${remote.node.port}`;

  if (circuitBreakers[nodeId] && circuitBreakers[nodeId].failures > 5) {
    if (Date.now() - circuitBreakers[nodeId].lastAttempt < 5000) {
      return callback(new Error(`Circuit open for ${nodeId}`));
    }
    // Reset completely after cooldown period
    circuitBreakers[nodeId].failures = 0; // Reset the counter, not just the timestamp
    circuitBreakers[nodeId].lastAttempt = Date.now();
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
            // This is still an error case
            callback(
              new Error(
                `errored in comm with status code ${res.statusCode} and message ${res.statusMessage}`
              )
            );
            return;
          }

          // Success case - reset circuit breaker
          if (circuitBreakers[nodeId]) {
            circuitBreakers[nodeId].failures = 0; // Reset failures on success
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

    req.on("error", async (e) => {
      if (!circuitBreakers[nodeId]) {
        circuitBreakers[nodeId] = { failures: 0, lastAttempt: Date.now() };
      }
      circuitBreakers[nodeId].failures++;
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

      if (e.code === "ECONNREFUSED" || e.code === "ECONNRESET") {
        console.log(
          `Node ${remote.node.ip}:${remote.node.port} appears to be down. Attempting to restart it...`
        );

        await killPortProcess(remote.node.port);

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
            setTimeout(() => {
              console.log(
                `Retrying request to restarted node ${remote.node.ip}:${remote.node.port}...`
              );
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
              );
            }, 2000);
          }
        );
      } else {
        callback(new Error(e.message));
      }
    });

    req.write(data);
    req.end();
  });
}

module.exports = { send };
