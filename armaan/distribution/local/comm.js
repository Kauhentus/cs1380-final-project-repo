/** @typedef {import("../types").Callback} Callback */
/** @typedef {import("../types").Node} Node */
const http = require("http");
const util = require("../util/util");

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};

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
function send(message, remote, callback) {
  callback = callback || cb;
  if (message === undefined || message === null) {
    // If no message is provided, we assume the default message is a node id
    message = ["nid"];
  }
  if (remote === undefined || remote === null || !remote.node) {
    callback(new Error("Remote node configuration is required"), null);
    return;
  }
  if (!remote.service || !remote.method) {
    callback(new Error("Service and method are required"), null);
    return;
  }
  let gid = remote.gid || "local";
  let service = remote.service;
  let method = remote.method;
  let nodeConfig = remote.node; // {ip: , port: }

  const path = `/${gid}/${service}/${method}`;

  const options = {
    hostname: nodeConfig.ip,
    port: nodeConfig.port,
    path: path,
    method: "PUT",
    headers: { "Content-Type": "application/json" },
  };

  const req = http.request(options, (res) => {
    // First we process the response by listening for data events
    let chunks = [];

    res.on("data", (chunk) => {
      chunks.push(chunk);
    });

    // Once all of the data has been processed, we can parse it
    res.on("end", () => {
      let body = Buffer.concat(chunks).toString();
      let parsed;
      try {
        parsed = JSON.parse(body);
        parsed = util.deserialize(parsed);
      } catch (e) {
        callback(new Error("Failed to parse JSON response"), null);
        return;
      }

      // The remote node typically returns [error, value]
      if (!Array.isArray(parsed) || parsed.length !== 2) {
        return callback(new Error("Invalid response format: " + body));
      }
      let [err, val] = parsed;

      // If the remote serialized an error, it might be a string or an object
      if (
        err &&
        (err instanceof Error ||
          (typeof err === "string" && err.trim() !== "") ||
          (typeof err === "object" && Object.keys(err).length > 0))
      ) {
        if (typeof err === "object" && !Array.isArray(err)) {
          err = JSON.stringify(err);
        }
        return callback(new Error(err), null);
      }

      // No error => pass the value
      if (gid !== "local") {
        callback(err, val);
      } else {
        callback(null, val);
      }
    });
  });

  req.on("error", (e) => {
    callback(new Error(e.message), null);
  });

  // Send the message as a JSON string
  req.write(JSON.stringify(util.serialize(message)));
  req.end();
}

module.exports = { send };
