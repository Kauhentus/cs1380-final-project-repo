#!/usr/bin/env node

const util = require("./jdistribution/util/util.js");
const log = require("./jdistribution/util/log.js");
const args = require("yargs").argv;

// Default configuration
global.nodeConfig = global.nodeConfig || {
  ip: "127.0.0.1",
  port: 1234,
  onStart: () => {
    console.log(`Node started!`);
  },
};

/*
You can pass "ip" and "port" arguments directly.
Use this to startup nodes from the terminal.

Usage:
./distribution.js --ip '127.0.0.1' --port 1234 # Start node on localhost:1234
  */
if (args.ip) {
  global.nodeConfig.ip = args.ip;
}

if (args.port) {
  global.nodeConfig.port = parseInt(args.port);
}

if (args.config) {
  const nodeConfig = util.deserialize(args.config);
  global.nodeConfig.ip = nodeConfig.ip ? nodeConfig.ip : global.nodeConfig.ip;
  global.nodeConfig.port = nodeConfig.port
    ? nodeConfig.port
    : global.nodeConfig.port;
  global.nodeConfig.onStart = nodeConfig.onStart
    ? nodeConfig.onStart
    : global.nodeConfig.onStart;
}

const distribution = function (config) {
  if (config) {
    global.nodeConfig = config;
    this.nodeConfig = config;
  }

  return global.distribution;
};

// Don't overwrite the distribution object if it already exists
if (global.distribution === undefined) {
  global.distribution = distribution;
}

distribution.util = require("./jdistribution/util/util.js");
distribution.local = require("./jdistribution/local/local.js");
distribution.node = require("./jdistribution/local/node.js");

for (const key in distribution.local) {
  distribution.local.routes.put(distribution.local[key], key);
}

/* Initialize distribution object */
distribution["all"] = {};
distribution["all"].status = require("./jdistribution/all/status.js")({
  gid: "all",
});
distribution["all"].comm = require("./jdistribution/all/comm.js")({
  gid: "all",
});
distribution["all"].gossip = require("./jdistribution/all/gossip.js")({
  gid: "all",
});
distribution["all"].groups = require("./jdistribution/all/groups.js")({
  gid: "all",
});
distribution["all"].routes = require("./jdistribution/all/routes.js")({
  gid: "all",
});
distribution["all"].mem = require("./jdistribution/all/mem.js")({ gid: "all" });
distribution["all"].store = require("./jdistribution/all/store.js")({
  gid: "all",
});

distribution.node.config = global.nodeConfig;
module.exports = distribution;

/* The following code is run when distribution.js is run directly */
if (require.main === module) {
  log(
    `[node] Starting node with configuration: ${JSON.stringify(
      global.nodeConfig
    )}`
  );
  distribution.node.start(global.nodeConfig.onStart);
}
