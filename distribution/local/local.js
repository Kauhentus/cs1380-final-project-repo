/*

Service  Description                                Methods
status   Status and control of the current node     get, spawn, stop
comm     A message communication interface          send
groups   A mapping from group names to nodes        get, put, add, rem, del
gossip   The receiver part of the gossip protocol   recv
routes   A mapping from names to functions          get, put

*/

/* Status Service */

const status = require("./status");

/* Groups Service */

const groups = require("./groups");

/* Routes Service */

const routes = require("./routes");

/* Comm Service */

const comm = require("./comm");

/* Gossip Service */

const gossip = require("./gossip");

/* Mem Service */

const mem = require("./mem");

/* Store Service */

const store = require("./store");

/* Crawler Service */

const crawler = require("./crawler");

/* Indexer Services */

const indexer = require("./indexer");

/* Ranged Indexer Service */

const indexer_ranged = require("./indexer_ranged");

/* Querier Service */

const querier = require("./querier");

module.exports = {
  status: status,
  routes: routes,
  comm: comm,
  groups: groups,
  gossip: gossip,
  mem: mem,
  store: store,

  crawler: crawler,
  indexer: indexer,
  indexer_ranged: indexer_ranged,
  querier: querier,
};
