const { id } = require('../util/util');
const fs = require('fs');

const groups = {};
const groups_map = {};

groups.groups_map = groups_map;

groups.get = function (name, callback) {
  if (!callback) throw Error('called groups.get without callback');

  if (name === "all") {
    const retrieved_groups = Object.keys(groups_map).map(key => groups_map[key]);
    const all_groups = Object.assign({}, ...retrieved_groups);
    return callback(null, all_groups);
  }
  else if (name in groups_map) {
    return callback(null, groups_map[name]);
  }
  else {
    return callback(new Error(`group ${name} not found for groups/get`), null);
  }
};

groups.put = function (config, group, callback) {
  // console.log("GROUPS PUT CALLED ON NODE", global.nodeConfig.port);
  callback = callback || function () { };

  // check config format
  let gid;
  if (typeof config === "string") {
    gid = config;
  } else if (typeof config === "object" && "gid" in config) {
    gid = config.gid;
  } else {
    return callback(new Error("Invalid group config"), null);
  }

  // put group to groups_map (overwrite OK since we can put add)
  groups_map[gid] = group;

  // add group to distribution.<gid>
  if (!(gid in distribution)) {
    distribution[gid] = {};

    // TODO check if this is how to populate it?
    distribution[gid].comm = require('../all/comm')(config);
    distribution[gid].status = require('../all/status')(config);
    distribution[gid].routes = require('../all/routes')(config);
    distribution[gid].groups = require('../all/groups')(config);
    distribution[gid].gossip = require('./gossip');
    distribution[gid].mem = require('../all/mem')(config);
    distribution[gid].store = require('../all/store')(config);
    distribution[gid].mr = require('../all/mr')(config);

    distribution[gid].crawler = require('../all/crawler')(config);
    distribution[gid].indexer = require('../all/indexer')(config);
    distribution[gid].indexer_ranged = require('../all/indexer_ranged')(config);
    distribution[gid].querier = require('../all/querier')(config);
  }

  callback(null, group);
};

groups.del = function (name, callback) {
  callback = callback || function () { };

  if (name in groups_map) {
    let group_to_delete = groups_map[name];
    delete groups_map[name];

    // delete distribution[name];

    return callback(null, group_to_delete);
  }
  else {
    return callback(new Error("group not found for groups/del"), null);
  }
};

groups.add = function (name, node, callback) {
  // console.log("ADDED NODE", node ,"TO GROUP", name, global.nodeConfig)

  callback = callback || function () { };

  if (!(name in groups_map)) {
    return callback(null);
  }

  const sid = id.getSID(node);
  groups_map[name][sid] = node;
  callback(null, node);
};

groups.rem = function (name, node, callback) {
  callback = callback || function () { };

  if (!(name in groups_map)) {
    return callback(null);
  }

  let sid;
  if (typeof node === "string") {
    sid = node;
  } else {
    sid = id.getSID(node);
  }

  if (!(sid in groups_map[name])) {
    return callback(null);
  }
  delete groups_map[name][sid];
  callback(null);
};

module.exports = groups;
