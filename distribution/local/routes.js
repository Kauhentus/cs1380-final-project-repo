/** @typedef {import("../types").Callback} Callback */

// This table will map from a service name to its configuration
// This will now be a mapping from gid -> service name -> configuration
// This is a global variable that will be used to store the routes
// The type nesting is string -> string -> object
global.routesTable = {};

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};

/**
 * @param {string} configuration
 * @param {Callback} callback
 * @return {void}
 */
function get(configuration, callback) {
  callback = callback || cb;
  let gid = 'local';
  let service = 'status';
  if (configuration === undefined || configuration === null) {
    callback(null, global.routesTable[gid][configuration]);
  }
  if (typeof configuration === "object") {
    if (configuration.gid) {
      gid = configuration.gid;
    }
    if (configuration.service) {
      service = configuration.service;
    }
  } else if (typeof configuration === "string") {
    service = configuration;
  }
  if (
    !global.routesTable || 
    !global.routesTable[gid] || 
    !global.routesTable[gid][service]
  ) {
    const rpc = global.toLocal[service];
    if (rpc) {
      callback(null, {call: rpc});
    } else {
      callback(new Error(`Service ${service} in the ${gid} group does not exist`), null);
      return;
    }
  } else {
    callback(null, global.routesTable[gid][service]);
  }
}

/**
 * @param {object} service
 * @param {string} configuration
 * @param {Callback} callback
 * @return {void}
 */
function put(service, configuration, callback = cb) {
  let gid = "local";

  if (!service) {
      return callback(new Error("Service cannot be null or undefined"));
  } else if (typeof service !== "object") {
    return callback(new Error("Service must be an object"));
  }

  if (!configuration) {
    return callback(new Error("Configuration cannot be null or undefined"));
  }

  if (typeof configuration === "object") {
    gid = configuration.gid || gid;
    configuration = configuration.service || "";
  } else if (typeof configuration !== "string") {
    return callback(new Error("Configuration must be a string or an object with a service key"));
  }

  global.routesTable[gid] = global.routesTable[gid] || {};
  global.routesTable[gid][configuration] = service;

  // console.log(`Successfully added service ${configuration} to the ${gid} group`)
  // console.log(global.routesTable)
  callback(null, `Successfully added service ${configuration} to the ${gid} group`);
}

/**
 * @param {string} configuration
 * @param {Callback} callback
 */
function rem(configuration, callback) {
  callback = callback || cb;
  let gid = "local";
  if (!configuration) {
    callback(null, "");
  }
  if (typeof configuration === "object") {
    if (configuration.gid) {
      gid = configuration.gid;
    }
    if (configuration.service) {
      configuration = configuration.service;
    }
  } else if (typeof configuration !== "string") {
    callback(new Error("Configuration must be a string or an object with a service key"), null);
  }
  if (
    !global.routesTable || 
    !global.routesTable[gid] || 
    !global.routesTable[gid][configuration]
  ) {
    callback(new Error(`Service ${configuration} for the ${gid} group does not exist`), null);
    return;
  } else {
    delete global.routesTable[gid][configuration];
    callback(null, `Successfully removed service ${configuration}`);
  }
}

module.exports = { get, put, rem };
