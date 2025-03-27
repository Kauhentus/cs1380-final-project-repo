const util = require('../util/util');
const id = util.id;

// This will be a memory mapping from 'gid' -> 'key' -> 'object reference'
const memory = {
  local: {},
  all: {}
};

const cb = (err, val) => {
    if (err) {
        console.error(err);
    } else {
        console.log(val);
    }
}

function put(state, configuration, callback) {
    callback = callback || cb;
  let key;
  let gid = 'local';

  // If no state is given, error out
  if (!state) {
    return callback(new Error('No state to put'));
  }

  /*
    We have four main cases for `configuration`:
    1) null (meaning "no key provided," so we use the sha256 ID of the object)
    2) an object (which might have .key or .gid)
    3) a string (the actual key)
    4) falsy/undefined (no key)
  */

  if (configuration === null || configuration === undefined) {
    // "no key" => we use sha256 of the object
    key = id.getID(state);
  } else if (typeof configuration === 'object') {
    // Ensure we are not dealing with null in here
    if (configuration !== null) {
      if (configuration.key) {
        key = configuration.key;
      }
      if (configuration.gid) {
        gid = configuration.gid;
      }
    }
    // If configuration.key was missing entirely, key remains undefined. 
    // That typically means we have to error or handle it. 
    // But the tests always pass an actual key or rely on the "no key" case = null
    if (!key) {
      // If the object config had no 'key' property -> fallback to hashing
      key = id.getID(state);
    }
  } else if (typeof configuration === 'string') {
    key = configuration;
  } else {
    // If it’s not null/undefined/object/string, it’s some other falsy => use hashed ID
    key = id.getID(state);
  }

  // Store the object reference directly
  // so that retrieving returns the identical object (pointer).
  if (!memory[gid]) {
    memory[gid] = {};
  }
  memory[gid][key] = state;

  // The tests want (e, v) => v to be exactly the same object
  return callback(null, state);
};

function get(configuration, callback) {
    callback = callback || cb;
  let key;
  let gid = 'local';

  // Check if the user passed in null => means "return all keys"
  if (configuration === null) {
    // Return an array of keys from memory[gid]
    if (!memory[gid]) {
      return callback(null, []); 
    }
    return callback(null, Object.keys(memory[gid]));
  }

  // Otherwise parse out a key/gid if config is an object or string
  if (typeof configuration === 'object') {
    if (configuration !== null) {
      if (configuration.key) {
        key = configuration.key;
      }
      if (configuration.gid) {
        gid = configuration.gid;
      }
    }
  } else if (typeof configuration === 'string') {
    key = configuration;
  } else {
    // No valid key was given
    return callback(new Error('No key to get'), null);
  }

  if (!key) {
    // If we never found a key, error out
    return callback(new Error('No key to get'), null);
  }

  if (!memory[gid] || !memory[gid][key]) {
    return callback(new Error('No value found for key: ' + key));
  }

  // Return the exact same object reference we stored in put()
  return callback(null, memory[gid][key]);
}

function del(configuration, callback) {
    callback = callback || cb;
  let key;
  let gid = 'local';

  if (configuration === null) {
    return callback(new Error('No key to delete'));
  }

  if (typeof configuration === 'object') {
    if (configuration !== null) {
      if (configuration.key) {
        key = configuration.key;
      }
      if (configuration.gid) {
        gid = configuration.gid;
      }
    }
  } else if (typeof configuration === 'string') {
    key = configuration;
  } else {
    // If nothing workable is provided
    return callback(new Error('No key to delete'));
  }

  if (!key) {
    return callback(new Error('No key to delete'));
  }

  if (!memory[gid] || !memory[gid][key]) {
    return callback(new Error('No value found for key: ' + key));
  }

  // Return and remove the same object
  const val = memory[gid][key];
  delete memory[gid][key];
  return callback(null, val);
};

module.exports = {put, get, del};
