/* Notes/Tips:

- Use absolute paths to make sure they are agnostic to where your code is running from!
  Use the `path` module for that.
*/

const util = require("../util/util");
const fs = require('fs');
const path = require('path');

// Helper to make a filename safe from weird characters
function sanitizeKey(key) {
  return String(key).replace(/[^a-zA-Z0-9_-]/g, '_');
}

function put(state, configuration, callback) {
  let nodeConfig = global.nodeConfig;
  let nodeID = util.id.getNID(nodeConfig);
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
    key = util.id.getID(state);
  } else if (typeof configuration === 'object') {
    // Ensure we are not dealing with null in here
      if (configuration.key) {
        key = configuration.key;
      }
      if (configuration.gid) {
        gid = configuration.gid;
      
    }
    // If configuration.key was missing entirely, key remains undefined. 
    // That typically means we have to error or handle it. 
    // But the tests always pass an actual key or rely on the "no key" case = null
    if (!key) {
      // If the object config had no 'key' property -> fallback to hashing
      key = util.id.getID(state);
    }
  } else if (typeof configuration === 'string') {
    key = configuration;
  } else {
    // If it’s not null/undefined/object/string, it’s some other falsy => use hashed ID
    key = util.id.getID(state);
  }

  // 3) Build full directory path: store/<NID>/<gid>
  const groupDir = path.join('store', nodeID, gid);
  fs.mkdirSync(groupDir, { recursive: true });

  const filePath = path.join(groupDir, sanitizeKey(key) + '.json');

  let serialized = util.serialize(state);

  let value = JSON.stringify(serialized);

  fs.writeFile(filePath, value, (err) => {
    if (err) return callback(err, null);
    // Return the original object for pointer equality or referencing
    return callback(null, state);
  });
}

function get(configuration, callback) {
  let nodeConfig = global.nodeConfig;
  let nodeID = util.id.getNID(nodeConfig);
  let key;
  let gid = 'local';

  if (configuration === null) {
    // "List all keys in the group"
    // build the dir path, read the filenames, remove .json, etc.
    const groupDir = path.join('store', nodeID, gid);
    // If the directory doesn’t exist or is empty, return []
    if (!fs.existsSync(groupDir)) {
      return callback(null, []);
    }
    const files = fs.readdirSync(groupDir); // e.g. [ 'jcarb.json', 'someKey.json' ]
    // remove the .json from each for "key" names
    const keys = files.map((f) => f.replace(/\.json$/, ''));
    return callback(null, keys);
  }

  // Otherwise parse out a key/gid if config is an object or string
  if (typeof configuration === 'object') {
      if (configuration.key) {
        key = configuration.key;
      }
      if (configuration.gid) {
        gid = configuration.gid;
      }
    
  } else if (typeof configuration === 'string') {
    key = configuration;
  } else {
    // No valid key was given
    return callback(new Error('No key to get'), null);
  }

  const groupDir = path.join('store', nodeID, gid);
  const filePath = path.join(groupDir, sanitizeKey(key) + '.json');

  // check file existence
  if (!fs.existsSync(filePath)) {
    return callback(new Error('No value found for key: ' + key), null);
  }

  // read & deserialize
  fs.readFile(filePath, 'utf8', (err, data) => {
    if (err) return callback(err, null);

    try {
      let value = JSON.parse(data);
      const obj = util.deserialize(value);
      return callback(null, obj);
    } catch (e) {
      return callback(e, null);
    }
  });
}

function getGroupKeys(gid, callback) {
  let nodeConfig = global.nodeConfig;
  let nodeID = util.id.getNID(nodeConfig);

  const groupDir = path.join('store', nodeID, gid);
    // If the directory doesn’t exist or is empty, return []
  if (!fs.existsSync(groupDir)) {
    return callback(null, []);
  }
  const files = fs.readdirSync(groupDir); // e.g. [ 'jcarb.json', 'someKey.json' ]
  // remove the .json from each for "key" names
  const keys = files.map((f) => f.replace(/\.json$/, ''));
  return callback(null, keys);
}

function del(configuration, callback) {
  let nodeConfig = global.nodeConfig;
  let nodeID = util.id.getNID(nodeConfig);
  let gid = 'local';
  let key;

  if (typeof configuration === 'object') {
    if (configuration.gid) {
      gid = configuration.gid};
    if (configuration.key) {
      key = configuration.key;
    } else {
      return callback(new Error('No key to delete'), null);
    }
  } else if (typeof configuration === 'string') {
    key = configuration;
  } else {
    return callback(new Error('No key to delete'), null);
  }

  const groupDir = path.join('store', nodeID, gid);
  const filePath = path.join(groupDir, sanitizeKey(key) + '.json');

  if (!fs.existsSync(filePath)) {
    return callback(new Error('No value found for key: ' + key), null);
  }

  // read contents first so we can return the object
  fs.readFile(filePath, 'utf8', (err, data) => {
    if (err) return callback(err, null);

    let obj;
    try {
      let value = JSON.parse(data);
      obj = util.deserialize(value);
    } catch (e) {
      return callback(e, null);
    }

    // now remove the file
    fs.unlink(filePath, (err) => {
      if (err) return callback(err, null);
      return callback(null, obj);
    });
  });
}

function append(state, configuration, callback) {
  let nodeConfig = global.nodeConfig;
  let nodeID = util.id.getNID(nodeConfig);
  let key;
  let gid = 'local';
  
  // If no state is given, error out
  if (!state) {
    return callback(new Error('No state to append'));
  }
  
  // Parse configuration to get key and gid - same as in put()
  if (configuration === null || configuration === undefined) {
    key = util.id.getID(state);
  } else if (typeof configuration === 'object') {
    if (configuration.key) {
      key = configuration.key;
    }
    if (configuration.gid) {
      gid = configuration.gid;
    }
    if (!key) {
      key = util.id.getID(state);
    }
  } else if (typeof configuration === 'string') {
    key = configuration;
  } else {
    key = util.id.getID(state);
  }
  
  // Build full directory path: store/<NID>/<gid>
  const groupDir = path.join('store', nodeID, gid);
  fs.mkdirSync(groupDir, { recursive: true });
  
  const filePath = path.join(groupDir, sanitizeKey(key) + '.json');
  
  // Check if the file already exists
  if (fs.existsSync(filePath)) {
    // Read existing data
    fs.readFile(filePath, 'utf8', (err, data) => {
      if (err) return callback(err, null);
      
      let existingData;
      try {
        const parsedData = JSON.parse(data);
        existingData = util.deserialize(parsedData);
      } catch (e) {
        return callback(new Error('Error parsing existing data: ' + e.message), null);
      }
      
      // New merging logic
      let result = existingData;
      
      // If existing data is not an object, convert to object for merging
      if (Array.isArray(existingData)) {
        // Handle legacy data - convert array to object if needed
        result = existingData.reduce((acc, item) => {
          Object.keys(item).forEach(key => {
            if (!acc[key]) acc[key] = [];
            acc[key].push(item[key]);
          });
          return acc;
        }, {});
      }
      
      // Now merge the new state
      Object.keys(state).forEach(key => {
        if (!result[key]) {
          result[key] = [state[key]]; // Initialize as array
        } else if (!Array.isArray(result[key])) {
          result[key] = [result[key], state[key]]; // Convert to array and append
        } else {
          result[key].push(state[key]); // Already an array, just append
        }
      });
      
      // Serialize and save the updated result
      const serialized = util.serialize(result);
      const value = JSON.stringify(serialized);
      
      fs.writeFile(filePath, value, (err) => {
        if (err) return callback(err, null);
        return callback(null, result);
      });
    });
  } else {
    // If file doesn't exist, create a new one with state keys initialized as arrays
    let initialData = {};
    
    Object.keys(state).forEach(key => {
      initialData[key] = [state[key]];
    });
    
    const serialized = util.serialize(initialData);
    const value = JSON.stringify(serialized);
    
    fs.writeFile(filePath, value, (err) => {
      if (err) return callback(err, null);
      return callback(null, initialData);
    });
  }
}

module.exports = {put, get, getGroupKeys, del, append};
