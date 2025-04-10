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

  let value = util.serialize(state);

  fs.writeFile(filePath, JSON.stringify(value), (err) => {
    if (err) {
      console.error(`Error writing file ${filePath}: ${err.message}`);
      return callback(err, null);
    }
    callback(null, state);
  });
}

function get(configuration, callback) {
  let nodeConfig = global.nodeConfig;
  let nodeID = util.id.getNID(nodeConfig);
  let key;
  let gid = 'local';

  
  if (configuration.key === null) {
    // "List all keys in the group"
    // build the dir path, read the filenames, remove .json, etc.
    const groupDir = path.join('store', nodeID, configuration.gid);
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
  // console.log("GETTING FROM THE FILEPATH: ", filePath)

  // check file existence
  if (!fs.existsSync(filePath)) {
    return callback(new Error('No value found for key: ' + key), null);
  }


  // read & deserialize
  fs.readFile(filePath, 'utf8', (err, data) => {
    if (err) return callback(err, null);

    try {
      const parsed = JSON.parse(data);
      const obj = util.deserialize(parsed);
      // console.log("NodeID: %s, retrieved file: %s", nodeID, filePath);
      // console.log("Deserialized object:", obj); // For debugging purposes
      return callback(null, obj);
    } catch (error) {
      console.error(`Error deserializing data for key ${key}: ${error.message}`);
      return callback(error, null);
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

    // console.log("NodeID: %s, deleting file: %s", nodeID, filePath);

    // now remove the file
    fs.unlink(filePath, (err) => {
      if (err) return callback(err, null);
      return callback(null, obj);
    });
  });
}

function append(state, configuration, callback) {
  
  // If no state is given, error out
  if (!state) {
    return callback(new Error('No state to append'), null);
  }
  
  // Parse the configuration to extract key and gid
  let nodeConfig = global.nodeConfig;
  let nodeID = util.id.getNID(nodeConfig);
  let key, gid = 'local';
  
  if (typeof configuration === 'object') {
    if (configuration.key) key = configuration.key;
    if (configuration.gid) gid = configuration.gid;
  } else if (typeof configuration === 'string') {
    key = configuration;
  } else {
    key = util.id.getID(state);
  }
  
  // Create directory if needed
  const groupDir = path.join('store', nodeID, gid);
  fs.mkdirSync(groupDir, { recursive: true });
  
  const filePath = path.join(groupDir, sanitizeKey(key) + '.json');
  
  // Check if file exists
  if (fs.existsSync(filePath)) {
    try {
      // Read file directly (don't use get to avoid double deserialization)
      const data = fs.readFileSync(filePath, 'utf8');
      
      // Parse the stored JSON
      const parsed = JSON.parse(data);
      const existingData = util.deserialize(parsed);
      
      // Create deep copy of existing data
      const result = JSON.parse(JSON.stringify(existingData));
      
      // Handle merging
      Object.keys(state).forEach(k => {
        if (!result.hasOwnProperty(k)) {
          // Key doesn't exist yet, add it
          result[k] = [state[k]];
        } else if (!Array.isArray(result[k])) {
          // Value exists but isn't an array, convert to array
          result[k] = [result[k], state[k]];
        } else {
          // Already an array, append the new value
          result[k].push(state[k]);
        }
      });
      
      // Serialize without double encoding
      const serialized = util.serialize(result);
      
      // Write atomically
      fs.writeFileSync(filePath,JSON.stringify(serialized));
      
      callback(null, result);
    } catch (error) {
      // Instead of overwriting, try to salvage what we can
      // Create an array with the new value
      const result = {};
      Object.keys(state).forEach(k => {
        result[k] = [state[k]];
      });
      
      const serialized = util.serialize(result);
      fs.writeFileSync(filePath, JSON.stringify(serialized));
      callback(null, result);
      }
  } else {
    // If file doesn't exist, create a new one
    put(state, configuration, callback);
  }
}


function bulk_append(data, callback) {
  const prefixBatches = data.prefixBatches || [];
  const gid = data.gid || 'index';
  
  const nodeConfig = global.nodeConfig;
  const nodeID = util.id.getNID(nodeConfig);
  const groupDir = path.join('store', nodeID, gid);
  
  // Create directory if needed
  fs.mkdirSync(groupDir, { recursive: true });
  
  const results = {};
  
  try {
    // Process each prefix batch
    for (const batch of prefixBatches) {
      const prefix = batch.prefix;
      const prefixData = batch.data;
      const filePath = path.join(groupDir, sanitizeKey(`prefix-${prefix}`) + '.json');
      
      // Read existing data for this prefix
      let existingData = {};
      if (fs.existsSync(filePath)) {
        try {
          const fileContent = fs.readFileSync(filePath, 'utf8');
          const parsed = JSON.parse(fileContent);
          existingData = util.deserialize(parsed);
        } catch (error) {
          console.error(`Error reading prefix data for ${prefix}: ${error.message}`);
        }
      }
      
      // Merge new data with existing data - ENHANCED ALGORITHM
      for (const term in prefixData) {
        // If term doesn't exist yet in our index, initialize it
        if (!existingData[term]) {
          existingData[term] = {
            df: 0, // Will be incremented below
            postings: {}
          };
        }
        
        // Process each document containing this term
        for (const docEntry of prefixData[term]) {
          const docId = docEntry.url;
          
          // Only increment df if this document wasn't already counted
          if (!existingData[term].postings[docId]) {
            existingData[term].df += 1;
          }
          
          // Create or update posting for this document with all enhanced data
          existingData[term].postings[docId] = {
            // Basic term frequency
            tf: docEntry.tf,
            
            // Enhanced ranking factors if available
            ranking: docEntry.ranking || {
              tf: docEntry.tf,
              taxonomyBoost: 1.0,
              binomialBoost: 1.0,
              positionBoost: 1.0,
              score: docEntry.tf // Default to tf if score not calculated
            },
            
            // Taxonomy information if available
            taxonomyLevel: docEntry.taxonomyLevel || null,
            isBinomial: docEntry.isBinomial || false,
            
            // Page metadata if available
            pageInfo: docEntry.pageInfo || {},
            
            // Always update timestamp
            timestamp: Date.now()
          };
        }
      }
      
      // Write the updated data back
      const serialized = util.serialize(existingData);
      fs.writeFileSync(filePath, JSON.stringify(serialized));
      
      // Return basic metrics about the operation
      results[prefix] = { 
        termsProcessed: Object.keys(prefixData).length,
        totalTermsStored: Object.keys(existingData).length
      };
    }
    
    // Return overall operation results
    callback(null, {
      status: 'success',
      processingTime: Date.now(), // For timing reference
      results: results,
      totalPrefixesProcessed: prefixBatches.length
    });
  } catch (error) {
    console.error("Error in bulk_append:", error);
    callback(error, null);
  }
}

module.exports = {put, get, getGroupKeys, del, append, bulk_append};
