/* Notes/Tips:

- Use absolute paths to make sure they are agnostic to where your code is running from!
  Use the `path` module for that.
*/

const fs = require('fs');
const path = require('path');
const base_path = path.join(__dirname, '../../store');

function sanitizeKey(key) {
  return String(key).replace(/[^a-zA-Z0-9_\-\.]/g, '_');
}

function put(state, configuration, callback) {
  const util = distribution.util;
  const id = util.id;
  const nid = id.getNID(global.nodeConfig);
  const node_store_path = path.join(base_path, nid);
  if (!fs.existsSync(base_path)) fs.mkdirSync(base_path, { recursive: true });
  if (!fs.existsSync(node_store_path)) fs.mkdirSync(node_store_path, { recursive: true });

  let file_key;
  if(configuration === null){
    file_key = id.getID(state);
  } else if(typeof configuration === "string"){
    file_key = configuration.replace(/[^a-zA-Z0-9_\-\.]/g, '_')
  } else if(typeof configuration === "object" && "key" in configuration){
    file_key = configuration.key.replace(/[^a-zA-Z0-9_\-\.]/g, '_');
  }
  
  let is_group_put = typeof configuration === "object" && "key" in configuration && "gid" in configuration;
  let gid_folder = is_group_put ? configuration.gid : 'all';
  let file_path = path.join(node_store_path, gid_folder, file_key);

  if(!fs.existsSync(path.join(node_store_path, gid_folder))) {
    fs.mkdirSync(path.join(node_store_path, gid_folder), { recursive: true });
  }
  
  fs.writeFileSync(file_path, util.serialize(state), { recursive: true });
  
  callback(null, state);
}

function get(configuration, callback) {
  const util = distribution.util;
  const id = util.id;
  const nid = id.getNID(global.nodeConfig);
  const node_store_path = path.join(base_path, nid);
  if (!fs.existsSync(node_store_path)) fs.mkdirSync(node_store_path, {recursive: true});

  // handle get all keys
  if(configuration === null){
    const all_keys = fs.readdirSync(path.join(node_store_path, 'all'));
    return callback(null, all_keys);
  } else if(typeof configuration === "object" && configuration.key === null && "gid" in configuration){
    const all_keys = fs.readdirSync(path.join(node_store_path, configuration.gid));
    return callback(null, all_keys);
  }
  
  let file_key;
  if(typeof configuration === "string"){
    file_key = configuration.replace(/[^a-zA-Z0-9_\-\.]/g, '_')
  } else if(typeof configuration === "object" && "key" in configuration){
    file_key = configuration.key.replace(/[^a-zA-Z0-9_\-\.]/g, '_');
  }
  let is_group_put = typeof configuration === "object" && "key" in configuration && "gid" in configuration;
  let gid_folder = is_group_put ? configuration.gid : 'all';
  let file_path = path.join(node_store_path, gid_folder, file_key);

  if(!fs.existsSync(file_path)){
    callback(new Error(`local store get configuration not found (${JSON.stringify(configuration)}) (${file_path})`));
    return;
  }

  const data = fs.readFileSync(file_path);
  const state = util.deserialize(data);
  callback(null, state);
}

function del(configuration, callback) {
  const util = distribution.util;
  const id = util.id;
  const nid = id.getNID(global.nodeConfig);
  const node_store_path = path.join(base_path, nid);
  if (!fs.existsSync(node_store_path)) fs.mkdirSync(node_store_path);

  let file_key;
  if(typeof configuration === "object" && "key" in configuration && "gid" in configuration){
    file_key = `${configuration.gid}-${configuration.key.replace(/[^a-zA-Z0-9_\-\.]/g, '_')}`;
  } else {
    file_key = configuration.replace(/[^a-zA-Z0-9_\-\.]/g, '_')
  }
  const file_path = path.join(node_store_path, file_key);

  if(!fs.existsSync(file_path)){
    callback(new Error('local store del configuration not found'));
    return;
  }

  fs.readFile(file_path, (e, data) => {
    if(e) {
      callback(e);
      return;
    }

    const state = util.deserialize(data);
    fs.rm(file_path, (e) => {
      if(e) {
        callback(e);
        return;
      }
      callback(null, state);
    });
  });
}

function bulk_append(data, callback) {
  const prefixBatches = data.prefixBatches || [];
  const gid = data.gid || 'index';
  
  const util = distribution.util;
  const nodeConfig = global.nodeConfig;
  const nodeID = util.id.getNID(nodeConfig);
  const groupDir = path.join('store', nodeID, gid);
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
            docId: docId,
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

function read_bulk(configuration, callback) {
  const util = distribution.util;
  const id = util.id;
  const nid = id.getNID(global.nodeConfig);
  const node_store_path = path.join(base_path, nid);

  if(!("key" in configuration) || !("gid" in configuration)) throw Error("bulk read requires key and gid");

  let file_key = configuration.key.replace(/[^a-zA-Z0-9_\-\.]/g, '_');
  let is_group_put = typeof configuration === "object" && "key" in configuration && "gid" in configuration;
  let gid_folder = is_group_put ? configuration.gid : 'all';
  let file_path = path.join(node_store_path, gid_folder, file_key);

  const data = fs.readFileSync(file_path).toString();
  callback(null, data);
}

module.exports = {put, get, del, bulk_append, read_bulk};
