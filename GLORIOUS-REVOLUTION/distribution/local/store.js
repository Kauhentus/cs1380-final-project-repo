/* Notes/Tips:

- Use absolute paths to make sure they are agnostic to where your code is running from!
  Use the `path` module for that.
*/

const fs = require('fs');
const fsp = require('fs/promises');
const lf = require('proper-lockfile')
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
  
  fs.writeFile(file_path, util.serialize(state), (err) => {
    callback(null, state);
  });
}

function get(configuration, callback) {
  const util = distribution.util;
  const id = util.id;
  const nid = id.getNID(global.nodeConfig);
  const node_store_path = path.join(base_path, nid);
  if (!fs.existsSync(node_store_path)) fs.mkdirSync(node_store_path, {recursive: true});

  // handle get all keys
  if(configuration === null){
    return fs.readdir(path.join(node_store_path, 'all'), (err, all_keys) => {
      callback(null, all_keys);
    });
  } else if(typeof configuration === "object" && configuration.key === null && "gid" in configuration){
    return fs.readdir(path.join(node_store_path, configuration.gid), (err, all_keys) => {
      callback(null, all_keys);
    });
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

  fs.readFile(file_path, {encoding: 'utf8'}, (err, data) => {
    const state = util.deserialize(data);
    callback(null, state);
  });
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
  (async () => {
    
  const perf = require('perf_hooks').performance;
  const start = perf.now();

  const prefixBatches = data.prefixBatches || [];
  const gid = data.gid || 'index';
  
  const util = distribution.util;
  const nodeConfig = global.nodeConfig;
  const nodeID = util.id.getNID(nodeConfig);
  const groupDir = path.join('store', nodeID, gid);
  fs.mkdirSync(groupDir, { recursive: true });
  
  const results = {};
  
  try {
    // Process each prefix batch, grouping batches with same prefix together
    const groupedBatches = {};
    for (const batch of prefixBatches) {
      const prefix = batch.prefix;
      if (!groupedBatches[prefix]) {
        groupedBatches[prefix] = [];
      }
      groupedBatches[prefix].push(batch.data);
    }

    // so we can safely parallelize them with Promise.all
    const process_promises = [];
    for (const prefix in groupedBatches) {
      process_promises.push(new Promise(async (resolve, reject) => {
        const mergedPrefixData = {};
        for (const dataObj of groupedBatches[prefix]) {
          for (const term in dataObj) {
            if (!mergedPrefixData[term]) {
              mergedPrefixData[term] = [];
            }
            mergedPrefixData[term] = mergedPrefixData[term].concat(dataObj[term]);
          }
        }
        const prefixData = mergedPrefixData;

        // Read existing data for this prefix
        const filePath = path.join(groupDir, sanitizeKey(`prefix-${prefix}`) + '.json');
        const file_exists = fs.existsSync(filePath)
        let existingData = {};
        if (file_exists) {
          try {
            // console.log(filePath);

            const fileContent = fs.readFileSync(filePath, { encoding: 'utf8' }).toString();

            // let release;
            // if(file_exists) release = await lf.lock(filePath, { retries: 10 })
            // const fileContent = await fsp.readFile(filePath, { encoding: 'utf8' });
            // if(file_exists) await release();

            existingData = JSON.parse(fileContent);
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
              docId: docId,
              tf: docEntry.tf,
              
              ranking: docEntry.ranking || {
                tf: docEntry.tf,
                taxonomyBoost: 1.0,
                binomialBoost: 1.0,
                positionBoost: 1.0,
                score: docEntry.tf // Default to tf if score not calculated
              },
              
              taxonomyLevel: docEntry.taxonomyLevel || null,
              isBinomial: docEntry.isBinomial || false,
              pageInfo: docEntry.pageInfo || {},
              timestamp: Date.now()
            };
          }
        }
        
        // Return basic metrics about the operation
        results[prefix] = { 
          termsProcessed: Object.keys(prefixData).length,
          totalTermsStored: Object.keys(existingData).length
        };

        // must be synchronous to prevent multi-thread race conditions on local file system
        fs.writeFileSync(filePath, JSON.stringify(existingData));

        // let release;
        // if(file_exists) release = await lf.lock(filePath, { retries: 10 })
        // await fsp.writeFile(filePath, JSON.stringify(existingData));
        // if(file_exists) await release();

        resolve();
      }));
    }
    await Promise.all(process_promises);
    
    // Return overall operation results
    fs.appendFileSync(global.logging_path, `runtime ${perf.now() - start}\n`);
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

  })();
}

function read_bulk(configuration, callback) {
  (async () => {

  const util = distribution.util;
  const id = util.id;
  const nid = id.getNID(global.nodeConfig);
  const node_store_path = path.join(base_path, nid);

  if(!("key" in configuration) || !("gid" in configuration)) throw Error("bulk read requires key and gid");

  let file_key = configuration.key.replace(/[^a-zA-Z0-9_\-\.]/g, '_');
  let is_group_put = typeof configuration === "object" && "key" in configuration && "gid" in configuration;
  let gid_folder = is_group_put ? configuration.gid : 'all';
  let file_path = path.join(node_store_path, gid_folder, file_key);

  try {
    const data = fs.readFileSync(file_path, { encoding: 'utf8'});
    callback(null, data);  
  } catch(err){
    callback(err, null);
  }

  })();
}

function bulk_range_append(level, name, nextName, callback) {
  try {
    const util = distribution.util;
    const nodeConfig = global.nodeConfig;
    const nodeID = util.id.getNID(nodeConfig);
    const groupDir = path.join('store', nodeID, 'indexer_ranged_group');
    if(!fs.existsSync(groupDir)) fs.mkdirSync(groupDir, { recursive: true });

    const filePath = path.join(groupDir, `${name.slice(0, 2)}.json`);
    fs.appendFileSync(filePath, `${name} => ${nextName}\n`);
    callback(null, true);
  } catch(err) {
    console.error(`Error in bulk_range_append for ${name}:`, err);
    callback(err, null);
  }
}

function clean_bulk_range_append(callback) {
  const util = distribution.util;
  const nodeConfig = global.nodeConfig;
  const nodeID = util.id.getNID(nodeConfig);
  const groupDir = path.join('store', nodeID, 'indexer_ranged_group');
  if(!fs.existsSync(groupDir)) fs.mkdirSync(groupDir, { recursive: true });

  const bulk_ranges_on_node = fs.readdirSync(groupDir);
  bulk_ranges_on_node.map((filename) => {
    const filepath = path.join(groupDir, filename);
    const data = fs.readFileSync(filepath, { encoding: 'utf8' });
    const lines = data.split('\n');
    const unique_lines = [...new Set(lines)]; // deduplicate lines
    const new_data = unique_lines.join('\n');
    fs.writeFileSync(filepath, new_data);
  });
  callback(null, true);
}

module.exports = {put, get, del, bulk_append, read_bulk, bulk_range_append, clean_bulk_range_append};
