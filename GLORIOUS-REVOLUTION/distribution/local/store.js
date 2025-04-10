/* Notes/Tips:

- Use absolute paths to make sure they are agnostic to where your code is running from!
  Use the `path` module for that.
*/

const fs = require('fs');
const path = require('path');
const base_path = path.join(__dirname, '../../store');

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
    file_key = configuration.replace(/[^a-zA-Z0-9_-]/g, '_')
  } else if(typeof configuration === "object" && "key" in configuration){
    file_key = configuration.key.replace(/[^a-zA-Z0-9_-]/g, '_');
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
    file_key = configuration.replace(/[^a-zA-Z0-9_-]/g, '_')
  } else if(typeof configuration === "object" && "key" in configuration){
    file_key = configuration.key.replace(/[^a-zA-Z0-9_-]/g, '_');
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
    file_key = `${configuration.gid}-${configuration.key.replace(/[^a-zA-Z0-9_-]/g, '_')}`;
  } else {
    file_key = configuration.replace(/[^a-zA-Z0-9_-]/g, '_')
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

module.exports = {put, get, del};
