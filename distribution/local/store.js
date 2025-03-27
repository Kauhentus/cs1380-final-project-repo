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
  } else if(typeof configuration === "object" && "key" in configuration && "gid" in configuration){
    if(typeof configuration.key === "string"){
      file_key = `${configuration.gid}-${configuration.key.replace(/[^a-zA-Z0-9]/g, '-')}`;
    } else {
      file_key = `${configuration.gid}-${id.getID(state)}`;
    }
  } else {
    if(typeof configuration === "string") {
      file_key = configuration.replace(/[^a-zA-Z0-9]/g, '-')
    } else {
      file_key = id.getID(state);
    }
  }
  const file_path = path.join(node_store_path, file_key);
  const file_value = util.serialize(state);
  fs.writeFile(file_path, file_value, (e) => {
    if (e) {
      callback(e);
    } else {
      callback(null, state);
    }
  });
}

function get(configuration, callback) {
  const util = distribution.util;
  const id = util.id;
  const nid = id.getNID(global.nodeConfig);
  const node_store_path = path.join(base_path, nid);
  if (!fs.existsSync(node_store_path)) fs.mkdirSync(node_store_path, { recursive: true });

  let file_key;
  if(configuration === null){    
    const all_files = fs.readdirSync(node_store_path);
    const all_keys = all_files.map(file => file.split('-').slice(1).join('-'));
    return callback(null, all_keys);

  } else if(typeof configuration === "object" && "key" in configuration && "gid" in configuration){

    const all_files = fs.readdirSync(node_store_path);
    const all_keys = all_files.map(file => {
      const pieces = file.split('-');
      if(pieces[0] !== configuration.gid) return null;
      else return pieces.slice(1).join('-');
    }).filter(file => file !== null);
    return callback(null, all_keys);

    file_key = `${configuration.gid}-${configuration.key.replace(/[^a-zA-Z0-9]/g, '-')}`;
  } else if(typeof configuration === "string"){
    file_key = configuration.replace(/[^a-zA-Z0-9]/g, '-')
  } else {
    return callback(new Error('local store get configuration is not a string or object'));
  }
  const file_path = path.join(node_store_path, file_key);

  if(!fs.existsSync(file_path)){
    callback(new Error(`local store get configuration not found (${JSON.stringify(configuration)}) (${file_path})`));
    return;
  }

  fs.readFile(file_path, (e, data) => {
    if(e) {
      callback(e);
      return;
    }
    const state = util.deserialize(data);
    callback(null, state);
  });
}

function del(configuration, callback) {
  const util = distribution.util;
  const id = util.id;
  const nid = id.getNID(global.nodeConfig);
  const node_store_path = path.join(base_path, nid);
  if (!fs.existsSync(node_store_path)) fs.mkdirSync(node_store_path, { recursive: true });

  let file_key;
  if(typeof configuration === "object" && "key" in configuration && "gid" in configuration){
    file_key = `${configuration.gid}-${configuration.key.replace(/[^a-zA-Z0-9]/g, '-')}`;
  } else {
    file_key = configuration.replace(/[^a-zA-Z0-9]/g, '-')
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
