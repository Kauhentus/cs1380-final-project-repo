const id = distribution.util.id;
const groups = {};

global.groupsTable = {
    'all': {},
};

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};

/**
 * 
 * @param {*} name 
 * @param {*} callback 
 * @returns 
 */
groups.get = function(name, callback) {
    callback = callback || cb;
    if (typeof name === 'object') {
        if (name.gid) {
            name = name.gid;
        } else {
            return callback(new Error('Invalid group name'));
        }
    } else if (typeof name !== 'string') {
        return callback(new Error('Invalid group name'));
    }
    if (global.groupsTable[name]) {
        return callback(null, global.groupsTable[name]);
    } else {
        return callback(new Error('Group not found'));
    }
};

/**
 * 
 * @param {*} config 
 * @param {*} group 
 * @param {*} callback 
 * @returns 
 */
groups.put = function(config, group, callback) {
    callback = callback || cb;
    let gid = config;
    let hash;
    if (typeof config === 'object') {
        if (config.gid) {
            gid = config.gid;
        if (config.hash) {
            hash = config.hash;
        }
        } else {
            return callback(new Error('Invalid group ID'));
        }
    } else if (typeof config !== 'string') {
        return callback(new Error('Invalid group ID'));
    }
    if (typeof group !== 'object') {
        return callback(new Error('Invalid group object'));
    }
    for (const sid in group) {
        global.groupsTable['all'][sid] = group[sid];
    }
    global.groupsTable[gid] = group;

    // Now we can add it to the distribution object for the node
    if (!global.distribution[gid]) {

        const allServices = require('../all/all.js');

        let serviceObject = {};
        for (const service in allServices) {
            const serviceTemplate = allServices[service];
            if (hash && (service === 'mem' || service === 'store')) {
                serviceObject[service] = serviceTemplate({gid: gid, hash: hash});
            } else {
                serviceObject[service] = serviceTemplate({gid: gid, hash: id.consistentHash});
            }
        }
        global.distribution[gid] = serviceObject;
        global.routesTable[gid] = global.routesTable[gid] || {};
        for (const service in global.distribution[gid]) {
            global.routesTable[gid][service] = global.distribution[gid][service];
        }
    }

    return callback(null, group);
};

/**
 * 
 * @param {*} name 
 * @param {*} callback 
 * @returns 
 */
groups.del = function(name, callback) {
    callback = callback || cb;
    if (typeof name === 'object') {
        if (name.gid) {
            name = name.gid;
        } else {
            return callback(new Error('Invalid group name'));
        }
    } else if (typeof name !== 'string') {
        return callback(new Error('Invalid group name'));
    }
    if (global.groupsTable[name]) {
        const group = global.groupsTable[name];
        delete global.groupsTable[name];
        return callback(null, group);
    } else {
        return callback(new Error('Group not found'));
    }
};

/**
 * @param {*} name 
 * @param {*} node 
 * @param {*} callback 
 * @returns 
 */
groups.add = function(name, node, callback) {
    callback = callback || cb;
    if (typeof name === 'object') {
        if (name.gid) {
            name = name.gid;
        } else {
            return callback(new Error('Invalid group name'));
        }
    } else if (typeof name !== 'string') {
        return callback(new Error('Invalid group name'));
    }

    if (typeof node !== 'object') {
        return callback(new Error('Invalid node object'));
    }
    if (global.groupsTable[name]) {
        global.groupsTable[name][id.getSID(node)] = node;
        global.groupsTable['all'][id.getSID(node)] = node;
        return callback(null, global.groupsTable[name][id.getSID(node)]);
    } else {
        this.put(name, {[id.getSID(node)]: node}, (e, v) => {
            if (e) {
                return callback(e);
            }
            return callback(null, global.groupsTable[name][id.getSID(node)]);
        });
    }
};

/**
 * 
 * @param {*} name 
 * @param {*} node 
 * @param {*} callback 
 * @returns 
 */
groups.rem = function(name, node, callback) {
    callback = callback || cb;
    if (typeof name === 'object') {
        if (name.gid) {
            name = name.gid;
        } else {
            return callback(new Error('Invalid group name'));
        }
    } else if (typeof name !== 'string') {
        return callback(new Error('Invalid group name'));
    }
    let key;
    if (typeof node === 'object') {
        key = id.getSID(node);
    } else if (typeof node === 'string') {
        key = node;
    } else {
        return callback(new Error('Invalid node object'));
    }
    if (global.groupsTable[name]) {
        delete global.groupsTable[name][key];
        return callback(null, global.groupsTable[name]);
    } else {
        return callback(null, {});
    }
};

module.exports = groups;
