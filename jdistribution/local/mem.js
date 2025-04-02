const crypto = require('crypto');
const { appendFileSync } = require('fs');

const map = new Map();

function put(state, configuration, callback) {
    // TODO: anti collision logic
    let key;
    if(configuration !== null){
        key = configuration;
    } else {
        key = distribution.util.id.getID(state);
    }

    map.set(key, state);
    // appendFileSync(`PLEASE-MEM-LOCAL.txt`, `MEM PUT ${key} ${state} ${[...map.keys()]} | ${map.size} | ${configuration} ${global.nodeConfig.port}\n`);
    callback(null, state);
};

function get(configuration, callback) {
    let key;

    if(configuration === null){
        return callback(new Error('mem get configuration is null'));
    } else if(typeof configuration === 'object'){
        key = configuration.key;
    } else if(typeof configuration === 'string'){
        key = configuration;
    } else {
        return callback(new Error('mem get configuration is not a string or object'));
    }
    
    if(!map.has(key)){
        callback(new Error('mem get configuration not found' + ` ${[...map.keys()]} | ${map.size} | ${JSON.stringify(configuration)} ${global.nodeConfig.port}`));
        return;
    }

    const state = map.get(key);
    callback(null, state);
}

function del(configuration, callback) {
    if(!map.has(configuration)){
        callback(new Error('mem del configuration not found' + ` ${[...map.keys()]} | ${map.size} | ${JSON.stringify(configuration)} ${global.nodeConfig.port}`));
        return;
    }

    const state = map.get(configuration);
    map.delete(configuration);
    callback(null, state);
};

module.exports = {put, get, del};
