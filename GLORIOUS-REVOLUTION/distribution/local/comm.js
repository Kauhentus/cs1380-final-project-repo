/** @typedef {import("../types").Callback} Callback */
/** @typedef {import("../types").Node} Node */

const { appendFileSync } = require("fs");
const http = require('http');

/**
 * @typedef {Object} Target
 * @property {string} service
 * @property {string} method
 * @property {Node} node
 */

/**
 * @param {Array} message
 * @param {Target} remote
 * @param {Callback} [callback]
 * @return {void}
 */
function send(message, remote, callback, retries = 3, backoff = 500) {
    // const serialize = require('../../config').util.serialize;
    // const deserialize = require('../../config').util.deserialize;
    const serialize = distribution.util.serialize;
    const deserialize = distribution.util.deserialize;

    // console.log("COMM SEND", message, remote);
    let data;
    try {
        data = serialize(message);
    } catch (error) {
        callback(error);
        return;
    }

    // console.log("REMOTE PARAMS", remote)
    if(!remote.node) { callback(new Error('no node')); return; }
    if(!remote.node.ip) { callback(new Error('no node ip')); return; }
    if(!remote.node.port) { callback(new Error('no node port')); return; }
    if(!remote.service) { callback(new Error('no service')); return; }
    if(!remote.method) { callback(new Error('no method')); return; }

    const has_gid = "gid" in remote;

    const options = {
        method: 'PUT',
        hostname: remote.node.ip,
        port: remote.node.port, 
        path: `/${has_gid ? remote.gid : 'local'}/${remote.service}/${remote.method}`,
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(data)
        }
    };
    if(remote.method !== 'stop') {
        // console.log("COMM SEND", options, data)
    }

    // use timeout to let first comm request finish before the next comm request (i.e. send a send)
    setTimeout(() => {  
        const req = http.request(options, (res) => {
            let response_data = '';
            res.on('error', (e) => {
                callback(e)
            });

            res.on('data', (chunk) => {
                response_data += chunk;
            });
        
            res.on('end', () => {
                try {
                    // appendFileSync("PLEASE-COMM-LOCAL.txt", `${res.statusCode} ${response_data} ${res.statusMessage}\n`);
                    if(res.statusCode !== 200){
                        callback(new Error(`errored in comm with status code ${res.statusCode} and message ${res.statusMessage}`));
                        return;
                    }
                    const result = deserialize(response_data);
                    if(typeof result === "object" && 'e' in result && 'v' in result){ // distributed callback
                        callback(result.e, result.v); 
                    } else { // non-distributed callback
                        callback(null, result);
                    }
                } catch (error) {
                    callback(new Error(`ERR: ${error.message} and ${response_data}`));
                }
            });
        });
        req.on('error', (e) => {
            // console.log("COMM ERROR", e);
            if (retries > 0 && (e.code === 'ECONNRESET' || e.code === 'ECONNREFUSED')) {
                if(retries === 1) console.log(`Connection error (${e.code}), retrying in ${backoff}ms. Retries left: ${retries} ${remote.service} ${remote.method} ${remote.node.port}`);
                setTimeout(() => {
                    send(message, remote, callback, retries - 1, backoff * 2);
                }, backoff);
                return;
            }

            callback(new Error(e))
            // console.error('💥 PANIC: exiting now due to', e, remote.service, remote.method, remote.node);
        });
        req.write(data);
        req.end();
    });
}

module.exports = {send};
