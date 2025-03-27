/** @typedef {import("../types").Callback} Callback */

let services = {};
let toLocal = {};

/**
 * @param {string} configuration
 * @param {Callback} callback
 * @return {void}
 */
function get(configuration, callback) {
    // console.log('GET !', configuration, '!', callback)
    callback = callback || function() { };

    let service_name;
    let gid;
    if(typeof configuration === "string"){
        service_name = configuration;
        gid = 'local';
    } else if(typeof configuration === "object"){
        if("gid" in configuration) gid = configuration.gid;
        else gid = 'local';

        if("service" in configuration) service_name = configuration.service;
        else callback("Invalid service config missing service information", null);
    } else {
        callback("Invalid service config missing gid/service information", null);
        return
    }

    // console.log('GET', gid, services[service_name]['get'] ? services[service_name]['get'].toString() : '')

    // handle local routes/get
    if(gid === 'local'){
        if(services.hasOwnProperty(service_name)) {
            callback(null, services[service_name]);
        } else {
            // RPC fix
            if (!(service_name in services)) {
                const rpc = global.toLocal[service_name];
                if (rpc) {
                    callback(null, { call: rpc });
                } else {
                    callback(new Error(`Service ${service_name} not found!`));
                }
            }
        }
    }

    // handle <gid> routes/get
    else {
        // let service;
        // if(gid === 'all') service = distribution.local[service_name]
        // else if(distribution[gid]) service = distribution[gid][service_name];
        // else service = distribution.local[service_name];
        // console.log("    ROUTES got service", service, "with gid", gid)

        const service = distribution[gid] ?
            distribution[gid][service_name] :
            distribution.local[service_name];   // TODO: do this properly LOL
        // // const service = distribution.local[service_name];

        if(service) {
            callback(null, service);
        } else {
            callback(new Error(`Service ${service_name} not found in group ${gid}!`));
        }
    }
}

/**
 * @param {object} service
 * @param {string} configuration
 * @param {Callback} callback
 * @return {void}
 */
function put(service, configuration, callback) {
    // console.log('PUT !', configuration, '!', service, callback)
    callback = callback || function() { };

    try {
        services[configuration] = service;
    } catch (error) { 
        callback(error);
        return;
    }

    callback(null);
}

/**
 * @param {string} configuration
 * @param {Callback} callback
 */
function rem(configuration, callback) {
    // console.log('REM !', configuration, '!', callback)
    callback = callback || function() { };
    // console.log("CALLBACK??", callback)

    try {
        if(typeof configuration === "string") delete services[configuration];
        else if(Array.isArray(configuration)) delete services[configuration[1]];
        else callback("Invalid service config missing service information", null);
    } catch (error) {
        callback(error);
        return;
    }

    callback(null);
};

module.exports = {get, put, rem, services, toLocal};
