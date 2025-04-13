
function store(config) {
  const context = {};
  context.gid = config.gid || 'all';
  context.hash = config.hash || global.distribution.util.id.naiveHash;

  /* For the distributed store service, the configuration will
          always be a string */
  return {
    get: (configuration, callback) => {
      const util = distribution.util;
      const key = configuration;
      const kid = util.id.getID(key);

      // should this be [context.gid]?
      distribution.local.comm.send(
        [context.gid], {service: 'groups', method: 'get', node: global.nodeConfig}, 
        (e, v) => {
          if(e) return callback(e);

          if(configuration === null){
            const all_nodes = Object.values(v);

            Promise.all(all_nodes.map(node => {
              return new Promise((resolve, reject) => {
                distribution.local.comm.send(
                  [{ key: key, gid: context.gid }], 
                  { gid: 'local', node: node, service: 'store', method: 'get' }, 
                  (e, v) => {
                    if(e) return reject(e);
                    resolve(v);
                  }
                );
              });
            })).then((results) => {
              callback(null, results.flat());
            }).catch(e => callback(e));
            
            return;
          }

          const sids = Object.keys(v);
          if(sids.length === 0) return callback(new Error(`no nodes found in group ${context.gid}`));
          const nids = Object.values(v).map(node => util.id.getNID(node));
          const chosen_sid = context.hash(kid, nids).substring(0, 5);
          const target_node = { ip: v[chosen_sid].ip, port: v[chosen_sid].port };

          distribution.local.comm.send(
            [{ key: key, gid: context.gid }], 
            { gid: 'local', node: target_node, service: 'store', method: 'get' }, 
            callback
          );
        }
      );    
    },

    put: (state, configuration, callback) => {
      const util = distribution.util;
      const key = configuration !== null ? configuration : util.id.getID(state);
      const kid = util.id.getID(key);

      distribution.local.comm.send(
        [context.gid], {service: 'groups', method: 'get', node: global.nodeConfig}, 
        (e, v) => {
          if(e) return callback(e);

          const sids = Object.keys(v);
          if(sids.length === 0) return callback(new Error(`no nodes found in group ${context.gid}`));
          const nids = Object.values(v).map(node => util.id.getNID(node));
          const chosen_sid = context.hash(kid, nids).substring(0, 5);
          const target_node = { ip: v[chosen_sid].ip, port: v[chosen_sid].port };

          distribution.local.comm.send(
            [state, { key: key, gid: context.gid }], 
            { gid: 'local', node: target_node, service: 'store', method: 'put' }, 
            callback
          );
        }
      );  
    },

    del: (configuration, callback) => {
      distribution[context.gid].comm.send(
        [{ key: configuration, gid: context.gid }], {service: 'store', method: 'del'}, 
        (e, v) => {
          if(e instanceof Error) return callback(e);
          // unpack distributed delete, will all error except for v
          if(v !== undefined && typeof v === "object" && Object.keys(v).length === 1) return callback(null, Object.values(v)[0]);
          if(typeof e === "object" && Object.keys(e).length > 0) return callback(new Error("distributed delete error"));
          return callback(e, v);
        }
      );  
    },

    reconf: (configuration, callback) => {
    },

    clean_bulk_range_append: (callback) => {
      distribution[context.gid].comm.send(
          [],
          { service: 'store', method: 'clean_bulk_range_append' },
          callback
      );
  },
  };
};

module.exports = store;
