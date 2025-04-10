const local = distribution.local;
const util = require("../util/util");
const id = util.id;

function indexer(config) {
  const context = {};
  context.gid = config.gid || 'all';

  const cb = (error, value) => {
    if (error) {
      console.error(error);
    } else {
      console.log(value);
    }
  };

  return {

    index: (configuration, callback) => {
      callback = callback || cb;
      if (configuration === undefined || configuration === null) {
        callback(new Error('Configuration is required'), null);
        return;
      }

      distribution.local.groups.get(context.gid, (err, group) => {
        if (err) {
          return callback(new Error(`Failed to get group: ${err.message}`), null);
        }

        const nodes = Object.values(group);
        if (nodes.length === 0) {
          return callback(new Error('No nodes available in the group'), null);
        }

        let nodeIndex = 0;
        if (configuration.url) {
          // Took this from u josh :)
          const kid = id.getID(configuration.url);
          const nids = nodes.map(node => id.getNID(node));
          const chosenNID = id.naiveHash(kid, nids);
          nodeIndex = nids.findIndex(nid => nid === chosenNID);
          if (nodeIndex === -1) nodeIndex = 0; // Fallback
        } else {
          nodeIndex = Math.floor(Math.random() * nodes.length);
        }

        const targetNode = nodes[nodeIndex];
        console.log(`Routing request for ${configuration.url || 'document'} to node ${targetNode.ip}:${targetNode.port}`);

        const remoteConfig = {
          service: 'indexer',
          method: 'index',
          node: targetNode
        };

        local.comm.send([configuration], remoteConfig, (err, val) => {
          if (err) {
            callback(err, null);
            return;
          }
          callback(null, val);
        });
      });
    },

    get_stats: (callback) => {
      callback = callback || cb;
      const remoteConfig = {
        service: 'indexer',
        method: 'get_stats'
      };
    
      distribution[context.gid].comm.send([], remoteConfig, (errMap, statsMap) => {
        callback(errMap, statsMap);
      });
    }
  };
}

module.exports = indexer;