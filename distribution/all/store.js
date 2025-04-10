const local = distribution.local;
const util = require("../util/util");
const id = util.id;

function store(config) {
  const context = {};
  context.gid = config.gid || 'all';
  context.hash = config.hash || global.distribution.util.id.naiveHash; // default to consistentHash if not provided

  const cb = (error, value) => {
    if (error) {
      console.error(error);
    } else {
      console.log(value);
    }
  };

  function getChosenNode(configuration, callback) {
    distribution.local.groups.get(context.gid, (err, group) => {
        if (err) {
          return null;
        }
      const nodeConfigs = Object.values(group); // an array of {ip, port} objects
      const nids = nodeConfigs.map((nc) => id.getNID(nc));
      const kid = id.getID(configuration);

      const chosenNID = context.hash(kid, nids);
      chosenNode = nodeConfigs.find((nc) => id.getNID(nc) === chosenNID);
      callback(null, chosenNode);
    });
  };

  function stripPrefix(key) {
    console.log("STRIPPING KEY: ", key)
    if (typeof key === 'string' && key.startsWith('prefix-')) {
      return key.substring(7); // Remove "prefix-" (7 characters)
    }
    console.log("STRIPPED KEY: ", key)
    return key;
  };

  return {
    get: (configuration, callback) => {
      callback = callback || cb;
      if (configuration === undefined ){
        callback(new Error('Configuration is required'), null);
        return;
      }

      if (configuration.key === null) {
        let keys = [];
        const remoteConfig = {
        service: 'store',
        method: 'get',
        }
        distribution[context.gid].comm.send({gid: context.gid, key: null}, remoteConfig, (errMap, valMap) => {

          for (const key in valMap) {
            if (valMap.hasOwnProperty(key)) {
              
              let nodeKeys = valMap[key].filter(key => !key.includes('.DS_Store'));
              
              keys = keys.concat(nodeKeys);
            }
          }
          callback(null, keys);
          // return;
        }
      );
        return;
      }


      // 3) Get the correct node
      getChosenNode(stripPrefix(configuration), (err, chosenNode) => {
        if (err) return callback(new Error('Could not find a node'), null);

        // 6) Send the key to the chosen node
        const config = {
          service: 'store',
          method: 'get',
          node: chosenNode
        };

        const messageConfig = {
          key: configuration,
          gid: context.gid
        }

        const message = [messageConfig];

        // console.log(`Sending get request to node: ${JSON.stringify(chosenNode)} with key: ${JSON.stringify(messageConfig)}`);
        local.comm.send(message, config, (err, val) => {
          if (err) {
            callback(err, null);
            return;
          }
          callback(null, val);
        })    
      });
    },

    put: (state, configuration, callback) => {
      
      callback = callback || cb;
      if (state === undefined || state === null ){
        callback(new Error('State is required'), null);
        return;
      }

      if (configuration === null) {
        configuration = id.getID(state);
      }

      // 3) Get the correct node
      getChosenNode(configuration, (err, chosenNode) => {
        if (err) return callback(new Error('Could not find a node'), null);

        // 6) Send the key to the chosen node
        const config = {
          service: 'store',
          method: 'put',
          node: chosenNode
        };

        const messageConfig = {
          key: configuration,
          gid: context.gid
        }

        const message = [state, messageConfig];

        local.comm.send(message, config, (err, val) => {
          if (err) {
            callback(err, null);
            return;
          }
          callback(null, val);
        });
      });
    },

    del: (configuration, callback) => {
      callback = callback || cb;
      if (configuration === undefined || configuration === null ){
        callback(new Error('Configuration is required'), null);
        return;
      }
      // 3) Get the correct node
      getChosenNode(configuration, (err, chosenNode) => {
        if (err) return callback(new Error('Could not find a node'), null);

        // 6) Send the key to the chosen node
        const config = {
          service: 'store',
          method: 'del',
          node: chosenNode
        };

        const messageConfig = {
          key: configuration,
          gid: context.gid
        }

        local.comm.send([messageConfig], config, (err, val) => {
          if (err) {
            callback(err, null);
            return;
          }
          callback(null, val);
        });
      });
    },

    append: (configuration, callback) => {
      callback = callback || cb;
      
      if (configuration === undefined || configuration === null ){
        callback(new Error('Configuration is required'), null);
        return;
      }

      

      if (configuration.key === null) {
        configuration = id.getID(state);
      }

      const key = Object.keys(configuration.entry)[0];

      // 3) Get the correct node
      getChosenNode(key, (err, chosenNode) => {
        if (err) return callback(new Error('Could not find a node'), null);

        // 6) Send the key to the chosen node
        const config = {
          service: 'store',
          method: 'append',
          node: chosenNode
        };

        const messageConfig = {
          key: "reduce@" + configuration.jid,
          gid: context.gid
        }

        const message = [configuration.entry, messageConfig];

        local.comm.send(message, config, (err, val) => {
          if (err) {
            callback(err, null);
            return;
          }
          callback(null, val);
        });
      });
    },

    reconf: (configuration, callback) => {
    },
  };
};

module.exports = store;
