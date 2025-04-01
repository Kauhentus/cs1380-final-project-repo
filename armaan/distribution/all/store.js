const local = distribution.local;
const util = require("../util/util");
const id = util.id;

function store(config) {
  const context = {};
  context.gid = config.gid || 'all';
  context.hash = config.hash || global.distribution.util.id.naiveHash;

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
      // 2) Build array of NIDs from the group’s node configs
      const nodeConfigs = Object.values(group); // an array of {ip, port} objects
      const nids = nodeConfigs.map((nc) => id.getNID(nc));

      // 3) Get the key id
      const kid = id.getID(configuration);

      // 4) Use our chosen hash function to pick exactly one NID
      const chosenNID = context.hash(kid, nids);

      // 5) find the node config whose NID matches chosenNID
      chosenNode = nodeConfigs.find((nc) => id.getNID(nc) === chosenNID);
      callback(null, chosenNode);
    });
  }

  /* For the distributed store service, the configuration will
          always be a string */
  return {
    get: (configuration, callback) => {
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
          method: 'get',
          node: chosenNode
        };

        const messageConfig = {
          key: configuration,
          gid: context.gid
        }

        const message = [messageConfig];

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

        // console.log("SENDING APPEND MESSAGE TO NODE:", chosenNode.port, "with configuration:", configuration);

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
