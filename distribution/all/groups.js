const groups = function(config) {
  const context = {};
  context.gid = config.gid || 'all';

  const cb = (error, value) => {
    if (error) {
      console.error(error);
    } else {
      console.log(value);
    }
  }

  return {
    put: (config, group, callback) => {
      callback = callback || cb;
      if (typeof config === 'object') {
        if (config.gid) {
          config = config.gid;
        } 
      } else if (typeof config !== 'string') {
        return callback(new Error('Invalid group name'));
      }
      if (typeof group !== 'object') {
        return callback(new Error('Invalid group object'));
      }
      const remoteConfig = {
        service: 'groups',
        method: 'put',
        gid: 'local'
      }
      const message = [config, group];
      global.distribution[context.gid].comm.send(message, remoteConfig, (errMap, resMap) => {
        callback(errMap, resMap);
      });
    },

    del: (name, callback) => {
      callback = callback || cb;
      if (typeof name === 'object') {
        if (name.gid) {
          name = name.gid;
        } 
      } else if (typeof name !== 'string') {
        return callback(new Error('Invalid group name'));
      }
      const remoteConfig = {
        service: 'groups',
        method: 'del',
        gid: 'local'
      }
      const message = [name];
      distribution[context.gid].comm.send(message, remoteConfig, (errMap, resMap) => {
        callback(errMap, resMap);
      });
    },

    get: (name, callback) => {
      callback = callback || cb;
      if (typeof name === 'object') {
        if (name.gid) {
          name = name.gid;
        } 
      } else if (typeof name !== 'string') {
        return callback(new Error('Invalid group name'));
      }
      const remoteConfig = {
        service: 'groups',
        method: 'get',
        gid: 'local'
      }
      const message = [name];
      distribution[context.gid].comm.send(message, remoteConfig, (errMap, resMap) => {
        callback(errMap, resMap);
      });
    },

    add: (name, node, callback) => {
      callback = callback || cb;
      if (typeof name === 'object') {
        if (name.gid) {
          name = name.gid;
        } 
      } else if (typeof name !== 'string') {
        return callback(new Error('Invalid group name'));
      }
      if (typeof node !== 'object' || node.ip === undefined || node.port === undefined) {
        return callback(new Error('Invalid node object'));
      }
      const remoteConfig = {
        service: 'groups',
        method: 'add',
        gid: 'local'
      }
      const message = [name, node];
      distribution.local.groups.add(name, node, (err, val) => {
        if (err) {
          return callback({err: err}, {});
        }
        distribution[context.gid].comm.send(message, remoteConfig, (errMap, resMap) => {
          callback(errMap, resMap);
        });
      });
      
    },

    rem: (name, node, callback) => {
      callback = callback || cb;
      if (typeof name === 'object') {
        if (name.gid) {
          name = name.gid;
        } 
      } else if (typeof name !== 'string') {
        return callback({err: new Error('Invalid group name')});
      }
      let key;
      if (typeof node === 'object') {
        if (!node.ip || !node.port) {
          return callback({key: new Error('Invalid node object')});
        }
          key = id.getSID(node);
      } else if (typeof node === 'string') {
          key = node;
      } else {
          return callback({key: new Error('Invalid node object')}, {});
      }
      distribution.local.groups.rem(name, key, (err, val) => {
        if (err) {
          return callback({err: err}, {});
        }
        const remoteConfig = {
        service: 'groups',
        method: 'rem',
        gid: 'local'
        }
        const message = [name, key];
        distribution[context.gid].comm.send(message, remoteConfig, (errMap, resMap) => {
          callback(errMap, resMap);
        });
      });
      
    },
  };
};

module.exports = groups;
