const status = function(config) {
  const context = {};
  context.gid = config.gid || 'all';

  cb = (error, value) => {
    if (error) {
      console.error(error);
    } else {
      console.log(value);
    }
  }

  return {
    get: (configuration, callback) => {
      callback = callback || cb;
      if (configuration === undefined || configuration === null ){
        callback({err: new Error('Configuration is required')}, {});
        return;
      }
      const remoteConfig = {
        service: 'status',
        method: 'get',
      }
      distribution[context.gid].comm.send(configuration, remoteConfig, (errMap, resMap) => {
        if (configuration === 'heapTotal' || configuration === 'heapUsed') {
          let total = 0;
          for (const key in resMap) {
            total += resMap[key];
          }
          resMap = {res: total};
        }
        callback(errMap, resMap);
      });
    },

    spawn: (configuration, callback) => {
      callback = callback || cb;
      if (configuration === undefined || configuration === null ){
        callback({err: new Error('Configuration is required')}, {});
        return;
      }
      if (configuration.ip === undefined || configuration.port === undefined) {
        callback({err: new Error('Invalid configuration provided')}, {});
        return;
      }
      distribution.local.status.spawn(configuration, (err, val) => {
        if (err) {
          callback({err: err}, {});
          return;
        }
        const remoteConfig = {
          service: 'groups',
          method: 'add',
        }
        const message = [context.gid, configuration];
        // TODO: Call both and nest the functions so the local is aware as well as the remote
        distribution.local.groups.add(context.gid, configuration, (err, val) => {
          if (err) {
            callback(err, null);
            return;
          }
           distribution[context.gid].comm.send(message, remoteConfig, (errMap, resMap) => {
            callback(null, configuration);
          });
        });
      })
    },

    stop: (callback) => {
      callback = callback || cb;
      const remoteConfig = {
        service: 'status',
        method: 'stop',
      }
      distribution[context.gid].comm.send([], remoteConfig, (errMap, resMap) => {
        callback(errMap, resMap);
      });
    },
  };
};

module.exports = status;
