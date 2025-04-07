
const status = function(config) {
  const context = {};
  context.gid = config.gid || 'all';

  return {
    get: (configuration, callback) => {
      distribution[context.gid].comm.send(
        configuration, 
        {service: 'status', method: 'get'}, 
        callback
      );
    },

    spawn: (configuration, callback) => {
      distribution.local.status.spawn(configuration, (e, v) => {
        if(e) return callback(e);

        // add to local group first so it can be reached with distributed calls
        distribution.local.groups.add(context.gid, v, (e, v) => {
          if(e) return callback(e);

          // and then to the remote group 
          distribution[context.gid].groups.add(context.gid, v, (e, v) => {
            if(e instanceof Error) return callback(e);
            else return callback(null, configuration);
          });
        });

      });
    },

    stop: (callback) => {
      distribution[context.gid].comm.send(
        [], 
        {service: 'status', method: 'stop'}, 
        callback
      );
    }
  };
};

module.exports = status;
