/** @typedef {import("../types").Callback} Callback */

function routes(config) {
  const context = {};
  context.gid = config.gid || 'all';

  const cb = (e, v) => {
    if (e) {
      console.error(e);
    } else {
      console.log(v);
    }
  };

  /**
   * @param {object} service
   * @param {string} name
   * @param {Callback} callback
   */
  function put(service, name, callback = cb) {
    if (typeof service !== 'object') {
      return callback(new Error('Invalid service object'));
    }
    if (typeof name !== 'string') {
      return callback(new Error('Invalid service name'));
    }

    const remote = {
      service: 'routes',
      method: 'put',
    }
    const message = [service, name];
    distribution[context.gid].comm.send(message, remote, (errMap, resMap) => {
      if (Object.keys(errMap).length > 0) {
        callback(errMap, null);
        return;
      }
      callback(null, `Added service ${name} to all nodes within the ${context.gid} group`);
    });
  }

  /**
   * @param {object} service
   * @param {string} name
   * @param {Callback} callback
   */
  function rem(service, name, callback = cb) {
    if (typeof service !== 'object') {
      return callback(new Error('Invalid service object'));
    }
    if (typeof name !== 'string') {
      return callback(new Error('Invalid service name'));
    }

    const remote = {
      service: 'routes',
      method: 'rem',
    }
    const message = [service, name];
    distribution[context.gid].comm.send(message, remote, (errMap, resMap) => {
      if (Object.keys(errMap).length > 0) {
        callback(errMap, null);
        return;
      }
      callback(null, `Removed service ${name} from all nodes within the ${context.gid} group`);
    });
  }

  return {put, rem};
}

module.exports = routes;
