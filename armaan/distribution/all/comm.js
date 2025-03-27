/** @typedef {import("../types").Callback} Callback */

const local = distribution.local;

/**
 * NOTE: This Target is slightly different from local.all.Target
 * @typdef {Object} Target
 * @property {string} service
 * @property {string} method
 */

/**
 * @param {object} config
 * @return {object}
 */
function comm(config) {
  const context = {};
  context.gid = config.gid || 'all';

  /**
   * @param {Array} message
   * @param {object} configuration
   * @param {Callback} callback
   */
  function send(message, configuration, callback) {
    if (message === undefined || message === null) {
      // If no message is provided, we assume the default message is a node id
      message = ['nid'];
    } else if (!Array.isArray(message)) {
      message = [message]
    }
    if (configuration === undefined || configuration === null || !configuration.service || !configuration.method
    ) {
      callback(new Error('Remote configuration is required'), null);
      return;
    }
    let service = configuration.service;
    let method = configuration.method;

    let errorMap = {};
    let responseMap = {};
    let responseCount = 0;

    distribution.local.groups.get(context.gid, (err, group) => {
      if (err) {
        callback(err, null);
        return;
      }

      responseCount = Object.keys(group).length;

      Object.entries(group).forEach(([sid, node]) => {
        let config = {
          service: service,
          method: method,
          node: node,
          gid: 'local'
        }
        local.comm.send(message, config, (error, response) => {
          if (error) {
            errorMap[sid] = error;
          } else {
            responseMap[sid] = response;
          }
          responseCount--;
          if (responseCount === 0) {
            callback(errorMap, responseMap);
          }
        })
      })
    });

  }

  return {send};
};

module.exports = comm;
