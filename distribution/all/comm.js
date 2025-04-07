/** @typedef {import("../types").Callback} Callback */

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
    distribution.local.groups.get(context.gid, async (e, v) => {
      if(e) return callback(e);

      const group_node_sids = Object.keys(v);
      const results = await Promise.all(group_node_sids.map((sid) => new Promise((res, rej) => {
        const remote = {
          node: v[sid],
          service: configuration.service,
          method: configuration.method
        }
      
        distribution.local.comm.send(message, remote, (e, v) => {
          if(e) return res(new Error(e));
          else return res(v);
        });  
      })));

      const errors = {};
      const aggregation = {};
      for(let i = 0; i < group_node_sids.length; i++){
        if(results[i] instanceof Error){
          errors[group_node_sids[i]] = results[i];
        } else {
          aggregation[group_node_sids[i]] = results[i];
        }
      }
      
      callback(errors, aggregation);
    });
  }

  return {send};
};

module.exports = comm;
