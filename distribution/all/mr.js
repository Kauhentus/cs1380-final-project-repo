/** @typedef {import("../types").Callback} Callback */

/**
 * Map functions used for mapreduce
 * @callback Mapper
 * @param {any} key
 * @param {any} value
 * @returns {object[]}
 */

/**
 * Reduce functions used for mapreduce
 * @callback Reducer
 * @param {any} key
 * @param {Array} value
 * @returns {object}
 */

/**
 * @typedef {Object} MRConfig
 * @property {Mapper} map
 * @property {Reducer} reduce
 * @property {string[]} keys
 */


/*
  Note: The only method explicitly exposed in the `mr` service is `exec`.
  Other methods, such as `map`, `reduce`, and `reduce`, should be dynamically
  installed on the remote nodes and not necessarily exposed to the user.
*/

function mr(config) {
  const context = {
    gid: config.gid || 'all',
  };

  /**
   * @param {MRConfig} configuration
   * @param {Callback} cb
   * @return {void}
   */
  function exec(configuration, cb) {
    const { keys, map, reduce } = configuration;
    console.log("EXEC STARTS", global.nodeConfig, 'with keys', keys);
    const distribution = require('../../config');
    const service_id = `mr-${distribution.util.id.getSID(Math.random())}-services`;
  
    // ###########################
    // SET UP FUNCTIONS AND LOGIC

    let num_nodes = 0;
    let num_nodes_finished_map = 0;
    let num_nodes_finished_reduce = 0;
    let orchestrator_map_aggregation = [];
    let orchestrator_reduce_aggregation = [];
    let worker_nid_aggregation = [];

    const complete_orchestrator = () => {
      const final_result = orchestrator_reduce_aggregation;

      distribution[context.gid].comm.send(
        [service_id],
        {service: 'routes', method: 'rem'},
        (e, v) => {
          // console.log('REMOVED TEMP SERVICE FROM WORKERS');
          distribution.local.routes.rem(service_id, (e, v) => {
            console.log(`COMPLETING ORCHESTRATION (deregistered custom route ${service_id})`)
              cb(null, final_result);
            }
          );
        }
      );
    }

    const begin_map_phase = () => {
      console.log("BEGINNING MAP PHASE");
      distribution[context.gid].comm.send(
        [{
          orchestrator_ip: global.nodeConfig.ip, orchestrator_port: global.nodeConfig.port,
          stage: 'map', service_id: service_id, gid: config.gid,
          data: keys
        }],
        {gid: 'local', service: service_id, method: 'notify'},
        (e, v) => {
          
        }
      );
    }
    const begin_reduce_phase = () => {
      console.log("BEGINNING REDUCE PHASE");

      distribution[context.gid].comm.send(
        [{
          orchestrator_ip: global.nodeConfig.ip, orchestrator_port: global.nodeConfig.port,
          stage: 'reduce', service_id: service_id, gid: config.gid, 
          data: orchestrator_map_aggregation, 
          worker_nids: worker_nid_aggregation
        }],
        {gid: 'local', service: service_id, method: 'notify'},
        (e, v) => {
          
        }
      );
    }

    const notify_orchestrator = (data, callback) => {
      if(data.stage === 'finished_map') {
        orchestrator_map_aggregation.push(...data.results.flat());
        worker_nid_aggregation.push(data.worker_nid);
        num_nodes_finished_map++;

        if(num_nodes_finished_map === num_nodes) begin_reduce_phase();
        callback();
      }

      else if(data.stage === 'finished_reduce') {
        orchestrator_reduce_aggregation.push(...data.results);
        num_nodes_finished_reduce++;

        if(num_nodes_finished_reduce === num_nodes) complete_orchestrator();
        callback();
      }

      else {
        throw Error('INVALID STAGE');
      }
    }
    const notify_worker = (data, callback) => {
      const fs = require('fs');
      // fs.appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `STARTING STAGE ${data.stage}\n`)
      
      if(data.stage === 'map') {
        const local_keys = data.data.map(key => `${data.gid}-${key}`);       
        // fs.appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `    1. ${JSON.stringify(local_keys)}\n`)

        const a = (async () => {
          const local_value_pairs = (await Promise.all(local_keys.map(async (key) => {
            return new Promise((resolve, reject) => {
              distribution.local.store.get(key, (e, v) => {
                if(e) resolve(e);
                else resolve([v, key]);
              });
            });
          }))).filter(v => !(v instanceof Error));
          const local_values = local_value_pairs.map(pair => pair[0]);
          const local_keys_on_node = local_value_pairs.map(pair => pair[1]);
          // fs.appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `    2. ${JSON.stringify(local_keys)} ${JSON.stringify(local_values)}\n`)
          
          distribution.local.routes.get(data.service_id, async (e, v) => {
            if(e) return callback(e);
            const map_function = v.map;
            // fs.appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `    25. ${local_keys_on_node}, ${local_values}\n`);
            const map_results = local_keys_on_node.map((key, i) => {
              const unprocessed_key = key.split('-').slice(1).join('-');
              return map_function(unprocessed_key, local_values[i])
            });
            
            // fs.appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `    3. ${JSON.stringify(e)}, ${Object.keys(v)}\n`)
            // fs.appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `    4. ${JSON.stringify(map_results)}\n`)

            // FINISHED MAP ON WORKER
            distribution.local.comm.send(
              [{ 
                stage: 'finished_map', 
                results: map_results, 
                worker_nid: distribution.util.id.getNID(global.nodeConfig)
              }],
              { node: { ip: data.orchestrator_ip, port: data.orchestrator_port}, gid: 'local', service: data.service_id, method: 'notify' }, 
              (e, v) => {
                callback(null);
              }
            );
          });
        });
        a();
      } 

      else if(data.stage === 'reduce') {
        const nids = data.worker_nids;
        const this_nid = distribution.util.id.getNID(global.nodeConfig);

        // fs.appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `    4. ${JSON.stringify(data.data)}\n`)

        const kv_pairs_on_this_node = data.data.filter(obj => {
          const id = distribution.util.id;
          const k = id.getID(Object.keys(obj)[0]);
          const chosen_nid = id.consistentHash(k, nids);
          return chosen_nid === this_nid;
        });
        const unique_keys = [...new Set(kv_pairs_on_this_node.map(obj => Object.keys(obj)[0]))];

        distribution.local.routes.get(data.service_id, async (e, v) => {
          if(e) return callback(e);
          const reduce_function = v.reduce;

          const reduced_results = unique_keys.map(key => {
            return reduce_function(
              key, 
              kv_pairs_on_this_node
                .filter(item => key in item)
                .map(item => Array.isArray(item[key]) ? item[key].flat() : item[key]) // TODO: ADDRESS HACK??
            );
          });

          // fs.appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `    4. ${JSON.stringify(kv_pairs_on_this_node)}\n`)
          // fs.appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `    5. ${JSON.stringify(unique_keys)}\n`)
          // fs.appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `    6. ${JSON.stringify(reduced_results)}\n`)
          // fs.appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `    7. ${JSON.stringify(kv_pairs_on_this_node
          //   .filter(item => unique_keys[0] in item)
          //   .map(item => item[unique_keys[0]]))}\n`)

          // FINISHED REDUCE ON WORKER
          distribution.local.comm.send(
            [{ 
              stage: 'finished_reduce', 
              results: reduced_results, 
              worker_nid: distribution.util.id.getNID(global.nodeConfig)
            }],
            { node: { ip: data.orchestrator_ip, port: data.orchestrator_port}, gid: 'local', service: data.service_id, method: 'notify' }, 
            (e, v) => {
              callback(null);
            }
          );
        });
      }
      
      else {
        throw Error('INVALID STAGE');
      }
    };

    const orchestrator_services = { notify: notify_orchestrator, reduce: reduce }
    const worker_services = { notify: notify_worker, map: map, reduce: reduce }

    // ############################
    // EXECUTE THE LOGIC!!

    // REGISTER TEMP SERVICE ON ORCHESTRATOR
    distribution.local.routes.put(orchestrator_services, service_id, (e, v) => {

      // REGISTER TEMP SERVICE ON WORKERS
      distribution[context.gid].comm.send(
        [worker_services, service_id],
        {service: 'routes', method: 'put'},
        (e, v) => {
  
          num_nodes = Object.keys(v).length;
          console.log(`CUSTOM ROUTE ${service_id} ADDED TO ${num_nodes} WORKERS`);
  
          begin_map_phase(keys);
        }
      );
    })
  }

  return {exec};
};

module.exports = mr;
