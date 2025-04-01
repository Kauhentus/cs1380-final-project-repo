/** @typedef {import("../types").Callback} Callback */
// const { log } = require("console");


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
  Other methods, such as `map`, `shuffle`, and `reduce`, should be dynamically
  installed on the remote nodes and not necessarily exposed to the user.
*/

function mr(config) {
  const context = {
    gid: config.gid || "all",
  };

  /**
   * @param {MRConfig} configuration
   * @param {Callback} cb
   * @return {void}
   */
  function exec(configuration, cb) {
    const keys = configuration.keys
    const mapper = configuration.map;
    const reducer = configuration.reduce;
    const distribution = require("../../config");
    const mrId = require("crypto").randomUUID().substring(0, 8); // Get first 8 chars as ID
    const mrServiceName = `mr@${mrId}`; // mr@<uuid>

    let results = [];

    let state_dict = {
      phase: "MAP",
      phase_count: 0,
    };
    /**
     * This is the notify service method which is called by each worker node whenever they are done with
     * a stage of the MapReduce. This method tracks the number of responses until it reaches the group size
     * at which point it makes a call each worker node to start the next part of the service. When the 
     * reducer returns, it provides its outputs which are then returned by the exec method
     * @param {*} config 
     *    phase: string of "MAP", "REDUCE", "SHUFFLE"
     *    status: string of "COMPLETED", "ERROR"
     *    gid: string of the group ID
     *    jid: string of the job ID (mr@<uuid>)
     *  
     * @param {*} cb 
     */
    const notify = (config, callback) => {
      const phase_map = {
        MAP: "SHUFFLE",
        SHUFFLE: "REDUCE",
        REDUCE: "DONE",
      };
      
      if (config.status === "ERROR") {
        callback(Error(config.error), null);
        return;
      } 
      
      // Special case for SETUP - initiates the map phase on all nodes
      if (config.phase === "SETUP") {
        // console.log(`Starting setup phase for all nodes`);
        const remote = {
          service: config.jid,
          method: 'map',
        }
        const setupConfig = {
          gid: config.gid,
          jid: config.jid
        }
        const message = [setupConfig];
        state_dict.phase = "MAP";
        state_dict.phase_count = 0;
        // console.log(`State dictionary updated to ${state_dict}`);
        distribution[context.gid].comm.send(message, remote, callback);
        return;
      }
      // Otherwise we get the local group node count by making a call to the group
      distribution.local.groups.get(config.gid, (err, group) => {
        if (err) {
          callback(err, null);
          return;
        }
        let groupNodeCount = Object.keys(group).length;

        // Increment the counter for responses received
        state_dict.phase_count = state_dict.phase_count + 1;

        if (config.phase !== state_dict.phase) {
          callback(
            Error(
              `Error: Phase mismatch. Expected ${state_dict.phase}, got ${config.phase}`
            ),
            null
          );
          return;
        }

        // Collect reduce results
        if (state_dict.phase === "REDUCE") {
          if (config.results) {
            results = results.concat(config.results);
          }
        }

        // When all nodes have responded for the current phase
        if (state_dict.phase_count === groupNodeCount) {
          // If we've finished reducing, return the results
          if (state_dict.phase === "REDUCE") {
            distribution[context.gid].comm.send([config.jid], {service: 'routes', method: 'rem'}, (e, v) => {
              // console.log(`COMPLETING ORCHESTRATION (deregistered custom route ${config.jid})`)
              cb(null, results);
              return;
            });
          }
          
          // Otherwise, move to the next phase
          let new_phase = phase_map[state_dict.phase];
          // console.log(`Moving to phase: ${new_phase}`);
          // Notify all nodes of the new phase
          state_dict.phase = new_phase;
          state_dict.phase_count = 0;
          // console.log(`Notifying all nodes of new phase: ${new_phase} for job: ${config.jid}`);

          let endPoint = config.jid;
          let method = state_dict.phase.toLowerCase();

          let remote = {
            service: endPoint,
            method: method,
          }
          
          let phaseConfig = {
            gid: config.gid,
            jid: config.jid
          }
          
          const message = [phaseConfig];
          distribution[context.gid].comm.send(message, remote, (err, val) => {

          });
        }
      });
    };

    /**
     * 
     * @param {*} config
     *    mapper: this is the serialized version of the user provided mapper
     *    gid: this is the groupID 
     *    jid: this is the jobID (mr@<uuid>)
     * @param {*} cb 
     */
    const map = (config, callback) => {
      // Config object should contain the serialized user map function
      const gid = config.gid;
      const job_id = config.jid;

      // First we get the serivce object for this worker node
      distribution.local.routes.get(job_id, (err, service) => {
        if (err) {
          // console.log("ERROR ERROR ERROR ", err);
          callback(err, null);
          return;
        }

        // We placed the service method mapper from the user provided function on each worker
        const mapper = service.mapper;

        let mapResults = [];
        
        distribution.local.store.get({gid: gid, key: null}, (err, localKeys) => {
          if (err) {
            
            callback(err, null);
            return;
          }

          let pendingOperations = localKeys.length;

          if (pendingOperations === 0) {
            // console.log(`${global.nodeConfig.port}: FINISHED MAPPING`)
            service.notify({phase: "MAP", status: "COMPLETED", gid: gid, jid: job_id}, callback);
          }
          
          localKeys.forEach((key) => {
            distribution.local.store.get({key: key, gid: gid}, (err, val) => {
              if (err) {
                callback(err, null);
                return;
              }
              // console.log(global.nodeConfig, err, val);

              try{
                let res = mapper(key, val) // apply the map

                // console.log(res)

                if (!Array.isArray(res)) {
                  res = [res];
                }

                mapResults = mapResults.concat(res);

                pendingOperations--;

                if (pendingOperations === 0) {
                  // console.log("MADE IT")
                  const mapResultName = "map@" + job_id;
                  distribution.local.store.put(mapResults, {key: mapResultName, gid: gid}, (err, val) => {
                    if (err) {
                      callback(err, null);
                      return;
                    }
                    // console.log(`${global.nodeConfig.port}: FINISHED MAPPING`)
                    service.notify({phase: "MAP", status: "COMPLETED", gid: gid, jid: job_id}, callback);
                  })
                }
              } catch (mapError) {
                callback(mapError, null);
                return;
              }
            })
          })
        })
      });
    }; // This is the end of the map method

    /**
     * For each node it should send the results of the map phase to the designated node 
     * using the given hash function provided by the user
     * @param {*} config 
     *    gid: this is the groupID
     *    jid: this is the jobID (mr@<uuid>)
     * @param {*} cb 
     */
    const shuffle = (config, callback) => {
      const gid = config.gid;
      const jid = config.jid;

      // console.log(`${global.nodeConfig.port}: Starting shuffle for ${jid}`)
      // Get the service for this job
      distribution.local.routes.get(jid, (err, service) => {
        if (err) {
          // console.log(`${global.nodeConfig.port}: ERROR WITH GETTING THE EPHEMERAL SERVICE ${err}`)
          callback(err, null);
          return;
        }
        
        // Get the map results from the local store
        const mapResultName = "map@" + jid;
        distribution.local.store.get({key: mapResultName, gid: gid}, (err, mapResults) => {
          if (err) {
            service.notify({phase: "SHUFFLE", status: "COMPLETED", gid: gid, jid: jid}, callback);
            return;
          }

          if (!mapResults || mapResults.length === 0) {
            // No results to shuffle
            // console.log(`${global.nodeConfig.port}: NO RESULTS FOUND FROM THE MAP PHASE MOVING TO REDUCE!`)
            service.notify({phase: "SHUFFLE", status: "COMPLETED", gid: gid, jid: jid}, callback);
            return;
          }

          // Now we have the map results, we need to distribute them to the correct nodes
          
          const entrySize = mapResults.length;
          let entriesProcessed = 0;

          // console.log(global.nodeConfig.port, ": FOUND MAP RESULTS",mapResults)
          
          // Process each mapped result - each is expected to be an object with a single key-value pair
          mapResults.forEach((entry) => {
            // console.log(global.nodeConfig.port, entry)
            const key = Object.keys(entry)[0];
            
            // Append the value to the appropriate reduce bucket
            const append_config = {
              key: key,
              entry: entry,
              jid: jid
            }

            // console.log(global.nodeConfig.port, ": SENDING JID, KEY, ENTRY TO APPEND" ,jid, key, entry)

            // We distribute the results across the nodes!
            distribution[gid].store.append(append_config, (err, res) => {
              if (err) {
                callback(err, null);
                return;
              }
              
              entriesProcessed++;

              // 
              if (entriesProcessed === entrySize) {
                // console.log("Shuffling for node: ", global.nodeConfig, ' finished!');
                service.notify({phase: "SHUFFLE", status: "COMPLETED", gid: gid, jid: jid}, callback);
              }
            });
          });
        });
      });
    };

    /**
     * The reduce function should pull all of the local information and then call the user provided reduce function
     * @param {*} config 
     *    reducer: user provided reducer
     *    gid: this is the groupID
     *    jid: this is the jobID (mr@<uuid>)
     * @param {*} cb 
     */
    const reduce = (config, callback) => {
      // Config object should contain the serialized user reduce function
      const gid = config.gid;
      const job_id = config.jid;

      // Get the service for this job
      distribution.local.routes.get(job_id, (err, service) => {
        if (err) {
          callback(err, null);
          return;
        }

        // console.log("Reducing for node: ", global.nodeConfig.port, ' started!');

        const reducer = service.reducer;
        
        const shuffleResultName = "reduce@" + job_id;
        // Get all keys from the store to find our reduce buckets
        distribution.local.store.get({gid: gid, key: shuffleResultName}, (err, shuffleResults) => {
          if (err) {
            // console.log(global.nodeConfig.port, "COMPLETED")
            service.notify({phase: "REDUCE", status: "COMPLETED", results: [], gid: gid, jid: job_id}, callback);
            callback(err, null);
            return;
          }

          let reduceKeys = Object.keys(shuffleResults)

          // console.log(global.nodeConfig.port, " : found", reduceKeys.length ,"reduced results :", shuffleResults);
          
          if (reduceKeys.length === 0) {
            // No keys to process on this node, but still notify completion
            service.notify({phase: "REDUCE", status: "COMPLETED", results: [], gid: gid, jid: job_id}, callback);
            return;
          }

          // Array to hold the results of the reduce operation
          let reduceResults = [];
          // Counter for pending operations
          let pendingOperations = reduceKeys.length;
          // Flag to track if an error has occurred
          let hasError = false;

          
          // Process each reduce key
          reduceKeys.forEach((key) => {
            // Extract the actual key from the full key (remove the prefix)
            let values = shuffleResults[key]

            // console.log(global.nodeConfig.port, " : processing key ", key, " with values: ", values);

            if (!Array.isArray(values)) {
              values = [values]
            }
            
            // If we already encountered an error, don't continue processing
            if (hasError) return;

            try {
              // Apply the reducer function
              let res = reducer(key, values);
              // console.log(global.nodeConfig.port, " : reducer produced ", res)
              
              // Add result to our collection
              reduceResults.push(res);
              
              // Decrement the counter of pending operations
              pendingOperations--;
              
              // If all operations are done, notify completion with results
              if (pendingOperations === 0) {
                // console.log("Reducing for node: ", global.nodeConfig.port, ' finished!');
                // console.log( global.nodeConfig.port, " : Final reduce results: ", reduceResults);
                service.notify({
                  phase: "REDUCE", 
                  status: "COMPLETED", 
                  results: reduceResults,
                  gid: gid, 
                  jid: job_id
                }, callback);
              }
            } catch (reduceError) {
              if (!hasError) {
                hasError = true;
                callback(reduceError, null);
              }
            }
          });
        });
      });
    };

    // Create an RPC version of the notify method so it runs on the coordinator
    let notifyRPC = distribution.util.wire.createRPC(distribution.util.wire.toAsync(notify));
    // let asyncMap = util.wire.toAsync(map);


    // Create the service object with all methods
    let mrServiceObject = {
      notify: notifyRPC,
      map: map,
      mapper: mapper,
      reducer: reducer,
      shuffle: shuffle,
      reduce: reduce
    };
    
    // Register the service on all nodes in the group
    // console.log("EXEC STARTS", global.nodeConfig, 'with keys', keys);
    distribution[context.gid].routes.put(mrServiceObject, mrServiceName, (err, res) => {
      if (err) {
        cb(err, null);
        return;
      }

      // console.log(`Successfuly placed service object for group ${context.gid}`);
      
      // distribution.local.routes.get({gid: context.gid, service: mrServiceName}, (err, service) => {
      //   console.log(`local service object: ${service}`)
      //   service.notify({phase: "SETUP", status: "START", gid: gid, jid: mrServiceName}, (err, val) => {
      //     cb(null, val);
      //   });
      // })
      const setupConfig = {
          gid: context.gid,
          jid: mrServiceName,
          keys: keys
        }
      const message = [setupConfig];
      distribution[context.gid].comm.send(message, {gid: 'local', service: mrServiceName, method: 'map'}, (e, v) => {

      })
    });


  }

  return { exec };
}

module.exports = mr;