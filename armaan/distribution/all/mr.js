/** @typedef {import("../types").Callback} Callback */
// const { log } = require("console");
const distribution = require("../../config");
const util = distribution.util;

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
    const mrId = require("crypto").randomUUID().substring(0, 8); // Get first 8 chars as ID
    const mrServiceName = `mr@${mrId}`; // mr@<uuid>
    const user_reduce = util.serialize(configuration.reduce)
    const user_map = util.serialize(configuration.map)

    let results = [];

    let state_dict = {
      phase: "SETUP",
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
    const notify = (config, cb) => {
      const phase_map = {
        MAP: "SHUFFLE",
        SHUFFLE: "REDUCE",
        REDUCE: "DONE",
      };
      
      if (config.status === "ERROR") {
        cb(Error(config.error), null);
        return;
      } 
      
      // Special case for SETUP - initiates the map phase on all nodes
      if (config.phase === "SETUP") {
        const remote = {
          service: config.jid,
          method: 'map',
        }
        const setupConfig = {
          mapper: user_map,
          gid: config.gid,
          jid: config.jid
        }
        const message = [setupConfig];
        state_dict.phase = "MAP";
        state_dict.phase_count = 0;
        
        distribution[context.gid].comm.send(message, remote, cb);
        return;
      }
      // Otherwise we get the local group node count by making a call to the group
      distribution.local.groups.get(config.gid, (err, group) => {
        if (err) {
          cb(err, null);
          return;
        }
        let groupNodeCount = Object.keys(group).length;

        // Increment the counter for responses received
        state_dict.phase_count = state_dict.phase_count + 1;

        if (config.phase !== state_dict.phase) {
          cb(
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
            cb(null, results);
            return;
          }
          
          // Otherwise, move to the next phase
          let new_phase = phase_map[state_dict.phase];
          state_dict.phase = new_phase;
          state_dict.phase_count = 0;

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
          
          // Include the reducer function if we're starting the reduce phase
          if (state_dict.phase === "REDUCE"){
            phaseConfig.reducer = user_reduce;
          }
          
          const message = [phaseConfig];
          distribution[context.gid].comm.send(message, remote, cb);
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
    const map = (config, cb) => {
      // Config object should contain the serialized user map function
      const ser_mapper = config.mapper;
      const gid = config.gid;
      const job_id = config.jid;
      const mapper = distribution.util.deserialize(ser_mapper);

      // Get the service for this job
      distribution.local.routes.get({gid: gid, service: job_id}, (err, service) => {
        if (err) {
          cb(err, null);
          return;
        }
        
        // get the keys for the group
        distribution.local.store.getGroupKeys(gid, (err, keys) => {
          if (err) {
            cb(err, null);
            return;
          }

          if (!keys || keys.length === 0) {
            // No keys to process on this node, but still notify completion
            const mapResultName = "map@" + job_id;
            distribution.local.store.put([], {key: mapResultName, gid: gid}, (err) => {
              if (err) {
                cb(err, null);
                return;
              }
              // Notify that the map phase is completed
              service.notify({phase: "MAP", status: "COMPLETED", gid: gid, jid: job_id}, cb);
            });
            return;
          }

          // Array to hold the results of the map operation
          let mapResults = [];
          // Counter for pending operations
          let pendingOperations = keys.length;
          // Flag to track if an error has occurred
          let hasError = false;

          // Process each key
          keys.forEach((key) => {
            // Get the value for this key
            distribution.local.store.get({ key: key, gid: gid }, (err, value) => {
              // If we already encountered an error, don't continue processing
              if (hasError) return;

              if (err) {
                hasError = true;
                cb(err, null);
                return;
              }

              try {
                // Apply the mapper function
                let res = mapper(key, value);
                
                // Make sure result is an array
                if (!Array.isArray(res)) {
                  res = [res];
                }
                
                // Add results to our collection
                mapResults = mapResults.concat(res);
                
                // Decrement the counter of pending operations
                pendingOperations--;
                
                // If all operations are done, store results and notify completion
                if (pendingOperations === 0) {
                  const mapResultName = "map@" + job_id;
                  distribution.local.store.put(mapResults, {key: mapResultName, gid: gid}, (err) => {
                    if (err) {
                      cb(err, null);
                      return;
                    }
                    service.notify({phase: "MAP", status: "COMPLETED", gid: gid, jid: job_id}, cb);
                  });
                }
              } catch (mapError) {
                if (!hasError) {
                  hasError = true;
                  cb(mapError, null);
                }
              }
            });
          });
        });
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
    const shuffle = (config, cb) => {
      const gid = config.gid;
      const jid = config.jid;

      // Get the service for this job
      distribution.local.routes.get({gid: gid, service: jid}, (err, service) => {
        if (err) {
          cb(err, null);
          return;
        }
        
        // Get the map results from the local store
        distribution.local.store.get({key: "map@" + jid, gid: gid}, (err, mapResults) => {
          if (err) {
            service.notify({phase: "SHUFFLE", status: "ERROR", error: err.message, gid: gid, jid: jid}, cb);
            return;
          }

          if (!mapResults || mapResults.length === 0) {
            // No results to shuffle
            service.notify({phase: "SHUFFLE", status: "COMPLETED", gid: gid, jid: jid}, cb);
            return;
          }

          // Now we have the map results, we need to distribute them to the correct nodes
          const entrySize = mapResults.length;
          let entriesProcessed = 0;
          
          // Process each mapped result - each is expected to be an object with a single key-value pair
          mapResults.forEach((entry) => {
            const key = Object.keys(entry)[0];
            const value = entry[key];
            
            // Append the value to the appropriate reduce bucket
            distribution[gid].store.append(value, "reduce@" + jid, (err, res) => {
              if (err) {
                cb(err, null);
                return;
              }
              
              entriesProcessed++;
              if (entriesProcessed === entrySize) {
                service.notify({phase: "SHUFFLE", status: "COMPLETED", gid: gid, jid: jid}, cb);
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
    const reduce = (config, cb) => {
      // Config object should contain the serialized user reduce function
      const ser_reducer = config.reducer;
      const gid = config.gid;
      const job_id = config.jid;
      const reducer = distribution.util.deserialize(ser_reducer);

      // Get the service for this job
      distribution.local.routes.get({gid: gid, service: job_id}, (err, service) => {
        if (err) {
          cb(err, null);
          return;
        }
        
        const shuffleResultName = "reduce@" + job_id;
        // Get all keys from the store to find our reduce buckets
        distribution.local.store.get({gid: gid, key: shuffleResultName}, (err, shuffleResults) => {
          if (err) {
            cb(err, null);
            return;
          }

          let reduceKeys = Object.keys(shuffleResults)
          
          if (reduceKeys.length === 0) {
            // No keys to process on this node, but still notify completion
            service.notify({phase: "REDUCE", status: "COMPLETED", results: [], gid: gid, jid: job_id}, cb);
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
            const values = shuffleResults[key]
            
            // If we already encountered an error, don't continue processing
            if (hasError) return;

            try {
              // Apply the reducer function
              let res = reducer(key, values);
              
              // Add result to our collection
              reduceResults.push(res);
              
              // Decrement the counter of pending operations
              pendingOperations--;
              
              // If all operations are done, notify completion with results
              if (pendingOperations === 0) {
                service.notify({
                  phase: "REDUCE", 
                  status: "COMPLETED", 
                  results: reduceResults,
                  gid: gid, 
                  jid: job_id
                }, cb);
              }
            } catch (reduceError) {
              if (!hasError) {
                hasError = true;
                cb(reduceError, null);
              }
            }
          });
        });
      });
    };

    // Create an RPC version of the notify method so it runs on the coordinator
    let notifyRPC = util.wire.createRPC(util.wire.toAsync(notify));
    let asyncMap = util.wire.toAsync(map);


    // Create the service object with all methods
    let mrServiceObject = {
      notify: notifyRPC,
      map: asyncMap,
      shuffle: shuffle,
      reduce: reduce
    };
    
    // Register the service on all nodes in the group
    distribution[context.gid].routes.put(mrServiceObject, mrServiceName, (err, res) => {
      if (err) {
        cb(err, null);
        return;
      }

      console.log(res);
      
      // THIS IS CRUCIAL: Start the MapReduce process by calling notify with SETUP phase
      const remote = {
        service: mrServiceName,
        method: 'notify'
      };
      
      const setupConfig = {
        phase: "SETUP",
        gid: context.gid,
        jid: mrServiceName
      };
      
      distribution[context.gid].comm.send([setupConfig], remote, cb);
    });


    // TODO: What if notify wasn't a service method (we would still need it to be an RPC though)
    // TODO: idea (we create notify as an RPC everytime exec is called, and add it to the MR service so it can be called by all worker nodes)


    // TODO: ALT2: We call notify for each stage. Notify only returns once all nodes have provided a response (HOW?)
    // TODO: What if each time notify waits to respond until all nodes have responded


  }

  return { exec };
}

module.exports = mr;