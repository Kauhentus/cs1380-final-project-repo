const mem = require("./mem");

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
    const mapper = configuration.map;
    const reducer = configuration.reduce;
    const distribution = require("../../config");
    const mrId = require("crypto").randomUUID().substring(0, 8); // Get first 8 chars as ID
    const mrServiceName = `mr@${mrId}`; // mr@<uuid>
    const batchIndex = configuration.batchIndex || 0; // The batch number we're processing
    const batchSize = configuration.batchSize || 10; // How many keys each node should process
    const batchInfo = configuration.batchInfo || { current: batchIndex + 1, total: 999 }; // Batch tracking info

    function isEmptyObject(obj) {
      return obj && typeof obj === 'object' && Object.keys(obj).length === 0;
    }

    
    // Ensure we have the correct group ID
    if (!configuration.gid) {
      console.log(`[MR-${mrId}] Warning: No group ID provided, using default "${config.gid}"`);
    }

    let results = [];

    console.log(`[MR-${mrId}] Starting MapReduce job for batch ${batchInfo.current}/${batchInfo.total} with batch size ${batchSize}`);

    // NEW: Tracking structures for key processing
    const keyTrackingMap = {
      processedKeys: new Set(),
      mapPhaseStats: {
        totalKeysProcessed: 0,
        totalDuration: 0
      },
      shufflePhaseStats: {
        totalEntries: 0,
        totalDuration: 0
      },
      reducePhaseStats: {
        totalKeysProcessed: 0,
        totalDuration: 0
      }
    };

    let state_dict = {
      phase: "MAP",
      phase_count: 0,
      job_start_time: Date.now(),
      batch_start_time: Date.now(),
      batchIndex: batchIndex,
      batchSize: batchSize
    };
    
    // Log memory usage at start of job
    const memUsage = process.memoryUsage();
    console.log(`[MR-${mrId}] Initial memory usage: heap=${Math.round(memUsage.heapUsed/1024/1024)}MB/${Math.round(memUsage.heapTotal/1024/1024)}MB (${Math.round((memUsage.heapUsed/memUsage.heapTotal)*100)}% used)`);

    /**
     * Notify function - runs on coordinator node
     */
    const notify = (config, callback) => {
      const phase_map = {
        MAP: "SHUFFLE",
        SHUFFLE: "REDUCE"
      };
      
      if (config.status === "ERROR") {
        console.error(`[MR-${mrId}] Error in phase ${config.phase}: ${config.error}`);
        callback(Error(config.error), null);
        return;
      } 

      function logPrefix(phase, nodeId = global.nodeConfig.port, subBatch = null) {
        const batchInfo = `BATCH:${state_dict.batchIndex}`;
        const subBatchInfo = subBatch ? `[SUB:${subBatch}]` : '';
        return `[MR-${mrId}][${phase}][NODE:${nodeId}][${batchInfo}]${subBatchInfo}`;
      }

      // Get the local group node count
      distribution.local.groups.get(config.gid, (err, group) => {
        if (err) {
          console.error(`[MR-${mrId}] Error getting group nodes: ${err.message}`);
          callback(err, null);
          return;
        }
        let groupNodeCount = Object.keys(group).length;

        // Increment the counter for responses received
        state_dict.phase_count = state_dict.phase_count + 1;

        if (config.phase !== state_dict.phase) {
          console.error(`[MR-${mrId}] Phase mismatch. Expected ${state_dict.phase}, got ${config.phase}`);
          callback(
            Error(
              `Error: Phase mismatch. Expected ${state_dict.phase}, got ${config.phase}`
            ),
            null
          );
          return;
        }

        // Track node statistics
        const nodeId = config.nodeId || "unknown";
        
        // Track processing stats for each phase
        if (config.phase === "MAP") {
          if (config.processedKeys && Array.isArray(config.processedKeys)) {
            // Add all processed keys to the global set
            config.processedKeys.forEach(key => keyTrackingMap.processedKeys.add(key));
            keyTrackingMap.mapPhaseStats.totalKeysProcessed += config.processedKeys.length;
          }
        } else if (config.phase === "SHUFFLE") {
          if (config.shuffleStats) {
            keyTrackingMap.shufflePhaseStats.totalEntries += config.shuffleStats.entriesProcessed || 0;
          }
        } else if (config.phase === "REDUCE") {
          if (config.reduceStats) {
            keyTrackingMap.reducePhaseStats.totalKeysProcessed += config.reduceStats.keysProcessed || 0;
          }
        }

        // Log progress
        const memUsage = process.memoryUsage();
        console.log(
          `[MR-${mrId}] Node ${global.nodeConfig.port}: Received notification for phase ${config.phase}. Current count: ${state_dict.phase_count}/${groupNodeCount}, batch: ${batchInfo.current}/${batchInfo.total}. Memory usage: heap=${Math.round(memUsage.heapUsed/1024/1024)}MB/${Math.round(memUsage.heapTotal/1024/1024)}MB (${Math.round((memUsage.heapUsed/memUsage.heapTotal)*100)}% used)`
        );
        
        // Collect reduce results
        if (state_dict.phase === "REDUCE" && config.results) {
          console.log(`${logPrefix('REDUCE-COLLECT')} Collecting results from ${config.nodeId}. Received ${config.results.length} results.`);
          
          // Add to results directly for this single batch
          results = results.concat(config.results);
          
          console.log(`[MR-${mrId}] Total collected results: ${results.length}`);
        }

        // When all nodes have responded for the current phase
        if (state_dict.phase_count === groupNodeCount) {
          // Calculate phase duration
          const phaseDuration = Date.now() - state_dict.batch_start_time;
          console.log(`${logPrefix(state_dict.phase + '-COMPLETE')} Phase complete in ${phaseDuration}ms. Received ${state_dict.phase_count}/${groupNodeCount} responses.`);
          
          // Update phase duration statistics
          if (state_dict.phase === "MAP") {
            keyTrackingMap.mapPhaseStats.totalDuration += phaseDuration;
          } else if (state_dict.phase === "SHUFFLE") {
            keyTrackingMap.shufflePhaseStats.totalDuration += phaseDuration;
          } else if (state_dict.phase === "REDUCE") {
            keyTrackingMap.reducePhaseStats.totalDuration += phaseDuration;
          }
          
          // Handle phase completion
          if (state_dict.phase === "REDUCE") {
            // Calculate batch duration
            const batchDuration = Date.now() - state_dict.job_start_time;
            console.log(`${logPrefix('COMPLETE')} Job completed in ${batchDuration}ms with ${results.length} results.`);
            
            // Print key processing summary
            const totalProcessed = keyTrackingMap.processedKeys.size;
            
            console.log(`
[MR-${mrId}] KEY PROCESSING SUMMARY:
- Total processed keys: ${totalProcessed}

Phase Statistics:
- Map phase: ${keyTrackingMap.mapPhaseStats.totalKeysProcessed} keys processed in ${keyTrackingMap.mapPhaseStats.totalDuration}ms
- Shuffle phase: ${keyTrackingMap.shufflePhaseStats.totalEntries} entries processed in ${keyTrackingMap.shufflePhaseStats.totalDuration}ms
- Reduce phase: ${keyTrackingMap.reducePhaseStats.totalKeysProcessed} keys processed in ${keyTrackingMap.reducePhaseStats.totalDuration}ms
`);
            
            // Clean up and return results
            distribution[context.gid].comm.send([config.jid], {service: 'routes', method: 'rem'}, (e, v) => {
              console.log(`[MR-${mrId}] Job service removed, returning ${results.length} results`);
              cb(null, results);
            });
          } else {
            // Move to the next phase
            let new_phase = phase_map[state_dict.phase];
            state_dict.phase = new_phase;
            state_dict.phase_count = 0;
            state_dict.batch_start_time = Date.now(); // Reset time for new phase
            
            console.log(`[MR-${mrId}] Node ${global.nodeConfig.port}: Moving to phase ${new_phase} for job: ${config.jid}`);
            
            let method = state_dict.phase.toLowerCase();
            let phaseConfig = {
              gid: config.gid,
              jid: config.jid
            };
            
            distribution[context.gid].comm.send([phaseConfig], {service: config.jid, method: method}, (err, val) => {
              // No callback handling needed here
            });
          }
        }
      });
    };

    /**
     * Map function that processes locally stored keys based on batch index
     * This is self-contained with no references to external variables
     */
    const map = (config, callback) => {
      // These variables are defined within the scope of this function
      const gid = config.gid;
      const job_id = config.jid; 
      const batchIndex = config.batch_index || 0;
      const batchSize = config.batch_size || 10;
      const nodeId = global.nodeConfig.port;

      // Performance metrics
      const mapStartTime = Date.now();
      const initialMemUsage = process.memoryUsage();
      console.log(`[MR-${job_id}] Node ${nodeId}: Starting map phase.  Memory: heap=${Math.round(initialMemUsage.heapUsed/1024/1024)}MB/${Math.round(initialMemUsage.heapTotal/1024/1024)}MB (${Math.round((initialMemUsage.heapUsed/initialMemUsage.heapTotal)*100)}% used)`);

      distribution.local.routes.get(job_id, (err, service) => {
        if (err) {
          console.error(`[MR-${job_id}] Node ${nodeId}: Error getting service: ${err.message}`);
          callback(err, null);
          return;
        }

        const mapper = service.mapper;
        const storageService = distribution.local.mem;
        
        // Get all local keys first, then calculate which ones to process in this batch
        distribution.local.store.get({gid: gid, key: null}, (err, localKeys) => {
          if (err) {
            console.error(`[MR-${job_id}] Node ${nodeId}: Error retrieving keys for gid ${gid}: ${err.message}`);
            callback(err, null);
            return;
          }

          // Filter out system files and sort for consistent batching
          let filteredKeys = localKeys.filter(key => !key.includes('.DS_Store')).sort();
          console.log(`[MR-${job_id}] Node ${nodeId}: Found ${filteredKeys.length} total keys on this node`);

          // Calculate which keys to process in this batch
          const startIdx = batchIndex * batchSize;
          const endIdx = Math.min(startIdx + batchSize, filteredKeys.length);
          const batchKeys = filteredKeys.slice(startIdx, endIdx);
          
          // Create array to track processed keys
          const processedKeys = [];
          
          if (batchKeys.length === 0) {
            console.log(`[MR-${job_id}] Node ${nodeId}: No keys to process in batch ${batchIndex} (range ${startIdx}-${endIdx}). Proceeding to shuffle phase.`);
            
            storageService.put([], {key: "map@" + job_id, gid: gid}, (err, val) => {
              if (err) {
                callback(err, null);
                return;
              }
              service.notify({
                phase: "MAP", 
                status: "COMPLETED", 
                gid: gid, 
                jid: job_id,
                nodeId: nodeId,
                noKeysToProcess: true,
                processedKeys: processedKeys // Empty array in this case
              }, callback);
            });
            return;
          }

          console.log(`[MR-${job_id}] Node ${nodeId}: Processing ${batchKeys.length} keys in batch ${batchIndex} (range ${startIdx}-${endIdx-1})`);
          
          // Process keys in sub-batches for efficiency
          let mapResults = [];
          let currentBatch = 0;
          const SUB_BATCH_SIZE = Math.min(5, batchKeys.length);
          const totalBatches = Math.ceil(batchKeys.length / SUB_BATCH_SIZE);

          console.log(`[MR-${job_id}] Node ${nodeId}: Processing in ${totalBatches} sub-batches, SUB_BATCH_SIZE: ${SUB_BATCH_SIZE}`);
          
          const processBatch = () => {
            const batchStartTime = Date.now();
            const bstartIdx = currentBatch * SUB_BATCH_SIZE;
            const bendIdx = Math.min(bstartIdx + SUB_BATCH_SIZE, batchKeys.length);
            const batchedKeys = batchKeys.slice(bstartIdx, bendIdx);
            
            console.log(`[MR-${job_id}] Node ${nodeId}: Processing sub-batch ${currentBatch + 1}/${totalBatches} with ${batchedKeys.length} keys`);
            
            let batchResults = [];
            let keysProcessed = 0;
            
            if (batchedKeys.length === 0) {
              // No more keys to process
              const mapResultName = "map@" + job_id;
              storageService.put(mapResults, {key: mapResultName, gid: gid}, (err, val) => {
                if (err) {
                  callback(err, null);
                  return;
                }
                
                const mapDuration = Date.now() - mapStartTime;
                const finalMemUsage = process.memoryUsage();
                console.log(`[MR-${job_id}] Node ${nodeId}: Map phase completed in ${mapDuration}ms. Results: ${mapResults.length},  Memory: heap=${Math.round(finalMemUsage.heapUsed/1024/1024)}MB/${Math.round(finalMemUsage.heapTotal/1024/1024)}MB (${Math.round((finalMemUsage.heapUsed/finalMemUsage.heapTotal)*100)}% used)`);
                
                service.notify({
                  phase: "MAP", 
                  status: "COMPLETED", 
                  gid: gid, 
                  jid: job_id,
                  nodeId: nodeId,
                  noKeysToProcess: false,
                  processedKeys: processedKeys,
                  keysProcessed: processedKeys.length
                }, callback);
              });
              return;
            }
            
            // Process each key in the batch
            batchedKeys.forEach(key => {
              distribution.local.store.get({key: key, gid: gid}, (err, val) => {
                if (err) {
                  // Just log the error and continue with other files
                  console.error(`[MR-${job_id}] Node ${nodeId}: Error processing ${key}: ${err.message}`);
                  keysProcessed++;
                } else {
                  try {
                    const mapStartMs = Date.now();
                    let res = mapper(key, val);
                    const mapDurationMs = Date.now() - mapStartMs;
                    
                    // Add to processed keys list
                    processedKeys.push(key);
                    
                    if (mapDurationMs > 500) { // Log slow mapper operations
                      console.log(`[MR-${job_id}] Node ${nodeId}: Slow mapper for key ${key}: ${mapDurationMs}ms`);
                    }
                    
                    if (!Array.isArray(res)) {
                      res = [res];
                    }
                    
                    batchResults = batchResults.concat(res);
                    
                  } catch (mapError) {
                    console.error(`[MR-${job_id}] Node ${nodeId}: Error mapping ${key}: ${mapError.message}`);
                  }
                  
                  keysProcessed++;
                }
                
                // Check if batch is complete
                if (keysProcessed === batchedKeys.length) {
                  // Add batch results to total results
                  const batchDuration = Date.now() - batchStartTime;
                  console.log(`[MR-${job_id}] Node ${nodeId}: Sub-batch ${currentBatch+1}/${totalBatches} completed in ${batchDuration}ms with ${batchResults.length} results`);
                  
                  mapResults = mapResults.concat(batchResults);
                  currentBatch++;

                  // console.log("[MR-%s] Node %s: Processed %s keys in this sub-batch, total results so far: %s, final result: %s", job_id, nodeId, keysProcessed, mapResults.length, JSON.stringify(batchResults));
                  
                  if (currentBatch < totalBatches) {
                    // Process next batch
                    processBatch();
                  } else {
                    // All batches processed, save results
                    const mapResultName = "map@" + job_id;
                    storageService.put(mapResults, {key: mapResultName, gid: gid}, (err, val) => {
                      if (err) {
                        callback(err, null);
                        return;
                      }
                      
                      const mapDuration = Date.now() - mapStartTime;
                      const finalMemUsage = process.memoryUsage();
                      console.log(`[MR-${job_id}] Node ${nodeId}: Map phase completed in ${mapDuration}ms. Results: ${mapResults.length}, Processed keys: ${processedKeys.length}, Memory: heap=${Math.round(finalMemUsage.heapUsed/1024/1024)}MB/${Math.round(finalMemUsage.heapTotal/1024/1024)}MB (${Math.round((finalMemUsage.heapUsed/finalMemUsage.heapTotal)*100)}% used)`);
                      
                      service.notify({
                        phase: "MAP", 
                        status: "COMPLETED", 
                        gid: gid, 
                        jid: job_id,
                        nodeId: nodeId,
                        noKeysToProcess: false,
                        processedKeys: processedKeys,
                        keysProcessed: processedKeys.length
                      }, callback);
                    });
                  }
                }
              });
            });
          };
          
          // Start processing the first batch
          processBatch();
        });
      });
    };

    /**
     * Shuffle function with enhanced tracking
     */
    const shuffle = (config, callback) => {
      const gid = config.gid;
      const jid = config.jid;
      // Reduce batch size to help with memory pressure
      const SHUFFLE_BATCH_SIZE = 500;
      const nodeId = global.nodeConfig.port;
      
      // Memory tracking
      const shuffleStartTime = Date.now();
      const initialMemUsage = process.memoryUsage();
      console.log(`[MR-${jid}] Node ${nodeId}: SHUFFLE PHASE STARTING -  Memory: heap=${Math.round(initialMemUsage.heapUsed/1024/1024)}MB/${Math.round(initialMemUsage.heapTotal/1024/1024)}MB (${Math.round((initialMemUsage.heapUsed/initialMemUsage.heapTotal)*100)}% used)`);
      
      // Detailed tracking variables
      let entriesProcessed = 0;
      let entriesSent = 0;
      let targetsFound = 0;
      const targetStats = {};
      
      // Track errors
      let errors = {
        count: 0,
        details: []
      };
      
      // Get the start time for map results retrieval
      const mapRetrievalStartTime = Date.now();
      
      distribution.local.routes.get(jid, (err, service) => {
        if (err) {
          console.error(`[MR-${jid}] Node ${nodeId}: Error getting service: ${err.message}`);
          callback(err, null);
          return;
        }
        
        // Get map results from memory
        const storageService = distribution.local.mem;
        const mapResultName = "map@" + jid;
        
        storageService.get({key: mapResultName, gid: gid}, (err, mapResults) => {
          const mapRetrievalDuration = Date.now() - mapRetrievalStartTime;
          
          if (err) {
            console.error(`[MR-${jid}] Node ${nodeId}: Error retrieving map results: ${err.message}`);
            errors.count++;
            errors.details.push({phase: 'map_retrieval', error: err.message});
          }
          
          // console.log("[MR-%s] Node %s: Retrieved map results : %s", jid,  nodeId, JSON.stringify(mapResults))
          
          if (!mapResults || mapResults.length === 0) {
            console.log(`[MR-${jid}] Node ${nodeId}: No map results found for gid ${gid} and job ${jid}. Cannot shuffle.`);
            service.notify({
              phase: "SHUFFLE", 
              status: "COMPLETED", 
              gid: gid, 
              jid: jid,
              nodeId: nodeId,
              noKeysToProcess: true,
              shuffleStats: {
                entriesProcessed: 0,
                entriesSent: 0,
                targetsFound: 0,
                errors: errors
              }
            }, callback);
            return;
          }
          
          console.log(`[MR-${jid}] Node ${nodeId}: Retrieved ${mapResults.length} map results in ${mapRetrievalDuration}ms`);
          
          entriesProcessed = mapResults.length;
          
          // Group results by target node to minimize network calls
          distribution.local.groups.get(gid, (err, groupNodes) => {
            if (err) {
              console.error(`[MR-${jid}] Node ${nodeId}: Error retrieving group nodes: ${err.message}`);
              errors.count++;
              errors.details.push({phase: 'group_retrieval', error: err.message});
              callback(err, null);
              return;
            }

            const groupNodeCount = Object.keys(groupNodes).length;
            console.log(`[MR-${jid}] Node ${nodeId}: Found ${groupNodeCount} group nodes for shuffling`);
            
            // Calculate target node for each key
            let nodeTargets = {};
            let keyCounts = {}; // For logging key distribution
            
            console.log(`[MR-${jid}] Node ${nodeId}: Starting to assign ${mapResults.length} entries to target nodes...`);
            
            for (let i = 0; i < mapResults.length; i++) {
              const entry = mapResults[i];
              const key = Object.keys(entry)[0];
              
              // Get node configs and IDs
              const nodeConfigs = Object.values(groupNodes);
              const nids = nodeConfigs.map((nc) => distribution.util.id.getNID(nc));
              
              // Hash the key to determine target node
              const kid = distribution.util.id.getID(key);
              const targetNID = distribution.util.id.consistentHash(kid, nids);
              const targetNode = nodeConfigs.find((nc) => distribution.util.id.getNID(nc) === targetNID);
              
              if (!targetNode) {
                console.error(`[MR-${jid}] Node ${nodeId}: No target node found for key ${key}`);
                continue;
              }
              
              const targetNodeId = distribution.util.id.getSID(targetNode);
              targetsFound++;
              
              // Track target statistics
              if (!targetStats[targetNodeId]) {
                targetStats[targetNodeId] = 0;
              }
              targetStats[targetNodeId]++;
              
              // Initialize arrays for this target
              if (!nodeTargets[targetNodeId]) {
                nodeTargets[targetNodeId] = [];
                keyCounts[targetNodeId] = 0;
              }
              
              // Add entry to target's batch
              nodeTargets[targetNodeId].push({
                key: key,
                entry: entry,
                jid: jid
              });
              
              keyCounts[targetNodeId]++;
            }
            
            console.log(`[MR-${jid}] Node ${nodeId}: Key distribution across nodes: ${JSON.stringify(keyCounts)}`);
            
            // Process each target node's batch
            const targetNodeIds = Object.keys(nodeTargets);
            
            if (targetNodeIds.length === 0) {
              console.log(`[MR-${jid}] Node ${nodeId}: No target nodes found for shuffling. Completing phase.`);
              service.notify({
                phase: "SHUFFLE", 
                status: "COMPLETED", 
                gid: gid, 
                jid: jid,
                nodeId: nodeId,
                noKeysToProcess: true,
                shuffleStats: {
                  entriesProcessed: entriesProcessed,
                  entriesSent: entriesSent,
                  targetsFound: targetsFound,
                  targetStats: targetStats,
                  errors: errors
                }
              }, callback);
              return;
            }
            
            // Track active transfers
            let activeTransfers = 0;
            let completedNodes = 0;
            const maxConcurrentTransfers = 2; // Limit concurrent transfers
            
            // Process nodes one by one to reduce memory pressure
            function processNextNode(nodeIndex) {
              if (nodeIndex >= targetNodeIds.length) {
                // All nodes processed, wait for remaining transfers
                if (activeTransfers === 0) {
                  completeShufflePhase();
                }
                return;
              }
              
              const targetNodeId = targetNodeIds[nodeIndex];
              const entries = nodeTargets[targetNodeId];
              const targetNodeConfig = groupNodes[targetNodeId];
              
              console.log(`[MR-${jid}] Node ${nodeId}: Processing ${entries.length} entries for node ${targetNodeId}`);
              
              // Process entries in batches
              let batchesProcessed = 0;
              let entriesSentToThisNode = 0;
              const totalBatches = Math.ceil(entries.length / SHUFFLE_BATCH_SIZE);
              
              // Process batches sequentially
              function processNextBatch() {
                if (batchesProcessed >= totalBatches) {
                  // All batches for this node processed
                  console.log(`[MR-${jid}] Node ${nodeId}: Completed sending ${entriesSentToThisNode} entries to node ${targetNodeId}`);
                  
                  // Free memory by clearing processed entries
                  nodeTargets[targetNodeId] = null;
                  
                  completedNodes++;
                  
                  // Process next node
                  processNextNode(nodeIndex + 1);
                  return;
                }
                
                // Check if we should wait due to too many active transfers
                if (activeTransfers >= maxConcurrentTransfers) {
                  setTimeout(processNextBatch, 100);
                  return;
                }
                
                const batchStartTime = Date.now();
                const startIdx = batchesProcessed * SHUFFLE_BATCH_SIZE;
                const endIdx = Math.min(startIdx + SHUFFLE_BATCH_SIZE, entries.length);
                const batchEntries = entries.slice(startIdx, endIdx);
                
                console.log(`[MR-${jid}] Node ${nodeId}: Sending batch ${batchesProcessed+1}/${totalBatches} (${batchEntries.length} entries) to node ${targetNodeId}`);
                
                // Set timeout for this batch
                const sendTimeout = setTimeout(() => {
                  console.error(`[MR-${jid}] Node ${nodeId}: TIMEOUT sending batch ${batchesProcessed+1} to ${targetNodeId}`);
                  errors.count++;
                  errors.details.push({
                    phase: 'batch_send_timeout',
                    targetNode: targetNodeId,
                    batchNumber: batchesProcessed+1,
                    entriesCount: batchEntries.length
                  });
                  
                  // Reduce active transfers count
                  activeTransfers--;
                  
                  // Continue with next batch despite timeout
                  batchesProcessed++;
                  processNextBatch();
                }, 60000); // 60-second timeout
                
                // Track active transfers
                activeTransfers++;
                
                // Send batch to target node
                const batchData = {
                  entries: batchEntries,
                  jid: jid,
                  gid: gid
                };
                
                const config = {
                  service: 'mem',
                  method: 'bulk_append',
                  node: targetNodeConfig
                };
                
                distribution.local.comm.send([batchData], config, (err, result) => {
                  // Clear timeout
                  clearTimeout(sendTimeout);
                  
                  // Decrease active transfers
                  activeTransfers--;
                  
                  // Handle errors
                  if (err) {
                    console.error(`[MR-${jid}] Node ${nodeId}: Error sending batch ${batchesProcessed+1} to node ${targetNodeId}: ${err.message}`);
                    errors.count++;
                    errors.details.push({
                      phase: 'batch_send_error',
                      targetNode: targetNodeId,
                      batchNumber: batchesProcessed+1,
                      error: err.message
                    });
                  } else {
                    // Update statistics
                    entriesSent += batchEntries.length;
                    entriesSentToThisNode += batchEntries.length;
                  }
                  
                  // Calculate batch duration
                  const batchDuration = Date.now() - batchStartTime;
                  batchesProcessed++;
                  
                  console.log(`[MR-${jid}] Node ${nodeId}: Batch ${batchesProcessed}/${totalBatches} for ${targetNodeId} completed in ${batchDuration}ms`);
                  
                  // Release references to help GC
                  batchEntries.length = 0;
                  
                  // Process next batch
                  processNextBatch();
                });
              }
              
              // Start processing first batch
              processNextBatch();
            }
            
            // Start processing first node
            processNextNode(0);
            
            // Function to complete the shuffle phase
            function completeShufflePhase() {
              const shuffleDuration = Date.now() - shuffleStartTime;
              
              console.log(`[MR-${jid}] Node ${nodeId}: SHUFFLE COMPLETED in ${shuffleDuration}ms`);
              
              // Clear mapped results to release memory
              storageService.del({gid: gid, key: mapResultName}, (err, val) => {
                if (err) {
                  console.error(`[MR-${jid}] Node ${nodeId}: Error deleting map results: ${err.message}`);
                  errors.count++;
                  errors.details.push({phase: 'cleanup', error: err.message});
                } else {
                  console.log(`[MR-${jid}] Node ${nodeId}: Successfully deleted map results after shuffling`);
                }
                
                // Force garbage collection if available
                if (global.gc) {
                  global.gc();
                  const memAfterGC = process.memoryUsage();
                  console.log(`[MR-${jid}] Node ${nodeId}: Memory after forced GC: heap=${Math.round(memAfterGC.heapUsed/1024/1024)}MB`);
                }
                
                // Notify completion
                service.notify({
                  phase: "SHUFFLE", 
                  status: "COMPLETED", 
                  gid: gid, 
                  jid: jid,
                  nodeId: nodeId,
                  noKeysToProcess: false,
                  shuffleStats: {
                    entriesProcessed: entriesProcessed,
                    entriesSent: entriesSent,
                    targetsFound: targetsFound,
                    errors: errors
                  }
                }, callback);
              });
            }
          });
        });
      });
    };

    /**
     * Reduce function with enhanced tracking
     */
    const reduce = (config, callback) => {
      const gid = config.gid;
      const job_id = config.jid;
      const nodeId = global.nodeConfig.port;
      
      const reduceStartTime = Date.now();
      const initialMemUsage = process.memoryUsage();
      console.log(`[MR-${job_id}] Node ${nodeId}: Starting reduce phase. Memory: heap=${Math.round(initialMemUsage.heapUsed/1024/1024)}MB/${Math.round(initialMemUsage.heapTotal/1024/1024)}MB (${Math.round((initialMemUsage.heapUsed/initialMemUsage.heapTotal)*100)}% used)`);
      
      // Tracking variables
      let keysProcessed = 0;
      let totalValues = 0;
      let slowKeysCount = 0;
      
      distribution.local.routes.get(job_id, (err, service) => {
        if (err) {
          console.error(`[MR-${job_id}] Node ${nodeId}: Error getting service: ${err.message}`);
          callback(err, null);
          return;
        }
        
        const reducer = service.reducer;
        const shuffleResultName = "reduce@" + job_id;
        const storageService = distribution.local.mem;
        
        storageService.get({gid: gid, key: shuffleResultName}, (err, shuffleResults) => {
          if (err || !shuffleResults || Object.keys(shuffleResults).length === 0) {
            console.error(`[MR-${job_id}] Node ${nodeId}: Error retrieving shuffle results: ${err ? err.message : "no results found"}`);
            service.notify({
              phase: "REDUCE", 
              status: "COMPLETED", 
              results: [], 
              gid: gid, 
              jid: job_id,
              nodeId: nodeId,
              port: nodeId,
              noKeysToProcess: true,
              reduceStats: {
                keysProcessed: 0,
                totalValues: 0,
                slowKeys: 0
              }
            }, callback);
            return;
          }
          
          const reduceKeys = Object.keys(shuffleResults);
          keysProcessed = reduceKeys.length;
          
          console.log(`[MR-${job_id}] Node ${nodeId}: Reducing ${reduceKeys.length} keys`);
          
          // Process keys in batches for better memory management
          const REDUCE_BATCH_SIZE = Math.min(50, reduceKeys.length);
          let currentBatch = 0;
          const totalBatches = Math.ceil(reduceKeys.length / REDUCE_BATCH_SIZE);
          let reduceResults = [];
          
          const processBatch = () => {
            const batchStartTime = Date.now();
            const startIdx = currentBatch * REDUCE_BATCH_SIZE;
            const endIdx = Math.min(startIdx + REDUCE_BATCH_SIZE, reduceKeys.length);
            const batchKeys = reduceKeys.slice(startIdx, endIdx);
            
            let batchResults = [];
            let keysProcessed = 0;
            let batchTotalValues = 0;
            let batchSlowKeys = 0;
            
            batchKeys.forEach(key => {
              let values = shuffleResults[key];
              
              if (!Array.isArray(values)) {
                values = [values];
              }
              
              batchTotalValues += values.length;
              totalValues += values.length;
              
              try {
                const reduceStartMs = Date.now();
                const result = reducer(key, values);
                const reduceDurationMs = Date.now() - reduceStartMs;
                
                if (reduceDurationMs > 500) { // Log slow reducer operations
                  console.log(`[MR-${job_id}] Node ${nodeId}: Slow reducer for key ${key}: ${reduceDurationMs}ms with ${values.length} values`);
                  batchSlowKeys++;
                  slowKeysCount++;
                }
                
                batchResults.push(result);
              } catch (reduceError) {
                console.error(`[MR-${job_id}] Node ${nodeId}: Error reducing key ${key}: ${reduceError.message}`);
              }
              
              keysProcessed++;
              
              if (keysProcessed === batchKeys.length) {
                // Add batch results to total results
                reduceResults = reduceResults.concat(batchResults);
                currentBatch++;

                // console.log("[MR-%s] Node %s: Processed batch %s/%s for reduce phase with %s results, total values: %s, slow keys in this batch: %s, and results: ",
                //   job_id, 
                //   nodeId, 
                //   currentBatch, 
                //   totalBatches, 
                //   batchResults.length, 
                //   batchTotalValues,
                //   batchSlowKeys,
                //   JSON.stringify(batchResults) // For debugging purposes
                // );
                
                if (currentBatch < totalBatches) {
                  // Process next batch
                  processBatch();
                } else {
                  // All batches processed, notify completion
                  storageService.del({gid: gid, key: shuffleResultName}, (err, val) => {
                    if (err) {
                      console.error(`[MR-${job_id}] Node ${nodeId}: Error deleting shuffle results: ${err.message}`);
                    } else {
                      console.log(`[MR-${job_id}] Node ${nodeId}: Deleted shuffle results after reducing`);
                    }
                    
                    const reduceDuration = Date.now() - reduceStartTime;
                    const finalMemUsage = process.memoryUsage();
                    // console.log(`[MR-${job_id}] Node ${nodeId}: Reduce phase completed in ${reduceDuration}ms. Results: ${reduceResults.length}, Values processed: ${totalValues}, Memory: heap=${Math.round(finalMemUsage.heapUsed/1024/1024)}MB`);
                    
                    service.notify({
                      phase: "REDUCE", 
                      status: "COMPLETED", 
                      results: reduceResults,
                      gid: gid, 
                      jid: job_id,
                      nodeId: nodeId,
                      port: nodeId, // Include port for debugging
                      noKeysToProcess: false,
                      reduceStats: {
                        keysProcessed: keysProcessed,
                        totalValues: totalValues,
                        slowKeys: slowKeysCount
                      }
                    }, callback);
                  });
                }
              }
            });
          };
          
          // Start processing the first batch
          processBatch();
        });
      });
    };

    // Create an RPC version of the notify method so it runs on the coordinator
    let notifyRPC = distribution.util.wire.createRPC(distribution.util.wire.toAsync(notify));

    // Create the service object with all methods
    let mrServiceObject = {
      notify: notifyRPC,
      map: map,
      mapper: mapper,
      reducer: reducer,
      shuffle: shuffle,
      reduce: reduce,
      // Add configuration info for debugging
      config: {
        batchInfo: batchInfo,
        contextGid: context.gid,
        batchSize: batchSize,
        batchIndex: batchIndex
      }
    };
    
    // Register the service on all nodes in the group
    console.log(`[MR-${mrId}] EXEC STARTS on node ${global.nodeConfig.port}, with batch size ${batchSize}`);
    
    distribution[context.gid].routes.put(mrServiceObject, mrServiceName, (err, res) => {
      if (err) {
        console.error(`[MR-${mrId}] Error registering service: ${err.message}`);
        cb(err, null);
        return;
      }

      console.log(`[MR-${mrId}] Service registered successfully, starting map phase for batch ${batchInfo.current}/${batchInfo.total}`);
      
      // Start with MAP phase with a small delay to ensure service registration is complete
      setTimeout(() => {
        console.log(`[MR-${mrId}] Starting map phase after service registration`);
        
        const setupConfig = {
          gid: context.gid,
          jid: mrServiceName,
          batch_index: batchIndex,
          batch_size: batchSize
        };
            
        distribution[context.gid].comm.send([setupConfig], {gid: 'local', service: mrServiceName, method: 'map'}, (e, v) => {
           if (e && !isEmptyObject(e)) {
            // Handle the error gracefully, log it and return
            console.error(`[MR-${mrId}] Error starting map phase:`, e);
            console.error(`[MR-${mrId}] Setup config:`, JSON.stringify(setupConfig, null, 2));
            cb(e, null); // Report the error back to the caller
          } else {
            console.log(`[MR-${mrId}] Map phase started successfully`);
          }
        });
      }, 500); // 500ms delay
    });
  }

  return { exec };
}

module.exports = mr;