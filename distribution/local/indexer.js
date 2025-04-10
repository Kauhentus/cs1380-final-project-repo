// distribution/local/indexer.js

// Default callback
const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};

const indexerMetrics = {
  documentsIndexed: 0,
  totalIndexTime: 0,
  totalTermsProcessed: 0,
  totalPrefixesProcessed: 0,
  batchesSent: 0,
  nodeDistribution: {}, // Track term distribution across nodes
  processingTimes: [], // Last 10 document processing times
  errors: 0,
  startTime: Date.now()
};

function index(configuration, callback) {
    callback = callback || cb;
    const metrics = {
      processingStartTime: Date.now(),
      totalTerms: 0,
      totalPrefixes: 0,
      prefixBatchSizes: [],
      nodeBatchTimes: new Map()
    };

    // TODO: Need to revist this to actually add in common prefixes
    const COMMON_PREFIXES = new Set([
      'th', 'an', 'co', 're', 'in', 'de', 'pr', 'st', 'en', 'tr', 'di', 'ch', 'pe'
    ]);

    function getSmartPrefix(term) {
      if (!term) return 'aa';
      
      const normalized = term.toLowerCase();
      const basePrefix = normalized.substring(0, 2);
      
      // For common prefixes, use 3 characters to distribute more evenly
      if (COMMON_PREFIXES.has(basePrefix) && term.length >= 3) {
        return normalized.substring(0, 3);
      }
      
      // For uncommon prefixes or short words, use 2 characters
      return basePrefix;
    }

    function getChosenNode(key, nids, nodes) {
      // 1) Get the key id
      const kid = distribution.util.id.getID(key);

      // 2) Use our chosen hash function to pick exactly one NID
      const chosenNID = distribution.util.id.naiveHash(kid, nids);

      // 3) find the node config whose NID matches chosenNID
      const chosenNode = nodes.find((nc) => distribution.util.id.getNID(nc) === chosenNID);
      return chosenNode;
    }
    
    if (!configuration) {
      return callback(new Error('Configuration is required for indexing'), null);
    }
    
    const document = configuration.value || configuration;
    
    if (!document || !document.url) {
      return callback(new Error('Document data is missing required fields'), null);
    }
    const indexStartTime = Date.now();
    
    try {
      
      const docId = document.url;
      const hierarchy = document.hierarchy || [];
      const binomialName = document.binomial_name || '';
      
      const wordCounts = document.word_counts ? 
                        new Map(Object.entries(document.word_counts)) : 
                        new Map();
      
      const totalWords = document.total_words || 0;
      
      console.log(`Processing document: ${docId}`);
      
      const taxonomyInfo = {};
      if (hierarchy && Array.isArray(hierarchy)) {
        hierarchy.forEach(pair => {
          if (Array.isArray(pair) && pair.length === 2) {
            const [label, value] = pair;
            taxonomyInfo[label] = value;
          }
        });
      }
      
      const kingdom = taxonomyInfo['kingdom'] || '';
      const family = taxonomyInfo['family'] || '';
      
      console.log(`Document ID: ${docId}, Total words: ${totalWords}, Unique terms: ${wordCounts.size}`);
      metrics.totalTerms = wordCounts.size;

      distribution.local.groups.get('index', (err, group) => {
        if (err || !group) {
          console.log(`Failed to get 'index' group: ${err ? err.message : 'Group not found'}`);
          return callback(new Error(`Failed to get 'index' group: ${err ? err.message : 'Group not found'}`), null);
        }
        
        const nodes = Object.values(group);
        const nids = nodes.map(node => distribution.util.id.getNID(node));
        
        const prefixGroups = new Map(); // prefix -> terms
        const nodeToPrefix = new Map(); // node -> prefixes
        
        for (const [word, count] of wordCounts) {
          const prefix = getSmartPrefix(word);
          if (!prefixGroups.has(prefix)) {
            prefixGroups.set(prefix, new Map());
          }
          prefixGroups.get(prefix).set(word, count);
        }
        
        metrics.totalPrefixes = prefixGroups.size;
        
        for (const [prefix, terms] of prefixGroups) {
          const chosenNode = getChosenNode(prefix, nids, nodes);
          if (!nodeToPrefix.has(chosenNode)) {
            nodeToPrefix.set(chosenNode, new Map());
          }
          nodeToPrefix.get(chosenNode).set(prefix, terms);
        }
        
        let completedBatches = 0;
        let totalBatches = 0;
        
        for (const [node, prefixes] of nodeToPrefix) {
          if (prefixes.size > 0) {
            totalBatches++;
          }
        }
        
        if (totalBatches === 0) {
          metrics.processingEndTime = Date.now();
          return callback(null, {
            status: 'success',
            docId: docId,
            metrics: {
              totalTerms: metrics.totalTerms,
              totalPrefixes: metrics.totalPrefixes,
              processingTime: metrics.processingEndTime - metrics.processingStartTime,
              batchesSent: 0
            }
          });
        }
        
        for (const [node, prefixes] of nodeToPrefix) {
          const nodeId = distribution.util.id.getNID(node);
          const nodePrefixBatches = [];
          
          if (!metrics.nodeBatchTimes.has(nodeId)) {
            metrics.nodeBatchTimes.set(nodeId, {
              batchCount: 0,
              totalTime: 0,
              termCount: 0
            });
          }
          
          let nodeTermCount = 0;
          
          for (const [prefix, terms] of prefixes) {
            const prefixData = {};
            for (const [word, count] of terms) {
              const tf = count / totalWords;              
              const taxonomyMatch = Object.entries(taxonomyInfo).find(
                ([key, value]) => value.toLowerCase().includes(word)
              );
              const inTaxonomy = !!taxonomyMatch;
              const taxonomyLevel = inTaxonomy ? taxonomyMatch[0] : null;
              const inBinomialName = binomialName.toLowerCase().includes(word);
              const inKingdom = kingdom.toLowerCase().includes(word);
              const inFamily = family.toLowerCase().includes(word);
              
              // !! New ranking mechnaism using the classification data
              const rankingFactors = {
                tf: tf,
                taxonomyBoost: inTaxonomy ? (
                  taxonomyLevel === 'kingdom' ? 5.0 :
                  taxonomyLevel === 'phylum' ? 4.0 :
                  taxonomyLevel === 'class' ? 3.0 :
                  taxonomyLevel === 'order' ? 2.5 :
                  taxonomyLevel === 'family' ? 2.0 :
                  taxonomyLevel === 'genus' ? 1.5 : 1.0
                ) : 1.0,
                binomialBoost: inBinomialName ? 4.0 : 1.0,
                positionBoost: inKingdom ? 3.0 : (inFamily ? 2.0 : 1.0),
                score: 0 // Calculated below
              };
              
              rankingFactors.score = tf * 
                rankingFactors.taxonomyBoost * 
                rankingFactors.binomialBoost * 
                rankingFactors.positionBoost;
              
              prefixData[word] = [{
                url: docId,
                tf: tf,
                ranking: rankingFactors,
                taxonomyLevel: taxonomyLevel,
                isBinomial: inBinomialName,
                pageInfo: {
                  kingdom: kingdom,
                  family: family,
                  binomialName: binomialName
                }
              }];
              
              nodeTermCount++;
            }
            
           nodePrefixBatches.push({
              prefix,
              data: prefixData
            });
          }
          metrics.nodeBatchTimes.get(nodeId).termCount = nodeTermCount;
          
          if (nodePrefixBatches.length > 0) {
            console.log(`Sending batch with ${nodePrefixBatches.length} prefixes to node ${nodeId}`);
            metrics.prefixBatchSizes.push(nodePrefixBatches.length);
            
            const batchStartTime = Date.now();
            distribution.local.comm.send([{
              prefixBatches: nodePrefixBatches,
              gid: 'index'
            }], {
              service: "store",
              method: "bulk_append",
              node: node
            }, (err, val) => {
              const batchEndTime = Date.now();
              const batchTime = batchEndTime - batchStartTime;
              metrics.nodeBatchTimes.get(nodeId).batchCount++;
              metrics.nodeBatchTimes.get(nodeId).totalTime += batchTime;
              
              if (err) {
                console.error(`Error sending to ${node.ip}:${node.port}:`, err);
                completedBatches++;
                if (completedBatches === totalBatches) {
                  finishProcessing(false);
                }
              } else {
                console.log(`Batch sent to ${node.ip}:${node.port} (${batchTime.toFixed(2)}ms)`);
                completedBatches++;
                
                if (completedBatches === totalBatches) {
                  finishProcessing(true);
                }
              }
            });
          }
        }
        
        function finishProcessing(success) {
          const memUsage = process.memoryUsage();
          console.log(`Memory usage: ${Math.round(memUsage.heapUsed/1024/1024)}MB/${Math.round(memUsage.heapTotal/1024/1024)}MB`);
          
          // Force garbage collection if available and memory usage is high
          if (global.gc && memUsage.heapUsed > 500 * 1024 * 1024) { // 500MB threshold
              console.log("Forcing garbage collection");
              global.gc();
          }
          metrics.processingEndTime = Date.now();
          
          // Log performance metrics (GPT CODE!!!)
          const totalProcessingTime = metrics.processingEndTime - metrics.processingStartTime;
          console.log(`Total processing time: ${totalProcessingTime}ms`);
          console.log(`Total terms processed: ${metrics.totalTerms}`);
          console.log(`Total prefixes: ${metrics.totalPrefixes}`);
          
          if (metrics.prefixBatchSizes.length > 0) {
            const avgBatchSize = metrics.prefixBatchSizes.reduce((sum, size) => sum + size, 0) / 
                               metrics.prefixBatchSizes.length;
            console.log(`Average batch size: ${avgBatchSize.toFixed(2)} prefixes`);
          }
          
          let totalBatchCount = 0;
          let totalBatchTime = 0;
          
          for (const [nodeId, stats] of metrics.nodeBatchTimes) {
            totalBatchCount += stats.batchCount;
            totalBatchTime += stats.totalTime;
            
            if (stats.batchCount > 0) {
              const avgNodeBatchTime = stats.totalTime / stats.batchCount;
              console.log(`Node ${nodeId}: ${stats.batchCount} batches, avg ${avgNodeBatchTime.toFixed(2)}ms, ${stats.termCount} terms`);
            }
          }
          
          if (totalBatchCount > 0) {
            const avgBatchTime = totalBatchTime / totalBatchCount;
            console.log(`Overall average batch time: ${avgBatchTime.toFixed(2)}ms`);
          }

          const indexingTime = Date.now() - indexStartTime;
          indexerMetrics.documentsIndexed++;
          indexerMetrics.totalIndexTime += indexingTime;
          indexerMetrics.totalTermsProcessed += metrics.totalTerms || 0;
          indexerMetrics.totalPrefixesProcessed += metrics.totalPrefixes || 0;
          indexerMetrics.batchesSent += totalBatchCount || 0;
          indexerMetrics.processingTimes.push(indexingTime);
          if (indexerMetrics.processingTimes.length > 20) {
            indexerMetrics.processingTimes.shift();
          }

          if (!success) {
            indexerMetrics.errors++;
          }

          callback(null, {
            status: success ? 'success' : 'partial_success',
            docId: docId,
            metrics: {
              totalTerms: metrics.totalTerms,
              totalPrefixes: metrics.totalPrefixes,
              processingTime: totalProcessingTime,
              batchesSent: totalBatchCount
            }
          });
        }
      });
    } catch (error) {
      console.error("Error processing document:", error);
      metrics.processingEndTime = Date.now();
      callback(error, null);
    }
  }

function get_stats(callback) {
  callback = callback || cb;

  const avgIndexTime = indexerMetrics.documentsIndexed > 0 
    ? indexerMetrics.totalIndexTime / indexerMetrics.documentsIndexed 
    : 0;

  const stats = {
    metrics: {
      documentsIndexed: indexerMetrics.documentsIndexed,
      totalIndexTime: indexerMetrics.totalIndexTime,
      avgIndexTime: avgIndexTime,
      totalTermsProcessed: indexerMetrics.totalTermsProcessed,
      totalPrefixesProcessed: indexerMetrics.totalPrefixesProcessed,
      batchesSent: indexerMetrics.batchesSent,
      errors: indexerMetrics.errors,
      uptime: Date.now() - indexerMetrics.startTime,
      recentTimes: indexerMetrics.processingTimes.slice(-5)
    }
  };
  
  callback(null, stats);
}

module.exports = { index, get_stats };