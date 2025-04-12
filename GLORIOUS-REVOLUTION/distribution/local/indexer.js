// distribution/local/indexer.js
const fs = require('fs');

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    if(log_index) console.log(v);
  }
};

const log_index = false;

const indexerMetrics = {
  documentsIndexed: 0,
  totalIndexTime: 0,
  totalTermsProcessed: 0,
  totalPrefixesProcessed: 0,
  batchesSent: 0,
  nodeDistribution: {},
  processingTimes: [],
  errors: 0,
  startTime: Date.now()
};

function initialize(callback) {
  callback = callback || cb;
  const distribution = require('../../config');
  fs.appendFileSync(global.logging_path, `INDEXER INITIALIZING... ${new Date()}\n`);

  const links_to_index_map = new Map();
  const indexed_links_map = new Map();

  distribution.local.mem.put(links_to_index_map, 'links_to_index_map', (e, v) => {
    distribution.local.mem.put(indexed_links_map, 'indexed_links_map', (e, v) => {

      distribution.local.store.get('links_to_index', (e1, v1) => {
        distribution.local.store.get('indexed_links', (e2, v2) => {

          if (!e1 && !e2 && typeof v1 === 'string' && typeof v2 === 'string') {
            const saved_links_to_index = v1.split('\n').filter(s => s.length > 0);
            const saved_indexed_links = v2.split('\n').filter(s => s.length > 0);
            saved_links_to_index.map(link => links_to_index_map.set(link, true));
            saved_indexed_links.map(link => indexed_links_map.set(link, true));
          }

          if(e1 && e2) {
            distribution.local.store.put('', 'links_to_index', (e, v) => {
              distribution.local.store.put('', 'indexed_links', (e, v) => {
                fs.appendFileSync(global.logging_path, `CREATED MAPS ${e} ${v}\n`);
              });
            });
          }

          callback(null, {
            status: 'success',
            message: 'Index service initialized',
            links_to_index: links_to_index_map.size,
            indexed_links: indexed_links_map.size
          });
        });
      });
    });
  });
}


function add_link_to_index(link, callback) {
  callback = callback || cb;

  distribution.local.mem.get('links_to_index_map', (e1, links_to_index_map) => {
    distribution.local.mem.get('indexed_links_map', (e2, indexed_links_map) => {
      if (links_to_index_map.has(link)) return callback(null, { status: 'skipped', reason: 'already_in_queue' });
      if (indexed_links_map.has(link)) return callback(null, { status: 'skipped', reason: 'already_crawled' });

      links_to_index_map.set(link, true);
      callback(null, { status: 'success', message: 'Link added to crawl queue', link: link });
    });
  });
}


function index_one(callback) {
  callback = callback || cb;

  const metrics = {
    processingStartTime: Date.now(),
    totalTerms: 0,
    totalPrefixes: 0,
    prefixBatchSizes: [],
    nodeBatchTimes: new Map()
  };
  const indexStartTime = Date.now();

  const COMMON_PREFIXES = new Set([
    'th', 'an', 'co', 're', 'in', 'de', 'pr', 'st', 'en', 'tr', 'di', 'ch', 'pe'
  ]);
  function getSmartPrefix(term) {
    if (!term) return 'aa';
    const normalized = term.toLowerCase();
    const basePrefix = normalized.substring(0, 2);
    if (COMMON_PREFIXES.has(basePrefix) && term.length >= 3) {
      return normalized.substring(0, 3);
    }
    return basePrefix;
  }
  function getChosenNode(key, nids, nodes) {
    const kid = distribution.util.id.getID(key);
    const chosenNID = distribution.util.id.naiveHash(kid, nids);
    const chosenNode = nodes.find((nc) => distribution.util.id.getNID(nc) === chosenNID);
    return chosenNode;
  }

  const fs = require('fs');
  // fs.appendFileSync(global.logging_path, `INDEXING ONE...\n`);

  distribution.local.mem.get('links_to_index_map', (e1, links_to_index_map) => {
    distribution.local.mem.get('indexed_links_map', (e2, indexed_links_map) => {

      if (links_to_index_map.size === 0) {
        fs.appendFileSync(global.logging_path, `INDEXER SKIPPED\n`);
        return callback(null, { status: 'skipped', reason: 'no_links' });
      }
      const [url, _] = links_to_index_map.entries().next().value;
      links_to_index_map.delete(url);
      if (indexed_links_map.has(url)) {
        fs.appendFileSync(global.logging_path, `INDEXER SKIPPED\n`);
        return callback(null, { status: 'skipped', reason: 'already_indexed' });
      }
      indexed_links_map.set(url, true);

      fs.appendFileSync(global.logging_path, `INDEXER SELECTED: ${url}\n`);

      // ####################################
      // 1. FETCH DOCUMENT FROM CRAWLER STORE
      // ####################################

      distribution.crawler_group.store.get(url, (e, v) => {
        // fs.appendFileSync(global.logging_path, `   Indexer got: ${url} ${e} ${Object.keys(v)}\n`);
        const document = v;
        const docId = document.url;
        const wordCounts = document.word_counts ? new Map(Object.entries(document.word_counts)) : new Map();
        const totalWords = wordCounts.size;
        metrics.totalTerms = totalWords;

        const hierarchy = document.hierarchy || [];
        const binomialName = document.binomial_name || '';
        const taxonomyInfo = Object.fromEntries(hierarchy.filter(entry => Array.isArray(entry) && entry.length === 2));
        const kingdom = taxonomyInfo['kingdom'] || '';
        const family = taxonomyInfo['family'] || '';

        distribution.local.groups.get('indexer_group', async (e, v) => {
          const nodes = Object.values(v);
          const num_nodes = nodes.length;
          const nids = nodes.map(node => distribution.util.id.getNID(node));

          // ####################################
          // 2. COMPUTE NODE -> PREFIX and PREFIX -> TERMS MAPS
          // ####################################

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

          // fs.appendFileSync(global.logging_path, `   Indexer prefix groups: ${JSON.stringify(Object.fromEntries(prefixGroups))}\n`);
          // fs.appendFileSync(global.logging_path, `   Indexer node to prefix: ${JSON.stringify(Object.fromEntries(nodeToPrefix))}\n`);          

          // ####################################
          // 3. BEGIN BATCHING PREP
          // ####################################

          let totalBatches = 0;
          for (const [node, prefixes] of nodeToPrefix) {
            if (prefixes.size > 0) totalBatches++;
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


          // ####################################
          // 4. CREATE BATCHES
          // ####################################

          let node_prefix_pairs = [];
          let bulk_batches_to_send = [];
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
            
            node_prefix_pairs.push([node, nodeId, prefixes]);
            bulk_batches_to_send.push(nodePrefixBatches);
          }
          if(bulk_batches_to_send.length !== nodeToPrefix.size) throw Error("RAH");

          // ####################################
          // 5. SEND BATCHES TO NODES
          // ####################################

          let success = true;
          for(let i = 0; i < nodeToPrefix.size; i++) {
            const [node, nodeId, prefixes] = node_prefix_pairs[i];
            const nodePrefixBatches = bulk_batches_to_send[i];
            if(nodePrefixBatches.length === 0) continue;

            if(log_index) console.log(`Sending batch with ${nodePrefixBatches.length} prefixes to node ${nodeId} ${global.nodeConfig.port}`);
            metrics.prefixBatchSizes.push(nodePrefixBatches.length);
            const batchStartTime = Date.now();
          
            await new Promise((resolve, reject) => {
              distribution.local.comm.send(
                [{ prefixBatches: nodePrefixBatches, gid: 'index' }], 
                { service: "store", method: "bulk_append", node: node }, 
                (err, val) => {
                  if(err) success = false;
                  const batchEndTime = Date.now();
                  const batchTime = batchEndTime - batchStartTime;
                  metrics.nodeBatchTimes.get(nodeId).batchCount++;
                  metrics.nodeBatchTimes.get(nodeId).totalTime += batchTime;
                  resolve();
                }
              )
            });
          }
          fs.appendFileSync(global.logging_path, `   Indexer finished: ${success}\n`);

          // ####################################
          // 6. FINISH PROCESSING AND METRICS
          // ####################################

          // Force garbage collection if available and memory usage is high
          const memUsage = process.memoryUsage();
          if(log_index) console.log(`Memory usage: ${Math.round(memUsage.heapUsed/1024/1024)}MB/${Math.round(memUsage.heapTotal/1024/1024)}MB`);
          if (global.gc && memUsage.heapUsed > 500 * 1024 * 1024) { // 500MB threshold
            if(log_index) console.log("Forcing garbage collection");
            global.gc();
          }
          metrics.processingEndTime = Date.now();
          
          // Log performance metrics
          const totalProcessingTime = metrics.processingEndTime - metrics.processingStartTime;
          if(log_index) console.log(`Total processing time: ${totalProcessingTime}ms`);
          if(log_index) console.log(`Total terms processed: ${metrics.totalTerms}`);
          if(log_index) console.log(`Total prefixes: ${metrics.totalPrefixes}`);
          
          if (metrics.prefixBatchSizes.length > 0) {
            const avgBatchSize = metrics.prefixBatchSizes.reduce((sum, size) => sum + size, 0) / metrics.prefixBatchSizes.length;
            if(log_index) console.log(`Average batch size: ${avgBatchSize.toFixed(2)} prefixes`);
          }
          let totalBatchCount = 0;
          let totalBatchTime = 0;
          for (const [nodeId, stats] of metrics.nodeBatchTimes) {
            totalBatchCount += stats.batchCount;
            totalBatchTime += stats.totalTime;
            if (stats.batchCount > 0) {
              const avgNodeBatchTime = stats.totalTime / stats.batchCount;
              if(log_index) console.log(`Node ${nodeId}: ${stats.batchCount} batches, avg ${avgNodeBatchTime.toFixed(2)}ms, ${stats.termCount} terms`);
            }
          }
          if (totalBatchCount > 0) {
            const avgBatchTime = totalBatchTime / totalBatchCount;
            if(log_index) console.log(`Overall average batch time: ${avgBatchTime.toFixed(2)}ms`);
          }

          const indexingTime = Date.now() - indexStartTime;
          indexerMetrics.documentsIndexed++;
          indexerMetrics.totalIndexTime += indexingTime;
          indexerMetrics.totalTermsProcessed += metrics.totalTerms || 0;
          indexerMetrics.totalPrefixesProcessed += metrics.totalPrefixes || 0;
          indexerMetrics.batchesSent += totalBatchCount || 0;
          indexerMetrics.processingTimes.push(indexingTime);
          if (indexerMetrics.processingTimes.length > 20) indexerMetrics.processingTimes.shift();
          if (!success) indexerMetrics.errors++;

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
        });
      });
    });
  });
}

function save_maps_to_disk(callback) {
  callback = callback || cb;

  distribution.local.mem.get('links_to_index_map', (e1, links_to_index_map) => {
    distribution.local.mem.get('indexed_links_map', (e2, indexed_links_map) => {
      const links_to_index_data = Array.from(links_to_index_map.keys()).join('\n');
      const indexed_links_data = Array.from(indexed_links_map.keys()).join('\n');

      distribution.local.store.put(links_to_index_data, 'links_to_index', (e, v) => {
        distribution.local.store.put(indexed_links_data, 'indexed_links', (e, v) => {
          callback(null, {
            status: 'success',
            links_to_index_saved: links_to_index_map.size,
            indexed_links_saved: indexed_links_map.size
          });
        });
      });
    });
  });
}

module.exports = {
  initialize,
  add_link_to_index,
  save_maps_to_disk,
  index_one
};