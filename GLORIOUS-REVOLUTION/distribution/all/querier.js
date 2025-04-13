const log_queries = false;

const querier = function (config) {
    const context = {};
    context.gid = config.gid || 'all';

    return {
        initialize: (config, group, callback) => {
            distribution[context.gid].comm.send(
                [config, group],
                { service: 'querier', method: 'initialize' },
                callback
            );
        },

        query_one: (query, options, callback) => {
            //   console.log(`QUERIER INITIALIZING... ${new Date()}`);
            //   console.log(`QUERIER QUERYING... ${query}`);
            //   console.log(`QUERIER GID: ${context.gid}`);
            const COMMON_PREFIXES = require("../util/common_prefixes");
            const stopWordsSet = require("../util/stopwords");
            function getSmartPrefix(term) {
              if (!term) return "aa";
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
              const chosenNode = nodes.find(
                (nc) => distribution.util.id.getNID(nc) === chosenNID
              );
              return chosenNode;
            }
            function processQuery(query) {
              const queryNorm = query.toLowerCase().trim();
      
              const terms = queryNorm
                .replace(/[^\w\s]/g, "")
                .split(/\s+/)
                .filter((term) => term.length > 0 && !stopWordsSet.has(term));
      
              const prefixMap = new Map();
              for (const term of terms) {
                const prefix = getSmartPrefix(term);
                if (!prefixMap.has(prefix)) {
                  prefixMap.set(prefix, []);
                }
                prefixMap.get(prefix).push(term);
              }
      
              return {
                original: query,
                normalized: queryNorm,
                terms: terms,
                prefixMap: prefixMap,
              };
            }
            function getTotalDocumentCount(callback) {
              distribution.indexer_group.comm.send(
                [],
                { service: "indexer", method: "get_idf_doc_count" },
                (errMap, resMap) => {
                  let totalCount = 0;
                  for (const nodeId in resMap) {
                    totalCount += resMap[nodeId].num_docs_on_node || 0;
                  }
                  if (totalCount === 0) {
                    totalCount = 10000;
                  }
      
                  callback(null, totalCount);
                }
              );
            }
            function merger(prefixResults) {
              const mergedDocs = new Map();
      
              prefixResults.forEach((prefixResult) => {
                if (!prefixResult || !prefixResult.results) return;
      
                prefixResult.results.forEach((doc) => {
                  const docId = doc.docId;
      
                  if (mergedDocs.has(docId)) {
                    const currDoc = mergedDocs.get(docId);
                    currDoc.score += doc.score;
                    currDoc.matchedTerms += doc.matchedTerms;
                    currDoc.matchRatio = Math.max(currDoc.matchRatio, doc.matchRatio);
                  } else {
                    mergedDocs.set(docId, { ...doc });
                  }
                });
              });
      
              const sortedResults = Array.from(mergedDocs.values());
      
              sortedResults.sort((a, b) => {
                // TODO: this should handle the spinach case
                if (b.matchedTerms !== a.matchedTerms) {
                  return b.matchedTerms - a.matchedTerms;
                }
                return b.score - a.score;
              });
      
              return sortedResults;
            }
      
            // TODO: Import the stop words from utils (can u unpack values like this in js)
            if(log_queries) console.log(`Processing query: "${query}"`);
            const { original, normalized, terms, prefixMap } = processQuery(query);
            if (terms.length === 0) {
              return callback(null, {
                query: original,
                results: [],
                message: "No valid search terms found after removing common words",
              });
            }
            if(log_queries) console.log(`Tokenized terms: ${JSON.stringify(terms)}`);
            getTotalDocumentCount(async (err, totalDocCount) => {
              distribution.local.groups.get("indexer_group", async (e, v) => {
                if (e) {
                  console.error(e);
                  callback(e);
                  return;
                }
                const nodes = Object.values(v);
                const nids = nodes.map((node) => distribution.util.id.getNID(node));
                const queryPromises = [];
      
                for (const [prefix, terms] of prefixMap.entries()) {
                  if(log_queries) console.log(`Processing prefix: ${prefix} with terms: ${terms}`);
                  const chosenNode = getChosenNode(prefix, nids, nodes);
                  if (!chosenNode) {
                    console.error(`No chosen node for prefix: ${prefix}`);
                    continue;
                  }
                  if(log_queries) console.log(
                    `Chosen Node for prefix ${prefix}: ${JSON.stringify(chosenNode)}`
                  );
      
                  const queryPromise = new Promise((resolve, reject) => {
                    const config = {
                      service: "querier",
                      method: "query_one",
                      node: chosenNode,
                    };
                    const message = [
                      {
                        terms: terms,
                        prefix: prefix,
                        totalDocCount: totalDocCount,
                      },
                    ];
                    distribution.local.comm.send(message, config, (err, response) => {
                      if (err) {
                        console.error(
                          `Error querying node for prefix ${prefix}:`,
                          err
                        );
                        resolve(null);
                      } else {
                        resolve(response);
                      }
                    });
                  });
                  queryPromises.push(queryPromise);
                }
      
                try {
                  const nodeResults = await Promise.all(queryPromises);
      
                  const mergedResults = merger(nodeResults);
      
                  const no_trim = options.no_trim || false;

                  const response = {
                    query: original,
                    terms: terms,
                    totalResults: mergedResults.length,
                    topResults: no_trim ? mergedResults : mergedResults.slice(0, 20),
                    prefixMapping: Object.fromEntries(prefixMap),
                    timing: {
                      processedAt: new Date().toISOString(),
                    },
                  };
      
                  callback(null, response);
                } catch (error) {
                  console.error("Error processing distributed query:", error);
                  callback(error);
                }
      
                // TODO: need to finish
              });
            });
        },

        query_range: (query, options, callback) => {
          query = query.trim().toLowerCase();

          function getChosenNode(key, nids, nodes) {
            const kid = distribution.util.id.getID(key);
            const chosenNID = distribution.util.id.naiveHash(kid, nids);
            const chosenNode = nodes.find((nc) => distribution.util.id.getNID(nc) === chosenNID);
            return chosenNode;
          }

          distribution.local.groups.get('indexer_ranged_group', async (e, v) => {
            const nodes = Object.values(v);
            const num_nodes = nodes.length;
            const nids = nodes.map(node => distribution.util.id.getNID(node));
            const chosen_node = getChosenNode(query, nids, nodes);

            distribution.local.comm.send(
              [ query, 0, [] ], 
              { service: "querier", method: "query_range", node: chosen_node }, 
              (err, val) => {
                if(err) return callback(err);

                const output_species = val;
                if(output_species.some(species => !species.includes('[SPECIES]'))) throw new Error("query_range compromised");
                const processed_urls = output_species.map(species => species.slice(10));
                callback(null, processed_urls);
              }
            );
          });
        }
    };
};

module.exports = querier;
