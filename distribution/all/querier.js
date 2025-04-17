const querier = function (config) {
  const context = {};
  context.gid = config.gid || "all";

  const TRACKING_QUERIES = [
    "plantae",
    "fungi",
    "eukaryota",
    "lepidoptera",
    "animalia",
    "citrus",
  ];

  function getChosenNode(key, nids, nodes) {
    const kid = distribution.util.id.getID(key);
    const chosenNID = distribution.util.id.naiveHash(kid, nids);
    const chosenNode = nodes.find(
      (nc) => distribution.util.id.getNID(nc) === chosenNID
    );
    return chosenNode;
  }

  return {
    initialize: (config, group, callback) => {
      distribution[context.gid].comm.send(
        [config, group],
        { service: "querier", method: "initialize" },
        callback
      );
    },

    query_one: (query, options, callback) => {
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
        const totalTerms =
          prefixResults.length > 0 && prefixResults[0].queryTerms
            ? prefixResults[0].queryTerms.length
            : 1;

        const termMatchesMap = new Map();

        let totalMatches = 0;

        // TODO: SPLIT THIS INTO TWO PASSES
        prefixResults.forEach((prefixResult) => {
          if (!prefixResult || !prefixResult.results) return;

          // console.log(`Prefix Result: ${prefixResult.totalMatches}`);
          totalMatches += prefixResult.totalMatches;
          // console.log(`Total Matches: ${totalMatches}`);

          prefixResult.results.forEach((doc) => {
            const docId = doc.docId;

            if (!termMatchesMap.has(docId)) {
              termMatchesMap.set(docId, new Set());
            }

            prefixResult.queryTerms.forEach((term) => {
              if (doc.matchedTerms > 0) {
                termMatchesMap.get(docId).add(term);
              }
            });
          });
        });

        const mergedDocs = new Map();

        prefixResults.forEach((prefixResult) => {
          if (!prefixResult || !prefixResult.results) return;

          prefixResult.results.forEach((doc) => {
            const docId = doc.docId;
            const totalMatchedTerms = termMatchesMap.get(docId)?.size || 0;

            if (mergedDocs.has(docId)) {
              const currDoc = mergedDocs.get(docId);
              currDoc.score += doc.score;
              currDoc.matchedTerms = totalMatchedTerms;
              currDoc.matchRatio = totalMatchedTerms / totalTerms;
            } else {
              mergedDocs.set(docId, {
                ...doc,
                matchedTerms: totalMatchedTerms,
                matchRatio: totalMatchedTerms / totalTerms,
              });
            }
          });
        });

        // !! This is how im solving the spinach issue!!
        for (const [___, doc] of mergedDocs.entries()) {
          if (doc.matchedTerms > 1) {
            doc.score *= Math.pow(3.0, doc.matchedTerms - 1);

            if (doc.matchRatio >= 0.75) {
              doc.score *= 2.0;
            } else if (doc.matchRatio >= 0.5) {
              doc.score *= 1.5;
            }
          }
        }

        const docsByBinomialName = new Map();

        for (const [docId, doc] of mergedDocs.entries()) {
          const binomialName =
            doc.termDetails?.pageInfo?.binomialName?.toLowerCase() || "";
          const title = doc.termDetails?.title?.toLowerCase() || "";
          const key = binomialName || title || docId;

          if (!docsByBinomialName.has(key)) {
            docsByBinomialName.set(key, []);
          }
          docsByBinomialName.get(key).push(doc);
        }

        const deduplicatedDocs = [];
        for (const [key, docs] of docsByBinomialName.entries()) {
          if (docs.length > 1) {
            docs.sort((a, b) => b.score - a.score);
            deduplicatedDocs.push(docs[0]);
          } else {
            deduplicatedDocs.push(docs[0]);
          }
        }

        deduplicatedDocs.sort((a, b) => {
          if (b.matchedTerms !== a.matchedTerms) {
            return b.matchedTerms - a.matchedTerms;
          }
          return b.score - a.score;
        });

        return { mergedResults: deduplicatedDocs, totalMatches: totalMatches };
      }

      const { original, normalized, terms, prefixMap } = processQuery(query);
      if (terms.length === 0) {
        return callback(null, {
          query: original,
          results: [],
          message: "No valid search terms found after removing common words",
        });
      }

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

          // console.log(
          // `Distributing query to ${prefixMap.size} node(s) for ${terms.length} term(s)`
          // );

          for (const [prefix, terms] of prefixMap.entries()) {
            // console.log(`Processing prefix: ${prefix} with terms: ${terms}`);
            const chosenNode = getChosenNode(prefix, nids, nodes);
            if (!chosenNode) {
              console.error(`No chosen node for prefix: ${prefix}`);
              continue;
            }

            // console.log(
            //   `Chosen Node for prefix ${prefix}: ${JSON.stringify(chosenNode)}`
            // );

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

              // console.log(
              //   `Sending query_one request to node ${chosenNode.port} for prefix ${prefix}`
              // );

              distribution.local.comm.send(message, config, (err, response) => {
                if (err) {
                  console.error(
                    `Error querying node for prefix ${prefix}:`,
                    err
                  );
                  resolve(null);
                } else {
                  // console.log(
                  //   `Received response from node ${chosenNode.port} for prefix ${prefix}`
                  // );
                  resolve(response);
                }
              });
            });

            queryPromises.push(queryPromise);
          }

          try {
            const nodeResults = await Promise.all(queryPromises);
            // console.log(`All ${queryPromises.length} prefix queries completed`);

            const { mergedResults, totalMatches } = merger(nodeResults);

            // console.log(original, totalMatches);
            const no_trim = options.no_trim || false;

            const response = {
              query: original,
              terms: terms,
              totalResults: totalMatches,
              topResults: no_trim ? mergedResults : mergedResults.slice(0, 20),
              prefixMapping: Object.fromEntries(prefixMap),
              timing: {
                processedAt: new Date().toISOString(),
              },
            };

            if (TRACKING_QUERIES.includes(normalized)) {
              distribution[context.gid].comm.send(
                [
                  {
                    timestamp: Date.now(),
                    query: normalized,
                    count: totalMatches,
                  },
                ],
                { service: "querier", method: "log_query_growth" },
                () => {}
              );
            }

            callback(null, response);
          } catch (error) {
            console.error("Error processing distributed query:", error);
            callback(error);
          }
        });
      });
    },

    query_range: (query, options, callback) => {
      query = query.trim().toLowerCase();
      const return_tree = options.return_tree || false;

      distribution.indexer_ranged_group.store.clean_bulk_range_append(
        (e, v) => {
          distribution.local.groups.get(
            "indexer_ranged_group",
            async (e, v) => {
              const nodes = Object.values(v);
              const nids = nodes.map((node) =>
                distribution.util.id.getNID(node)
              );
              const chosen_node = getChosenNode(query, nids, nodes);

              distribution.local.comm.send(
                [query, 0, [], options],
                {
                  service: "querier",
                  method: "query_range",
                  node: chosen_node,
                },
                (err, val) => {
                  if (err) return callback(err);

                  if (return_tree) {
                    callback(err, val);
                  } else {
                    const output_species = val;
                    if (
                      output_species.some(
                        (species) => !species.includes("[SPECIES]")
                      )
                    )
                      throw new Error("query_range compromised");
                    const processed_urls = output_species.map((species) =>
                      species.slice(10)
                    );
                    callback(null, processed_urls);
                  }
                }
              );
            }
          );
        }
      );
    },

    get_stats: (callback) => {
      distribution[context.gid].comm.send(
        [],
        { service: "querier", method: "get_stats" },
        callback
      );
    },
  };
};

module.exports = querier;
