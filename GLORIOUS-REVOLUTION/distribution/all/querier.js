const querier = function (config) {
  const context = {};
  context.gid = config.gid || "all";

  return {
    initialize: (config, group, callback) => {
      distribution[context.gid].comm.send(
        [config, group],
        { service: "querier", method: "initialize" },
        callback
      );
    },

    query_one: (query, callback) => {
      //   console.log(`QUERIER INITIALIZING... ${new Date()}`);
      //   console.log(`QUERIER QUERYING... ${query}`);
      //   console.log(`QUERIER GID: ${context.gid}`);
      const COMMON_PREFIXES = require("../util/common_prefixes");
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
      //   distribution.local.groups.get("indexer_group", (e, v) => {
      //     if (e) {
      //       console.error(e);
      //       callback(e);
      //       return;
      //     }
      //     const nodes = Object.values(v);
      //     const nids = nodes.map((node) => distribution.util.id.getNID(node));
      //     const query_words = query
      //       .split(" ")
      //       .filter((word) => word.trim() !== "")
      //       .map((word) => word.trim().toLowerCase());
      //     const query_prefixes = query_words.map((word) => {
      //       let prefix = getSmartPrefix(word);
      //     });
      //   });
      distribution[context.gid].comm.send(
        [query],
        { service: "querier", method: "query_one" },
        (e, v) => {
          const query_words = query
            .split(" ")
            .filter((word) => word.trim() !== "")
            .map((word) => word.trim().toLowerCase());

          const data = Object.values(v)
            .filter((result) => Array.isArray(result))
            .flat();

          const all_docs_query_tf_idf = {};
          data.map((posting) => {
            const docId = posting.pageInfo.binomialName;
            if (all_docs_query_tf_idf[docId] === undefined)
              all_docs_query_tf_idf[docId] = {};
            const current_word = posting.query_word;
            all_docs_query_tf_idf[docId][current_word] = posting;
          });
          const final_collation = [];
          Object.keys(all_docs_query_tf_idf).map((key) => {
            const this_doc_query_tf_idf = all_docs_query_tf_idf[key];
            const queries_on_doc = Object.keys(this_doc_query_tf_idf);
            const missing_queries = query_words.filter(
              (query) => !queries_on_doc.includes(query)
            );
            missing_queries.map((query_word) => {
              this_doc_query_tf_idf[query_word] = {
                docId: key,
                pageInfo: {
                  binomialName:
                    this_doc_query_tf_idf[queries_on_doc[0]].pageInfo
                      .binomialName,
                },
                tf: 0.0000001,
                ranking: {
                  tf: 0.0000001,
                  score: 0.0000001,
                },
                tf_idf: 0.0000001,
                query_word: query_word,
              };
            });
            Object.values(this_doc_query_tf_idf).map((posting) =>
              final_collation.push(posting)
            );
          });

          const combine_by_binomial_name = (results) => {
            const acc = results.reduce((map, item) => {
              const key = item.pageInfo.binomialName;
              if (!map[key]) {
                map[key] = { ...item, ranking: { ...item.ranking } };
              } else {
                map[key].tf *= item.tf;
                map[key].tf_idf *= item.tf_idf;
                map[key].ranking.tf *= item.ranking.tf;
                map[key].ranking.score *= item.ranking.score;
              }
              return map;
            }, {});
            return Object.values(acc);
          };
          const results = combine_by_binomial_name(final_collation).sort(
            (a, b) => b.tf_idf - a.tf_idf
          );

          results.map(
            (result) => (result.tf_idf = +result.tf_idf.toPrecision(4))
          );

          callback(null, results);
        }
      );
    },
  };
};

module.exports = querier;
