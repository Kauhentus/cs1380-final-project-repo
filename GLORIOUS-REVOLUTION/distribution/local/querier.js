// distribution/local/querier.js
const fs = require("fs");
const path = require("path");

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};

function initialize(callback) {
  callback = callback || cb;
  const distribution = require("../../config");
  fs.appendFileSync(
    global.logging_path,
    `QUERIER INITIALIZING... ${new Date()}\n`
  );

  callback();
}

function query_one(query, callback) {
  const fs = require("fs");
  callback = callback || cb;
  query = query;
  const query_words = query
    .split(" ")
    .filter((word) => word.trim() !== "")
    .map((word) => word.trim().toLowerCase());

  fs.appendFileSync(
    global.logging_path,
    `QUERIER QUERYING... ${query} ${query_words}\n`
  );

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

  console.log(`Querier: querying... ${query}`);
  distribution.local.groups.get("crawler_group", (e, v) => {
    const nodes = Object.values(v);
    const nids = nodes.map((node) => distribution.util.id.getNID(node));
    const num_nodes = nodes.length;

    console.log(`Querier: nodes: ${JSON.stringify(nodes)}`);
    console.log(`Querier: nids: ${JSON.stringify(nids)}`);

    distribution.indexer_group.indexer.get_idf_doc_count(async (e, v) => {
      const total_doc_count = Object.values(v).reduce(
        (acc, val) => acc + val.num_docs_on_node,
        0
      );

      console.log(`Querier Words: ${JSON.stringify(query_words)}`);
      console.log(
        `Querier: total_doc_count: ${total_doc_count} ${JSON.stringify(v)}`
      );
      // configs to query bulk appended prefix data
      const query_word_configs = query_words
        .map((word) => {
          const prefix = getSmartPrefix(word);
          const chosenNode = getChosenNode(prefix, nids, nodes);

          console.log(
            `Querier: Word: ${word}, prefix: ${prefix}, Chosen Node: ${JSON.stringify(
              chosenNode
            )}`
          );

          const matching_ip = global.nodeConfig.ip === chosenNode.ip;
          const matching_port = global.nodeConfig.port === chosenNode.port;
          if (!matching_ip || !matching_port) return false;

          return {
            key: `prefix-${prefix}.json`,
            gid: "indexer_group",
            word: word,
          };
        })
        .filter((config) => config !== false);
      console.log(
        `Querier: query_word_configs: ${JSON.stringify(query_word_configs)}`
      );
      if (query_word_configs.length === 0) return callback(null, false);

      // get results from each node and compute tf-idf on the fly (cached tf, cached idf)
      const get_results_from_query_word_config = (config) =>
        new Promise((resolve, reject) => {
          const word = config.word;

          console.log(`Querier: querying word: ${word}`);
          distribution.local.store.read_bulk(config, (e, v) => {
            // const data = distribution.util.deserialize(JSON.parse(v));
            const data = JSON.parse(v);
            const keys = Object.keys(data);
            console.log(`Querier: keys: ${keys}`);
            if (!keys.includes(word)) resolve([]);

            const results = data[word]; // raw data
            const num_docs_with_term = Object.keys(results.postings).length;
            const idf = Math.log(total_doc_count / num_docs_with_term);

            const page_results = Object.keys(results.postings).map((key) => {
              const posting = results.postings[key];
              posting.tf_idf = parseFloat(posting.tf) * idf;
              posting.query_word = word;
              return posting;
            });

            resolve(page_results);
          });
        });

      // combine everything and return local results to distributed querier to combine
      const results = await Promise.all(
        query_word_configs.map(get_results_from_query_word_config)
      );
      const flattened = results.flat();

      console.log(`Querier: flattened results: ${JSON.stringify(flattened)}`);

      callback(null, flattened);
    });
  });
}

module.exports = {
  initialize,
  query_one,
};
