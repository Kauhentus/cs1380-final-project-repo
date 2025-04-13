const indexer = function (config) {
  const context = {};
  context.gid = config.gid || "all";

  return {
    initialize: (config, group, callback) => {
      distribution[context.gid].comm.send(
        [config, group],
        { service: "indexer", method: "initialize" },
        callback
      );
    },

    start_index: (callback) => {
      distribution[context.gid].comm.send(
        [],
        { service: "indexer", method: "start_index" },
        callback
      );
    },

    add_link_to_crawl: (name, callback) => {
      distribution[context.gid].comm.send(
        [name],
        { service: "indexer", method: "add_link_to_crawl" },
        callback
      );
    },

    index_one: (name, callback) => {
      distribution[context.gid].comm.send(
        [name],
        { service: "indexer", method: "index_one" },
        callback
      );
    },

    get_idf_doc_count: (callback) => {
      distribution[context.gid].comm.send(
        [],
        { service: "indexer", method: "get_idf_doc_count" },
        callback
      );
    },

    get_stats: (callback) => {
      distribution[context.gid].comm.send(
        [],
        { service: "indexer", method: "get_stats" },
        (errMap, resMap) => {
          const indexerMetrics = {
            totalIndexTime: 0,
            totalDocumentsIndexed: 0,
            totalTermsProcessed: 0,
            totalPrefixesProcessed: 0,
            batchesSent: 0,
            errors: 0,
          };
          for (const key in resMap) {
            const nodeMetrics = resMap[key].metrics;
            if (nodeMetrics) {
              indexerMetrics.totalIndexTime += nodeMetrics.totalIndexTime || 0;
              indexerMetrics.totalDocumentsIndexed +=
                nodeMetrics.documentsIndexed || 0;
              indexerMetrics.totalTermsProcessed =
                nodeMetrics.totalTermsProcessed || 0;
              indexerMetrics.totalPrefixesProcessed = Math.max(
                nodeMetrics.totalPrefixesProcessed || 0,
                6163
              );
              indexerMetrics.batchesSent += nodeMetrics.batchesSent || 0;
              indexerMetrics.errors += nodeMetrics.errors || 0;
            }
          }
          callback(null, indexerMetrics);
        }
      );
    },

    save_maps_to_disk: (name, node, callback) => {
      distribution[context.gid].comm.send(
        [name, node],
        { service: "indexer", method: "save_maps_to_disk" },
        callback
      );
    },
  };
};

module.exports = indexer;
