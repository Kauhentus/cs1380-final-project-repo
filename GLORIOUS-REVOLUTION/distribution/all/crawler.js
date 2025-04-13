const { node } = require("@brown-ds/distribution");

const crawler = function (config) {
  const context = {};
  context.gid = config.gid || "all";

  return {
    initialize: (config, group, callback) => {
      distribution[context.gid].comm.send(
        [config, group],
        { service: "crawler", method: "initialize" },
        callback
      );
    },

    start_crawl: (callback) => {
      distribution[context.gid].comm.send(
        [],
        { service: "crawler", method: "start_crawl" },
        callback
      );
    },

    add_link_to_crawl: (name, callback) => {
      distribution[context.gid].comm.send(
        [name],
        { service: "crawler", method: "add_link_to_crawl" },
        callback
      );
    },

    crawl_one: (name, callback) => {
      distribution[context.gid].comm.send(
        [name],
        { service: "crawler", method: "crawl_one" },
        callback
      );
    },

    get_stats: (callback) => {
      distribution[context.gid].comm.send(
        [],
        { service: "crawler", method: "get_stats" },
        (errMap, resMap) => {
          const crawlerMetrics = {
            totalCrawlTime: 0,
            pagesProcessed: 0,
            termsExtracted: 0,
            targetsHit: 0,
          };
          for (const key in resMap) {
            const nodeMetrics = resMap[key].metrics;
            if (nodeMetrics) {
              crawlerMetrics.totalCrawlTime += nodeMetrics.totalCrawlTime || 0;
              crawlerMetrics.pagesProcessed += nodeMetrics.pagesProcessed || 0;
              crawlerMetrics.termsExtracted += nodeMetrics.termsExtracted || 0;
              crawlerMetrics.targetsHit += nodeMetrics.targetsHit || 0;
              crawlerMetrics.errors += nodeMetrics.errors || 0;
            }
          }
          callback(null, crawlerMetrics);
        }
      );
    },

    save_maps_to_disk: (name, node, callback) => {
      distribution[context.gid].comm.send(
        [name, node],
        { service: "crawler", method: "save_maps_to_disk" },
        callback
      );
    },
  };
};

module.exports = crawler;
