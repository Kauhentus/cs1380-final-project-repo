const { resolve } = require("path");
const distribution = require("./config.js");
const id = distribution.util.id;

const num_nodes = 4;
const nodes = [];
//
const nids = [];
const crawler_group = {};
const crawler_group_config = { gid: "crawler_group", hash: id.naiveHash };
const indexer_group = {};
const indexer_group_config = { gid: "indexer_group", hash: id.naiveHash };
const indexer_ranged_group = {};
const indexer_ranged_group_config = {
  gid: "indexer_ranged_group",
  hash: id.naiveHash,
};
const querier_group = {};
const querier_group_config = { gid: "querier_group", hash: id.naiveHash };

// const nodes = [
//   { ip: "18.191.20.248", port: 1234 },
//   { ip: "3.141.28.230", port: 1234 },
//   { ip: "18.119.165.204", port: 1234 },
//   //   { ip: "18.116.47.118", port: 1234 },
//   //   { ip: "18.216.71.206", port: 1234 },
//   //   { ip: "3.147.54.10", port: 1234 },
//   //   { ip: "18.119.110.33", port: 1234 },
//   //   { ip: "3.145.75.154", port: 1234 },
// ];

for (let i = 0; i < num_nodes; i++) {
  nodes.push({ ip: "127.0.0.1", port: 7110 + i });
  nids.push(id.getNID(nodes[i]));

  const sid = id.getSID(nodes[i]);
  crawler_group[sid] = nodes[i];
  indexer_group[sid] = nodes[i];
  indexer_ranged_group[sid] = nodes[i];
  querier_group[sid] = nodes[i];
}

distribution.node.start(async (server) => {
  const spawn_nx = (nx) =>
    new Promise((resolve, reject) => {
      distribution.local.status.spawn(nx, (e, v) => {
        resolve(e, v);
      });
    });

  const stop_nx = (nx) =>
    new Promise((resolve, reject) => {
      distribution.local.comm.send(
        [],
        { service: "status", method: "stop", node: nx },
        (e, v) => {
          resolve(e, v);
        }
      );
    });

  const get_nx = (link) =>
    nodes[parseInt(id.getID(link).slice(0, 8), 16) % num_nodes];

  for (let i = 0; i < num_nodes; i++) await spawn_nx(nodes[i]);

  // ##############
  // INITIALIZATION
  // ##############
  const healthCheack = (group) => {
    return new Promise((resolve, reject) => {
      distribution[group].comm.send(
        ["nid"],
        { service: "status", method: "get" },
        (e, v) => {
          console.log(`HEALTH CHECK: ${group} - ${JSON.stringify(v)}`);
          resolve(true);
        }
      );
    });
  };
  const init_group = (group, config) =>
    new Promise((resolve, reject) => {
      distribution.local.groups.put(config, group, (e, v) => {
        distribution[config.gid].groups.put(config, group, (e, v) => {
          resolve();
        });
      });
    });
  await init_group(crawler_group, crawler_group_config);
  await init_group(indexer_group, indexer_group_config);
  await init_group(indexer_ranged_group, indexer_ranged_group_config);
  await init_group(querier_group, querier_group_config);
  console.log("GROUPS CREATED");
  await healthCheack("crawler_group");
  await healthCheack("indexer_group");
  await healthCheack("indexer_ranged_group");
  await healthCheack("querier_group");
  console.log("GROUPS HEALTH CHECKED");

  const run_remote = (group_name, remote, args = []) =>
    new Promise((resolve, reject) => {
      distribution[group_name].comm.send(args, remote, (e, v) => {
        resolve();
      });
    });
  await run_remote("crawler_group", {
    gid: "local",
    service: "crawler",
    method: "initialize",
  });
  await run_remote("indexer_group", {
    gid: "local",
    service: "indexer",
    method: "initialize",
  });
  await run_remote("indexer_ranged_group", {
    gid: "local",
    service: "indexer_ranged",
    method: "initialize",
  });
  await run_remote("querier_group", {
    gid: "local",
    service: "querier",
    method: "initialize",
  });
  console.log("GROUPS INITIALIZED");

  // #######################
  // POPULATE WITH MISC INFO
  // #######################
  await new Promise((resolve, reject) => {
    const link = "/wiki/Cnidaria";
    console.log(
      `Adding link to crawl: ${link} to node ${JSON.stringify(get_nx(link))}`
    );
    const remote = {
      node: get_nx(link),
      gid: "local",
      service: "crawler",
      method: "add_link_to_crawl",
    };
    distribution.local.comm.send([link], remote, (e, v) => {
      const remote = {
        node: get_nx(link),
        gid: "local",
        service: "crawler",
        method: "save_maps_to_disk",
      };
      distribution.local.comm.send([], remote, (e, v) => {
        resolve();
      });
    });
  });

  // ######################
  // MANUAL CONTROL PANEL
  // ######################
  const do_query = true;
  const query_string = "citrus";

  const do_crawl_and_indexing = false;

  if (do_query) {
    await new Promise((resolve, reject) => {
      distribution.querier_group.querier.query_one(
        query_string,
        async (e, v) => {
          if (e) {
            console.error("Query failed:", e);
            return resolve();
          }

          console.log("\n=== SEARCH METADATA ===");
          console.log(`Query: "${v.query}"`);
          console.log(`Terms searched for: ${v.terms.join(", ")}`);
          console.log(`Total results found: ${v.totalResults}\n`);

          if (!v.topResults || v.topResults.length === 0) {
            console.log("No results found for this query.");
            return resolve();
          }

          console.log("Top Results Summary:");
          console.log("--------------------");
          v.topResults.slice(0, 5).forEach((result, index) => {
            // console.log(result);
            const pageInfo = result.termDetails?.pageInfo || {};
            console.log(
              `${index + 1}. ${
                pageInfo.binomialName.toUpperCase() || result.docId
              }`
            );
            console.log(`   URL: https://www.wikipedia.org/${result.docId}`);
            console.log(
              `   Score: ${result.score.toFixed(4)} (matched ${
                result.matchedTerms
              }/${v.terms.length} terms)`
            );
            console.log(`   Kingdom: ${pageInfo.kingdom || "Unknown"}`);
            console.log(`   Family: ${pageInfo.family || "Unknown"}`);

            if (result.termDetails?.taxonomyLevel) {
              console.log(
                `  * BOOSTED!!! Taxonomy match: ${result.termDetails.taxonomyLevel}`
              );
            }

            if (result.termDetails?.isBinomial) {
              console.log(`   * BOOSTED!!! Term appears in binomial name`);
            }
            console.log("");
          });

          if (v.topResults.length > 0) {
            // TODO: do we want to have displayed results for all of them?
            const topResult = v.topResults[0];
            try {
              await new Promise((resolve2, reject2) => {
                distribution.crawler_group.store.get(
                  topResult.docId,
                  (err, data) => {
                    if (err) {
                      console.error(
                        "Error fetching detailed information:",
                        err
                      );
                      return resolve2();
                    }

                    if (!data) {
                      console.log(
                        "No detailed information available for this result."
                      );
                      return resolve2();
                    }

                    const title = data.title || "Unknown Title";
                    const headerLine = "#".repeat(title.length + 4);

                    console.log(headerLine);
                    console.log(`# ${title} #`);
                    console.log(`${headerLine}\n`);

                    if (data.binomial_name) {
                      console.log(`Scientific name: ${data.binomial_name}`);
                    }

                    if (data.hierarchy && data.hierarchy.length > 0) {
                      console.log("\nTaxonomic Classification:");
                      data.hierarchy.forEach((entry) => {
                        if (Array.isArray(entry) && entry.length === 2) {
                          console.log(`  ${entry[0]}: ${entry[1]}`);
                        }
                      });
                    }

                    if (data.description) {
                      console.log("\nDescription:");
                      console.log(data.description);
                    }

                    resolve2();
                  }
                );
              });
            } catch (detailError) {
              console.error(
                "Error processing detailed information:",
                detailError
              );
            }
          }

          resolve();
        }
      );
    });
  }

  if (do_crawl_and_indexing) {
    let max_iter = 10000;
    let crawl_loop_iters = 0;
    let index_loop_iters = 0;

    const crawl_one = () =>
      new Promise((resolve, reject) => {
        const remote = {
          gid: "local",
          service: "crawler",
          method: "crawl_one",
        };
        distribution.crawler_group.comm.send([], remote, (e, v) => {
          crawl_loop_iters++;
          resolve();
        });
      });
    const crawl_loop = async () => {
      try {
        while (crawl_loop_iters < max_iter) {
          if (crawl_loop_iters % 10 == 0)
            console.log(`crawler ${crawl_loop_iters}`);
          await crawl_one();
        }
      } catch (err) {
        console.error("crawlLoop failed:", err);
        setTimeout(crawl_loop, 10000);
      }
    };

    const index_one = () =>
      new Promise((resolve, reject) => {
        setTimeout(() => {
          const remote = {
            gid: "local",
            service: "indexer",
            method: "index_one",
          };
          distribution.indexer_group.comm.send([], remote, (e, v) => {
            if (Object.values(v).some((data) => data.status !== "skipped"))
              index_loop_iters++;
            resolve();
          });
        }, 100);
      });
    const index_loop = async () => {
      try {
        while (index_loop_iters < max_iter) {
          if (index_loop_iters % 10 == 0)
            console.log(`indexer ${index_loop_iters}`);
          await index_one();
        }
      } catch (err) {
        console.error("indexLoop failed:", err);
        setTimeout(index_loop, 1000);
      }
    };

    crawl_loop();
    index_loop();

    setTimeout(async () => {
      await new Promise((resolve, reject) => {
        const remote = {
          gid: "local",
          service: "crawler",
          method: "save_maps_to_disk",
        };
        distribution.indexer_group.comm.send([], remote, (e, v) => {
          resolve();
        });
      });
      await new Promise((resolve, reject) => {
        const remote = {
          gid: "local",
          service: "indexer",
          method: "save_maps_to_disk",
        };
        distribution.indexer_group.comm.send([], remote, (e, v) => {
          resolve();
        });
      });
    }, 30000);
  }

  //   for (let i = 0; i < num_nodes; i++) await stop_nx(nodes[i]);
  server.close();
});
