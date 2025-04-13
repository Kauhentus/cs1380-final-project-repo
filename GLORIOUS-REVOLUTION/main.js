const { resolve } = require("path");
const distribution = require("./config.js");
const id = distribution.util.id;

const num_nodes = 8;
const spawn_nodes_locally = true;
const nodes = [];
// const nodes = [
//   { ip: "3.87.36.179", port: 8000 },
//   { ip: "54.205.32.141", port: 8000 },
//   { ip: "18.207.186.50", port: 8000 },
//   { ip: "3.89.92.113", port: 8000 },
//   { ip: "52.205.252.133", port: 8000 },
//   { ip: "44.201.146.230", port: 8000 },
//   { ip: "44.201.140.46", port: 8000 },
//   { ip: "3.83.105.244", port: 8000 },
// ];

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

for (let i = 0; i < num_nodes; i++) {
  if (spawn_nodes_locally) nodes.push({ ip: "127.0.0.1", port: 7110 + i });
  nids.push(id.getNID(nodes[i]));

  const sid = id.getSID(nodes[i]);
  crawler_group[sid] = nodes[i];
  indexer_group[sid] = nodes[i];
  indexer_ranged_group[sid] = nodes[i];
  querier_group[sid] = nodes[i];
}

console.log(nodes);

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

  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  // ##############
  // INITIALIZATION
  // ##############
  if (spawn_nodes_locally) {
    for (let i = 0; i < num_nodes; i++) await spawn_nx(nodes[i]);
  }

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
  const do_range_query = false;
  const query_string = "aquatic water seed";
  const range_query_string = "Anthozoa"; // Angiosperms, Rosids, Anthozoa

  const do_crawl_and_indexing = false;

  const headerLine = (text) => "#".repeat(text.length + 4);
  if (do_query) {
    await new Promise((resolve, reject) => {
      distribution.querier_group.querier.query_one(
        query_string,
        {},
        async (e, v) => {
          if (e) {
            console.error("Query failed:", e);
            return resolve();
          }

          console.log(headerLine("QUERY RESULTS"));
          console.log(`# QUERY: "${query_string}" #`);
          console.log(headerLine("QUERY RESULTS\n"));

          console.log(`Query: "${v.query}"`);
          console.log(`Terms searched for: ${v.terms.join(", ")}`);
          console.log(`Total results found: ${v.totalResults}\n`);

          if (!v.topResults || v.topResults.length === 0) {
            console.log("No results found for this query.");
            return resolve();
          }

          console.log(headerLine("TOP RESULTS"));
          console.log(`# TOP RESULTS #`);
          console.log(headerLine("TOP RESULTS\n"));
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
                `   * BOOSTED!!! Taxonomy match: ${result.termDetails.taxonomyLevel}`
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
                    console.log(headerLine(title));
                    console.log(`# ${title} #`);
                    console.log(`${headerLine(title)}\n`);

                    if (data.binomial_name)
                      console.log(`Binomial name: ${data.binomial_name}`);

                    if (data.hierarchy && data.hierarchy.length > 0) {
                      console.log("\nTaxonomic Classification:");
                      data.hierarchy.forEach((entry) => {
                        if (Array.isArray(entry) && entry.length === 2) {
                          console.log(
                            `  ${entry[0].padStart(8, " ")}: ${entry[1]}`
                          );
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

          console.log("");
          resolve();
        }
      );
    });
  }

  if (do_range_query) {
    await new Promise((resolve, reject) => {
      distribution.querier_group.querier.query_range(
        range_query_string,
        {},
        async (e, v) => {
          if (e) return resolve("Invalid range query string");
          const results = v;

          let headline = `RANGE QUERY: ${range_query_string} (${results.length} results)`;
          console.log(headerLine(headline));
          console.log(`# ${headline} #`);
          console.log(headerLine(headline), "\n");

          results
            .slice(0, 8)
            .map((result, i) =>
              console.log(`${`${i + 1}`.padStart(3, " ")}. ${result}`)
            );
          if (results.length > 8)
            console.log(`... and ${results.length - 10} more results`);

          resolve();
        }
      );
    });
  }

  if (do_crawl_and_indexing) {
    // ############################
    // START CRAWLING AND INDEXING
    // ############################
    await new Promise((resolve, reject) => {
      distribution.crawler_group.crawler.start_crawl((e, v) => resolve());
    });
    await new Promise((resolve, reject) => {
      distribution.crawler_group.indexer.start_index((e, v) => resolve());
    });
    await new Promise((resolve, reject) => {
      distribution.crawler_group.indexer_ranged.start_index((e, v) =>
        resolve()
      );
    });

    // ############################
    // LOGGING STATS
    // ############################
    const log_stats = () => {
      distribution.crawler_group.crawler.get_stats((e, v1) => {
        distribution.indexer_group.indexer.get_stats((e, v2) => {
          distribution.indexer_ranged_group.indexer_ranged.get_stats(
            (e, v3) => {
              let total_links_to_crawl = 0;
              let total_crawled_links = 0;
              let crawler_throughput = 0;
              Object.keys(v1).map((key) => {
                total_links_to_crawl += v1[key].links_to_crawl || 0;
                total_crawled_links += v1[key].crawled_links || 0;
                crawler_throughput +=
                  v1[key].metrics.crawling.pagesProcessed /
                    (v1[key].metrics.crawling.totalCrawlTime / 1000) || 0;
              });
              console.log(`CRAWLER_STATS:`);
              console.log(`  links_to_crawl = ${total_links_to_crawl}`);
              console.log(`  crawled_links = ${total_crawled_links}`);
              console.log(`  throughput = ${crawler_throughput} pages/sec`);
              console.log("");

              let total_links_to_index = 0;
              let total_indexed_links = 0;
              let indexer_throughput = 0;
              Object.keys(v2).map((key) => {
                total_links_to_index += v2[key].links_to_index || 0;
                total_indexed_links += v2[key].indexed_links || 0;
                indexer_throughput +=
                  v2[key].metrics.documentsIndexed /
                    (v2[key].metrics.totalIndexTime / 1000) || 0;
              });
              console.log(`INDEXER_STATS:`);
              console.log(`  links_to_index = ${total_links_to_index}`);
              console.log(`  indexed_links = ${total_indexed_links}`);
              console.log(`  throughput = ${indexer_throughput} pages/sec`);
              console.log("");

              let total_links_to_range_index = 0;
              let total_range_indexed_links = 0;
              let range_indexer_throughput = 0;
              Object.keys(v3).map((key) => {
                total_links_to_range_index += v3[key].links_to_range_index || 0;
                total_range_indexed_links += v3[key].range_indexed_links || 0;
                range_indexer_throughput +=
                  v3[key].metrics.documentsIndexed /
                    (v3[key].metrics.totalIndexTime / 1000) || 0;
              });
              console.log(`RANGE_INDEXER_STATS:`);
              console.log(
                `  links_to_range_index = ${total_links_to_range_index}`
              );
              console.log(
                `  range_indexed_links = ${total_range_indexed_links}`
              );
              console.log(
                `  throughput = ${range_indexer_throughput} pages/sec`
              );
              console.log("");
            }
          );
        });
      });
    };
    setInterval(() => log_stats(), 30000);
    log_stats();

    // ############################
    // QUERY STATS
    // ############################
    setTimeout(() => {
      setInterval(async () => {
        const avg = (...array) => array.reduce((a, b) => a + b) / array.length;
        const query_one = (query_string) =>
          new Promise((resolve, reject) => {
            const start_time = new Date();
            distribution.querier_group.querier.query_one(
              query_string,
              { no_trim: true },
              async (e, v) => {
                if (e) return resolve([0, 0]);
                const elapsed_time = new Date() - start_time;
                const num_results = v.topResults.length;
                resolve([elapsed_time, num_results]);
              }
            );
          });
        const [t1, n1] = await query_one("citrus");
        await sleep(100);
        const [t2, n2] = await query_one("leafy sour");
        await sleep(100);
        const [t3, n3] = await query_one("north water seed");
        await sleep(100);
        const avg_time = avg(t1, t2, t3);
        const avg_num_results = avg(n1, n2, n3);

        // const query_ranged = (query_string) => new Promise((resolve, reject) => {
        //   const start_time = new Date();
        //   distribution.querier_group.querier.query_range(query_string, {}, async (e, v) => {
        //     if(e) return resolve([0, 0]);
        //     const elapsed_time = new Date() - start_time;
        //     const num_results = v.length;
        //     resolve([elapsed_time, num_results]);
        //   });
        // });
        // const [t4, n4] = await query_ranged('Anthozoa');
        // await sleep(100);
        // const [t5, n5] = await query_ranged('Rosids');
        // await sleep(100);
        // const [t6, n6] = await query_ranged('Angiosperms');
        // await sleep(100);
        // const avg_time_ranged = avg(t4, t5, t6);
        // const avg_num_results_ranged = avg(n4, n5, n6);

        console.log(`QUERIER_STATS:`);
        console.log(`  query_one avg_time = ${avg_time} ms`);
        console.log(`  query_one avg_num_results = ${avg_num_results}`);
        // console.log(`  query_range avg_time = ${avg_time_ranged} ms`);
        // console.log(`  query_range avg_num_results = ${avg_num_results_ranged}`);
        console.log("");
      }, 120000);
    }, 15000);

    // ############################
    // SAVE MAPS TO DISK
    // ############################
    setInterval(async () => {
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
      await new Promise((resolve, reject) => {
        const remote = {
          gid: "local",
          service: "indexer_ranged",
          method: "save_maps_to_disk",
        };
        distribution.indexer_group.comm.send([], remote, (e, v) => {
          resolve();
        });
      });
    }, 60000);
  }

  // for(let i = 0; i < num_nodes; i++) await stop_nx(nodes[i]);
  // server.close();
});
