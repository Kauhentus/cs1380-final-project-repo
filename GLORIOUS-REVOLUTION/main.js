const { resolve } = require('path');
const fs = require('fs');
const distribution = require('./config.js');
const id = distribution.util.id;
const chalk = require('chalk');

const spawn_nodes_locally = true;
const num_nodes = 8;
const nodes = spawn_nodes_locally ? [] : [
    { ip: '3.87.36.179', port: 8000 },
    { ip: '54.205.32.141', port: 8000 },
    { ip: '18.207.186.50', port: 8000 },
    { ip: '3.89.92.113', port: 8000 },
    { ip: '52.205.252.133', port: 8000 },
    { ip: '44.201.146.230', port: 8000 },
    { ip: '44.201.140.46', port: 8000 },
    { ip: '3.83.105.244', port: 8000 }
];

const nids = [];
const crawler_group = {};
const crawler_group_config = { gid: 'crawler_group', hash: id.naiveHash };
const indexer_group = {};
const indexer_group_config = { gid: 'indexer_group', hash: id.naiveHash };
const indexer_ranged_group = {};
const indexer_ranged_group_config = { gid: 'indexer_ranged_group', hash: id.naiveHash };
const querier_group = {};
const querier_group_config = { gid: 'querier_group', hash: id.naiveHash };

const log_and_append = (string) => {
  console.log(string);
  fs.appendFileSync('log.txt', string + '\n');
}

for (let i = 0; i < num_nodes; i++) {
  if(spawn_nodes_locally) nodes.push({ ip: '127.0.0.1', port: 7110 + i });
  nids.push(id.getNID(nodes[i]));

  const sid = id.getSID(nodes[i]);
  crawler_group[sid] = nodes[i];
  indexer_group[sid] = nodes[i];
  indexer_ranged_group[sid] = nodes[i];
  querier_group[sid] = nodes[i];
}

distribution.node.start(async (server) => {
  const spawn_nx = (nx) => new Promise((resolve, reject) => {
    distribution.local.status.spawn(nx, (e, v) => {
      resolve(e, v);
    });
  });

  const stop_nx = (nx) => new Promise((resolve, reject) => {
    distribution.local.comm.send([], { service: 'status', method: 'stop', node: nx }, (e, v) => {
      resolve(e, v);
    });
  });

  const get_nx = (link) => nodes[parseInt(id.getID(link).slice(0, 8), 16) % num_nodes];

  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

  // ##############
  // INITIALIZATION
  // ##############
  if (spawn_nodes_locally) {
    for (let i = 0; i < num_nodes; i++) await spawn_nx(nodes[i]);
  }

  const init_group = (group, config) => new Promise((resolve, reject) => {
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


  const run_remote = (group_name, remote, args = []) => new Promise((resolve, reject) => {
    distribution[group_name].comm.send(args, remote, (e, v) => {
      resolve();
    });
  });
  await run_remote('crawler_group', { gid: 'local', service: 'crawler', method: 'initialize' });
  await run_remote('indexer_group', { gid: 'local', service: 'indexer', method: 'initialize' });
  await run_remote('indexer_ranged_group', { gid: 'local', service: 'indexer_ranged', method: 'initialize' });
  await run_remote('querier_group', { gid: 'local', service: 'querier', method: 'initialize' });
  console.log("GROUPS INITIALIZED");

  // #######################
  // POPULATE WITH MISC INFO
  // #######################
  await new Promise((resolve, reject) => {
    const link = '/wiki/Cnidaria';
    const remote = { node: get_nx(link), gid: 'local', service: 'crawler', method: 'add_link_to_crawl' };
    distribution.local.comm.send([link], remote, (e, v) => {

      const remote = { node: get_nx(link), gid: 'local', service: 'crawler', method: 'save_maps_to_disk' }
      distribution.local.comm.send([], remote, (e, v) => {
        resolve();
      });
    });
  });

  // ######################
  // MANUAL CONTROL PANEL 
  // ######################
  const do_query = false;
  const query_string = 'aquatic water seed';

  const do_range_query = false;
  const range_query_string = 'plantae'; // Angiosperms, Rosids, Anthozoa, cnidaria, haliclystidae
  const range_query_tree = true;

  const do_crawl_and_indexing = true;

  const headerLine = (text) => "#".repeat(text.length + 4);
  if (do_query) {
    await new Promise((resolve, reject) => {
      distribution.querier_group.querier.query_one(
        query_string, {},
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
              `${index + 1}. ${pageInfo.binomialName.toUpperCase() || result.docId
              }`
            );
            console.log(`   URL: https://www.wikipedia.org/${result.docId}`);
            console.log(
              `   Score: ${result.score.toFixed(4)} (matched ${result.matchedTerms
              }/${v.terms.length} terms)`
            );
            console.log(`   Kingdom: ${pageInfo.kingdom || "Unknown"}`);
            console.log(`   Family: ${pageInfo.family || "Unknown"}`);

            if (result.termDetails?.taxonomyLevel) {
              console.log(`   * BOOSTED!!! Taxonomy match: ${result.termDetails.taxonomyLevel}`);
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

                    if (data.binomial_name) console.log(`Binomial name: ${data.binomial_name}`);

                    if (data.hierarchy && data.hierarchy.length > 0) {
                      console.log("\nTaxonomic Classification:");
                      data.hierarchy.forEach((entry) => {
                        if (Array.isArray(entry) && entry.length === 2) {
                          console.log(`  ${entry[0].padStart(8, ' ')}: ${entry[1]}`);
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
          
          console.log('');
          resolve();
        }
      );
    });
  }

  if (do_range_query) {
    await new Promise((resolve, reject) => {
      const start_time = Date.now();
      distribution.querier_group.querier.query_range(range_query_string, { return_tree: range_query_tree }, async (e, v) => {
        if(e) return resolve('Invalid range query string');
        const results = v;

        if(range_query_tree){
          const collapse_species = false;
          let num_species_encountered = 0;
          let num_taxa_encountered = 0;
          const recursive_print = (node, depth, is_last_child = true, prefix = '') => {
            const is_species = node.is_species;
            const has_children = node.children && node.children.length > 0;
            const num_species_children = has_children ? node.children.filter(child => child.is_species).length : 0;
            const name_formatted = (is_species ? chalk.ansi256(219)('*') + `${node.name.replace('[SPECIES] /wiki/', ' ')}` : node.name) + (collapse_species && num_species_children > 0 ? ` (${num_species_children})` : '')
            const name_formatted_color = is_species ? chalk.green(name_formatted) : chalk.ansi256(191)(name_formatted);

            if (is_species) num_species_encountered++;
            else num_taxa_encountered++;

            const branch_ansi = chalk.ansi256(180);

            if(depth == 0) console.log(' '.repeat(depth * 2) + name_formatted_color);
            else console.log(prefix + (is_last_child ? branch_ansi('└─') : branch_ansi('├─')) + name_formatted_color);

            const new_prefix = prefix + (is_last_child ? '   ' : branch_ansi('│  '));

            if (node.children) {
              node.children.sort((a, b) => {
                if (a.is_species && !b.is_species) return 1;
                if (!a.is_species && b.is_species) return -1;
                return a.name.localeCompare(b.name);
              });
              node.children.map((child, i) => {
                if(collapse_species && child.is_species) return;
                recursive_print(child, depth + 1, i === node.children.length - 1, new_prefix)
              });
            }
          }
          recursive_print(results, 0);
          console.log('');
          console.log(`Found ${num_taxa_encountered} taxa and ${num_species_encountered} species!`);
          console.log(`(${num_taxa_encountered + num_species_encountered} results in ${Date.now() - start_time}ms)`)
        }

        else {
          let headline = `RANGE QUERY: ${range_query_string} (${results.length} results)`;
          console.log(headerLine(headline));
          console.log(`# ${headline} #`);
          console.log(headerLine(headline), '\n');
  
          results.slice(0, 8).map((result, i) => console.log(`${`${i + 1}`.padStart(3, ' ')}. ${result}`));
          if (results.length > 8) console.log(`... and ${results.length - 10} more results`);
  
        }

        resolve();
      });
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
      distribution.crawler_group.indexer_ranged.start_index((e, v) => resolve());
    });

    // ############################
    // LOGGING & QUERY STATS
    // ############################
    const log_core_stats = () => new Promise(async (resolve, reject) => {
      distribution.crawler_group.crawler.get_stats((e, v1) => {
        distribution.indexer_group.indexer.get_stats((e, v2) => {
          distribution.indexer_ranged_group.indexer_ranged.get_stats((e, v3) => {
            let total_links_to_crawl = 0;
            let total_crawled_links = 0;
            let crawler_throughput = 0;
            Object.keys(v1).map(key => {
              total_links_to_crawl += v1[key].links_to_crawl || 0;
              total_crawled_links += v1[key].crawled_links || 0;
              crawler_throughput += (v1[key].metrics.crawling.pagesProcessed / (v1[key].metrics.crawling.totalCrawlTime / 1000)) || 0;
            });
            log_and_append(`CRAWLER_STATS:`);
            log_and_append(`  links_to_crawl = ${total_links_to_crawl}`);
            log_and_append(`  crawled_links = ${total_crawled_links}`);
            log_and_append(`  throughput = ${crawler_throughput} pages/sec`);
            log_and_append('');

            let total_links_to_index = 0;
            let total_indexed_links = 0;
            let indexer_throughput = 0;
            Object.keys(v2).map(key => {
              total_links_to_index += v2[key].links_to_index || 0;
              total_indexed_links += v2[key].indexed_links || 0;
              indexer_throughput += (v2[key].metrics.documentsIndexed / (v2[key].metrics.totalIndexTime / 1000)) || 0;
            });
            log_and_append(`INDEXER_STATS:`);
            log_and_append(`  links_to_index = ${total_links_to_index}`);
            log_and_append(`  indexed_links = ${total_indexed_links}`);
            log_and_append(`  throughput = ${indexer_throughput} pages/sec`);
            log_and_append('');

            let total_links_to_range_index = 0;
            let total_range_indexed_links = 0;
            let range_indexer_throughput = 0;
            Object.keys(v3).map(key => {
              total_links_to_range_index += v3[key].links_to_range_index || 0;
              total_range_indexed_links += v3[key].range_indexed_links || 0;
              range_indexer_throughput += (v3[key].metrics.documentsIndexed / (v3[key].metrics.totalIndexTime / 1000)) || 0;
            });
            log_and_append(`RANGE_INDEXER_STATS:`);
            log_and_append(`  links_to_range_index = ${total_links_to_range_index}`);
            log_and_append(`  range_indexed_links = ${total_range_indexed_links}`);
            log_and_append(`  throughput = ${range_indexer_throughput} pages/sec`);
            log_and_append('');

            resolve();
          });
        });
      });
    });

    const log_query_stats = () => new Promise(async (resolve, reject) => {
      const avg = (...array) => array.reduce((a, b) => a + b) / array.length;
      const query_one = (query_string) => new Promise((resolve, reject) => {
        const start_time = new Date();
        distribution.querier_group.querier.query_one(query_string, { no_trim: true }, async (e, v) => {
          if(e) return resolve([0, 0]);
          const elapsed_time = new Date() - start_time;
          const num_results = v.topResults.length;
          resolve([elapsed_time, num_results]);
        });
      });
      const [t1, n1] = await query_one('citrus');
      await sleep(1000);
      const [t2, n2] = await query_one('leafy sour');
      await sleep(1000);
      const [t3, n3] = await query_one('north water seed');
      await sleep(1000);
      const avg_time = avg(t1, t2, t3);
      const avg_num_results = avg(n1, n2, n3);

      // await sleep(3000);

      // const query_ranged = (query_string) => new Promise((resolve, reject) => {
      //   const start_time = new Date();
      //   distribution.querier_group.querier.query_range(query_string, { return_tree: false }, async (e, v) => {
      //     if(e) return resolve([0, 0]);
      //     const elapsed_time = new Date() - start_time;
      //     const num_results = v.length;
      //     resolve([elapsed_time, num_results]);
      //   });
      // });
      // const [t4, n4] = await query_ranged('Anthozoa');
      // await sleep(1000);
      // const [t5, n5] = await query_ranged('Rosids');
      // await sleep(1000);
      // const [t6, n6] = await query_ranged('Angiosperms');
      // await sleep(1000);
      // const avg_time_ranged = avg(t4, t5, t6);
      // const avg_num_results_ranged = avg(n4, n5, n6);

      log_and_append(`QUERIER_STATS:`);
      log_and_append(`  query_one avg_time = ${avg_time} ms`);
      log_and_append(`  query_one avg_num_results = ${avg_num_results}`);
      // log_and_append(`  query_range avg_time = ${avg_time_ranged} ms`);
      // log_and_append(`  query_range avg_num_results = ${avg_num_results_ranged}`);
      log_and_append('');

      resolve();
    });

    const main_metric_loop = async () => {
      console.log("PAUSING CORE SERVICES...\n");
      const t1 = Date.now();
      await new Promise((resolve, reject) => {
        distribution.crawler_group.crawler.set_service_state(true, (e, v) => {
          distribution.indexer_group.indexer.set_service_state(true, (e, v) => {
            distribution.indexer_ranged_group.indexer_ranged.set_service_state(true, (e, v) => {
              resolve();
            });
          });
        });
      });
      const t2 = Date.now();
      // console.log(`  (PAUSED CORE SERVICES IN ${t2 - t1}ms)`);
      log_and_append(`RECOVERY TIME FOR CORE SERVICES: ${t2 - t1}ms`);

      await sleep(1000);

      await log_core_stats();
      const t3 = Date.now();
      console.log(`  (LOGGED CORE STATS IN ${t3 - t2}ms)`);

      await sleep(1000);

      await log_query_stats();
      const t4 = Date.now();
      console.log(`  (LOGGED QUERY STATS IN ${t4 - t3}ms)`);

      await sleep(1000);

      console.log("RESUMING CORE SERVICES...\n");
      const t5 = Date.now();
      await new Promise((resolve, reject) => {
        distribution.crawler_group.crawler.set_service_state(false, (e, v) => {
          distribution.indexer_group.indexer.set_service_state(false, (e, v) => {
            distribution.indexer_ranged_group.indexer_ranged.set_service_state(false, (e, v) => {
              resolve();
            });
          });
        });
      });
      const t6 = Date.now();
      console.log(`  (RESUMED CORE SERVICES IN ${t6 - t5}ms)`);

      setTimeout(() => main_metric_loop(), 120000);
    }
    // setInterval(main_metric_loop, 120000);
    setTimeout(() => main_metric_loop(), 3000);

    // ############################
    // SAVE MAPS TO DISK
    // ############################
    setInterval(async () => {
      await new Promise((resolve, reject) => {
        const remote = { gid: 'local', service: 'crawler', method: 'save_maps_to_disk' }
        distribution.indexer_group.comm.send([], remote, (e, v) => {
          resolve();
        });
      });
      await new Promise((resolve, reject) => {
        const remote = { gid: 'local', service: 'indexer', method: 'save_maps_to_disk' }
        distribution.indexer_group.comm.send([], remote, (e, v) => {
          resolve();
        });
      });
      await new Promise((resolve, reject) => {
        const remote = { gid: 'local', service: 'indexer_ranged', method: 'save_maps_to_disk' }
        distribution.indexer_group.comm.send([], remote, (e, v) => {
          resolve();
        });
      });
    }, 60000);
  }

  // for(let i = 0; i < num_nodes; i++) await stop_nx(nodes[i]);
  // server.close();
});