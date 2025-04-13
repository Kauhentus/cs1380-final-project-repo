// distribution/local/indexer-ranged.js
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};
let metrics = null;

function initialize(callback) {
  callback = callback || cb;
  const distribution = require('../../config');
  fs.appendFileSync(global.logging_path, `RANGED INDEXER INITIALIZING... ${new Date()}\n`);

  const crawlerDir = path.join('crawler-files');
  const metricsDir = path.join(crawlerDir, 'metrics');
  if (!fs.existsSync(crawlerDir)) fs.mkdirSync(crawlerDir, { recursive: true });
  if (!fs.existsSync(metricsDir)) fs.mkdirSync(metricsDir, { recursive: true });
  const metrics_file_path = path.join(metricsDir, `metrics-indexer-ranged-${global.nodeConfig.port}.json`);

  metrics = {
    totalIndexTime: 0,
    documentsIndexed: 0,
    
    current_time: Date.now(),
    time_since_previous: 0,
  };

  if (fs.existsSync(metrics_file_path)) {
    const old_metrics = JSON.parse(fs.readFileSync(metrics_file_path).toString());
    metrics = old_metrics.at(-1);
  } else {
    fs.writeFileSync(metrics_file_path, JSON.stringify([metrics], null, 2));
  }

  metricsInterval = setInterval(async () => {
    metrics.time_since_previous = Date.now() - metrics.current_time;
    metrics.current_time = Date.now();

    const old_metrics = JSON.parse(fs.readFileSync(metrics_file_path).toString());
    old_metrics.push(metrics);
    await fsp.writeFile(metrics_file_path, JSON.stringify(old_metrics, null, 2));

    distribution.indexer_ranged_group.store.clean_bulk_range_append((e, v) => {});
  }, 60000);

  const links_to_range_index_map = new Map();
  const range_indexed_links_map = new Map();

  distribution.local.mem.put(links_to_range_index_map, 'links_to_range_index_map', (e, v) => {
    distribution.local.mem.put(range_indexed_links_map, 'range_indexed_links_map', (e, v) => {

      distribution.local.store.get('links_to_range_index', (e1, v1) => {
        distribution.local.store.get('range_indexed_links', (e2, v2) => {

          if (!e1 && !e2 && typeof v1 === 'string' && typeof v2 === 'string') {
            const saved_links_to_range_index = v1.split('\n').filter(s => s.length > 0);
            const saved_range_indexed_links = v2.split('\n').filter(s => s.length > 0);
            saved_links_to_range_index.map(link => links_to_range_index_map.set(link, true));
            saved_range_indexed_links.map(link => range_indexed_links_map.set(link, true));
          }

          if(e1 && e2) {
            distribution.local.store.put('', 'links_to_range_index', (e, v) => {
              distribution.local.store.put('', 'range_indexed_links', (e, v) => {
                fs.appendFileSync(global.logging_path, `CREATED MAPS ${e} ${v}\n`);
              });
            });
          }

          callback(null, {
            status: 'success',
            message: 'Index service initialized',
            links_to_range_index: links_to_range_index_map.size,
            range_indexed_links: range_indexed_links_map.size
          });
        });
      });
    });
  });
}

function start_index(callback) {
  callback = callback || cb;

  const index_loop = async () => {
    try {
      while (true) {
        await new Promise((resolve, reject) => {
          distribution.local.indexer_ranged.index_one((e, v) => {
            if(e || ('status' in v && v.status !== 'success')) {
              setTimeout(() => {
                resolve();
              }, 1000);
            } else {
              resolve();
            }
          })
        });
      }
    } catch (err) {
      console.error('indexLoop failed:', err);
      setTimeout(index_loop, 1000);
    }
  }
  index_loop();

  callback(null, true);
}

function index_one(callback) {
  callback = callback || cb;
  const index_start_time = Date.now();

  function getChosenNode(key, nids, nodes) {
    const kid = distribution.util.id.getID(key);
    const chosenNID = distribution.util.id.naiveHash(kid, nids);
    const chosenNode = nodes.find((nc) => distribution.util.id.getNID(nc) === chosenNID);
    return chosenNode;
  }

  const fs = require('fs');
  fs.appendFileSync(global.logging_path, `RANGE INDEXING ONE...\n`);

  distribution.local.mem.get('links_to_range_index_map', (e1, links_to_range_index_map) => {
    distribution.local.mem.get('range_indexed_links_map', (e2, range_indexed_links_map) => {
      // ####################################
      // 0. DECIDE ON LINK TO INDEX
      // ####################################
      if (links_to_range_index_map.size === 0) {
        fs.appendFileSync(global.logging_path, `RANGE INDEXER SKIPPED\n`);
        return callback(null, { status: 'skipped', reason: 'no_links' });
      }
      const [url, _] = links_to_range_index_map.entries().next().value;
      links_to_range_index_map.delete(url);
      if (range_indexed_links_map.has(url)) {
        fs.appendFileSync(global.logging_path, `RANGE INDEXER SKIPPED\n`);
        return callback(null, { status: 'skipped', reason: 'already_indexed' });
      }
      range_indexed_links_map.set(url, true);

      fs.appendFileSync(global.logging_path, `RANGE INDEXER SELECTED: ${url}\n`);

      // ####################################
      // 1. FETCH DOCUMENT FROM CRAWLER STORE
      // ####################################

      distribution.crawler_group.store.get(url, (e, v) => {
        if(e) {
          fs.appendFileSync(global.logging_path, ` RANGE INDEXER ERROR: ${e}\n`);
          console.error(`Range indexer error: ${e}`);
          return callback(e, null);
        }
        const document = v;
        const hierarchy = document.hierarchy || [];
        const binomialName = document.binomial_name || '';
        const taxonomyInfo = hierarchy
          .filter(entry => Array.isArray(entry) && entry.length === 2)
          .map(([level, name]) => [level, name.split('\n')[0].replace(/[\p{Zs}\u200B\uFEFF\u200C\u200D]/gu, ' ')]);

        distribution.local.groups.get('indexer_ranged_group', async (e, v) => {
          const nodes = Object.values(v);
          const num_nodes = nodes.length;
          const nids = nodes.map(node => distribution.util.id.getNID(node));

          // 2. ADD TO INDEX
          const ranged_link_promises = [];
          for(let i = 0; i < taxonomyInfo.length - 1; i++) {
            const next_is_species = i === taxonomyInfo.length - 2;
            let [level, name] = taxonomyInfo[i];
            let [_, nextName] = taxonomyInfo[i + 1];
            nextName = next_is_species ? `[SPECIES] ${url}` : nextName;
            const chosen_node = getChosenNode(name, nids, nodes);

            ranged_link_promises.push(new Promise((resolve, reject) => {
              distribution.local.comm.send(
                [ level, name, nextName ], 
                { service: "store", method: "bulk_range_append", node: chosen_node }, 
                (err, val) => {
                  if(err) console.log(err, "HELP")
                  resolve();
                }
              );
            }));
          }
          await Promise.all(ranged_link_promises);

          metrics.documentsIndexed += 1;
          metrics.totalIndexTime += Date.now() - index_start_time;

          callback(null, { status: "not implemented" });
        });
      });
    });
  });
}

function add_link_to_index(link, callback) {
  callback = callback || cb;

  distribution.local.mem.get('links_to_range_index_map', (e1, links_to_range_index_map) => {
    distribution.local.mem.get('range_indexed_links_map', (e2, range_indexed_links_map) => {
      if (links_to_range_index_map.has(link)) return callback(null, { status: 'skipped', reason: 'already_in_queue' });
      if (range_indexed_links_map.has(link)) return callback(null, { status: 'skipped', reason: 'already_crawled' });

      links_to_range_index_map.set(link, true);
      callback(null, { status: 'success', message: 'Link added to crawl queue', link: link });
    });
  });
}

function get_stats(callback) {
  callback = callback || cb;

  distribution.local.mem.get('links_to_range_index_map', (e1, links_to_range_index_map) => {
    distribution.local.mem.get('range_indexed_links_map', (e2, range_indexed_links_map) => {

      const stats = {
        links_to_range_index: links_to_range_index_map.size,
        range_indexed_links: range_indexed_links_map.size,
        metrics: metrics
      };

      callback(null, stats);
    });
  });
}

function save_maps_to_disk(callback) {
  callback = callback || cb;

  distribution.local.mem.get('links_to_range_index_map', (e1, links_to_range_index_map) => {
    distribution.local.mem.get('range_indexed_links_map', (e2, range_indexed_links_map) => {
      const links_to_range_index_data = Array.from(links_to_range_index_map.keys()).join('\n');
      const range_indexed_links_data = Array.from(range_indexed_links_map.keys()).join('\n');

      distribution.local.store.put(links_to_range_index_data, 'links_to_range_index', (e, v) => {
        distribution.local.store.put(range_indexed_links_data, 'range_indexed_links', (e, v) => {
          callback(null, {
            status: 'success',
            links_to_range_index_saved: links_to_range_index_map.size,
            range_indexed_links_saved: range_indexed_links_map.size
          });
        });
      });
    });
  });
}

module.exports = {
  initialize,
  start_index,
  index_one,
  add_link_to_index,
  get_stats,
  save_maps_to_disk
};