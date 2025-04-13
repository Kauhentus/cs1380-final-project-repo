// distribution/local/indexer.js
const fs = require("fs");
const fsp = require("fs/promises");
const lf = require("proper-lockfile");
const path = require("path");

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    if (log_index) console.log(v);
  }
};

let metrics = null;
let metricsInterval = null;
const log_index = false;

function initialize(callback) {
  callback = callback || cb;
  const distribution = require("../../config");
  fs.appendFileSync(
    global.logging_path,
    `INDEXER INITIALIZING... ${new Date()}\n`
  );

  const crawlerDir = path.join("crawler-files");
  const metricsDir = path.join(crawlerDir, "metrics");
  if (!fs.existsSync(crawlerDir)) fs.mkdirSync(crawlerDir, { recursive: true });
  if (!fs.existsSync(metricsDir)) fs.mkdirSync(metricsDir, { recursive: true });
  const metrics_file_path = path.join(
    metricsDir,
    `metrics-indexer-${global.nodeConfig.port}.json`
  );

  metrics = {
    totalIndexTime: 0,
    documentsIndexed: 0,
    totalTermsProcessed: 0,
    totalPrefixesProcessed: 0,
    batchesSent: 0,
    errors: 0,

    processing_times: [],
    current_time: Date.now(),
    time_since_previous: 0,
  };

  if (fs.existsSync(metrics_file_path)) {
    const old_metrics = JSON.parse(
      fs.readFileSync(metrics_file_path).toString()
    );
    metrics = old_metrics.at(-1);
  } else {
    fs.writeFileSync(metrics_file_path, JSON.stringify([metrics], null, 2));
  }

  metricsInterval = setInterval(async () => {
    metrics.time_since_previous = Date.now() - metrics.current_time;
    metrics.current_time = Date.now();

    const old_metrics = JSON.parse(
      fs.readFileSync(metrics_file_path).toString()
    );
    old_metrics.push(metrics);
    await fsp.writeFile(
      metrics_file_path,
      JSON.stringify(old_metrics, null, 2)
    );

    metrics.processing_times = [];
  }, 60000);

  const links_to_index_map = new Map();
  const indexed_links_map = new Map();

  distribution.local.mem.put(
    links_to_index_map,
    "links_to_index_map",
    (e, v) => {
      distribution.local.mem.put(
        indexed_links_map,
        "indexed_links_map",
        (e, v) => {
          distribution.local.store.get("links_to_index", (e1, v1) => {
            distribution.local.store.get("indexed_links", (e2, v2) => {
              if (
                !e1 &&
                !e2 &&
                typeof v1 === "string" &&
                typeof v2 === "string"
              ) {
                const saved_links_to_index = v1
                  .split("\n")
                  .filter((s) => s.length > 0);
                const saved_indexed_links = v2
                  .split("\n")
                  .filter((s) => s.length > 0);
                saved_links_to_index.map((link) =>
                  links_to_index_map.set(link, true)
                );
                saved_indexed_links.map((link) =>
                  indexed_links_map.set(link, true)
                );
              }

              if (e1 && e2) {
                distribution.local.store.put("", "links_to_index", (e, v) => {
                  distribution.local.store.put("", "indexed_links", (e, v) => {
                    fs.appendFileSync(
                      global.logging_path,
                      `CREATED INDEX MAPS ${e} ${v}\n`
                    );
                  });
                });
              }

              callback(null, {
                status: "success",
                message: "Index service initialized",
                links_to_index: links_to_index_map.size,
                indexed_links: indexed_links_map.size,
              });
            });
          });
        }
      );
    }
  );
}

function add_link_to_index(link, callback) {
  callback = callback || cb;

  distribution.local.mem.get("links_to_index_map", (e1, links_to_index_map) => {
    distribution.local.mem.get("indexed_links_map", (e2, indexed_links_map) => {
      if (links_to_index_map.has(link))
        return callback(null, {
          status: "skipped",
          reason: "already_in_queue",
        });
      if (indexed_links_map.has(link))
        return callback(null, { status: "skipped", reason: "already_crawled" });

      links_to_index_map.set(link, true);
      callback(null, {
        status: "success",
        message: "Link added to crawl queue",
        link: link,
      });
    });
  });
}

function index_one(callback) {
  callback = callback || cb;
  const index_start_time = Date.now();

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

  const fs = require("fs");
  fs.appendFileSync(global.logging_path, `INDEXING ONE...\n`);

  distribution.local.mem.get("links_to_index_map", (e1, links_to_index_map) => {
    distribution.local.mem.get("indexed_links_map", (e2, indexed_links_map) => {
      // ####################################
      // 0. DECIDE ON LINK TO INDEX
      // ####################################
      if (links_to_index_map.size === 0) {
        fs.appendFileSync(global.logging_path, `INDEXER SKIPPED\n`);
        return callback(null, { status: "skipped", reason: "no_links" });
      }
      const [url, _] = links_to_index_map.entries().next().value;
      links_to_index_map.delete(url);
      if (indexed_links_map.has(url)) {
        fs.appendFileSync(global.logging_path, `INDEXER SKIPPED\n`);
        return callback(null, { status: "skipped", reason: "already_indexed" });
      }
      indexed_links_map.set(url, true);

      fs.appendFileSync(global.logging_path, `INDEXER SELECTED: ${url}\n`);

      // ####################################
      // 1. FETCH DOCUMENT FROM CRAWLER STORE
      // ####################################

      distribution.crawler_group.store.get(url, (e, v) => {
        // fs.appendFileSync(global.logging_path, `   Indexer got: ${url} ${e} ${Object.keys(v)}\n`);
        if (e) {
          fs.appendFileSync(global.logging_path, ` INDEXER ERROR: ${e}\n`);
          console.error(`Indexer error: ${e}`);
          return callback(e, null);
        }
        const document = v;
        const docId = document.url;
        const wordCounts = document.word_counts
          ? new Map(Object.entries(document.word_counts))
          : new Map();
        const totalWords = wordCounts.size;

        const hierarchy = document.hierarchy || [];
        const binomialName = document.binomial_name || "";
        const taxonomyInfo = Object.fromEntries(
          hierarchy.filter(
            (entry) => Array.isArray(entry) && entry.length === 2
          )
        );
        const kingdom = taxonomyInfo["kingdom"] || "";
        const family = taxonomyInfo["family"] || "";

        distribution.local.groups.get("indexer_group", async (e, v) => {
          const nodes = Object.values(v);
          const num_nodes = nodes.length;
          const nids = nodes.map((node) => distribution.util.id.getNID(node));

          // ####################################
          // 2. COMPUTE NODE -> PREFIX and PREFIX -> TERMS MAPS
          // ####################################

          const prefixGroups = new Map(); // prefix -> terms
          const nodeToPrefix = new Map(); // node -> prefixes

          for (const [word, count] of wordCounts) {
            const prefix = getSmartPrefix(word);
            if (!prefixGroups.has(prefix)) {
              prefixGroups.set(prefix, new Map());
            }
            prefixGroups.get(prefix).set(word, count);
          }

          for (const [prefix, terms] of prefixGroups) {
            const chosenNode = getChosenNode(prefix, nids, nodes);
            if (!nodeToPrefix.has(chosenNode)) {
              nodeToPrefix.set(chosenNode, new Map());
            }
            nodeToPrefix.get(chosenNode).set(prefix, terms);
          }

          const timecheck_1 = Date.now();

          // fs.appendFileSync(global.logging_path, `   Indexer prefix groups: ${JSON.stringify(Object.fromEntries(prefixGroups))}\n`);
          // fs.appendFileSync(global.logging_path, `   Indexer node to prefix: ${JSON.stringify(Object.fromEntries(nodeToPrefix))}\n`);

          // ####################################
          // 3. BEGIN BATCHING PREP
          // ####################################

          let totalBatches = 0;
          for (const [node, prefixes] of nodeToPrefix) {
            if (prefixes.size > 0) totalBatches++;
          }
          if (totalBatches === 0) {
            return callback(null, {
              status: "success",
              docId: docId,
            });
          }

          // ####################################
          // 4. CREATE BATCHES
          // ####################################

          let node_prefix_pairs = [];
          let bulk_batches_to_send = [];
          for (const [node, prefixes] of nodeToPrefix) {
            const nodeId = distribution.util.id.getNID(node);
            const nodePrefixBatches = [];

            let nodeTermCount = 0;
            for (const [prefix, terms] of prefixes) {
              const prefixData = {};
              for (const [word, count] of terms) {
                const tf = count / totalWords;
                const taxonomyMatch = Object.entries(taxonomyInfo).find(
                  ([key, value]) => value.toLowerCase().includes(word)
                );
                const inTaxonomy = !!taxonomyMatch;
                const taxonomyLevel = inTaxonomy ? taxonomyMatch[0] : null;
                const inBinomialName = binomialName
                  .toLowerCase()
                  .includes(word);
                const inKingdom = kingdom.toLowerCase().includes(word);
                const inFamily = family.toLowerCase().includes(word);

                // !! New ranking mechnaism using the classification data
                const rankingFactors = {
                  tf: tf,
                  taxonomyBoost: inTaxonomy
                    ? taxonomyLevel === "kingdom"
                      ? 5.0
                      : taxonomyLevel === "phylum"
                      ? 4.0
                      : taxonomyLevel === "class"
                      ? 3.0
                      : taxonomyLevel === "order"
                      ? 2.5
                      : taxonomyLevel === "family"
                      ? 2.0
                      : taxonomyLevel === "genus"
                      ? 1.5
                      : 1.0
                    : 1.0,
                  binomialBoost: inBinomialName ? 4.0 : 1.0,
                  positionBoost: inKingdom ? 3.0 : inFamily ? 2.0 : 1.0,
                  score: 0, // Calculated below
                };

                rankingFactors.score =
                  tf *
                  rankingFactors.taxonomyBoost *
                  rankingFactors.binomialBoost *
                  rankingFactors.positionBoost;

                prefixData[word] = [
                  {
                    url: docId,
                    tf: tf,
                    ranking: rankingFactors,
                    taxonomyLevel: taxonomyLevel,
                    isBinomial: inBinomialName,
                    pageInfo: {
                      kingdom: kingdom,
                      family: family,
                      binomialName: binomialName,
                    },
                  },
                ];
                nodeTermCount++;
              }

              nodePrefixBatches.push({
                prefix,
                data: prefixData,
              });
            }

            node_prefix_pairs.push([node, nodeId, prefixes]);
            bulk_batches_to_send.push(nodePrefixBatches);
          }
          if (bulk_batches_to_send.length !== nodeToPrefix.size)
            throw Error("RAH");

          const timecheck_2 = Date.now();

          // ####################################
          // 5. SEND BATCHES TO NODES
          // ####################################

          let success = true;
          let total_batches_sent = 0;
          let time_steps = [];
          let run_batch_promises = [];

          for (let i = 0; i < nodeToPrefix.size; i++) {
            const [node, nodeId, prefixes] = node_prefix_pairs[i];
            const nodePrefixBatches = bulk_batches_to_send[i];
            if (nodePrefixBatches.length === 0) continue;

            run_batch_promises.push(
              new Promise((resolve, reject) => {
                const temp_time = Date.now();
                if (log_index)
                  console.log(
                    `Sending batch with ${nodePrefixBatches.length} prefixes to node ${nodeId} ${global.nodeConfig.port}`
                  );
                distribution.local.comm.send(
                  [{ prefixBatches: nodePrefixBatches, gid: "indexer_group" }],
                  { service: "store", method: "bulk_append", node: node },
                  (err, val) => {
                    if (err) success = false;
                    total_batches_sent += 1;
                    time_steps.push(Date.now() - temp_time);
                    resolve();
                  }
                );
              })
            );
          }
          await Promise.all(run_batch_promises);

          fs.appendFileSync(
            global.logging_path,
            `   Indexer finished: ${success}\n`
          );

          // ####################################
          // 6. FINISH PROCESSING AND METRICS
          // ####################################

          // Force garbage collection if available and memory usage is high
          const memUsage = process.memoryUsage();
          if (log_index)
            console.log(
              `Memory usage: ${Math.round(
                memUsage.heapUsed / 1024 / 1024
              )}MB/${Math.round(memUsage.heapTotal / 1024 / 1024)}MB`
            );
          if (global.gc && memUsage.heapUsed > 500 * 1024 * 1024) {
            // 500MB threshold
            if (log_index) console.log("Forcing garbage collection");
            global.gc();
          }

          const timecheck_3 = Date.now();

          // Log performance metrics
          let indexing_time = Date.now() - index_start_time;
          metrics.documentsIndexed += 1;
          metrics.totalIndexTime += indexing_time;
          metrics.totalTermsProcessed += totalWords || 0;
          metrics.totalPrefixesProcessed += prefixGroups.size || 0;
          metrics.batchesSent += totalBatches || 0;
          metrics.processing_times.push(indexing_time);
          // console.log(`TOTAL INDEXING TIME: ${indexing_time}ms`);
          // console.log(`    step 1 : ${timecheck_1 - index_start_time}ms`);
          // console.log(`    step 2 : ${timecheck_2 - timecheck_1}ms`);
          // console.log(`    step 3 : ${timecheck_3 - timecheck_2}ms`);
          // console.log(`           i : ${time_steps}ms`);

          if (!success) metrics.errors += 1;

          callback(null, {
            status: success ? "success" : "partial_success",
            docId: docId,
          });
        });
      });
    });
  });
}

function get_idf_doc_count(callback) {
  callback = callback || cb;
  const fs = require("fs");
  const nid = distribution.util.id.getNID(global.nodeConfig);
  const num_docs_on_node = fs.readdirSync(`store/${nid}/crawler_group`).length;
  callback(null, { num_docs_on_node: num_docs_on_node });
}

function save_maps_to_disk(callback) {
  callback = callback || cb;

  distribution.local.mem.get("links_to_index_map", (e1, links_to_index_map) => {
    distribution.local.mem.get("indexed_links_map", (e2, indexed_links_map) => {
      const links_to_index_data = Array.from(links_to_index_map.keys()).join(
        "\n"
      );
      const indexed_links_data = Array.from(indexed_links_map.keys()).join(
        "\n"
      );

      distribution.local.store.put(
        links_to_index_data,
        "links_to_index",
        (e, v) => {
          distribution.local.store.put(
            indexed_links_data,
            "indexed_links",
            (e, v) => {
              callback(null, {
                status: "success",
                links_to_index_saved: links_to_index_map.size,
                indexed_links_saved: indexed_links_map.size,
              });
            }
          );
        }
      );
    });
  });
}

module.exports = {
  initialize,
  add_link_to_index,
  get_idf_doc_count,
  save_maps_to_disk,
  index_one,
};
