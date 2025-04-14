// distribution/local/crawler.js
const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const parse = require("node-html-parser").parse;

const errorStats = {
  timeoutErrors: 0,
  networkErrors: 0,
  total: 0,
  lastReportTime: Date.now(),
};

// Rate limiter to control requests to Wikipedia
class RateLimiter {
  constructor(maxRequests = 5, timeWindow = 1000) {
    this.maxRequests = maxRequests;
    this.timeWindow = timeWindow;
    this.requestTimes = [];
  }

  async throttle() {
    const now = Date.now();
    // Remove timestamps outside the time window
    this.requestTimes = this.requestTimes.filter(
      (time) => now - time < this.timeWindow
    );

    if (this.requestTimes.length >= this.maxRequests) {
      // Wait until the oldest request falls out of the time window
      const oldestTime = this.requestTimes[0];
      const waitTime = this.timeWindow - (now - oldestTime) + 10; // Add 10ms buffer
      await new Promise((resolve) => setTimeout(resolve, waitTime));
      return this.throttle(); // Check again after waiting
    }

    // Add current request timestamp
    this.requestTimes.push(now);
  }
}

// Shared rate limiter instance
const wikiRateLimiter = new RateLimiter(3, 1000); // 3 requests per second

// Fetch with retry and timeout
async function fetchWithRetry(
  url,
  maxRetries = 3,
  initialDelay = 1000,
  timeout = 15000
) {
  let retries = 0;

  while (retries < maxRetries) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    try {
      await wikiRateLimiter.throttle(); // Rate limit our requests
      const response = await fetch(url, { signal: controller.signal });
      clearTimeout(timeoutId);
      return response;
    } catch (error) {
      clearTimeout(timeoutId);

      // Log error stats
      errorStats.total++;
      if (error.cause && error.cause.code === "ETIMEDOUT") {
        errorStats.timeoutErrors++;
      } else {
        errorStats.networkErrors++;
      }

      // Report error rates periodically
      const now = Date.now();
      if (now - errorStats.lastReportTime > 60000) {
        // Every minute
        fs.appendFileSync(
          global.logging_path,
          `ERROR RATES: ${errorStats.timeoutErrors} timeouts, ${errorStats.networkErrors} other errors out of ${errorStats.total} total requests\n`
        );
        errorStats.lastReportTime = now;
      }

      // Last retry, propagate the error
      if (retries === maxRetries - 1) throw error;

      // Exponential backoff
      const delay = initialDelay * Math.pow(2, retries);
      fs.appendFileSync(
        global.logging_path,
        `Retry ${retries + 1}/${maxRetries} for ${url} after ${delay}ms (${
          error.message
        })\n`
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
      retries++;
    }
  }
}

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};

let metrics = null;
let stopWordsSet = require("../util/stopwords");

function initialize(callback) {
  callback = callback || cb;
  const distribution = require("../../config");

  const crawlerDir = path.join("crawler-files");
  const metricsDir = path.join(crawlerDir, "metrics");
  const loggingDir = path.join(crawlerDir, "logging");
  if (!fs.existsSync(crawlerDir)) fs.mkdirSync(crawlerDir, { recursive: true });
  if (!fs.existsSync(metricsDir)) fs.mkdirSync(metricsDir, { recursive: true });
  if (!fs.existsSync(loggingDir)) fs.mkdirSync(loggingDir, { recursive: true });
  const metrics_file_path = path.join(
    metricsDir,
    `metrics-crawler-${global.nodeConfig.port}.json`
  );
  global.logging_path = path.join(
    loggingDir,
    `LOG-${global.nodeConfig.port}.txt`
  );
  fs.writeFileSync(global.logging_path, `LOGGING STARTED ${new Date()}\n`);
  fs.writeFileSync(
    global.logging_path,
    `CRAWLER INITIALIZING... ${new Date()}\n`
  );

  metrics = {
    crawling: {
      totalCrawlTime: 0,
      pagesProcessed: 0,
      bytesDownloaded: 0,
      bytesTransferred: 0,
      termsExtracted: 0,
      targetsHit: 0,
    },

    memory: {
      timestamp: Date.now(),
      heapUsed: 0,
      heapTotal: 0,
    },

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
    const memUsage = process.memoryUsage();
    metrics.memory = {
      timestamp: Date.now(),
      heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024),
      heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024),
    };

    const old_metrics = JSON.parse(
      fs.readFileSync(metrics_file_path).toString()
    );
    old_metrics.push(metrics);
    fsp.writeFile(metrics_file_path, JSON.stringify(old_metrics, null, 2));

    metrics.processing_times = [];
  }, 60000);

  const links_to_crawl_map = new Map();
  const crawled_links_map = new Map();

  distribution.local.mem.put(
    links_to_crawl_map,
    "links_to_crawl_map",
    (e, v) => {
      distribution.local.mem.put(
        crawled_links_map,
        "crawled_links_map",
        (e, v) => {
          distribution.local.store.get("links_to_crawl", (e1, v1) => {
            distribution.local.store.get("crawled_links", (e2, v2) => {
              if (
                !e1 &&
                !e2 &&
                typeof v1 === "string" &&
                typeof v2 === "string"
              ) {
                const saved_links_to_crawl = v1
                  .split("\n")
                  .filter((s) => s.length > 0);
                const saved_crawled_links = v2
                  .split("\n")
                  .filter((s) => s.length > 0);
                saved_links_to_crawl.map((link) =>
                  links_to_crawl_map.set(link, true)
                );
                saved_crawled_links.map((link) =>
                  crawled_links_map.set(link, true)
                );
              }

              if (e1 && e2) {
                distribution.local.store.put("", "links_to_crawl", (e, v) => {
                  distribution.local.store.put("", "crawled_links", (e, v) => {
                    fs.appendFileSync(
                      global.logging_path,
                      `CREATED CRAWLER MAPS ${e} ${v}\n`
                    );
                  });
                });
              }

              callback(null, {
                status: "success",
                message: "Crawler service initialized",
                links_to_crawl: links_to_crawl_map.size,
                crawled_links: crawled_links_map.size,
              });
            });
          });
        }
      );
    }
  );
}

let service_paused = false;

function start_crawl(callback) {
  callback = callback || cb;

  const crawl_loop = async () => {
    try {
      while (true) {
        await new Promise((resolve, reject) => {
          if (service_paused) {
            setTimeout(() => resolve(), 1000);
          } else {
            let resolve_timeout = setTimeout(() => resolve(), 10000);
            distribution.local.crawler.crawl_one((e, v) => {
              clearTimeout(resolve_timeout);
              if (e || ("status" in v && v.status !== "success")) {
                setTimeout(() => {
                  resolve();
                }, 1000);
              } else {
                resolve();
              }
            });
          }
        });
      }
    } catch (err) {
      console.error("crawlLoop failed:", err);
      setTimeout(crawl_loop, 1000);
    }
  };
  crawl_loop();

  callback(null, true);
}

function set_service_state(state, callback) {
  service_paused = state;
  callback();
}

function add_link_to_crawl(link, callback) {
  callback = callback || cb;

  const fs = require("fs");
  fs.appendFileSync(global.logging_path, `ADDING LINK ${link}\n`);

  distribution.local.mem.get("links_to_crawl_map", (e1, links_to_crawl_map) => {
    distribution.local.mem.get("crawled_links_map", (e2, crawled_links_map) => {
      if (links_to_crawl_map.has(link))
        return callback(null, {
          status: "skipped",
          reason: "already_in_queue",
        });
      if (crawled_links_map.has(link))
        return callback(null, { status: "skipped", reason: "already_crawled" });

      links_to_crawl_map.set(link, true);
      fs.appendFileSync(
        global.logging_path,
        `LINK ADDED TO CRAWL QUEUE: ${link}\n`
      );
      callback(null, {
        status: "success",
        message: "Link added to crawl queue",
        link: link,
      });
    });
  });
}

function crawl_one(callback) {
  callback = callback || cb;
  const crawlStartTime = Date.now();

  const fs = require("fs");

  distribution.local.mem.get("links_to_crawl_map", (e1, links_to_crawl_map) => {
    distribution.local.mem.get("crawled_links_map", (e2, crawled_links_map) => {
      if (links_to_crawl_map.size === 0)
        return callback(null, { status: "skipped", reason: "no_links" });

      const [url, _] = links_to_crawl_map.entries().next().value;
      links_to_crawl_map.delete(url);

      if (crawled_links_map.has(url))
        return callback(null, { status: "skipped", reason: "already_crawled" });

      fs.appendFileSync(global.logging_path, `CRAWLING ONE: ${url}\n`);

      // Use our improved fetch with retry
      (async () => {
        try {
          // First fetch - page content
          const response = await fetchWithRetry(
            `https://en.wikipedia.org${url}`
          );
          const contentLength = response.headers.get("content-length") || 0;
          metrics.crawling.bytesDownloaded += parseInt(contentLength);
          const html = await response.text();

          const root = parse(html);

          // Extract biota info
          const biota = root.querySelector("table.infobox.biota");
          const biota_rows = biota?.querySelectorAll("tr");

          const hierarchy = biota_rows
            ?.map((row) => {
              const td_data = row.querySelectorAll("td");
              if (td_data.length !== 2) return null;

              const label = td_data[0].text
                .trim()
                .toLocaleLowerCase()
                .slice(0, -1);
              const value = td_data[1].text.trim().toLocaleLowerCase();
              return [label, value];
            })
            .filter((item) => item !== null);

          const binomial_name =
            biota
              ?.querySelector("span.binomial")
              ?.text?.trim()
              .toLocaleLowerCase() || "";

          // Extract links
          const links_on_page = root
            .querySelectorAll("a")
            .map((link) => link.getAttribute("href"))
            .filter((link) => link !== null && link !== undefined)
            .filter((link) => link.startsWith("/wiki/"))
            .filter((link) => !link.includes(".JPG"))
            .filter((link) => !link.includes(".jpg"))
            .filter((link) => !link.includes(".JPEG"))
            .filter((link) => !link.includes(".jpeg"))
            .filter((link) => !link.includes(".PNG"))
            .filter((link) => !link.includes(".png"))
            .filter((link) => !link.includes("#"))
            .filter((link) => !link.includes(":"));

          // Check if this is a target class
          const is_plant = hierarchy?.find(
            (pair) => pair[0] === "kingdom" && pair[1].includes("plantae")
          );
          const is_fungi = hierarchy?.find(
            (pair) => pair[0] === "kingdom" && pair[1].includes("fungi")
          );
          const is_sealife = hierarchy?.find(
            (pair) => pair[0] === "phylum" && pair[1].includes("cnidaria")
          );
          const is_butterfly = hierarchy?.find(
            (pair) => pair[0] === "order" && pair[1].includes("lepidoptera")
          );
          const is_target_class =
            is_plant || is_fungi || is_sealife || is_butterfly;

          let result = {
            status: "success",
            url: url,
            is_target_class: !!is_target_class,
            links_found: links_on_page.length,
          };

          if (is_target_class && binomial_name) {
            // Process target class
            const page_text = root.text;
            const alphaOnlyPattern = /^[a-z]+$/;

            const all_words = (page_text.match(/\b\w+\b/g) || [])
              .map((word) => word.toLocaleLowerCase())
              .filter((word) => word.length > 2)
              .filter((word) => alphaOnlyPattern.test(word))
              .filter((word) => !stopWordsSet.has(word));

            const wordCounts = new Map();
            for (const word of all_words) {
              wordCounts.set(word, (wordCounts.get(word) || 0) + 1);
            }

            // Get metadata with retry
            const stripped_url = url.replace(/\/wiki\//, "");
            const meta_data_endpoint = `https://en.wikipedia.org/api/rest_v1/page/summary/${stripped_url}`;
            let title = binomial_name;
            let description = "";

            try {
              const metaResponse = await fetchWithRetry(meta_data_endpoint);
              const data = await metaResponse.json();
              title = data.title !== "Not found." ? data.title : binomial_name;
              description = data.extract || "";
              // Make the first letter uppercase
              if (title[0] === title[0].toLowerCase()) {
                title = title.charAt(0).toUpperCase() + title.slice(1);
              }
            } catch (error) {
              // Fallback if metadata fetch fails
              fs.appendFileSync(
                global.logging_path,
                `Error fetching metadata for ${url}: ${error.message}\n`
              );
              // Continue with default values set above
            }

            description = description.replace(/[\u0000-\u001F]/g, " ");

            const species_data = {
              hierarchy: hierarchy,
              binomial_name: binomial_name,
              url: url,
              title: title,
              description: description,
              word_counts: Object.fromEntries(wordCounts),
            };

            const uncompressed_data = JSON.stringify(species_data);
            const dataSize = Buffer.byteLength(uncompressed_data, "utf8");
            metrics.crawling.bytesTransferred += dataSize;
            metrics.crawling.termsExtracted += wordCounts.size;
            metrics.crawling.targetsHit += 1;

            processCrawlResult(
              url,
              links_on_page,
              result,
              crawlStartTime,
              is_target_class,
              () => {
                distribution.crawler_group.store.put(
                  species_data,
                  url,
                  (e, v) => {
                    distribution.local.groups.get("indexer_group", (e, v) => {
                      if (e) {
                        console.error("Error getting group info:", e);
                        return callback(null, {
                          status: "error",
                          error: "Failed to get group info",
                          url: url,
                        });
                      }

                      const nodes = Object.values(v);
                      const num_nodes = nodes.length;

                      const get_nx = (link) =>
                        nodes[
                          parseInt(
                            distribution.util.id.getID(link).slice(0, 8),
                            16
                          ) % num_nodes
                        ];
                      const remote_1 = {
                        node: get_nx(url),
                        service: "indexer",
                        method: "add_link_to_index",
                      };
                      const remote_2 = {
                        node: get_nx(url),
                        service: "indexer_ranged",
                        method: "add_link_to_index",
                      };
                      distribution.local.comm.send([url], remote_1, (e, v) => {
                        fs.appendFileSync(
                          global.logging_path,
                          `   CRAWL SEND TARGET PAGE ${url} TO INDEXER\n`
                        );
                        distribution.local.comm.send(
                          [url],
                          remote_2,
                          (e, v) => {
                            callback(null, { status: "success" });
                          }
                        );
                      });
                    });
                  }
                );
              }
            );
          } else {
            // Not a target class, skip indexing
            result.indexing = {
              status: "skipped",
              reason: "not_target_or_no_binomial",
            };
            processCrawlResult(
              url,
              links_on_page,
              result,
              crawlStartTime,
              is_target_class,
              () => {
                callback(null, { status: "success" });
              }
            );
          }
        } catch (error) {
          // Handle any fetch or processing errors
          fs.appendFileSync(
            global.logging_path,
            `ERROR processing ${url}: ${error.message}\n${
              error.stack ? error.stack : ""
            }\n`
          );

          // Mark as crawled so we don't retry indefinitely
          crawled_links_map.set(url, true);

          const crawlEndTime = Date.now();
          const crawlDuration = crawlEndTime - crawlStartTime;
          metrics.crawling.pagesProcessed += 1;
          metrics.crawling.totalCrawlTime += crawlDuration;
          metrics.processing_times.push(crawlDuration);

          // Return error status but don't crash
          callback(null, {
            status: "error",
            url: url,
            error: error.message,
            duration_ms: crawlDuration,
          });
        }
      })();
    });
  });
}

function processCrawlResult(
  url,
  links_on_page,
  result,
  crawlStartTime,
  is_target_class,
  callback
) {
  distribution.local.mem.get("crawled_links_map", (e, crawled_links_map) => {
    crawled_links_map.set(url, true);

    distribution.local.groups.get("crawler_group", (e, v) => {
      if (e) {
        console.error("Error getting group info:", e);
        return callback(null, {
          status: "error",
          error: "Failed to get group info",
          url: url,
        });
      }

      const nodes = Object.values(v);
      const num_nodes = nodes.length;

      const get_nx = (link) =>
        nodes[
          parseInt(distribution.util.id.getID(link).slice(0, 8), 16) % num_nodes
        ];
      const new_links = [...new Set(is_target_class ? links_on_page : [])];

      let processed = 0;
      const total = new_links.length;

      if (total === 0) {
        finishCrawl();
        return;
      }

      new_links.forEach((link) => {
        const remote = {
          node: get_nx(link),
          gid: "local",
          service: "crawler",
          method: "add_link_to_crawl",
        };
        distribution.local.comm.send([link], remote, (e, v) => {
          processed++;
          if (processed === total) {
            finishCrawl();
          }
        });
      });

      function finishCrawl() {
        const crawlEndTime = Date.now();
        const crawlDuration = crawlEndTime - crawlStartTime;
        metrics.crawling.pagesProcessed += 1;
        metrics.crawling.totalCrawlTime += crawlDuration;
        metrics.processing_times.push(crawlDuration);

        callback(null, result);
      }
    });
  });
}

function get_stats(callback) {
  callback = callback || cb;

  distribution.local.mem.get("links_to_crawl_map", (e1, links_to_crawl_map) => {
    distribution.local.mem.get("crawled_links_map", (e2, crawled_links_map) => {
      const stats = {
        links_to_crawl: links_to_crawl_map.size,
        crawled_links: crawled_links_map.size,
        metrics: metrics,
      };

      callback(null, stats);
    });
  });
}

function save_maps_to_disk(callback) {
  callback = callback || cb;

  distribution.local.mem.get("links_to_crawl_map", (e1, links_to_crawl_map) => {
    distribution.local.mem.get("crawled_links_map", (e2, crawled_links_map) => {
      const links_to_crawl_data = Array.from(links_to_crawl_map.keys()).join(
        "\n"
      );
      const crawled_links_data = Array.from(crawled_links_map.keys()).join(
        "\n"
      );

      distribution.local.store.put(
        links_to_crawl_data,
        "links_to_crawl",
        (e, v) => {
          distribution.local.store.put(
            crawled_links_data,
            "crawled_links",
            (e, v) => {
              callback(null, {
                status: "success",
                links_to_crawl_saved: links_to_crawl_map.size,
                crawled_links_saved: crawled_links_map.size,
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
  start_crawl,
  set_service_state,
  add_link_to_crawl,
  crawl_one,
  get_stats,
  save_maps_to_disk,
};
