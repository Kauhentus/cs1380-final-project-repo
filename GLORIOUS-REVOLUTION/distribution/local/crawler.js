// distribution/local/crawler.js
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const parse = require('node-html-parser').parse;

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};

let metrics = null;
let stopWordsSet = require('../util/stopwords');;

function initialize(callback) {
  callback = callback || cb;
  const distribution = require('../../config');

  const crawlerDir = path.join('crawler-files');
  const metricsDir = path.join(crawlerDir, 'metrics');
  const loggingDir = path.join(crawlerDir, 'logging');
  if (!fs.existsSync(crawlerDir)) fs.mkdirSync(crawlerDir, { recursive: true });
  if (!fs.existsSync(metricsDir)) fs.mkdirSync(metricsDir, { recursive: true });
  if (!fs.existsSync(loggingDir)) fs.mkdirSync(loggingDir, { recursive: true });
  const metrics_file_path = path.join(metricsDir, `metrics-crawler-${global.nodeConfig.port}.json`);
  global.logging_path = path.join(loggingDir, `LOG-${global.nodeConfig.port}.txt`);
  fs.writeFileSync(global.logging_path, `LOGGING STARTED ${new Date()}\n`);
  fs.writeFileSync(global.logging_path, `CRAWLER INITIALIZING... ${new Date()}\n`);

  metrics = {
    crawling: {
      totalCrawlTime: 0,
      pagesProcessed: 0,
      bytesDownloaded: 0,
      bytesTransferred: 0,
      termsExtracted: 0,
      targetsHit: 0
    },

    memory: {
      timestamp: Date.now(),
      heapUsed: 0,
      heapTotal: 0
    },

    processing_times: [],
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
    const memUsage = process.memoryUsage();
    metrics.memory = {
      timestamp: Date.now(),
      heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024),
      heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024)
    };

    const old_metrics = JSON.parse(fs.readFileSync(metrics_file_path).toString());
    old_metrics.push(metrics);
    fsp.writeFile(metrics_file_path, JSON.stringify(old_metrics, null, 2));

    metrics.processing_times = [];
  }, 60000);

  const links_to_crawl_map = new Map();
  const crawled_links_map = new Map();

  distribution.local.mem.put(links_to_crawl_map, 'links_to_crawl_map', (e, v) => {
    distribution.local.mem.put(crawled_links_map, 'crawled_links_map', (e, v) => {

      distribution.local.store.get('links_to_crawl', (e1, v1) => {
        distribution.local.store.get('crawled_links', (e2, v2) => {
          if (!e1 && !e2 && typeof v1 === 'string' && typeof v2 === 'string') {
            const saved_links_to_crawl = v1.split('\n').filter(s => s.length > 0);
            const saved_crawled_links = v2.split('\n').filter(s => s.length > 0);
            saved_links_to_crawl.map(link => links_to_crawl_map.set(link, true));
            saved_crawled_links.map(link => crawled_links_map.set(link, true));
          }

          if(e1 && e2) {
            distribution.local.store.put('', 'links_to_crawl', (e, v) => {
              distribution.local.store.put('', 'crawled_links', (e, v) => {
                fs.appendFileSync(global.logging_path, `CREATED CRAWLER MAPS ${e} ${v}\n`);
              });
            });
          }

          callback(null, {
            status: 'success',
            message: 'Crawler service initialized',
            links_to_crawl: links_to_crawl_map.size,
            crawled_links: crawled_links_map.size
          });
        });
      });
    });
  });
}

function start_crawl(callback) {
  callback = callback || cb;

  const crawl_loop = async () => {
    try {
      while (true) {
        await new Promise((resolve, reject) => {
          distribution.local.crawler.crawl_one((e, v) => {
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
      console.error('crawlLoop failed:', err);
      setTimeout(crawl_loop, 1000);
    }
  }
  crawl_loop();

  callback(null, true);
}


function add_link_to_crawl(link, callback) {
  callback = callback || cb;
  
  // const fs = require('fs');
  // fs.appendFileSync(global.logging_path, `ADDING LINK ${link}\n`);

  distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
    distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {
      if (links_to_crawl_map.has(link)) return callback(null, { status: 'skipped', reason: 'already_in_queue' });
      if (crawled_links_map.has(link)) return callback(null, { status: 'skipped', reason: 'already_crawled' });

      links_to_crawl_map.set(link, true);
      callback(null, { status: 'success', message: 'Link added to crawl queue', link: link });
    });
  });
}

function crawl_one(callback) {
  callback = callback || cb;
  const crawlStartTime = Date.now();

  const fs = require('fs');
  // fs.appendFileSync(global.logging_path, `CRAWLING ONE...\n`);

  distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
    distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {
      if (links_to_crawl_map.size === 0) return callback(null, { status: 'skipped', reason: 'no_links' });
      const [url, _] = links_to_crawl_map.entries().next().value;
      links_to_crawl_map.delete(url);
      if (crawled_links_map.has(url)) return callback(null, { status: 'skipped', reason: 'already_crawled' });

      fs.appendFileSync(global.logging_path, `CRAWLING ONE: ${url}\n`);



      fetch(`https://en.wikipedia.org${url}`)
        .then((response) => {
          const contentLength = response.headers.get('content-length') || 0;
          metrics.crawling.bytesDownloaded += parseInt(contentLength);
          return response.text();
        })
        .then(async (html) => {
          const root = parse(html);

          const biota = root.querySelector('table.infobox.biota');
          const biota_rows = biota?.querySelectorAll('tr');

          const hierarchy = biota_rows?.map((row) => {
            const td_data = row.querySelectorAll('td');
            if (td_data.length !== 2) return null;

            const label = td_data[0].text.trim().toLocaleLowerCase().slice(0, -1);
            const value = td_data[1].text.trim().toLocaleLowerCase();
            return [label, value];
          }).filter(item => item !== null);

          const binomial_name = biota?.querySelector('span.binomial')?.text?.trim().toLocaleLowerCase() || '';

          const links_on_page = root.querySelectorAll('a').map(link => link.getAttribute('href'))
            .filter(link => link !== null && link !== undefined)
            .filter(link => link.startsWith('/wiki/'))
            .filter(link => !link.includes('.JPG'))
            .filter(link => !link.includes('.jpg'))
            .filter(link => !link.includes('.JPEG'))
            .filter(link => !link.includes('.jpeg'))
            .filter(link => !link.includes('.PNG'))
            .filter(link => !link.includes('.png'))
            .filter(link => !link.includes('#'))
            .filter(link => !link.includes(':'));

          const is_plant = hierarchy?.find(pair => pair[0] === 'kingdom' && pair[1].includes('plantae'));
          const is_fungi = hierarchy?.find(pair => pair[0] === 'kingdom' && pair[1].includes('fungi'));
          const is_sealife = hierarchy?.find(pair => pair[0] === 'phylum' && pair[1].includes('cnidaria'));
          const is_butterfly = hierarchy?.find(pair => pair[0] === 'order' && pair[1].includes('lepidoptera'));
          const is_target_class = is_plant || is_fungi || is_sealife || is_butterfly;

          let result = {
            status: 'success',
            url: url,
            is_target_class: !!is_target_class,
            links_found: links_on_page.length
          };

          if (is_target_class && binomial_name) {
            const page_text = root.text;
            const alphaOnlyPattern = /^[a-z]+$/;

            const all_words = (page_text.match(/\b\w+\b/g) || [])
              .map(word => word.toLocaleLowerCase())
              .filter(word => word.length > 2) // Filter out very short words
              .filter(word => alphaOnlyPattern.test(word)) // Only alphabetic words
              .filter(word => !stopWordsSet.has(word)); // Filter out stop words

            const wordCounts = new Map();
            for (const word of all_words) {
              wordCounts.set(word, (wordCounts.get(word) || 0) + 1);
            }

            const stripped_url = url.replace(/\/wiki\//, '');
            const meta_data_endpoint = `https://en.wikipedia.org/api/rest_v1/page/summary/${stripped_url}`;
            let title, description;
            try {
              await new Promise((resolve, reject) => {
                fetch(meta_data_endpoint)
                  .then(response => response.json())
                  .then(data => {
                    title = data.title;
                    description = data.extract;
                    if(title === "Not found.") {
                      title = binomial_name;
                      title[0] = title[0].toLocaleUpperCase();
                    }
                    resolve();
                  })
                  .catch(error => {
                    console.error(`Error fetching metadata: ${error}`);
                    reject(error);
                  });
              });
            } catch(e) {
              title = binomial_name;
              title[0] = title[0].toLocaleUpperCase();
              description = '';
            }
            description = description.replace(/[\u0000-\u001F]/g, ' ');

            const species_data = {
              hierarchy: hierarchy,
              binomial_name: binomial_name,
              url: url,
              title: title,
              description: description,

              // !! ONLY SENDING wordCounts instead of all words to reduce transfer size
              word_counts: Object.fromEntries(wordCounts)
            };

            const uncompressed_data = JSON.stringify(species_data);
            const dataSize = Buffer.byteLength(uncompressed_data, 'utf8');
            metrics.crawling.bytesTransferred += dataSize;
            metrics.crawling.termsExtracted += wordCounts.size;
            metrics.crawling.targetsHit += 1;

            processCrawlResult(url, links_on_page, result, crawlStartTime, is_target_class, () => {
              // fs.appendFileSync(global.logging_path, `   CRAWL FOUND TARGET PAGE\n`);
              distribution.crawler_group.store.put(species_data, url, (e, v) => {

                distribution.local.groups.get('crawler_group', (e, v) => {
                  if (e) {
                    console.error('Error getting group info:', e);
                    return callback(null, {
                      status: 'error',
                      error: 'Failed to get group info',
                      url: url
                    });
                  }
            
                  const nodes = Object.values(v);
                  const num_nodes = nodes.length;

                  const get_nx = (link) => nodes[parseInt(distribution.util.id.getID(link).slice(0, 8), 16) % num_nodes];
                  const remote_1 = { node: get_nx(url), service: 'indexer', method: 'add_link_to_index' };
                  const remote_2 = { node: get_nx(url), service: 'indexer_ranged', method: 'add_link_to_index' };
                  distribution.local.comm.send([url], remote_1, (e, v) => {
                    distribution.local.comm.send([url], remote_2, (e, v) => {
                      callback(null, { status: 'success' });
                    });
                  });

                });

              });
            });
          } 
          
          else {
            result.indexing = { status: 'skipped', reason: 'not_target_or_no_binomial' };
            processCrawlResult(url, links_on_page, result, crawlStartTime, is_target_class, () => {
              callback(null, { status: 'success' });
            });
          }
        })
        .catch((error) => {
          console.error(`ERROR: fetching ${url}:`, error);
          crawled_links_map.set(url, true);

          const crawlEndTime = Date.now();
          const crawlDuration = crawlEndTime - crawlStartTime;
          metrics.crawling.pagesProcessed += 1;
          metrics.crawling.totalCrawlTime += crawlDuration;
          metrics.processing_times.push(indexing_time);

          callback(null, {
            status: 'error',
            url: url,
            error: error.message,
            duration_ms: crawlDuration
          });
        });
    });
  });
}

function processCrawlResult(url, links_on_page, result, crawlStartTime, is_target_class, callback) {

  distribution.local.mem.get('crawled_links_map', (e, crawled_links_map) => {
    crawled_links_map.set(url, true);

    distribution.local.groups.get('crawler_group', (e, v) => {
      if (e) {
        console.error('Error getting group info:', e);
        return callback(null, {
          status: 'error',
          error: 'Failed to get group info',
          url: url
        });
      }

      const nodes = Object.values(v);
      const num_nodes = nodes.length;

      const get_nx = (link) => nodes[parseInt(distribution.util.id.getID(link).slice(0, 8), 16) % num_nodes];
      const new_links = [...new Set(is_target_class ? links_on_page : [])];

      let processed = 0;
      const total = new_links.length;

      if (total === 0) {
        finishCrawl();
        return;
      }

      new_links.forEach(link => {
        const remote = { node: get_nx(link), gid: 'local', service: 'crawler', method: 'add_link_to_crawl' };
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

  distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
    distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {

      const stats = {
        links_to_crawl: links_to_crawl_map.size,
        crawled_links: crawled_links_map.size,
        metrics: metrics
      };

      callback(null, stats);
    });
  });
}

function save_maps_to_disk(callback) {
  callback = callback || cb;

  distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
    distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {
      const links_to_crawl_data = Array.from(links_to_crawl_map.keys()).join('\n');
      const crawled_links_data = Array.from(crawled_links_map.keys()).join('\n');

      distribution.local.store.put(links_to_crawl_data, 'links_to_crawl', (e, v) => {
        distribution.local.store.put(crawled_links_data, 'crawled_links', (e, v) => {
          callback(null, {
            status: 'success',
            links_to_crawl_saved: links_to_crawl_map.size,
            crawled_links_saved: crawled_links_map.size
          });
        });
      });
    });
  });
}

module.exports = {
  initialize,
  start_crawl,
  add_link_to_crawl,
  crawl_one,
  get_stats,
  save_maps_to_disk
};