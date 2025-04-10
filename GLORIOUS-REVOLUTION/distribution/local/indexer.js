// distribution/local/indexer.js
const fs = require('fs');
const path = require('path');
const parse = require('node-html-parser').parse;

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};

function initialize(callback) {
  callback = callback || cb;
  const distribution = require('../../config');
  fs.appendFileSync(global.logging_path, `INDEXER INITIALIZING... ${new Date()}\n`);

  const links_to_index_map = new Map();
  const indexed_links_map = new Map();

  distribution.local.mem.put(links_to_index_map, 'links_to_index_map', (e, v) => {
    distribution.local.mem.put(indexed_links_map, 'indexed_links_map', (e, v) => {

      distribution.local.store.get('links_to_index', (e1, v1) => {
        distribution.local.store.get('indexed_links', (e2, v2) => {

          if (!e1 && !e2 && typeof v1 === 'string' && typeof v2 === 'string') {
            const saved_links_to_index = v1.split('\n').filter(s => s.length > 0);
            const saved_indexed_links = v2.split('\n').filter(s => s.length > 0);
            saved_links_to_index.map(link => links_to_index_map.set(link, true));
            saved_indexed_links.map(link => indexed_links_map.set(link, true));
          }

          if(e1 && e2) {
            distribution.local.store.put('', 'links_to_index', (e, v) => {
              distribution.local.store.put('', 'indexed_links', (e, v) => {
                fs.appendFileSync(global.logging_path, `CREATED MAPS ${e} ${v}\n`);
              });
            });
          }

          callback(null, {
            status: 'success',
            message: 'Index service initialized',
            links_to_index: links_to_index_map.size,
            indexed_links: indexed_links_map.size
          });
        });
      });
    });
  });
}


function add_link_to_index(link, callback) {
  callback = callback || cb;

  distribution.local.mem.get('links_to_index_map', (e1, links_to_index_map) => {
    distribution.local.mem.get('indexed_links_map', (e2, indexed_links_map) => {
      if (links_to_index_map.has(link)) return callback(null, { status: 'skipped', reason: 'already_in_queue' });
      if (indexed_links_map.has(link)) return callback(null, { status: 'skipped', reason: 'already_crawled' });

      links_to_index_map.set(link, true);
      callback(null, { status: 'success', message: 'Link added to crawl queue', link: link });
    });
  });
}

function index_one(callback) {
  callback = callback || cb;
  const indexStartTime = Date.now();

  const fs = require('fs');
  fs.appendFileSync(global.logging_path, `INDEXING ONE...\n`);

  distribution.local.mem.get('links_to_index_map', (e1, links_to_index_map) => {
    distribution.local.mem.get('indexed_links_map', (e2, indexed_links_map) => {

      if (links_to_index_map.size === 0) return callback(null, { status: 'skipped', reason: 'no_links' });
      const [url, _] = links_to_index_map.entries().next().value;
      // links_to_index_map.delete(url);
      // if (indexed_links_map.has(url)) return callback(null, { status: 'skipped', reason: 'already_indexed' });

      fs.appendFileSync(global.logging_path, `   Indexer selected: ${url}\n`);

      callback();
    });
  });
}

function save_maps_to_disk(callback) {
  callback = callback || cb;

  distribution.local.mem.get('links_to_index_map', (e1, links_to_index_map) => {
    distribution.local.mem.get('indexed_links_map', (e2, indexed_links_map) => {
      const links_to_index_data = Array.from(links_to_index_map.keys()).join('\n');
      const indexed_links_data = Array.from(indexed_links_map.keys()).join('\n');

      distribution.local.store.put(links_to_index_data, 'links_to_index', (e, v) => {
        distribution.local.store.put(indexed_links_data, 'indexed_links', (e, v) => {
          callback(null, {
            status: 'success',
            links_to_index_saved: links_to_index_map.size,
            indexed_links_saved: indexed_links_map.size
          });
        });
      });
    });
  });
}

module.exports = {
  initialize,
  add_link_to_index,
  save_maps_to_disk,
  index_one
};