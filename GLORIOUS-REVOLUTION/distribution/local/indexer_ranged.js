// distribution/local/indexer-ranged.js
const fs = require('fs');

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
  fs.appendFileSync(global.logging_path, `RANGED INDEXER INITIALIZING... ${new Date()}\n`);

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

function index_one(callback) {
  callback = callback || cb;

  const fs = require('fs');
  fs.appendFileSync(global.logging_path, `RANGE INDEXING ONE (TODO)...\n`);

  callback();
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
  add_link_to_index,
  save_maps_to_disk,
  index_one
};