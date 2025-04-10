const distribution = require('./config.js');
const id = distribution.util.id;

const num_nodes = 4;
const nodes = [];

const nids = [];
const crawler_group = {};
const crawler_group_config = { gid: 'crawler_group', hash: id.naiveHash };
const crawler_finished_group = {};
const crawler_finished_group_config = { gid: 'crawler_finished_group', hash: id.naiveHash };

const indexer_group = {};
const indexer_group_config = { gid: 'indexer_group', hash: id.naiveHash };
const querier_group = {};
const querier_group_config = { gid: 'querier_group', hash: id.naiveHash };

for(let i = 0; i < num_nodes; i++) {
    nodes.push({ ip: '127.0.0.1', port: 7110 + i });
    nids.push(id.getNID(nodes[i]));

    const sid = id.getSID(nodes[i]);
    crawler_group[sid] = nodes[i];
    crawler_finished_group[sid] = nodes[i];
    indexer_group[sid] = nodes[i];
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

    const setup_cluster = async (cb) => {
        console.log("SETTING UP CLUSTER...")
        const crawler_services = {
            initialize: (cb) => {
                const fs = require('fs');
                if(!fs.existsSync(`./crawler-files`)) fs.mkdirSync(`./crawler-files`, { recursive: true });
                if(!fs.existsSync(`./crawler-files/logs`)) fs.mkdirSync(`./crawler-files/logs`, { recursive: true });
                global.log_file_path = `./crawler-files/logs/log-${global.nodeConfig.port}.txt`;
                fs.writeFileSync(global.log_file_path, ``);

                const links_to_crawl_map = new Map();
                const crawled_links_map = new Map();
                distribution.local.mem.put(links_to_crawl_map, 'links_to_crawl_map', (e, v) => {
                    distribution.local.mem.put(crawled_links_map, 'crawled_links_map', (e, v) => {

                        distribution.local.store.get('links_to_crawl', (e1, v1) => {
                            distribution.local.store.get('crawled_links', (e2, v2) => {

                                fs.appendFileSync(global.log_file_path, `\n\nWPW1 ${v1}\n`);
                                fs.appendFileSync(global.log_file_path, `\n\nWPW2 ${v2}\n`);

                                if(!e1 && !e2) {
                                    const saved_links_to_crawl = v1.split('\n').filter(s => s.length > 0);
                                    const saved_crawled_links = v2.split('\n').filter(s => s.length > 0);
                                    saved_links_to_crawl.map(link => links_to_crawl_map.set(link, true));
                                    saved_crawled_links.map(link => crawled_links_map.set(link, true));
                                }
        
                                cb();
                            });
                        });
                    });
                });
            },

            update: (cb) => {
                setInterval(async () => {
                    distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
                        distribution.local.mem.get('crawled_links_map', async (e2, crawled_links_map) => {
                            const fs = require('fs');
                            fs.appendFileSync(global.log_file_path, `\n\nCRAWL on ${global.nodeConfig.port}\n`);
                            
                            const get_one_key = () => new Promise((resolve, reject) => {
                                resolve(links_to_crawl_map.keys().next().value);
                            });
        
                            const fetch_one_key = (key) => new Promise((resolve, reject) => {
                                fetch(`https://en.wikipedia.org${key.replace(/.wiki./, '/wiki/')}`)
                                    .then((response) => response.text())
                                    .then((html) => {
                                        const parse = require('node-html-parser').parse;
                                        const root = parse(html);
        
                                        const biota = root.querySelector('table.infobox.biota');
                                        const biota_rows = biota?.querySelectorAll('tr');
                                    
                                        const hierarchy = biota_rows?.map((row) => {
                                            const td_data = row.querySelectorAll('td');
                                            if(td_data.length !== 2) return null;
                                        
                                            const label = td_data[0].text.trim().toLocaleLowerCase().slice(0, -1);
                                            const value = td_data[1].text.trim().toLocaleLowerCase();
                                            return [label, value];
                                        }).filter(item => item !== null);
                                    
                                        const binomial_name = biota?.querySelector('span.binomial')?.text?.trim().toLocaleLowerCase();
                                        
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
                                            .filter(link => !link.includes(':'))
                                
                                        const is_plant = hierarchy?.find(pair => pair[0] === 'kingdom' && pair[1].includes('plantae'));
                                        const is_fungi = hierarchy?.find(pair => pair[0] === 'kingdom' && pair[1].includes('fungi'));
                                        const is_sealife = hierarchy?.find(pair => pair[0] === 'phylum' && pair[1].includes('cnidaria'));
                                        const is_butterfly = hierarchy?.find(pair => pair[0] === 'order' && pair[1].includes('lepidoptera'));
                                        const is_target_class = is_plant || is_fungi || is_sealife || is_butterfly;
        
                                        let is_species_page = hierarchy && binomial_name && is_target_class;
                                        let new_links = [...new Set(is_target_class ? links_on_page : [])];
                                        let page_data = undefined;
        
                                        if(is_species_page) {
                                            const page_text = root.text;
                                            const stop_words = require('./9-stop-words.js').stop_words;
        
                                            const all_words = (page_text.match(/\b\w+\b/g) || [])
                                                .map(word => word.toLocaleLowerCase())
                                                .filter(word => !/^\d+$/.test(word))
                                                .filter(word => !stop_words.includes(word));
        
                                            const species_data = {
                                                hierarchy: hierarchy,
                                                binomial_name: binomial_name,
                                                url: url,
                                                article_words: all_words,
                                            }
        
                                            page_data = JSON.stringify(species_data);
                                        }
        
                                        resolve({ is_species_page, page_data, links_on_page });
                                    });
                            });
        
                            const process_results = (is_species_page, page_data, links_on_page) => new Promise((resolve, reject) => {
                                links_on_page.map(link => {
                                    if(links_to_crawl_map.has(link)) return;
                                    if(crawled_links_map.has(link)) return;
                                    links_to_crawl_map.set(link, true)
                                });
                                links_to_crawl_map.delete(one_key);
                                crawled_links_map.set(one_key, true);
        
                                if(is_species_page){
                                    distribution.indexer_group.store.put(page_data, one_key, (e, v) => {
                                        resolve();
                                    });
                                } else {
                                    resolve();
                                }
                            });
        
                            const one_key = await get_one_key();
                            fs.appendFileSync(global.log_file_path, `    got key ${one_key}\n`);
                            if(one_key === undefined) return;
        
                            const { is_species_page, page_data, links_on_page } = await fetch_one_key(one_key);
                            fs.appendFileSync(global.log_file_path, `    found ${links_on_page.length} links\n`);
        
                            await process_results(is_species_page, page_data, links_on_page);
                            fs.appendFileSync(global.log_file_path, `    finished processing\n`);
        
                            // distribution.local.crawler_services.update(cb);
                            // distribution.local.comm.send([], { gid: 'local', service: 'crawler_services', method: 'update' }, (e, v) => {
        
                            // });
                        });
                    });
                }, 1000);
            }
        }
        const indexer_services = {
            initialize: (cb) => {
                cb();
            },

            update: (cb) => {
                setTimeout(() => {
                    const fs = require('fs');
                    fs.appendFileSync(global.log_file_path, `INDEX on ${global.nodeConfig.port}\n`);

                    cb();
                }, 1000);
            }
        }
        const querier_services = {
            initialize: (cb) => {
                cb();
            },

            update: (cb) => {
                setTimeout(() => {
                    const fs = require('fs');
                    fs.appendFileSync(global.log_file_path, `QUERY on ${global.nodeConfig.port}\n`);

                    cb();
                }, 1000);
            }
        }

        const init_group = (group, config) => new Promise((resolve, reject) => {
            distribution.local.groups.put(config, group, (e, v) => {
                distribution[config.gid].groups.put(config, group, (e, v) => {
                    resolve();
                });
            });
        });
        await init_group(crawler_group, crawler_group_config);
        await init_group(crawler_finished_group, crawler_finished_group_config);
        await init_group(indexer_group, indexer_group_config);
        await init_group(querier_group, querier_group_config);
        console.log("GROUPS CREATED");
        
        const put_services_in_group = (group_name, service_obj, service_name) => new Promise((resolve, reject) => {
            distribution[group_name].routes.put(service_obj, service_name, (e, v) => {
                resolve();
            });
        });
        await put_services_in_group('crawler_group', crawler_services, 'crawler_services');
        await put_services_in_group('indexer_group', indexer_services, 'indexer_services');
        await put_services_in_group('querier_group', querier_services, 'querier_services');
        console.log("SERVICES ADDED TO GROUP");

        const run_remote = (group_name, remote, args = []) => new Promise((resolve, reject) => {
            distribution[group_name].comm.send(args, remote, (e, v) => {
                resolve();
            });
        });

        const add_one_link = (link) => new Promise((resolve, reject) => {

            const links_to_crawl_data = Array.from([link].keys()).join('\n');
            const crawled_links_data = Array.from([''].keys()).join('\n');

            distribution.crawler_group.store.put(links_to_crawl_data, 'links_to_crawl', (e1, v1) => {
                distribution.crawler_group.store.put(crawled_links_data, 'crawled_links', (e2, v2) => {
                    console.log(e1, v1, e2, v2)
                    resolve();
                });
            });
        });
        await add_one_link('.wiki.Cnidaria');

        await run_remote('crawler_group', { gid: 'local', service: 'crawler_services', method: 'initialize'});
        await run_remote('indexer_group', { gid: 'local', service: 'indexer_services', method: 'initialize'});
        await run_remote('querier_group', { gid: 'local', service: 'querier_services', method: 'initialize'});
        console.log("GROUPS INITIALIZED");

        console.log("GROUPS STARTING...");
        await Promise.all([
            run_remote('crawler_group', { gid: 'local', service: 'crawler_services', method: 'update'}),
            run_remote('indexer_group', { gid: 'local', service: 'indexer_services', method: 'update'}),
            run_remote('querier_group', { gid: 'local', service: 'querier_services', method: 'update'})
        ]);

        cb();
    };

    for(let i = 0; i < num_nodes; i++) {
        await spawn_nx(nodes[i]);
    }

    setup_cluster(() => {
        finish();
    });

    const finish = async () => {
        console.log("SHUTTING DOWN CLUSTER...");
        for(let i = 0; i < num_nodes; i++) await stop_nx(nodes[i]);
        server.close();
    }
});