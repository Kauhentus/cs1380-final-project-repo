const distribution = require('./config.js');
const id = distribution.util.id;

const num_nodes = 8;
const nodes = [
    { ip: '54.146.240.134', port: 8000},
    { ip: '35.175.141.16', port: 8000},
    { ip: '34.201.116.60', port: 8000},
    { ip: '44.203.173.187', port: 8000},
    { ip: '52.91.248.98', port: 8000},
    { ip: '52.90.42.105', port: 8000},
    { ip: '54.208.109.135', port: 8000},
    { ip: '184.72.110.157', port: 8000}
];
// const nodes = [];
const nids = [];
const taxonomy_group_group = {};
const taxonomy_group_config = { gid: 'taxonomy_group' };
for(let i = 0; i < num_nodes; i++) {
    // nodes.push({ ip: '127.0.0.1', port: 7110 + i });
    nids.push(id.getNID(nodes[i]));
    taxonomy_group_group[id.getSID(nodes[i])] = nodes[i];
}

distribution.node.start(async (server) => {
    const spawn_nx = (nx) => new Promise((resolve, reject) => 
        distribution.local.status.spawn(nx, (e, v) => 
            resolve(e, v)));

    const stop_nx = (nx) => new Promise((resolve, reject) =>
        distribution.local.comm.send([], { service: 'status', method: 'stop', node: nx }, (e, v) =>
            resolve(e, v)));

    const get_nx = (link) => nodes[parseInt(id.getID(link).slice(0, 8), 16) % num_nodes];

    const setup_cluster = (cb) => {
        console.log("SETTING UP CLUSTER...")
        const crawlerService = {
            initialize: (cb) => {
                const fs = require('fs');

                if(!fs.existsSync(`./crawler-files`)) fs.mkdirSync(`./crawler-files`, { recursive: true });
                if(!fs.existsSync(`./crawler-files/logs`)) fs.mkdirSync(`./crawler-files/logs`, { recursive: true });
                global.log_file_path = `./crawler-files/logs/log-${global.nodeConfig.port}.txt`;
                // fs.writeFileSync(global.log_file_path, '');

                const links_to_crawl_map = new Map();
                const crawled_links_map = new Map();
                distribution.local.mem.put(links_to_crawl_map, 'links_to_crawl_map', (e, v) => {
                    distribution.local.mem.put(crawled_links_map, 'crawled_links_map', (e, v) => {

                        distribution.local.store.get('links_to_crawl', (e1, v1) => {
                            distribution.local.store.get('crawled_links', (e2, v2) => {

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

            add_link_to_crawl: (link, cb) => {
                // const fs = require('fs');
                // fs.appendFileSync(global.log_file_path, `Adding ${link} to crawl\n`);

                distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
                    distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {

                        if(links_to_crawl_map.has(link)) return cb();
                        if(crawled_links_map.has(link)) return cb();

                        distribution.local.mem.get('links_to_crawl_map', (e1, v1) => {
                            v1.set(link, true);
                            cb();
                        });
                    });
                });
            },

            get_stats: (cb) => {
                distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
                    distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {

                        const fs = require('fs');
                        const store_exists = fs.existsSync('/home/ec2-user/cs1380-final-project-repo/store');
                        const num_target_found = store_exists ? fs.readdirSync('/home/ec2-user/cs1380-final-project-repo/store')
                            .filter(folder => !folder.includes('.'))
                            .map(folder => `/home/ec2-user/cs1380-final-project-repo/store/${folder}`)
                            .map(folder => fs.readdirSync(folder).length)
                            .reduce((a, b) => a + b, 0) - num_nodes * 2 : 0;

                        const stats = {
                            links_to_crawl: links_to_crawl_map.size,
                            crawled_links: crawled_links_map.size,
                            num_target_found: num_target_found
                        }

                        cb(null, stats);
                    });
                });
            },

            save_maps_to_disk: (cb) => {
                distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
                    distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {

                        const links_to_crawl_data = Array.from(links_to_crawl_map.keys()).join('\n');
                        const crawled_links_data = Array.from(crawled_links_map.keys()).join('\n');

                        distribution.local.store.put(links_to_crawl_data, 'links_to_crawl', (e, v) => {
                            distribution.local.store.put(crawled_links_data, 'crawled_links', (e, v) => {
                                cb();
                            });
                        });
                    });
                });
            },

            crawl_one: (cb) => {
                distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
                    distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {                        
                        // get link to crawl
                        if(links_to_crawl_map.size === 0) return cb();
                        const [url, _] = links_to_crawl_map.entries().next().value;
                        links_to_crawl_map.delete(url);
                        if(crawled_links_map.has(url)) return cb();

                        // crawl it!
                        fetch(`https://en.wikipedia.org${url}`)
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
                                
                                // const is_sealife = hierarchy?.find(pair => pair[0] === 'phylum' && pair[1].includes('cnidaria'));
                                // const is_target_class = is_sealife;

                                const is_species_page = hierarchy && binomial_name && is_target_class;
                                if(is_species_page) {
                                    const page_text = root.text;
                                    const all_words = (page_text.match(/\b\w+\b/g) || [])
                                        .map(word => word.toLocaleLowerCase())
                                        .filter(word => !/^\d+$/.test(word));

                                    const species_data = {
                                        hierarchy: hierarchy,
                                        binomial_name: binomial_name,
                                        url: url,
                                        article_words: all_words,
                                    }
                                    const path_safe_url = url.replace(/\//g, '.');
                                    const LZ = require('lz-string');
                                    const compressed_data = LZ.compressToBase64(JSON.stringify(species_data));
                                    distribution.local.store.put(compressed_data, path_safe_url, (e, v) => {

                                    });
                                }
                                crawled_links_map.set(url, true);

                                distribution.local.mem.get('global_info', (e, v) => {
                                    const { nodes, num_nodes } = v;

                                    const get_nx = (link) => nodes[parseInt(distribution.util.id.getID(link).slice(0, 8), 16) % num_nodes];
                                    const new_links = [...new Set(is_target_class ? links_on_page : [])];
                                    new_links.map(link => {
                                        const remote = { node: get_nx(link), gid: 'local', service: 'crawler', method: 'add_link_to_crawl'};
                                        distribution.local.comm.send([link], remote, (e, v) => {});
                                    });
            
                                    setTimeout(() => {
                                        const fs = require('fs');
                                        // fs.appendFileSync(global.log_file_path, `Crawled ${url} with ${crawled_links_map.size} crawled with ${links_to_crawl_map.size} left\n`);
                                        cb();
                                    }, 1000);
                                });
                            });

                    });
                });
            }
        }

        distribution.local.groups.put(taxonomy_group_config, taxonomy_group_group, (e, v) => {
            distribution.taxonomy_group.groups.put(taxonomy_group_config, taxonomy_group_group, (e, v) => {

                distribution.taxonomy_group.routes.put(crawlerService, 'crawler', (e, v) => {

                    const remote = {gid: 'local', service: 'crawler', method: 'initialize'};
                    distribution.taxonomy_group.comm.send([], remote, (e, v) => {

                        const remote = { gid: 'local', service: 'mem', method: 'put'};
                        distribution.taxonomy_group.comm.send([{ nodes, num_nodes }, 'global_info'], remote, (e, v) => {

                            // const link = '/wiki/Plant';
                            // const link = '/wiki/Animal';
                            const link = '/wiki/Cnidaria';

                            const remote = { node: get_nx(link), gid: 'local', service: 'crawler', method: 'add_link_to_crawl'};
                            distribution.local.comm.send([link], remote, (e, v) => {

                                cb();
                                

                            });

                        });
                        
                    });
                    
                });
        
            });
                    
        });
    };

    const run_task = async (cb) => {
        console.log("STARTING MAIN RUN TASK...")      

        const crawl_iter = () => new Promise((resolve, reject) => {
            const remote = { gid: 'local', service: 'crawler', method: 'crawl_one'};
            distribution.taxonomy_group.comm.send([], remote, (e, v) => {
                resolve();
            });
        })

        const save_iter = () => new Promise((resolve, reject) => {
            const remote = { gid: 'local', service: 'crawler', method: 'save_maps_to_disk'};
            distribution.taxonomy_group.comm.send([], remote, (e, v) => {
                resolve();
            });
        });

        const sleep_iter = () => new Promise((resolve, reject) => {
            setTimeout(() => {
                resolve();
            }, 100 + 100 * Math.random());
        });

        const stat_iter = () => new Promise((resolve, reject) => {
            const remote = { gid: 'local', service: 'crawler', method: 'get_stats'};
            distribution.taxonomy_group.comm.send([], remote, (e, v) => {
                console.log(v);
                let sum_links_to_crawl = 0;
                let sum_crawled_links = 0;
                let sum_num_target_found = 0;
                Object.keys(v).forEach(key => {
                    sum_links_to_crawl += v[key].links_to_crawl;
                    sum_crawled_links += v[key].crawled_links;
                    sum_num_target_found += v[key].num_target_found;
                });
                console.log(`sum_links_to_crawl = ${sum_links_to_crawl}, sum_crawled_links = ${sum_crawled_links}`);
                console.log("TOTAL PAGES SO FAR =", sum_num_target_found);

                resolve();
            });
        });

        for(let i = 0; i < 100000; i++){
            console.log("ITER =", i);
            await sleep_iter();
            await crawl_iter();
            await stat_iter();
            if(i % 5 === 0) {
                await save_iter();
            }
        }
        await save_iter();

        cb();
    };

    // for(let i = 0; i < num_nodes; i++) {
    //     await spawn_nx(nodes[i]);
    // }

    setup_cluster(() => {
        run_task(() => {
            finish();
        })
    });

    const finish = async () => {
        console.log("SHUTTING DOWN CLUSTER...");
        for(let i = 0; i < num_nodes; i++) await stop_nx(nodes[i]);
        server.close();
    }
});