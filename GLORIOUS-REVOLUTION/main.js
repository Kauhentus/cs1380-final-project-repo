const { resolve } = require('path');
const distribution = require('./config.js');
const id = distribution.util.id;

const num_nodes = 8;
const nodes = [];
// const nodes = [
//     { ip: '3.87.36.179', port: 8000 },
//     { ip: '54.205.32.141', port: 8000 },
//     { ip: '18.207.186.50', port: 8000 },
//     { ip: '3.89.92.113', port: 8000 },
//     { ip: '52.205.252.133', port: 8000 },
//     { ip: '44.201.146.230', port: 8000 },
//     { ip: '44.201.140.46', port: 8000 },
//     { ip: '3.83.105.244', port: 8000 }
// ];
const spawn_nodes_locally = true;

const nids = [];
const crawler_group = {};
const crawler_group_config = { gid: 'crawler_group', hash: id.naiveHash };
const indexer_group = {};
const indexer_group_config = { gid: 'indexer_group', hash: id.naiveHash };
const indexer_ranged_group = {};
const indexer_ranged_group_config = { gid: 'indexer_ranged_group', hash: id.naiveHash };
const querier_group = {};
const querier_group_config = { gid: 'querier_group', hash: id.naiveHash };

for(let i = 0; i < num_nodes; i++) {
    nodes.push({ ip: '127.0.0.1', port: 7110 + i });
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

    // ##############
    // INITIALIZATION
    // ##############
    if(spawn_nodes_locally){
        for(let i = 0; i < num_nodes; i++) await spawn_nx(nodes[i]);
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
    await run_remote('crawler_group', { gid: 'local', service: 'crawler', method: 'initialize'});
    await run_remote('indexer_group', { gid: 'local', service: 'indexer', method: 'initialize'});
    await run_remote('indexer_ranged_group', { gid: 'local', service: 'indexer_ranged', method: 'initialize'});
    await run_remote('querier_group', { gid: 'local', service: 'querier', method: 'initialize'});
    console.log("GROUPS INITIALIZED");

    // #######################
    // POPULATE WITH MISC INFO
    // #######################
    await new Promise((resolve, reject) => {
        const link = '/wiki/Cnidaria';
        const remote = { node: get_nx(link), gid: 'local', service: 'crawler', method: 'add_link_to_crawl'};
        distribution.local.comm.send([link], remote, (e, v) => {
        
            const remote = { node: get_nx(link), gid: 'local', service: 'crawler', method: 'save_maps_to_disk'}
            distribution.local.comm.send([], remote, (e, v) => {
                resolve();
            });
        });
    });

    // ######################
    // MANUAL CONTROL PANEL 
    // ######################
    const do_query = false;
    const do_range_query = false;
    const query_string = 'leafy sour';
    const range_query_string = 'Antipatharia';

    const do_crawl_and_indexing = true;

    if(do_query){
        await new Promise((resolve, reject) => {
            distribution.querier_group.querier.query_one(query_string, async (e, v) => {
                const results = v.map(result => ({
                    binomialName: result.pageInfo.binomialName,
                    url: result.docId,
                    tf_idf: result.tf_idf,
                }));
                console.log(e, results);

                // getting the data from the store
                await new Promise((resolve, reject) => {
                    distribution.crawler_group.store.get(results[0].url, (e, v) => {
                        console.log(`${'#'.repeat(v.title.length + 4)}`);
                        console.log(`# ${v.title} #`);
                        console.log(`${'#'.repeat(v.title.length + 4)} \n`);
                        console.log(v.description);
                        resolve();
                    })
                });
            
                resolve();
            });
        });
    }

    if(do_range_query){
        await new Promise((resolve, reject) => {
            distribution.querier_group.querier.query_range(range_query_string, async (e, v) => {
                const results = v;
                console.log(e, results);

                // 283 of hexacorallia
                // 100 of octocorallia
                // 386 of anthozoa

                resolve();
            });
        });
    }

    if(do_crawl_and_indexing){

        await new Promise((resolve, reject) => {
            distribution.crawler_group.crawler.start_crawl((e, v) => resolve());
        });
        await new Promise((resolve, reject) => {
            distribution.crawler_group.indexer.start_index((e, v) => resolve());
        });
        await new Promise((resolve, reject) => {
            distribution.crawler_group.indexer_ranged.start_index((e, v) => resolve());
        });

        const log_stats = () => {
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
                        console.log(`CRAWLER_STATS:`);
                        console.log(`  links_to_crawl = ${total_links_to_crawl}`);
                        console.log(`  crawled_links = ${total_crawled_links}`);
                        console.log(`  throughput = ${crawler_throughput} pages/sec`);

                        let total_links_to_index = 0;
                        let total_indexed_links = 0;
                        let indexer_throughput = 0;
                        Object.keys(v2).map(key => {
                            total_links_to_index += v2[key].links_to_index || 0;
                            total_indexed_links += v2[key].indexed_links || 0;
                            indexer_throughput += (v2[key].metrics.documentsIndexed / (v2[key].metrics.totalIndexTime / 1000)) || 0;
                        });
                        console.log(`INDEXER_STATS:`);
                        console.log(`  links_to_index = ${total_links_to_index}`);
                        console.log(`  indexed_links = ${total_indexed_links}`);
                        console.log(`  throughput = ${indexer_throughput} pages/sec`);

                        let total_links_to_range_index = 0;
                        let total_range_indexed_links = 0;
                        let range_indexer_throughput = 0;
                        Object.keys(v3).map(key => {
                            total_links_to_range_index += v3[key].links_to_range_index || 0;
                            total_range_indexed_links += v3[key].range_indexed_links || 0;
                            range_indexer_throughput += (v3[key].metrics.documentsIndexed / (v3[key].metrics.totalIndexTime / 1000)) || 0;
                        });
                        console.log(`RANGE_INDEXER_STATS:`);
                        console.log(`  links_to_range_index = ${total_links_to_range_index}`);
                        console.log(`  range_indexed_links = ${total_range_indexed_links}`);
                        console.log(`  throughput = ${range_indexer_throughput} pages/sec`);
                        console.log('');
                    });
                });
            });
        }
        setInterval(() => log_stats(), 10000);
        log_stats();


        setTimeout(async () => {
            await new Promise((resolve, reject) => {
                const remote = { gid: 'local', service: 'crawler', method: 'save_maps_to_disk'}
                distribution.indexer_group.comm.send([], remote, (e, v) => {
                    resolve();
                });
            });
            await new Promise((resolve, reject) => {
                const remote = { gid: 'local', service: 'indexer', method: 'save_maps_to_disk'}
                distribution.indexer_group.comm.send([], remote, (e, v) => {
                    resolve();
                });
            });
            await new Promise((resolve, reject) => {
                const remote = { gid: 'local', service: 'indexer_ranged', method: 'save_maps_to_disk'}
                distribution.indexer_group.comm.send([], remote, (e, v) => {
                    resolve();
                });
            });
        }, 10000);
    }

    // for(let i = 0; i < num_nodes; i++) await stop_nx(nodes[i]);
    // server.close();
});