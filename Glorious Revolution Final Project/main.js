const { resolve } = require('path');
const distribution = require('./config.js');
const id = distribution.util.id;

const num_nodes = 4;
const nodes = [];

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

    // for(let i = 0; i < num_nodes; i++) await spawn_nx(nodes[i]);

    // ##############
    // INITIALIZATION
    // ##############
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

    await new Promise((resolve, reject) => {
        distribution.querier_group.querier.query_one('leafy sour', (e, v) => {

        // distribution.querier_group.querier.query_one('juveniles', async (e, v) => {
            const results = v.map(result => ({
                binomialName: result.pageInfo.binomialName,
                url: result.docId,
                tf_idf: result.tf_idf,
            }));
            console.log(e, results);

            // getting the data from the store
            // await new Promise((resolve, reject) => {
            //     distribution.crawler_group.store.get('/wiki/Lion%27s_mane_jellyfish', (e, v) => {
            //         console.log(e, v);
            //         resolve();
            //     })
            // });
        
            resolve();
        });
    });

    let max_iter = 10000;
    let crawl_loop_iters = 0;
    let index_loop_iters = 0;

    const crawl_one = () => new Promise((resolve, reject) => {
        const remote = { gid: 'local', service: 'crawler', method: 'crawl_one'}
        distribution.crawler_group.comm.send([], remote, (e, v) => {
            crawl_loop_iters++;
            resolve();
        });
    });
    const crawl_loop = async () => {
        try {
            while (crawl_loop_iters < max_iter) {
                if(crawl_loop_iters % 10 == 0) console.log(`crawler ${crawl_loop_iters}`);
                await crawl_one();
            }
        } catch (err) {
            console.error('crawlLoop failed:', err);
            setTimeout(crawl_loop, 1000);
        }
    }

    const index_one = () => new Promise((resolve, reject) => {
        setTimeout(() => {
            const remote = { gid: 'local', service: 'indexer', method: 'index_one'}
            distribution.indexer_group.comm.send([], remote, (e, v) => {
                if(Object.values(v).some(data => data.status !== 'skipped')) index_loop_iters++;
                resolve();
            });
        }, 100);
    });
    const index_loop = async () => {
        try {
            while (index_loop_iters < max_iter) {
                if(index_loop_iters % 10 == 0) console.log(`indexer ${index_loop_iters}`);
                await index_one();
            }
        } catch (err) {
            console.error('indexLoop failed:', err);
            setTimeout(index_loop, 1000);
        }
    }

    // crawl_loop();
    // index_loop();

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
    }, 30000);

    // for(let i = 0; i < num_nodes; i++) await stop_nx(nodes[i]);
    // server.close();
});