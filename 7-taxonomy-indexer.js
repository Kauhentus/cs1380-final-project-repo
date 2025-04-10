const distribution = require('./config.js');
const id = distribution.util.id;

const num_nodes = 4;

const nodes = [];
const nids = [];
const taxonomy_group_group = {};
const taxonomy_group_config = { gid: 'taxonomy_group' };
for(let i = 0; i < num_nodes; i++) {
    nodes.push({ ip: '127.0.0.1', port: 7110 + i });
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

    const get_nx = (link) => nodes[parseInt(id.getID(link).slice(0, num_nodes), 16) % num_nodes];

    const setup_cluster = (cb) => {
        console.log("SETTING UP CLUSTER...")
        const indexerService2 = {
            initialize: (cb) => {
                const fs = require('fs');

                if(!fs.existsSync(`./crawler-files`)) fs.mkdirSync(`./crawler-files`, { recursive: true });
                if(!fs.existsSync(`./crawler-files/logs`)) fs.mkdirSync(`./crawler-files/logs`, { recursive: true });
                global.log_file_path = `./crawler-files/logs/log-${global.nodeConfig.port}.txt`;
                fs.writeFileSync(global.log_file_path, '');

                // ################################
                // distributed hierarchy extraction
                if(true){
                    distribution.local.store.put('', 'hierarchy_stage_1', (e, v) => {

                        fs.appendFileSync(global.log_file_path, `Node ${global.nodeConfig.port} initialized hierarchy_stage_1. ${e} ${v}\n`)

                        distribution.local.store.get(null, async (e, v) => {

                            fs.appendFileSync(global.log_file_path, `Node ${global.nodeConfig.port} initialized store with ${v ? v.length : 0} entries.\n`);
                            
                            const species_keys = v.filter(key => key.startsWith('wiki'));
                            const batch_size = 1000; 
                            const batches = [];
                            for(let i = 0; i < species_keys.length; i += batch_size) {
                                const batch = species_keys.slice(i, i + batch_size);
                                batches.push(batch);
                            }
        
                            for(let i = 0; i < batches.length; i++) {
                                const batch = batches[i];
                                fs.appendFileSync(global.log_file_path, `Indexing batch ${i + 1} / ${batches.length}\n`);

                                await Promise.all(batch.map(key => new Promise((resolve, reject) => {
                                    distribution.local.store.get(`-${key}`, async (e, v) => {
                                        if(e) {
                                            fs.appendFileSync(global.log_file_path, `${key} Error: ${e}\n`);
                                            return;
                                        }

                                        const content = JSON.parse(v);
                                        const hierarchy = content['hierarchy'];
        
                                        const append_to_store = (state, config) => new Promise((resolve, reject) => {
                                            distribution.local.store.append(state, config, (e, v) => {
                                                resolve(1);
                                            });
                                        });
        
                                        const appends_to_do = [['', 'hierarchy_stage_1']];
                                        for(let i = 0; i < hierarchy.length; i++) {
                                            const associative_map = `A: ${hierarchy[i]} <=> -${key}`;
                                            appends_to_do.push([associative_map, 'hierarchy_stage_1']);
                                        }
                                        for(let i = 0; i < hierarchy.length; i++) {
                                            if(i > 0){
                                                const backward_map = `B: ${hierarchy[i - 1]} => ${hierarchy[i]}`;
                                                appends_to_do.push([backward_map, 'hierarchy_stage_1']);
                                            }
                                        }
                                        await Promise.all(appends_to_do.map(pair => append_to_store(pair[0], pair[1])));
        
                                        resolve();
                                    });
                                })));
                            }
        
                            cb();
                        });
                    });
                } else {
                    cb();
                }
            },

            separate_AB: (cb) => {
                distribution.local.store.get_as_string('hierarchy_stage_1', async (e, v) => {
                    const fs = require('fs');
                    const lines = v
                        .split('\n')
                        .slice(1)
                        .filter(line => !(line.includes('{"type":"string","value":""}')))
                        .filter(line => line.length > 0);

                    const lines_A = lines.filter(line => line.includes('{"type":"string","value":"A: '));
                    const lines_B = lines.filter(line => line.includes('{"type":"string","value":"B: '));
                    const serialized_A = lines_A.join('\n');

                    distribution.local.store.put(serialized_A, 'hierarchy_stage_2_A', (e, v) => {
                        cb(null, lines_B);
                    });
                });
            }
        }

        distribution.local.groups.put(taxonomy_group_config, taxonomy_group_group, (e, v) => {
            distribution.taxonomy_group.groups.put(taxonomy_group_config, taxonomy_group_group, (e, v) => {

                distribution.taxonomy_group.routes.put(indexerService2, 'indexer', (e, v) => {

                    // create hierachy_1
                    const remote = {gid: 'local', service: 'indexer', method: 'initialize'};
                    distribution.taxonomy_group.comm.send([], remote, (e, v) => {

                        const remote = { gid: 'local', service: 'mem', method: 'put'};
                        distribution.taxonomy_group.comm.send([{ nodes, num_nodes }, 'global_info'], remote, (e, v) => {

                            // create hierarchy_2
                            if(true) {
                                const remote = {gid: 'local', service: 'indexer', method: 'separate_AB'};
                                distribution.taxonomy_group.comm.send([], remote, (e, v) => {

                                    const cat_filter = [
                                        'kingdom',
                                        'clade',
                                        'order',
                                        'family',
                                        'genus',
                                        'subfamily',
                                        'supertribe',
                                        'tribe',
                                        'subtribe',
                                        'domain',
                                        'phylum',
                                        'class',
                                        'superfamily',
                                        'division',
                                        'subgenus',
                                        'section',
                                        'series',
                                        'subclass',
                                        'subphylum',
                                        'suborder',
                                        'species group',
                                        'subsection',
                                        'subdivision',
                                        'infraorder',
                                        'species',
                                        'stem group',
                                        'superorder',
                                        'species complex',
                                        'plesion'
                                    ];

                                    const all_lines_B = [...new Set(Object.keys(v).map(key => v[key]).flat())]
                                        .map(line => distribution.util.deserialize(line))
                                        .map(line => line.slice(3))
                                        .map(line => line.split(' => '))
                                        .map(pair => pair.map(item => item.split(',')))
                                        .filter(pair => cat_filter.includes(pair[0][0]))

                                    const mappings = {};

                                    // enforce top -> bottom order
                                    all_lines_B.forEach(mapping => {
                                        const [p_cat, p_name] = mapping[0];
                                        const [c_cat, c_name] = mapping[1];

                                        if(!mappings[p_name]) mappings[p_name] = { 'category': p_cat, 'name': p_name, children: [], parent: '' };
                                        if(!mappings[c_name]) mappings[c_name] = { 'category': c_cat, 'name': c_name, children: [], parent: '' };
                                        mappings[p_name].children.push(c_name);
                                    });

                                    // and build bottom -> top order for consistent maps
                                    Object.keys(mappings).map(p_key => {
                                        mappings[p_key].children.map(c => c.parent = p_key);
                                    });

                                    const remote = { gid: 'local', service: 'store', method: 'put'};
                                    distribution.taxonomy_group.comm.send([mappings, 'hierarchy-stage-2-B'], remote, (e, v) => {
                                        cb();
                                    });
                                });
                            } 
                            
                            else {
                                cb();
                            }

                        });
                        
                    });
                    
                });
        
            });
                    
        });
    };

    const run_task = async (cb) => {
        console.log("STARTING MAIN RUN TASK...")      

        cb();
    };

    for(let i = 0; i < num_nodes; i++) {
        await spawn_nx(nodes[i]);
    }

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