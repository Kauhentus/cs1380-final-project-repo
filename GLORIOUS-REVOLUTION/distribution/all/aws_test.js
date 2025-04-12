const distribution = require('../../config');
const id = distribution.util.id;

const mygroupGroup = {};
let localServer = null;

const n1 = {ip: '52.87.227.26', port: 8001};
const n2 = {ip: '54.86.86.95', port: 8002};
const n3 = {ip: '34.227.104.192', port: 8003};

mygroupGroup[id.getSID(n1)] = n1;
mygroupGroup[id.getSID(n2)] = n2;
mygroupGroup[id.getSID(n3)] = n3;

  // Now, start the base listening node
distribution.node.start((server) => {
    localServer = server;
    const mygroupConfig = {gid: 'mygroup'};
    distribution.local.groups.put(mygroupConfig, mygroupGroup, async (e, v) => {
        console.log(e, v);

        const random_kv_pairs = [];
        for(let i = 0; i < 10000; i++){
            random_kv_pairs.push({k: id.getID(Math.random()), v: id.getID(Math.random())});
        }

        const perf = require('perf_hooks').performance;
        const start = perf.now();
        
        const results_1 = await Promise.all(random_kv_pairs.map(pair => new Promise((resolve, reject) => {
            distribution.mygroup.mem.put(pair.v, pair.k, (e, v) => resolve(e, v));
        })));
        console.log(`T1: ${perf.now() - start}ms`);
        console.log(results_1)

        const results_2 = await Promise.all(random_kv_pairs.map(pair => new Promise((resolve, reject) => {
            distribution.mygroup.mem.get(pair.k, (e, v) => resolve(e, v));
        })));
        console.log(`T2: ${perf.now() - start}ms`);
    });
});

// 1000 KV pairs on 3 nodes:
// T1: 373.179375ms, throughput = 0.373ms per put
// T2: 773.915917ms, throughput = 0.773ms per get

// 10000 KV pairs on 3 nodes:
// T1: 6615.147167ms, throughput = 0.661ms per put
// T2: 13280.36746ms, throughput = 1.328ms per get

// first it's SO COOL that our distributed KV store is working on AWS EC2!!
// put is a lot faster than get which is interesting ... I would expect them to be the same so maybe I have some implementation issue
// and it seems like it's not scaling linearly with the number of nodes ... maybe there's an HTTP request bottleneck that needs to be addressed
// since my computer, the network, and EC2 probably won't allow 10k HTTP requests to be sent in parallel
// but this is a great start and I think we can make it work