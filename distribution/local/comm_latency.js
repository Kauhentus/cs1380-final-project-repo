// const distribution = require('../../config.js');

distribution.node.start((server) => {
    const local = distribution.local;
    const id = distribution.util.id;

    const node = distribution.node.config;
    const remote = {node: node, service: 'status', method: 'get'};
    const message = ['sid'];

    const perf = require('perf_hooks').performance;
    const start = perf.now();
    let counter = 0;
    for(let i = 0; i < 10000; i++){
        if(i % 1000 == 0) console.log("step", i)
        local.comm.send(message, remote, (e, v) => {
            counter += 1;
        });
    }

    console.log(`T6: ${perf.now() - start}ms`);
    server.close();
});