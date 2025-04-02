const distribution = require('../../config');
const id = distribution.util.id;

const n1 = { ip: '127.0.0.1', port: 7110 };
const n2 = { ip: '127.0.0.1', port: 7111 };
const n3 = { ip: '127.0.0.1', port: 7112 };
const testXGroup = {};

distribution.node.start((server) => {

  testXGroup[id.getSID(n1)] = n1;
  testXGroup[id.getSID(n2)] = n2;
  testXGroup[id.getSID(n3)] = n3;

  const test = () => {    
    const perf = require('perf_hooks').performance;
    const start = perf.now();

    const mapper = (key, value) => {
      const chars = value.replace(/\s+/g, '').split('');
      return chars.map(w => ({[w]: 1}));
    };
  
    const reducer = (key, values) => {
      const out = {};
      out[key] = values.reduce((a, b) => a + b);
      return out;
    };
  
    const dataset = [];
    
    const num_docs = 300;
    const doc_length = 100;
    for(let i = 0; i < num_docs; i++){
      const doc = [];
      for(let j = 0; j < doc_length; j++){
        doc.push(String.fromCharCode(Math.floor(Math.random() * 26) + 97));
      }
      dataset.push({[i]: doc.join('')});
    }
  
    const doMapReduce = (cb) => {
      distribution.testX.store.get(null, (e, v) => {
        distribution.testX.mr.exec({keys: v, map: mapper, reduce: reducer}, (e, v) => {
          console.log("DONE!!", v);
          console.log(`T5: ${perf.now() - start}ms`);

          // num_docs = 100, doc_length = 100 -> 1991ms
          // num_docs = 100, doc_length = 200 -> 3901ms
          // num_docs = 100, doc_length = 300 -> 5648ms

          // num_docs = 200, doc_length = 100 -> 3201ms
          // num_docs = 300, doc_length = 100 -> 3978ms

          finish();
        });
      });
    };
    let cntr = 0;
    dataset.forEach((o) => {
      const key = Object.keys(o)[0];
      distribution.testX.store.put(o[key], key, (e, v) => {
        if(++cntr === dataset.length) doMapReduce();
      });
    });
  };

  distribution.local.status.spawn(n1, (e, v) => {
    distribution.local.status.spawn(n2, (e, v) => {
      distribution.local.status.spawn(n3, (e, v) => {
        const testXConfig = {gid: 'testX'};
        distribution.local.groups.put(testXConfig, testXGroup, (e, v) => {
          distribution.testX.groups.put(testXConfig, testXGroup, (e, v) => {
            test();
          });
        });
      });
    });
  });

  const finish = () => {
    const remote = {service: 'status', method: 'stop'};
    remote.node = n1;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n2;
      distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n3;
        distribution.local.comm.send([], remote, (e, v) => {
          server.close();
        });
      });
    });
  }
});