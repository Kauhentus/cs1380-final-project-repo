const serialize = require('./serialization').serialize;
const deserialize = require('./serialization').deserialize;

// time this with node perf
const perf = require('perf_hooks').performance;
let start = perf.now();
for(let i = 0; i < 1000000; i++){
    deserialize(serialize(1));
    deserialize(serialize("WOWZER"));
    deserialize(serialize(false));
    deserialize(serialize(null));
    deserialize(serialize(undefined));
}
console.log(`T2: ${perf.now() - start}ms`);

start = perf.now();
for(let i = 0; i < 1000000; i++){
    deserialize(serialize(() => {}));
    deserialize(serialize((n) => n * (n - 1)));
    deserialize(serialize(function abba() {}));
    deserialize(serialize(function () {}));
    deserialize(serialize(function magic(n) { return n * 2; }));
}
console.log(`T3: ${perf.now() - start}ms`);

start = perf.now();
for(let i = 0; i < 1000000; i++){
    deserialize(serialize([]));
    deserialize(serialize({}));
    deserialize(serialize(new Error("pogmilk")));
    deserialize(serialize(new Date()));
    deserialize(serialize([4, 2, "!"]));
}
console.log(`T4: ${perf.now() - start}ms`);