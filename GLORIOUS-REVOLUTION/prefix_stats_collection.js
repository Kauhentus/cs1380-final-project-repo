const fs = require('fs');

const indexer_group_paths = fs.readdirSync('./store_big')
    .filter(folder => folder !== '.DS_Store')
    .map(folder => `./store_big/${folder}`)
    .map(path => fs.readdirSync(`${path}/indexer_group`)
        .map(file => `${path}/indexer_group/${file}`))
    .flat();

let word_counter = [];
let prefix_counter = [];

const batch_size = 100;
console.log(`Total number of batches: ${indexer_group_paths.length / batch_size | 0}`)
for(let i = 0; i < indexer_group_paths.length; i += batch_size) {
    console.log(`Batch ${i / batch_size | 0} of ${indexer_group_paths.length / batch_size | 0}`);
    let batch_word_counter = {};
    let batch_prefix_counter = {};

    const batch = indexer_group_paths.slice(i, i + batch_size);
    batch.map(filepath => fs.readFileSync(filepath, 'utf8'))
        .map(data => JSON.parse(data))
        .map(data => {
            const keys = Object.keys(data);
            keys.map(key => batch_word_counter[key] = data[key].df);
            keys.map(key => {
                const prefix = key.slice(0, 2);
                batch_prefix_counter[prefix] = (batch_prefix_counter[prefix] || 0) + 1;
            });
        });

    batch_word_counter = Object.entries(batch_word_counter);
    batch_prefix_counter = Object.entries(batch_prefix_counter);

    word_counter.push(...batch_word_counter);
    prefix_counter.push(...batch_prefix_counter);
}

word_counter = word_counter.sort(([, va], [, vb]) => -(va - vb))
prefix_counter = prefix_counter.sort(([, va], [, vb]) => va - vb)

// fs.writeFileSync('./word_counter.json', word_counter.map(pair => `${pair[0]} ${pair[1]}`).join('\n'));
fs.writeFileSync('./prefix_counter.json', prefix_counter.map(pair => `${pair[0]} ${pair[1]}`).join('\n'));
fs.writeFileSync('./prefix_counter_copy_paste.json', prefix_counter.filter(pair => pair[1] > 100).map(pair => `'${pair[0]}'`).join(', ')); 

console.log(word_counter);
console.log(prefix_counter);

// GOALS
// 1. most common prefixes
// 2. most common words

// console.log(indexer_group_paths)