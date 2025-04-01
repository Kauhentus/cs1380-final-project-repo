const LZ = require('lz-string');
const fs = require('fs');

const data = fs.readFileSync('./store_plantae_fungi/69d7978acde9d2fecb0c34c78597c9e590afeb6f5dc1fc2c4238f52871cd729e/-wiki-Cosmarium-botrytis').toString();
const decompressed = LZ.decompressFromBase64(JSON.parse(data).value);
console.log(decompressed);