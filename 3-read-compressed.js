const LZ = require("lz-string");
const fs = require("fs");

const data = fs
  .readFileSync(
    "armaan/store/c65b0147f5b849e3906c4667dd83b19dc3f684e63d9614bc9fbfd9404266e0d6/tfidf/-wiki--C3-97-Aegilotriticum-erebunii"
  )
  .toString();
const decompressed = LZ.decompressFromBase64(JSON.parse(data).value);
console.log(data, decompressed);
