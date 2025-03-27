#!/usr/bin/env node

// timeQueries.js
//
// This script is just a simple way to measure the throughput of the query

const { execSync } = require('child_process');
const { performance } = require('perf_hooks');

// ----- 1) Define some queries -----
const queries = [
  // Randomly chose queries from the global index
  "includ ural",
  "incred challeng qubit",
  "independ particl separ",
  "insect firefli bioluminesc",
  "instanc swirl blue",
  "link check stuff",
  "live organ marin",
  "maintain stabl quantum",
  "phenomenon quantum entangl",
  "level check stuff"
];


function runQuery(queryString) {
  return execSync(`./query.js ${queryString}`, { encoding: 'utf-8' });
}

function main() {
  const startTime = performance.now();

  queries.forEach((q) => {
    runQuery(q);
  });

  const endTime = performance.now();

  const elapsedSec = (endTime - startTime) / 1000;
  const numQueries = queries.length;
  const qps = numQueries / elapsedSec;

  console.log(`${numQueries} queries in ${elapsedSec.toFixed(3)} s`);
  console.log(`Throughput: ${qps.toFixed(3)} queries/second`);
}

main();