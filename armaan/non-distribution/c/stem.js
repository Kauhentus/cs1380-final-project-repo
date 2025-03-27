#!/usr/bin/env node

/*
Convert each term to its stem
Usage: ./stem.js <input >output
*/

const readline = require('readline');
const natural = require('natural');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false,
});

rl.on('line', function(line) {
  // Print the Porter stem from `natural` for each element of the stream.
  const tokens = line.split(/\s+/);
  const stems = tokens.map((token) => natural.PorterStemmer.stem(token));
  console.log(stems.join(' '));
});
