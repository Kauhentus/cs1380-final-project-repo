#!/usr/bin/env node

/*
Extract all URLs from a web page.
Usage: ./getURLs.js <base_url>
*/

const readline = require('readline');
const {JSDOM} = require('jsdom');
const {URL} = require('url');

// 1. Read the base URL from the command-line argument using `process.argv`.

// arg[0] is the path to node
// arg[1] is the path to this script
// arg[2] is the base URL

if (process.argv.length !== 3) {
  console.error('Usage: ./getURLs.js <base_url>');
  process.exit(1);
}

let baseURL = process.argv[2];

if (baseURL.endsWith('index.html')) {
  baseURL = baseURL.slice(0, baseURL.length - 'index.html'.length);
} else {
  baseURL += '/';
}

const rl = readline.createInterface({
  input: process.stdin,
});

let htmlBuffer = '';

rl.on('line', (line) => {
  // 2. Read HTML input from standard input (stdin) line by line using the `readline` module.
  htmlBuffer += line + '\n';
});

rl.on('close', () => {
  // 3. Parse HTML using jsdom
  const dom = new JSDOM(htmlBuffer);
  const document = dom.window.document;

  // Get the list of all anchor tags with an href attribute
  const urls = document.querySelectorAll('a[href]');

  // Process each URL
  // - If the URL is relative, prepend the base URL
  // - If the URL is absolute, print it
  for (const url of urls) {
    const href = url.getAttribute('href');

    try {
      // Resolve the href against the base URL
      const absoluteURL = new URL(href, baseURL).href;
      // 5. Print each absolute URL to the console, one per line.
      console.log(absoluteURL);
    } catch (e) {
      // Ignore invalid URLs
    }
  }
});


