// distribution/local/querier.js
const fs = require('fs');
const path = require('path');

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};

function initialize(callback) {
  callback = callback || cb;
  const distribution = require('../../config');
  fs.appendFileSync(global.logging_path, `QUERIER INITIALIZING... ${new Date()}\n`);

  callback();
}

module.exports = {
  initialize,
};