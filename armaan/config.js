/*
    This file is used to determine which implementation of the distribution library to use.
    It can either be:
    1. The reference implementation from the library @brown-ds/distribution
    2. Your own, local implementation

    Which one to be used by the tests is determined by the value of the property "useLibrary" in the package.json file.
*/

const {useLibrary} = require('./package.json');

let distribution = null;

if (useLibrary) {
  try {
    distribution = require('@brown-ds/distribution'); // Reference implementation
  } catch (error) {
    if (error.code === 'MODULE_NOT_FOUND') {
      console.error('Library not found, using local implementation');
      distribution = require('./distribution'); // Local implementation
    }
  }
} else {
  distribution = require('./distribution'); // Local implementation
}

if (require.main === module) {
  distribution.node.start(distribution.node.config.onStart);
}

module.exports = distribution;
