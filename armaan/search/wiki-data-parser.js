// This module provides functions to handle the wiki data from your distributed storage system

// If using in Node.js with the LZ compression:
const LZ = require("lz-string");

/**
 * Decompresses and parses wiki data from LZ-compressed string format
 * @param {string} compressedData - The LZ-compressed data string
 * @returns {Object|null} Parsed object or null if failed
 */
function decompressAndParseWikiData(compressedData) {
  try {
    // Handle input as JSON if it has value property (from your distribution.tfidf.store.get)
    let dataToDecompress = compressedData;
    if (
      typeof compressedData === "string" &&
      compressedData.startsWith("{") &&
      compressedData.includes('"value"')
    ) {
      const jsonData = JSON.parse(compressedData);
      dataToDecompress = jsonData.value;
    }

    // Decompress the data
    const decompressed = LZ.decompressFromBase64(dataToDecompress);

    // Parse the decompressed JSON
    return JSON.parse(decompressed);
  } catch (error) {
    console.error("Error decompressing or parsing data:", error);
    return null;
  }
}

/**
 * Parses wiki data from string format (if not compressed)
 * @param {string} dataString - The string data
 * @returns {Object|null} Parsed object or null if failed
 */
function parseWikiData(dataString) {
  try {
    return JSON.parse(dataString);
  } catch (error) {
    console.error("Error parsing data:", error);
    return null;
  }
}

/**
 * Utility to process multiple wiki data items
 * @param {Array} keys - Array of keys to process
 * @param {Function} getDataCallback - Callback function to get data for a key
 * @returns {Promise<Array>} Array of processed data objects
 */
async function processWikiDataBatch(keys, getDataCallback) {
  const results = [];

  for (const key of keys) {
    try {
      // Get data using the provided callback
      const data = await getDataCallback(key);

      // Determine if we need to decompress
      let processedData;
      if (typeof data === "string" && data.includes("binomial_name")) {
        // Data is already a string in JSON format
        processedData = parseWikiData(data);
      } else {
        // Data might be compressed
        processedData = decompressAndParseWikiData(data);
      }

      if (processedData) {
        results.push({
          key,
          data: processedData,
        });
      }
    } catch (err) {
      console.error(`Error processing key ${key}:`, err);
    }
  }

  return results;
}

/**
 * Example usage with your distribution system:
 *
 * // Get keys from the store
 * distribution.local.comm.send([
 *   { gid: "tfidf", key: null } // Get all keys
 * ], {
 *   service: "store",
 *   method: "get",
 *   node: node
 * }, (err, keys) => {
 *   if (err) return console.error(err);
 *
 *   // Process each key
 *   processWikiDataBatch(keys, (key) => {
 *     return new Promise((resolve, reject) => {
 *       distribution.tfidf.store.get(key, (err, value) => {
 *         if (err) return reject(err);
 *         resolve(value);
 *       });
 *     });
 *   }).then(results => {
 *     // Work with the processed data
 *     results.forEach(({ key, data }) => {
 *       console.log(`Data for ${key}:`);
 *       console.log(`Binomial name: ${data.binomial_name}`);
 *       // etc.
 *     });
 *   });
 * });
 */

// Helper functions to work with the parsed data

/**
 * Extract taxonomy information in a readable format
 * @param {Object} wikiData - The parsed wiki data object
 * @returns {Object} Formatted taxonomy object
 */
function extractTaxonomy(wikiData) {
  if (!wikiData.hierarchy) return {};

  const taxonomy = {};
  wikiData.hierarchy.forEach(([level, name]) => {
    taxonomy[level] = name;
  });

  return taxonomy;
}

/**
 * Get word frequency from article_words
 * @param {Object} wikiData - The parsed wiki data object
 * @returns {Object} Word frequency map
 */
function getWordFrequency(wikiData) {
  if (!wikiData.article_words) return {};

  const frequency = {};
  wikiData.article_words.forEach((word) => {
    frequency[word] = (frequency[word] || 0) + 1;
  });

  return frequency;
}

/**
 * Find common words between multiple wiki articles
 * @param {Array} wikiDataArray - Array of wiki data objects
 * @returns {Array} Array of common words
 */
function findCommonWords(wikiDataArray) {
  if (!wikiDataArray.length) return [];

  // Get the first article's words as a set
  const commonWords = new Set(wikiDataArray[0].article_words);

  // Intersect with each other article
  for (let i = 1; i < wikiDataArray.length; i++) {
    const currentWords = new Set(wikiDataArray[i].article_words);
    for (const word of commonWords) {
      if (!currentWords.has(word)) {
        commonWords.delete(word);
      }
    }
  }

  return Array.from(commonWords);
}

// Export the functions for use in other modules
module.exports = {
  decompressAndParseWikiData,
  parseWikiData,
  processWikiDataBatch,
  extractTaxonomy,
  getWordFrequency,
  findCommonWords,
};
