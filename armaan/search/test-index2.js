const distribution = require("../config.js");
const LZ = require("lz-string");
const fs = require("fs");
const mr = require("../../jdistribution/all/mr.js");

// Set up a single node for testing
const node = { ip: "127.0.0.1", port: 7110 };
const testGroup = {};
const testConfig = { gid: "tfidf" };
testGroup[distribution.util.id.getSID(node)] = node;

// Main function to run the TF-IDF calculation
distribution.node.start(async (server) => {
  console.log("SETTING UP TF-IDF TEST NODE...");

  // Helper function to spawn a node
  const spawn_node = (node) =>
    new Promise((resolve, reject) =>
      distribution.local.status.spawn(node, (e, v) => {
        console.log(
          `Spawned node at ${node.ip}:${node.port} with result:`,
          e ? e : v
        );
        resolve(e, v);
      })
    );

  // Helper function to stop a node
  const stop_node = (node) =>
    new Promise((resolve, reject) =>
      distribution.local.comm.send(
        [],
        { service: "status", method: "stop", node: node },
        (e, v) => resolve(e, v)
      )
    );

  // Start the node
  await spawn_node(node);

  // Set up the group
  distribution.local.groups.put(testConfig, testGroup, (e, v) => {
    if (e) {
      console.error("Error setting up group:", e);
      finish();
      return;
    }

    console.log("Group set up successfully, starting TF-IDF calculation...");

    global.LZString = LZ;

    // Define the mapper function
    // This processes each document and emits word -> [doc, count] pairs
    const mapper = function (key, value) {
      try {
        console.log(`Mapper processing key: ${key}`);
        const decompressed = LZ.decompressFromBase64(value.value);
        if (!decompressed) {
          console.log(`Failed to decompress document: ${key}`);
          return [];
        }

        console.log(
          `Decompressed document for key: ${key}, length: ${decompressed.length}`
        );

        console.log(
          `Decompressed document for key: ${key}, length: ${decompressed.length}`
        );

        const docData = JSON.parse(decompressed);
        const docId = docData.url;
        const words = docData.article_words || [];

        // Count word occurrences in this document
        const wordCounts = {};
        words.forEach((word) => {
          // Skip very short words and normalize to lowercase
          if (word.length <= 2) return;
          const cleanWord = word.toLowerCase();
          wordCounts[cleanWord] = (wordCounts[cleanWord] || 0) + 1;
        });

        // Emit each word with document ID and count
        return Object.entries(wordCounts).map(([word, count]) => {
          console.log(
            `Mapper emitting word: ${word}, docId: ${docId}, count: ${count}`
          );
          return { [word]: { docId, count, totalWords: words.length } };
        });
      } catch (err) {
        console.error(`Error in mapper for ${key}:`, err);
        return [];
      }
    };

    // Define the reducer function
    // This calculates TF-IDF for each word across all documents
    const reducer = function (word, values) {
      console.log(
        `Reducer processing word: ${word}, with ${
          values.length
        } values : ${JSON.stringify(values).substring(0, 100)}...`
      );
      try {
        // Total number of documents
        const totalDocs = values.length;

        // Calculate term frequency for each document
        const docScores = values.map((value) => {
          const { docId, count, totalWords } = value;
          console.log(
            `Calculating TF for docId: ${docId}, count: ${count}, totalWords: ${totalWords}`
          );
          // TF = (Number of times term t appears in document) / (Total number of terms in document)
          const tf = count / totalWords;
          return { docId, tf, count };
        });

        // Calculate inverse document frequency
        // IDF = log(Total number of documents / Number of documents containing the term)
        const idf = Math.log(global.totalDocuments / totalDocs);

        // Calculate TF-IDF for each document
        const tfidfScores = docScores.map((doc) => {
          return {
            docId: doc.docId,
            tf: doc.tf,
            count: doc.count,
            idf: idf,
            tfidf: doc.tf * idf,
          };
        });

        // Return word with its TF-IDF scores across documents
        return {
          word: word,
          documentFrequency: totalDocs,
          scores: tfidfScores,
        };
      } catch (err) {
        console.error(`Error in reducer for ${word}:`, err);
        return { word: word, error: err.message };
      }
    };

    console.log(distribution.util.id.getNID(node)); // Log the node ID for debugging

    distribution.local.groups.get("tfidf", (err, group) => {
      console.log(`Retrieved group: ${JSON.stringify(group)}`);
    });

    // First, count the total number of documents
    let messageConfig = {
      service: "store",
      method: "get",
      node: node,
    };
    let message = [
      {
        gid: "tfidf", // Group ID where documents are stored
        key: null, // Get all keys in the group
      },
    ];
    distribution.local.comm.send(message, messageConfig, (err, keys) => {
      //   s
      // });
      // distribution.local.store.get({ gid: "tfidf", key: null }, (err, keys) => {
      //   if (err) {
      //     console.error("Error getting document keys:", err);
      //     finish();
      //     return;
      //   }

      // Set total documents count as a global variable for use in reducer
      global.totalDocuments = keys.length;
      console.log(`Processing ${global.totalDocuments} documents...`);

      if (global.totalDocuments === 0) {
        console.log("No documents found to process!");
      }

      // Configure and run the MapReduce job
      const mrConfig = {
        map: mapper,
        reduce: reducer,
        keys: keys, // Use all document keys from the store
      };

      //   console.log(`MapReduce job configuration: ${JSON.stringify(mrConfig)}`);

      // Execute the MapReduce job
      distribution.tfidf.mr.exec(mrConfig, (err, results) => {
        if (err) {
          console.error("Error executing MapReduce job:", err);
          finish();
          return;
        }

        console.log(
          `MapReduce job completed. Processed ${results.length} unique terms.`
        );

        // Save results to a file for inspection
        const resultsDir = "./tfidf-results";
        if (!fs.existsSync(resultsDir)) {
          fs.mkdirSync(resultsDir, { recursive: true });
        }

        fs.writeFileSync(
          `${resultsDir}/tfidf-results.json`,
          JSON.stringify(results, null, 2)
        );

        console.log(`Results saved to ${resultsDir}/tfidf-results.json`);

        // Optionally, save top terms for each document for quick lookup
        const docIndex = {};

        results.forEach((result) => {
          if (!result.scores) return;

          result.scores.forEach((score) => {
            if (!docIndex[score.docId]) {
              docIndex[score.docId] = [];
            }

            docIndex[score.docId].push({
              word: result.word,
              tfidf: score.tfidf,
              count: score.count,
            });
          });
        });

        // Sort terms by TF-IDF score for each document and keep top terms
        Object.keys(docIndex).forEach((docId) => {
          docIndex[docId].sort((a, b) => b.tfidf - a.tfidf);
          // Keep only top 100 terms per document
          docIndex[docId] = docIndex[docId].slice(0, 100);
        });

        fs.writeFileSync(
          `${resultsDir}/document-index.json`,
          JSON.stringify(docIndex, null, 2)
        );

        console.log(
          `Document index saved to ${resultsDir}/document-index.json`
        );

        finish();
      });
    });
  });

  // Cleanup function
  const finish = async () => {
    console.log("SHUTTING DOWN...");
    await stop_node(node);
    server.close();
  };
});
