// distribution/local/querier.js
const fs = require("fs");
const path = require("path");

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};

function initialize(callback) {
  callback = callback || cb;
  const distribution = require("../../config");
  fs.appendFileSync(
    global.logging_path,
    `QUERIER INITIALIZING... ${new Date()}\n`
  );

  callback();
}
/**
 *
 * @param {*} query this is a list or term
 * @param {*} callback
 */
function query_one(queryConfiguration, callback) {
  const fs = require("fs");
  callback = callback || cb;
  const { terms, prefix, totalDocCount } = queryConfiguration;
  const query = terms.join(" ");

  fs.appendFileSync(
    global.logging_path,
    `QUERIER QUERYING... ${query}:  ${terms}\n`
  );
  console.log(`Querier: querying... ${query}`);
  function calculateIDF(df, totalDocuments) {
    return Math.log((totalDocuments + 1) / (df + 1)) + 1;
  }
  function combineScores(docScores, termScores) {
    for (const [docId, score] of Object.entries(termScores)) {
      if (docScores[docId]) {
        docScores[docId].score += 1.2 * score;
        docScores[docId].matchedTerms += 1;
      } else {
        docScores[docId] = {
          score: score,
          matchedTerms: 1,
        };
      }
    }
    return docScores;
  }
  const bulkReadConfig = {
    key: `prefix-${prefix}.json`,
    gid: "indexer_group",
  };
  distribution.local.store.read_bulk(bulkReadConfig, (error, prefixData) => {
    if (error) {
      console.error(`Error reading prefix data for ${prefix}:`, error);
      return callback(error);
    }
    try {
      const parsedBulk = JSON.parse(prefixData);

      let docScores = {};
      let termDeets = {};
      const docTermMetaData = {};

      for (const term of terms) {
        if (!parsedBulk[term]) {
          // TODO: Continue or do we skip this term?
          console.log(`Term '${term}' not found in prefix ${prefix}`);
          continue;
        }

        const termData = parsedBulk[term];
        const df = termData.df;
        const idf = calculateIDF(df, totalDocCount);
        // console.log(`TERM: ${JSON.stringify(termData)}`);

        termDeets[term] = {
          df: df,
          idf: idf,
          documents: Object.keys(termData.postings).length,
        };
        const termScores = {};

        for (const [docId, posting] of Object.entries(termData.postings)) {
          let score = posting.tf * idf;

          if (posting.ranking) {
            score *= posting.ranking.taxonomyBoost || 1.0;
            score *= posting.ranking.binomialBoost || 1.0;
            score *= posting.ranking.positionBoost || 1.0;
          }

          // console.log(posting);

          termScores[docId] = score;
          docTermMetaData[docId] = {
            taxonomyLevel: posting ? posting.taxonomyLevel : null,
            isBinomial: posting ? posting.isBinomial : null,
            pageInfo: posting ? posting.pageInfo : null,
          };
        }

        docScores = combineScores(docScores, termScores);
      }

      const results = Object.entries(docScores).map(([docId, data]) => {
        return {
          docId: docId,
          score: data.score,
          matchedTerms: data.matchedTerms,
          matchRatio: data.matchedTerms / terms.length,
          termDetails: docTermMetaData[docId] || {},
        };
      });
      results.sort((a, b) => b.score - a.score);

      const topResults = results.slice(0, 50);
      const response = {
        prefix: prefix,
        queryTerms: terms,
        totalMatches: results.length,
        termStatistics: termDeets,
        results: topResults,
      };

      callback(null, response);
    } catch (error) {
      console.error(`Error processing term details: ${error}`);
    }
  });
}

module.exports = {
  initialize,
  query_one,
};
