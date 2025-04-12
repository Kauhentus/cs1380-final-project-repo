const querier = function (config) {
    const context = {};
    context.gid = config.gid || 'all';

    return {
        initialize: (config, group, callback) => {
            distribution[context.gid].comm.send(
                [config, group],
                { service: 'querier', method: 'initialize' },
                callback
            );
        },

        query_one: (query, callback) => {
            distribution[context.gid].comm.send(
                [query],
                { service: 'querier', method: 'query_one' },
                (e, v) => {
                    const query_words = query
                        .split(' ')
                        .filter(word => word.trim() !== '')
                        .map(word => word.trim().toLowerCase());

                    const data = Object
                        .values(v)
                        .filter(result => Array.isArray(result))
                        .flat();

                    // console.log(query_words);
                    // const fs= require('fs');
                    // fs.appendFileSync('TEMP.json', `${JSON.stringify(data, null, 2)}\n`);

                    const all_docs_query_tf_idf = {};
                    data.map(posting => {
                        const docId = posting.docId;
                        if(all_docs_query_tf_idf[docId] === undefined) all_docs_query_tf_idf[docId] = {};

                        const current_word = posting.word;
                        all_docs_query_tf_idf[docId][current_word] = posting;
                    });
                    Object.keys(all_docs_query_tf_idf).map(key => {
                        const this_doc_query_tf_idf = all_docs_query_tf_idf[key];
                        const queries_on_doc = Object.keys(this_doc_query_tf_idf);
                        const missing_queries = query_words.filter(query => !queries_on_doc.includes(query));
                        missing_queries.map(query_word => {
                            this_doc_query_tf_idf[query_word] = {
                                docId: key,
                                tf: 0.0000001,
                                ranking: {
                                  tf: 0.0000001,
                                  score: 0.0000001
                                },
                                tf_idf: 0.0000001,
                                query_word: query_word
                            };
                        });
                    });
                    console.log(all_docs_query_tf_idf);

                    const combine_by_binomial_name = (results) => {
                        const acc = results.reduce((map, item) => {
                            const key = item.pageInfo.binomialName;
                            if (!map[key]) {
                                map[key] = { ...item, ranking: { ...item.ranking }};
                            } else {
                                map[key].tf += item.tf;
                                map[key].tf_idf += item.tf_idf;
                                map[key].ranking.tf += item.ranking.tf;
                                map[key].ranking.score += item.ranking.score;
                            }
                            return map;
                        }, {});
                        return Object.values(acc);
                    }
                          
                    const results = combine_by_binomial_name(data)
                        .sort((a, b) => b.tf_idf - a.tf_idf);
                        
                    results.map(result => result.tf_idf = +result.tf_idf.toPrecision(4));

                    callback(null, results);
                }
            );
        }
    };
};

module.exports = querier;
