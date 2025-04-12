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
                    const data = Object
                        .values(v)
                        .filter(result => Array.isArray(result))
                        .flat();

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
