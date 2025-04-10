const querier = function (config) {
    const context = {};
    context.gid = config.gid || 'all';

    return {
        initialize: (config, group, callback) => {
            distribution[context.gid].comm.send(
                [config, group],
                { service: 'indexer', method: 'initialize' },
                callback
            );
        }
    };
};

module.exports = querier;
