const indexer_ranged = function (config) {
    const context = {};
    context.gid = config.gid || 'all';

    return {
        initialize: (config, group, callback) => {
            distribution[context.gid].comm.send(
                [config, group],
                { service: 'indexer_ranged', method: 'initialize' },
                callback
            );
        },

        start_index: (callback) => {
            distribution[context.gid].comm.send(
                [],
                { service: 'indexer_ranged', method: 'start_index' },
                callback
            );
        },

        index_one: (name, callback) => {
            distribution[context.gid].comm.send(
                [name],
                { service: 'indexer_ranged', method: 'index_one' },
                callback
            );
        },

        add_link_to_crawl: (name, callback) => {
            distribution[context.gid].comm.send(
                [name],
                { service: 'indexer_ranged', method: 'add_link_to_crawl' },
                callback
            );
        },

        get_stats: (callback) => {
            distribution[context.gid].comm.send(
                [],
                { service: 'indexer_ranged', method: 'get_stats' },
                callback
            );
        },
        
        save_maps_to_disk: (name, node, callback) => {
            distribution[context.gid].comm.send(
                [name, node],
                { service: 'indexer_ranged', method: 'save_maps_to_disk' },
                callback
            );
        },
    };
};

module.exports = indexer_ranged;
