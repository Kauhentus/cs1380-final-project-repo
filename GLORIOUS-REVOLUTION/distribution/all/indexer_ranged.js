const indexer_ranged = function (config) {
    const context = {};
    context.gid = config.gid || 'all';

    return {
        initialize: (config, group, callback) => {
            distribution[context.gid].comm.send(
                [config, group],
                { service: 'indexer', method: 'initialize' },
                callback
            );
        },

        add_link_to_crawl: (name, callback) => {
            distribution[context.gid].comm.send(
                [name],
                { service: 'indexer', method: 'add_link_to_crawl' },
                callback
            );
        },

        save_maps_to_disk: (name, node, callback) => {
            distribution[context.gid].comm.send(
                [name, node],
                { service: 'indexer', method: 'save_maps_to_disk' },
                callback
            );
        },
    };
};

module.exports = indexer_ranged;
