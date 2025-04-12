const crawler = function (config) {
    const context = {};
    context.gid = config.gid || 'all';

    return {
        initialize: (config, group, callback) => {
            distribution[context.gid].comm.send(
                [config, group],
                { service: 'crawler', method: 'initialize' },
                callback
            );
        },

        add_link_to_crawl: (name, callback) => {
            distribution[context.gid].comm.send(
                [name],
                { service: 'crawler', method: 'add_link_to_crawl' },
                callback
            );
        },

        crawl_one: (name, callback) => {
            distribution[context.gid].comm.send(
                [name],
                { service: 'crawler', method: 'crawl_one' },
                callback
            );
        },

        get_stats: (name, node, callback) => {
            distribution[context.gid].comm.send(
                [name, node],
                { service: 'crawler', method: 'get_stats' },
                callback
            );
        },

        save_maps_to_disk: (name, node, callback) => {
            distribution[context.gid].comm.send(
                [name, node],
                { service: 'crawler', method: 'save_maps_to_disk' },
                callback
            );
        }
    };
};

module.exports = crawler;
