const indexer = function (config) {
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

        start_index: (callback) => {
            distribution[context.gid].comm.send(
                [],
                { service: 'indexer', method: 'start_index' },
                callback
            );
        },

        set_service_state: (state, callback) => {
            distribution[context.gid].comm.send(
                [state],
                { service: 'indexer', method: 'set_service_state' },
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

        index_one: (name, callback) => {
            distribution[context.gid].comm.send(
                [name],
                { service: 'indexer', method: 'index_one' },
                callback
            );
        },       
        
        get_idf_doc_count: (callback) => {
            distribution[context.gid].comm.send(
                [],
                { service: 'indexer', method: 'get_idf_doc_count' },
                callback
            );
        },

        get_stats: (callback) => {
            distribution[context.gid].comm.send(
                [],
                { service: 'indexer', method: 'get_stats' },
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

module.exports = indexer;
