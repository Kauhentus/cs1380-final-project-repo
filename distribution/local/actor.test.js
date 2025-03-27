const distribution = require('../../config');

test('testing status get local nid', (done) => {
    distribution.node.start((server) => {
        function cleanup(callback) {
            server.close();
            callback();
        }

        distribution.local.status.get("nid", (e, v) => {
            if(e){
                cleanup(() => done(e));
                return;
            }
            try {
                const nid = distribution.util.id.getID(global.nodeConfig);
                expect(v).toBe(nid);
                cleanup(done);
            } catch (error) {
                cleanup(() => done(error));
            }
        });
    });
});

test('testing local routes get returns actual object as well', (done) => {
    distribution.node.start((server) => {
        function cleanup(callback) {
            server.close();
            callback();
        }

        distribution.local.routes.get('status',
            (e, v) => {
            try {
                const status_object = v;
                expect(
                    status_object.hasOwnProperty('get') &&
                    status_object.hasOwnProperty('spawn') &&
                    status_object.hasOwnProperty('stop')
                ).toBe(true);
                cleanup(done);
            } catch (error) {
                if(e){
                    cleanup(() => done(e));
                    return;
                }
                cleanup(() => done(error));
            }
        });
    });
});

test('testing comm send returns desired stuff', (done) => {
    distribution.node.start((server) => {
        function cleanup(callback) {
            server.close();
            callback();
        }

        distribution.local.comm.send(
            ['nid'], 
            {node: global.nodeConfig, service: 'status', method: 'get'},
            (e, v) => {
                try {
                    const nid = distribution.util.id.getID(global.nodeConfig);
                    expect(v).toBe(nid);
                    cleanup(done);
                } catch (error) {
                    if(e){
                        cleanup(() => done(e));
                        return;
                    }
                    cleanup(() => done(error));
                }
        });
    });
});

test('testing comm sending a comm send (argument mismatch problem)', (done) => {
    distribution.node.start((server) => {
        function cleanup(callback) {
            server.close();
            callback();
        }
        console.log("REACH 1");

        distribution.local.comm.send(
            [['nid'], {node: global.nodeConfig, service: 'status', method: 'get'}], 
            {node: global.nodeConfig, service: 'comm', method: 'send'},
            (e, v) => {
                console.log("REACH 2", e, v)
                try {
                    const nid = distribution.util.id.getID(global.nodeConfig);
                    expect(v).toBe(nid);
                    cleanup(done);
                } catch (error) {
                    if(e){
                        cleanup(() => done(e));
                        return;
                    }
                    cleanup(() => done(error));
                }
        });
    });
});