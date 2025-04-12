const id = require('../util/id');

const status = {};

global.moreStatus = {
  sid: global.distribution.util.id.getSID(global.nodeConfig),
  nid: global.distribution.util.id.getNID(global.nodeConfig),
  counts: 0,
};

status.get = function(configuration, callback) {
  // console.log("STATUS GET", configuration, callback.toString())
  callback = callback || function() { };

  if(Array.isArray(configuration)){
    if(configuration.length != 1){
      callback(new Error('status get configutation only takes 1 argument'));
      return;
    } else {
      configuration = configuration[0];
    }
  }

  if(configuration === 'nid') {
    callback(null, id.getNID(global.nodeConfig));
    return;
  }

  if (configuration === 'sid') {
    callback(null, id.getSID(global.nodeConfig));
    return;
  }

  if(configuration === 'ip') {
    callback(null, global.nodeConfig.ip);
    return;
  }

  if (configuration === 'port') {
    callback(null, global.nodeConfig.port);
    return;
  }

  if (configuration === 'counts') {
    callback(null, global.moreStatus.counts);
    return;
  }

  if (configuration === 'heapTotal') {
    callback(null, process.memoryUsage().heapTotal);
    return;
  }
  if (configuration === 'heapUsed') {
    callback(null, process.memoryUsage().heapUsed);
    return;
  }

  callback(new Error(`Status key <${configuration}> not found`));
};

status.spawn = require('@brown-ds/distribution/distribution/local/status').spawn; 
status.stop = require('@brown-ds/distribution/distribution/local/status').stop; 

module.exports = status;
