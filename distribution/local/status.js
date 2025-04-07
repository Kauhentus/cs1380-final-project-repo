const log = require('../util/log');

const status = {
};

global.moreStatus = {
  sid: global.distribution.util.id.getSID(global.nodeConfig),
  nid: global.distribution.util.id.getNID(global.nodeConfig),
  counts: 0,
  memory: {
    heapTotal: process.memoryUsage().heapTotal,
    heapUsed: process.memoryUsage().heapUsed
  }
};

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    log(v);
  }
};

status.get = function(configuration, callback) {
  callback = callback || cb;
  if (!configuration) {
    callback(new Error('Configuration is required'), {});
    return;
  }
  switch(configuration) {
    case 'sid':
      callback(null, global.moreStatus.sid);
      break;
    case 'nid':
      callback(null, global.moreStatus.nid);
      break;
    case 'counts':
      callback(null, global.moreStatus.counts);
      break;
    case 'ip':
      callback(null, global.nodeConfig.ip);
      break;
    case 'port':
      callback(null, global.nodeConfig.port);
      break;
    case 'heapTotal':
      callback(null, process.memoryUsage().heapTotal);
      break;
    case 'heapUsed':
      callback(null, process.memoryUsage().heapUsed);
      break;
    case 'memory':
      // Return the entire memory usage object
      callback(null, {
        heapTotal: process.memoryUsage().heapTotal,
        heapUsed: process.memoryUsage().heapUsed
      });
      break;
    default:
      callback(new Error(`Status property "${configuration}" not found`), null);
  }
};

status.spawn = require('@brown-ds/distribution/distribution/local/status').spawn;

status.stop = require('@brown-ds/distribution/distribution/local/status').stop; 

module.exports = status;
