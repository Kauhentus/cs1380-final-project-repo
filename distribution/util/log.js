/* Create log file */
const fs = require('fs');
const path = require('path');

const logFile = path.join('/tmp', 'log.txt');

function log(message, severity) {
  if (severity === undefined) {
    severity = 'info';
  }

  const now = new Date();
  const date = `${new Intl.DateTimeFormat('en-GB',
      {
        day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit',
        minute: '2-digit', second: '2-digit', hour12: false,
      })
      .format(now)}.${String(now.getMilliseconds() * 1000).padStart(6, '0')}`;

  fs.appendFileSync(logFile, `${date} [${global.nodeConfig.ip}:${global.nodeConfig.port}\
(${global.moreStatus.sid})] [${severity}] ${message}\n`);
}

module.exports = log;
