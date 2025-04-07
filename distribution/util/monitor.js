// Add this as distribution/util/monitor.js
const os = require('os');

// Simple monitoring system that doesn't require external dependencies
function createMonitor(jobId, groupId) {
  const startTime = Date.now();
  const stats = {
    jobId,
    groupId,
    phases: {
      MAP: { status: 'pending', progress: 0, startTime: 0, endTime: 0, errors: 0, processed: 0, total: 0 },
      SHUFFLE: { status: 'pending', progress: 0, startTime: 0, endTime: 0, errors: 0, processed: 0, total: 0 },
      REDUCE: { status: 'pending', progress: 0, startTime: 0, endTime: 0, errors: 0, processed: 0, total: 0 }
    },
    memoryUsage: [],
    nodes: {},
    errors: []
  };

  // Log memory usage every 30 seconds
  const memoryInterval = setInterval(() => {
    const used = process.memoryUsage();
    stats.memoryUsage.push({
      timestamp: Date.now(),
      heapTotal: Math.round(used.heapTotal / 1024 / 1024),
      heapUsed: Math.round(used.heapUsed / 1024 / 1024),
      rss: Math.round(used.rss / 1024 / 1024),
      cpuUsage: os.loadavg()[0]
    });
  }, 30000);

  const api = {
    // Start tracking a phase
    startPhase: (phase, total) => {
      if (stats.phases[phase]) {
        stats.phases[phase].status = 'running';
        stats.phases[phase].startTime = Date.now();
        stats.phases[phase].total = total || 0;
        stats.phases[phase].processed = 0;
        stats.phases[phase].progress = 0;
        logStatus(`Started ${phase} phase`);
      }
    },

    // Update progress for a phase
    updateProgress: (phase, processed, total) => {
      if (stats.phases[phase]) {
        stats.phases[phase].processed = processed;
        stats.phases[phase].total = total || stats.phases[phase].total;
        stats.phases[phase].progress = total ? Math.round((processed / total) * 100) : 0;
        
        // Log progress at meaningful intervals (5%, 25%, 50%, 75%, 100%)
        const progress = stats.phases[phase].progress;
        if (progress === 5 || progress === 25 || progress === 50 || 
            progress === 75 || progress === 100 || progress % 10 === 0) {
          logStatus(`${phase} phase: ${progress}% complete (${processed}/${total})`);
        }
      }
    },

    // End tracking a phase
    endPhase: (phase, results) => {
      if (stats.phases[phase]) {
        stats.phases[phase].status = 'completed';
        stats.phases[phase].endTime = Date.now();
        stats.phases[phase].progress = 100;
        
        const duration = (stats.phases[phase].endTime - stats.phases[phase].startTime) / 1000;
        logStatus(`Completed ${phase} phase in ${duration.toFixed(2)}s with ${results ? results.length : 0} results`);
      }
    },

    // Log an error
    logError: (phase, error, key) => {
      stats.errors.push({
        timestamp: Date.now(),
        phase,
        message: error.message,
        stack: error.stack,
        key
      });
      
      if (stats.phases[phase]) {
        stats.phases[phase].errors++;
      }
      
      console.error(`[ERROR][${phase}] ${error.message} ${key ? `(${key})` : ''}`);
    },

    // Register a node's status
    registerNode: (nodeId, status) => {
      stats.nodes[nodeId] = {
        ...stats.nodes[nodeId],
        ...status,
        lastUpdate: Date.now()
      };
    },

    // Get full stats
    getStats: () => {
      return {
        ...stats,
        currentTime: Date.now(),
        duration: (Date.now() - startTime) / 1000,
        currentMemory: process.memoryUsage()
      };
    },

    // Clean up monitoring resources
    shutdown: () => {
      clearInterval(memoryInterval);
      
      // Log final stats
      const fullStats = api.getStats();
      const totalDuration = fullStats.duration;
      
      logStatus(`Job completed in ${totalDuration.toFixed(2)}s`);
      logStatus(`MAP: ${stats.phases.MAP.processed} items processed with ${stats.phases.MAP.errors} errors`);
      logStatus(`SHUFFLE: ${stats.phases.SHUFFLE.processed} items processed with ${stats.phases.SHUFFLE.errors} errors`);
      logStatus(`REDUCE: ${stats.phases.REDUCE.processed} items processed with ${stats.phases.REDUCE.errors} errors`);
      
      return fullStats;
    }
  };

  // Helper to log status with timestamp and job ID
  function logStatus(message) {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}][${jobId}][${global.nodeConfig.port}] ${message}`);
  }

  return api;
}

module.exports = { createMonitor };