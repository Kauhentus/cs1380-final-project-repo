const log = require('../util/log');

/*
  The toAsync function transforms a synchronous function that returns a value into an asynchronous one,
  which accepts a callback as its final argument and passes the value to the callback.
*/
global.toLocal = {};
function toAsync(func) {
  log(`Converting function to async: ${func.name}: ${func.toString().replace(/\n/g, '|')}`);

  // It's the caller's responsibility to provide a callback
  const asyncFunc = (...args) => {
    const callback = args.pop();
    try {
      const result = func(...args);
      callback(null, result);
    } catch (error) {
      callback(error);
    }
  };

  /* Overwrite toString to return the original function's code.
   Otherwise, all functions passed through toAsync would have the same id. */
  asyncFunc.toString = () => func.toString();
  return asyncFunc;
}

/**
 * This function generates an RPC stub by creating a service on the local node that is
 * then called everytime this RPC function is called. This allows for the code to always
 * run on this node, no matter where it is called from.
 */
let createRPC = require('@brown-ds/distribution/distribution/util/wire').createRPC;


module.exports = {
  createRPC: createRPC,
  toAsync: toAsync,
};
