const http = require('http');
const url = require('url');
const log = require('../util/log');
const { writeFileSync, appendFileSync } = require('fs');

/*
    The start function will be called to start your node.
    It will take a callback as an argument.
    After your node has booted, you should call the callback.
*/

const services_list = [
  'status', 'routes', 'comm', 'groups', 'mem', 'store', 
  'crawler', 'indexer', 'indexer_ranged', 'querier'
];

const start = function(callback) {

  // console.log("STARTING NODE HTTP SERVER...", global.nodeConfig.port, global.nodeConfig.ip)
  const server = http.createServer((req, res) => {
    // const serialize = require('../../config').util.serialize;
    // const deserialize = require('../../config').util.deserialize;
    const serialize = distribution.util.serialize;
    const deserialize = distribution.util.deserialize;

    /* Your server will be listening for PUT requests. */
    // Write some code...
    if(req.method !== 'PUT'){
      res.writeHead(405, 'method not allowed');
      res.end();
      return;
    }

    /*
      The path of the http request will determine the service to be used.
      The url will have the form: http://node_ip:node_port/service/method
    */

    // Write some code...
    const parsed_url = url.parse(req.url);
    const gid = parsed_url.pathname.split('/')[1];
    const service = parsed_url.pathname.split('/')[2];
    const method = parsed_url.pathname.split('/')[3];
    if(!gid || !service || !method){
      res.writeHead(400, 'bad request');
      res.end();
      return;
    }
    // console.log("NODE REQUEST", global.nodeConfig, gid, service, method);
    // appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `NODE REQUEST ${global.nodeConfig.gid} ${gid} ${service} ${method}\n`);
    // appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `    $${Object.keys(distribution)}\n`);
    // appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `    !${Object.keys(global.distribution)}\n`);

    /*

      A common pattern in handling HTTP requests in Node.js is to have a
      subroutine that collects all the data chunks belonging to the same
      request. These chunks are aggregated into a body variable.

      When the req.on('end') event is emitted, it signifies that all data from
      the request has been received. Typically, this data is in the form of a
      string. To work with this data in a structured format, it is often parsed
      into a JSON object using JSON.parse(body), provided the data is in JSON
      format.

      Our nodes expect data in JSON format.
    */

    // Write some code...
    let body_data = '';
    req.on('data', (chunk) => {
      body_data += chunk;
    });
    req.on('end', () => {
      try {
        let data;
        if(body_data === ''){
          data = [];
        } else {
          data = deserialize(body_data);
          if(!Array.isArray(data)) data = [data];
        }

        // console.log('NODE', gid, service, method);

        if(method === 'call' && !services_list.includes(service)){
          const rpc = global.toLocal[service];

          rpc(...data, (err, result) => {
            if(err){
              res.writeHead(400, 'rpc error');
              res.end();
              return;
            }
            const serialized_result = serialize(result);
            res.writeHead(200, 'ok', {'Content-Type': 'application/json'});
            res.end(serialized_result);
            return;
          });
          return;
        }

        // comment out this case cuz I don't think its needed?
        // else if(services_list.includes(service)){

          distribution.local.routes.get({gid, service}, (err, service_function) => {
            if(err instanceof Error){
              res.writeHead(400, 'node local routing error');
              res.end();
              return;
            }

            let method_function = service_function[method];
            // appendFileSync('./PLEASE-NODE-ERROR.txt', `${JSON.stringify(data)}\n`);
            method_function(...data, (err, result) => {
              if(err instanceof Error){
                // appendFileSync('./PLEASE-NODE-ERROR.txt', `${err}\n`);
                res.writeHead(400, 'service function error ' + `${JSON.stringify(err)} (${gid} ${service} ${method})`);
                res.end();
                return;
              }
  
              let serialized_result;
              if(typeof err === "object" && typeof result === "object"){
                serialized_result = serialize({e: err, v: result});
              } else {
                serialized_result = serialize(result);
              }
              res.writeHead(200, 'ok', {'Content-Type': 'application/json'});
              res.end(serialized_result);
              return;
            });
          });
          return;

        // } else {
        //   res.writeHead(400, `service [${service}] does not exist error`);
        //   res.end();
        //   return;
        // }
      } catch (error) {
        // appendFileSync(`./temp-${global.nodeConfig.port}.txt`, `${error}\n`);
        
        res.writeHead(400, 'parsing or response errors: ' + error);
        res.end();
        return;
      }
    });
  });

  // Write some code...

  /*
    Your server will be listening on the port and ip specified in the config
    You'll be calling the `callback` callback when your server has successfully
    started.

    At some point, we'll be adding the ability to stop a node
    remotely through the service interface.
  */

  server.listen(global.nodeConfig.port, global.nodeConfig.ip, () => {
    // log(`Server running at http://${global.nodeConfig.ip}:${global.nodeConfig.port}/`);
    // console.log("STARTED SERVER", `at http://${global.nodeConfig.ip}:${global.nodeConfig.port}/`)
    global.distribution.node.server = server;
    callback(server, () => {});
  });

  server.on('error', (error) => {
    // server.close();
    log(`Server error: ${error}`);
    throw error;
  });
};

module.exports = {
  start: start,
};
