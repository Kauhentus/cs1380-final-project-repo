const http = require('http');
const url = require('url');
const log = require('../util/log');
const routes = require('../local/routes');
const util = require('../util/util');

/*
    The start function will be called to start your node.
    It will take a callback as an argument.
    After your node has booted, you should call the callback.
*/


const start = function(callback) {
  const server = http.createServer((req, res) => {
    /* Your server will be listening for PUT requests. */

    // Write some code...
    res.writeHead(200, { 'Content-Type': 'application/json' });

    /*
      The path of the http request will determine the service to be used.
      The url will have the form: http://node_ip:node_port/service/method
    */


    // Write some code...
    let parsedUrl = url.parse(req.url);
    let path = parsedUrl.pathname; // /service/method
    let pathParts = path.split('/').filter(Boolean); // Remove empty parts
    let gid = pathParts[0]; // The first part of the path is the gid
    let service = pathParts[1];
    let method = pathParts[2];

    let routeConfig = {
      gid: gid,
      service: service
    }

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

    let body = [];

    req.on('data', (chunk) => {
      body.push(chunk);
    });

    req.on('end', () => {
      /* Here, you can handle the service requests. 
4
      Use the local routes service to get the service you need to call.
      You need to call the service with the method and arguments provided in the request.
      Then, you need to serialize the result and send it back to the caller.
      */

      // Write some code...
      let bodyData = Buffer.concat(body).toString();
      let args = [];
      try {
        const parsed = JSON.parse(bodyData);
        args = util.deserialize(parsed);
      } catch (e) {
        log(`Error parsing JSON: ${e}`);
      }
      routes.get(routeConfig, (err, serviceObj) => {
        if (err) {
          // Couldn’t get the service
          res.end(JSON.stringify(util.serialize([err, null])));
        } else {
          // Wrap the call in a try/catch in case the method call throws synchronously
          if (!serviceObj[method]) {
            res.end(JSON.stringify(util.serialize([new Error(`Method ${method} not found`), null])));
            return
          } 
          const distributedFlag = gid && gid !== 'local'

          const formatResp = (error, value) => {
            if (distributedFlag) {
              const errMap = error || {}
              const resMap = value || {}
              res.end(JSON.stringify(util.serialize([errMap, resMap])));
            } else {
              if (error) {
                res.end(JSON.stringify(util.serialize([error, null])));
              } else {
                res.end(JSON.stringify(util.serialize([null, value])));
              }
            }
          }



          try {
            if (args.length > 0) {
              serviceObj[method](...args, (error, value) => {
                formatResp(error, value);
              });
            } else {
              serviceObj[method]((error, value) => {
                formatResp(error, value);
              });
            }
          } catch (e) {
            // If service[method] threw an error before calling any callback
            res.end(JSON.stringify(util.serialize([e, null])));
          }
        
        }
      });
    }
  );
  });


  /*
    Your server will be listening on the port and ip specified in the config
    You'll be calling the `callback` callback when your server has successfully
    started.

    At some point, we'll be adding the ability to stop a node
    remotely through the service interface.
  */

  server.listen(global.nodeConfig.port, global.nodeConfig.ip, () => {
    log(`Server running at http://${global.nodeConfig.ip}:${global.nodeConfig.port}/`);
    global.distribution.node.server = server;
    callback(server);
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
