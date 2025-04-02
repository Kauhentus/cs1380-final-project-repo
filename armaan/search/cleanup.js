const { node } = require("@brown-ds/distribution");
const distribution = require("../distribution");

const n1 = { ip: "127.0.0.1", port: 7110 };
// const n2 = { ip: "127.0.0.1", port: 7111 };
// const n3 = { ip: "127.0.0.1", port: 7112 };
// const n3 = {ip: '172.31.3.4', port: 9003};
// const n4 = {ip: '127.0.0.1', port: 1234};
// const n5 = {ip: '127.0.0.1', port: 9001};

// let nodeList = [n1, n2, n3, n4, n5];

// Slice off the first two elements (node and script path)
// const args = process.argv.slice(2);

// Check if an argument was provided
// if (args.length === 0) {
//   console.error('Please provide an index argument.');
//   process.exit(1);
// }

// const index = args[0];

// let spawnNode = nodeList[index - 1];
// console.log(spawnNode)

const remote = { service: "status", method: "stop" };
remote.node = n1;
distribution.local.comm.send([], remote, (e, v) => {
  //   remote.node = n2;
  //   distribution.local.comm.send([], remote, (e, v) => {
  //     remote.node = n3;
  //     distribution.local.comm.send([], remote, (e, v) => {
  process.exit(0);
  //     });
  //   });
});
