const crypto = require("crypto");
const assert = require("assert");

/** ---- Copied from your code snippets ---- **/

function getID(obj) {
  const hash = crypto.createHash("sha256");
  hash.update(JSON.stringify(obj));
  return hash.digest("hex");
}

function getNID(node) {
  node = { ip: node.ip, port: node.port };
  return getID(node);
}

function idToNum(id) {
  const n = parseInt(id, 16);
  assert(!isNaN(n), "idToNum: id is not in KID form!");
  return n;
}

function naiveHash(kid, nids) {
  // Sort lexicographically (as hex strings)
  nids.sort();
  return nids[idToNum(kid) % nids.length];
}

function consistentHash(kid, nids) {
  const kidID = idToNum(kid);
  const numToID = {};
  for (const nid of nids) {
    numToID[idToNum(nid)] = nid;
  }
  // Build the ring
  const ring = [kidID];
  for (const nid of nids) {
    ring.push(idToNum(nid));
  }
  ring.sort((a, b) => a - b);

  const index = ring.indexOf(kidID);
  if (index === ring.length - 1) {
    // if kidID is the largest on the ring, wrap to the first
    return numToID[ring[0]];
  }
  // else pick the next in the ring
  return numToID[ring[index + 1]];
}

/** 
 * A typical Rendezvous hash is: 
 *   pick the node with the highest hash of (kid + nodeID). 
 * We'll define that quickly here. 
 */
function rendezvousHash(kid, nids) {
  let bestNode = null;
  let bestValue = null;
  for (const nid of nids) {
    // Combine the key and the node’s ID in some consistent way
    const combo = kid + nid;
    const val = parseInt(crypto.createHash("sha256").update(combo).digest("hex"), 16);

    if (bestValue === null || val > bestValue) {
      bestValue = val;
      bestNode = nid;
    }
  }
  return bestNode;
}

/** ---- Set up the nodes exactly as in your test ---- **/
const nodeIds = [
  getNID({ ip: "192.168.0.1", port: 8000 }),
  getNID({ ip: "192.168.0.2", port: 8000 }),
  getNID({ ip: "192.168.0.3", port: 8000 }),
  getNID({ ip: "192.168.0.4", port: 8000 }),
];

/** ---- Brute force some simple keys until we find a match ---- **/
function findKey() {
  for (let i = 0; i < 100000; i++) {
    // Try i as a string
    const key = i.toString();
    const kid = getID(key);

    const a = naiveHash(kid, [...nodeIds]);        // pass a copy so .sort() doesn't mutate
    const b = rendezvousHash(kid, [...nodeIds]);
    const c = consistentHash(kid, [...nodeIds]);

    if (a === b && b === c) {
      return key; // Return the first key that works
    }
  }
  return null;
}

const result = findKey();
console.log("Found a key that works:", result);