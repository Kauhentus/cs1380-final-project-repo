/** @typedef {import("../types.js").Node} Node */

const assert = require('assert');
const crypto = require('crypto');

// The ID is the SHA256 hash of the JSON representation of the object
/** @typedef {!string} ID */

/**
 * @param {any} obj
 * @return {ID}
 */
function getID(obj) {
  const hash = crypto.createHash('sha256');
  hash.update(JSON.stringify(obj));
  return hash.digest('hex');
}

/**
 * The NID is the SHA256 hash of the JSON representation of the node
 * @param {Node} node
 * @return {ID}
 */
function getNID(node) {
  node = {ip: node.ip, port: node.port};
  return getID(node);
}

/**
 * The SID is the first 5 characters of the NID
 * @param {Node} node
 * @return {ID}
 */
function getSID(node) {
  return getNID(node).substring(0, 5);
}

function getMID(message) {
  const msg = {};
  msg.date = new Date().getTime();
  msg.mss = message;
  return getID(msg);
}

function idToNum(id) {
  const n = BigInt("0x" + id);
  // assert(!isNaN(n), "idToNum: id is not in KID form!");
  return n;
}

function naiveHash(kid, nids) {
  nids.sort();

  const index = idToNum(kid) % BigInt(nids.length);
  return nids[Number(index)];
}

function consistentHash(kid, nids) {
  let kidID = idToNum(kid);
  nids.sort((a, b) => idToNum(a) - idToNum(b));
  for (const nid of nids) {
    if (idToNum(nid) >= kidID) {
      return nid;
    }
  }
  return nids[0];
}

function rendezvousHash(kid, nids) {
  // We want to combine the KID with each NID
  // and then we want to convert them to a 
  // numerical representation and sort them 
  // to choose the max

  let max = -Infinity;
  let maxNID = '';
  for (const nid of nids) {
    const combined = kid + nid;
    const combinedHash = getID(combined);
    const combinedNum = idToNum(combinedHash);
    if (combinedNum > max) {
      max = combinedNum;
      maxNID = nid;
    }
  }
  return maxNID;
}

module.exports = {
  getID,
  getNID,
  getSID,
  getMID,
  naiveHash,
  consistentHash,
  rendezvousHash,
};
