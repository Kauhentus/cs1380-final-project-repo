let crypto = require('crypto');
let fs = require('fs');
let os = require('os');

function getSerialized(type, object) {
  return JSON.stringify({type: type, value: object});
}

function parseBoolean(value) {
  return value === 'true';
}


const solSerialize = require('@brown-ds/distribution/distribution/util/serialization').serialize;
const solDeserialize = require('@brown-ds/distribution/distribution/util/serialization').deserialize;

module.exports = {
  serialize: solSerialize,
  deserialize: solDeserialize,
};
