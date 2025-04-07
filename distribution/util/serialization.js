let crypto = require('crypto');
let fs = require('fs');
let os = require('os');

function getSerialized(type, object) {
  return JSON.stringify({type: type, value: object});
}

function parseBoolean(value) {
  return value === 'true';
}

function serialize(object, seen = new Map()) {
  const native_func = {'error': "console.error", 'warn': "console.warn", 'readFile': "fs.readFile", 'getOSType': "os.type"};
  if (seen.has(object)) {
    return getSerialized('register', seen.get(object))
  }
  if (typeof object === 'string') {
    return getSerialized('string', object);
  }
  if (typeof object === 'number') {
    return getSerialized('number', object.toString());
  }
  if (typeof object === 'boolean') {
    return getSerialized('boolean', object.toString());
  }
  if (object === null) {
    return getSerialized('null', '');
  }
  if (object === undefined) {
    return getSerialized('undefined', '');
  }
  if (typeof object === 'function') {
    if (object.name === 'log' && object.toString().includes("[native code")) {
      return getSerialized('native', "console.log");
    } else if (native_func[object.name]) {
      return getSerialized('native', native_func[object.name]);
    }
    return getSerialized('function', object.toString());
  }
  if (typeof object === 'object') {
    const id = crypto.randomUUID();
    seen.set(object, id);
    let serialized = {id: id, type: null, value: {}};
    if (Array.isArray(object)) {
      serialized.type = "array";
      for (let i = 0; i < object.length; i++) {
        serialized.value[i] = serialize(object[i], seen);
      }
      return JSON.stringify(serialized);
    } else if (object instanceof Date) {
      serialized.type = "date";
      serialized.value = object.toISOString();
      return JSON.stringify(serialized);
    } else if (object instanceof Error) {
      serialized.type = "error";
      serialized.value = {
        type: "object",
        value: {
          name: getSerialized('string', object.name),
          message: getSerialized('string', object.message),
          cause: object.cause? getSerialized('string', object.cause) : getSerialized('undefined', ''),
        }
      };
      return JSON.stringify(serialized);
    } else {
      serialized.type = "object";
      for (let key in object) {
        serialized.value[key] = serialize(object[key], seen);
      }
      return JSON.stringify(serialized);
    }
  }
  throw new Error(`Unknown type: ${typeof object}`);
}

function deserialize(string, seen = new Map()) {
  const native_func = {'console.log': console.log, 'console.error': console.error, 'console.warn': console.warn, 'fs.readFile': fs.readFile, 'os.type': os.type};
  const json = JSON.parse(string);
  if (json.type === 'native') {
    return native_func[json.value];
  }
  if (json.type === 'string') {
    return json.value;
  }
  if (json.type === 'number') {
    return parseFloat(json.value);
  }
  if (json.type === 'boolean') {
    return parseBoolean(json.value);
  }
  if (json.type === 'null') {
    return null;
  }
  if (json.type === 'undefined') {
    return undefined;
  }
  if (json.type === 'function') {
    // Need to use eval instead of Function because of arrow serialization
    return eval('(' + json.value + ')');
  }
  if (json.type === 'date') {
    return new Date(json.value);
  }
  if (json.type === 'error') {
    return new Error(deserialize(json.value.value.message));
  }
  if (json.type === 'array') {
    let array = [];
    for (let val in json.value) {
      array.push(deserialize(json.value[val], seen));
    }
    return array;
  }
  if (json.type === 'object') {
    if (seen.has(json.id)) {
      return seen.get(json.id);
    }
    let object = {};
    seen.set(json.id, object);
    for (let key in json.value) {
      object[key] = deserialize(json.value[key], seen);
    }
    return object;
  }
  if (json.type === "register") {
    return seen.get(json.value);
  }
  throw new Error(`Unknown type: ${json.type}`);
}

const solSerialize = require('@brown-ds/distribution/distribution/util/serialization').serialize;
const solDeserialize = require('@brown-ds/distribution/distribution/util/serialization').deserialize;

module.exports = {
  serialize: solSerialize,
  deserialize: solDeserialize,
};
