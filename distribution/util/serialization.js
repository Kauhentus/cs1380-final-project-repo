/*
    Checklist:

    1. Serialize strings
    2. Serialize numbers
    3. Serialize booleans
    4. Serialize (non-circular) Objects
    5. Serialize (non-circular) Arrays
    6. Serialize undefined and null
    7. Serialize Date, Error objects
    8. Serialize (non-native) functions
    9. Serialize circular objects and arrays
    10. Serialize native functions
*/

const crypto = require('crypto');
const uuidv4 = () => {
  return "10000000-1000-4000-8000-100000000000".replace(/[018]/g, c =>
    (+c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> +c / 4).toString(16)
  );
}

function serialize(thing, seen = new Map()) {
  let object;
  if(thing === null){
    object = {
      type: "null",
      value: "null"
    }
  } else if(thing === undefined){
    object = {
      type: "undefined",
      value: "undefined"
    }
  } else if(typeof thing === "string"){
    object = {
      type: "string",
      value: thing.toString()
    }
  } else if(typeof thing === "number"){
    object = {
      type: "number",
      value: thing.toString()
    }
  } else if(typeof thing === "boolean"){
    object = {
      type: "boolean",
      value: thing.toString()
    }
  } 
  
  else if(typeof thing === "function"){
    object = {
      type: "function",
      value: thing.toString()
    }
  } 
  
  else if(Array.isArray(thing)){
    if(seen.has(thing)){
      object = {
        type: "reference",
        value: seen.get(thing)
      }
    } else {
      const uuid = uuidv4();
      seen.set(thing, uuid);
      object = {
        type: "array",
        id: uuid,
        value: thing.map(val => serialize(val, seen))
      }
      seen.delete(thing);
    }
  } else if(thing instanceof Error){
    object = {
      type: "error",
      value_cause: thing.cause,
      value_message: thing.message,
      value_name: thing.name,
      value_stack: thing.stack
    }
  } else if(thing instanceof Date){
    object = {
      type: "date",
      value: thing.toISOString()
    }
  } else if(typeof thing === "object"){
    if(seen.has(thing)){
      object = {
        type: "reference",
        value: seen.get(thing)
      }
    } else {
      const uuid = uuidv4();
      seen.set(thing, uuid);
      object = {
        type: "object",
        id: uuid,
        keys: Object.keys(thing),
        vals: Object.values(thing).map(val => serialize(val, seen))
      }
      seen.delete(thing);
    }
  } 

  return JSON.stringify(object);
}


function deserialize(string, refs = {}) {
  const thing = JSON.parse(string);
  if(thing.type === "null"){
    return null;
  } else if(thing.type === "undefined"){
    return undefined;
  } else if(thing.type === "string"){
    return thing.value;
  } else if(thing.type === "number"){
    return Number(thing.value);
  } else if(thing.type === "boolean"){
    return thing.value === "true" ? true : false
  } 
  
  else if(thing.type === "function"){
    // console.log("FUNC", thing)
    const func = eval(`(${thing.value})`); // must surround with parenthesis to make it an expression rather than a function definition
    return func;
  } 
  
  else if(thing.type === "object"){
    let obj = {};
    refs[thing.id] = obj;
    for(let i = 0; i < thing.keys.length; i++){
      obj[thing.keys[i]] = deserialize(thing.vals[i], refs);
    }
    return obj;
  } else if(thing.type === "array"){
    let arr = thing.value.map(val => {
      return deserialize(val, refs)
    });
    refs[thing.id] = arr; // assume arr is not circular
    return arr;
  } else if(thing.type === "error"){
    let err = new Error(thing.value_message);
    err.cause = thing.value_cause;
    err.name = thing.value_name;
    err.stack = thing.value_stack;
    return err;
  } else if(thing.type === "date"){
    return new Date(thing.value);
  } else if(thing.type === "reference"){
    return refs[thing.value];
  }

  throw Error("did not match a valid structure type");
}

// const serialize = require('@brown-ds/distribution').util.serialize;
// const deserialize = require('@brown-ds/distribution').util.deserialize;

module.exports = {
  serialize: serialize,
  deserialize: deserialize,
};
