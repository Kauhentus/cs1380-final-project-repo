// let util = require("@brown-ds/distribution").util;

let serialize = require('./serialization').serialize;
let deserialize = require('./serialization').deserialize;
let util = { serialize, deserialize };

let cs1380 = {title: "Distr. Systems", instructor: "Nikos", cap: 200}; 
let s = util.serialize(cs1380); 
let o = util.deserialize(s);

// write a jest test for util.serialize and deserialize

test('1. serialize and deserialize recursive object', () => {
  const original = {a: 1, b: 2, c: 3};
  const serialized = util.serialize(original);
  expect(original).toEqual(util.deserialize(serialized));
});

test('2. string and number are differentiated', () => {
    const og_string = "3";
    const og_number = 3;
    const serialized_string = util.serialize(og_string);
    const serialized_number = util.serialize(og_number);
    expect(og_string).toEqual(util.deserialize(serialized_string));
    expect(og_number).toEqual(util.deserialize(serialized_number));
    expect(og_string).not.toEqual(og_number);
    expect(og_number).not.toEqual(og_string);
});

test('3. can serialize twice', () => { 
    const og_string = "3";
    const serialized_string = util.serialize(og_string);
    const serialized_string_twice = util.serialize(serialized_string);
    expect(og_string).toEqual(util.deserialize(util.deserialize(serialized_string_twice)));
});

test('4. deserializing identical strings should result in same object', () => {
    const og_string = "3";
    const serialized_string_fst = util.serialize(og_string);
    const serialized_string_snd = util.serialize(og_string);
    expect(util.deserialize(serialized_string_fst)).toEqual(util.deserialize(serialized_string_snd));
});

test('5. deserializing identical objects should result in different objects', () => {
    const og_object = {a: 1, b: 2, c: 3};
    const serialized_object_fst = util.serialize(og_object);
    const serialized_object_snd = util.serialize(og_object);
    expect(util.deserialize(serialized_object_fst)).not.toBe(util.deserialize(serialized_object_snd));
});

test('6. can serialize circular without infinite loop', () => { 
    const obj1 = {};
    const obj2 = {next: obj1};
    obj1.next = obj2;
    
    expect(() => {
        util.serialize(obj1);
    }).not.toThrow();
});

test('7. can deserialize circular', () => { 
    const obj1 = {};
    const obj2 = {next: obj1};
    obj1.next = obj2;
    
    const serialized = util.serialize(obj1);
    const deserialized = util.deserialize(serialized);
    console.log(deserialized);
    expect(deserialized).toEqual(obj1);
});