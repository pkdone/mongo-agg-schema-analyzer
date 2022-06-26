# MongoDB Aggregation Schema Analyzer

Provides a JavaScript function to act as a _macro_ to generate a MongoDB Aggregation expression to introspect a collection of documents and infer its schema. The generated aggregation expression will construct the outline schema by inspecting a collection's documents, even where some or all are composed of a complex nested hierarchy of sub-documents. It descends through each document's nested fields collecting each sub-document and associated metadata into a flattened array of elements in the result.

Currently the function only supports MongoDB version 5+ due to the use of the [$getField](https://www.mongodb.com/docs/manual/reference/operator/aggregation/getField/) operator. However, for earlier versions of MongoDB you can replace _$getField_ in the JavaScript library code with [@asya999](https://twitter.com/asya999)'s [getField() function](https://github.com/asya999/bits-n-pieces/blob/master/scripts/getField.js) which performs the equivalent in older versions of MongoDB.

&nbsp;


## Sample Collection Data Population

> _Note, this step is only necessary if you haven't already got a MongoDB database collection that you want to introspect the schema for._

Via the [MongoDB Shell](https://www.mongodb.com/docs/mongodb-shell/), connect to your MongoDB database and then execute the following to drop any old version of the database (if it exists) and populate a new collection with two made-up documents, each consisting of a hierarchy of nested sub-documents.

```javascript
use mongo-agg-schema-analyzer;
db.dropDatabase();

db.mydata.insertMany([
  {
    "val": 999,
    "simples": [7,8,9],
    "myobj": {
      "s": 6,
      "t": 7,      
    },
    "aa": [
      {
        "val": 111,
        "bb": [
          {
            "val": 123,
            "arr": [1, "items"],
          },
          {
            "val": 456,
            "stuff": 5,
          },
          {
            "val": 456,
            "dd": [
              {
                "val": 333,
              },
              {
                "val": 222,
                "children": "it",
              },
              {
                "val": 111,
                "children": [],
              },
              {
                "val": 66,
                "children": [
                  {
                    "eee": "qqq",
                    "bob": [],
                  },
                  {
                    "ww": "rrr",
                    "vvv": [],
                  },
                ],
              },
            ]
          },
        ]        
      },
    ],    
    "xx": [
      {
        "val": 111,
        "yyy": [
          {
            "val": 123,
          },
          {
            "val": 456,
            "children": 5,
          },
          {
            "val": 456,
            "children": [
              {
                "val": 333,
              },
              {
                "val": 222,
                "children": "it",
              },
              {
                "val": 111,
                "children": [],
              },
            ]
          },
        ]        
      },
      {
        "val": 222,
        "stuff": [
          {
            "val": 789,
            "children": {"x": 1},
          },
          {
            "val": 120,
            "children": [],
          },
          {
            "val": 22,
            "children": null,
          },
        ]        
      }       
    ],
    "tt": {
      "a": 1,
      "b": 2,
      "c": [
        {
          "eee": "qqq",
          "bob": [],
        },
      ],
    },    
  },
    
  {
    "val": "abc",
    "otherval": false,
    "stuff": [
      {
        "val": "xyz",
        "otherval": true,
        "children": [],
      }, 
    ],
  },
]);

```

&nbsp;


## Load The Schema Extraction JavaScript Functions

Via the [MongoDB Shell](https://www.mongodb.com/docs/mongodb-shell/), connected to your MongoDB database, execute the following to load the `extractSchema()` JavaScript function and related supporting functions, ready to be used by the subsequent aggregation pipeline.

```javascript
load('mongo-agg-extract-schema.js');

```

&nbsp;


## Run The Analyze Schema Aggregation Pipeline

Define a pipeline to use the `extractSchema()` function to capture and output the schema for the collection and execute the pipeline:

> _Change the value of the `sampleSize` variable if you want to sample less or more than 10,000 documents from the collection (the larger this value, the longer the process will take to run). Also, where indicated in the code comment, add the `maxElements=???` number parameter with an appropriate value to `extractSchema()` (default is _500_) if you believe there are more than 500 fields in some of the collection's documents to be inspected (you will see a warning in the aggregation's output the first time you run it if it detects this is the case)_

```javascript
var sampleSize = 10000;

var pipeline = [
  {"$sample": {
    "size": sampleSize
  }},

  {"$replaceWith": 
    extractSchema()      // or: extractSchema(maxElements=1000)
  },
  
  {"$unwind": 
    "$content"
  },

  {"$unwind": 
    "$content.schema"
  },

  {"$group": {
    "_id": {"subdocpath": "$content.subdocpath", "fieldname": "$content.schema.fieldname", "fieldtype": "$content.schema.fieldtype"},
    "count": {"$sum": 1},
    "min": {"$min": "$content.schema.fieldvalue"},
    "max": {"$max": "$content.schema.fieldvalue"},
  }},  

  {"$group": {
    "_id": {"subdocpath": "$_id.subdocpath", "fieldname": "$_id.fieldname"},
    "fieldtypes": {"$push": {
      "fieldtype": "$_id.fieldtype",
      "count": "$count",
      "min": {"$cond": [{"$in": ["$_id.fieldtype", ["null", "array", "object"]]}, "$$REMOVE", "$min"]},
      "max": {"$cond": [{"$in": ["$_id.fieldtype", ["null", "array", "object"]]}, "$$REMOVE", "$max"]},
    }},
  }},    

  {"$sort": {
    "_id.subdocpath": 1,
    "_id.fieldname": 1,    
  }},    

  {"$set": {
    "path": "$_id.subdocpath",
    "field": "$_id.fieldname",
    "types": "$fieldtypes",
    "_id": "$$REMOVE",
    "fieldtypes": "$$REMOVE",
  }},            
];

db.mydata.aggregate(pipeline);

```

> _Note, change the name of the collection from `mydata` to the name of your collection in the `aggregate()` command if you are not using the mock data set._

&nbsp;


## Example Pipeline Output

For the sample data set, the executed pipeline should yield the following result output:

```javascript
[
  {
    path: '',
    field: '_id',
    types: [
      { fieldtype: 'objectId', count: 2, min: ObjectId("62ab8bae760c0de490851e89"),max: ObjectId("62ab8bae760c0de490851e8a") }
    ]
  },
  {
    path: '',
    field: 'aa',
    types: [ { fieldtype: 'array', count: 1 } ]
  },
  {
    path: '',
    field: 'myobj',
    types: [ { fieldtype: 'object', count: 1 } ]
  },
  {
    path: '',
    field: 'otherval',
    types: [ { fieldtype: 'bool', count: 1, min: false, max: false } ]
  },
  {
    path: '',
    field: 'simples',
    types: [ { fieldtype: 'array', count: 1 } ]
  },
  {
    path: '',
    field: 'stuff',
    types: [ { fieldtype: 'array', count: 1 } ]
  },
  {
    path: '',
    field: 'tt',
    types: [ { fieldtype: 'object', count: 1 } ]
  },
  {
    path: '',
    field: 'val',
    types: [
      { fieldtype: 'int', count: 1, min: 999, max: 999 },
      { fieldtype: 'string', count: 1, min: 'abc', max: 'abc' }
    ]
  },
  {
    path: '',
    field: 'xx',
    types: [ { fieldtype: 'array', count: 1 } ]
  },
  {
    path: 'aa',
    field: 'bb',
    types: [ { fieldtype: 'array', count: 1 } ]
  },
  {
    path: 'aa',
    field: 'val',
    types: [ { fieldtype: 'int', count: 1, min: 111, max: 111 } ]
  },
  {
    path: 'aa.bb',
    field: 'arr',
    types: [ { fieldtype: 'array', count: 1 } ]
  },
  {
    path: 'aa.bb',
    field: 'dd',
    types: [ { fieldtype: 'array', count: 1 } ]
  },
  {
    path: 'aa.bb',
    field: 'stuff',
    types: [ { fieldtype: 'int', count: 1, min: 5, max: 5 } ]
  },
  {
    path: 'aa.bb',
    field: 'val',
    types: [ { fieldtype: 'int', count: 3, min: 123, max: 456 } ]
  },
  {
    path: 'aa.bb.arr',
    field: '<arrayitem>',
    types: [
      { fieldtype: 'int', count: 1, min: 1, max: 1 },
      { fieldtype: 'string', count: 1, min: 'items', max: 'items' }
    ]
  },
  {
    path: 'aa.bb.dd',
    field: 'children',
    types: [
      { fieldtype: 'string', count: 1, min: 'it', max: 'it' },
      { fieldtype: 'array', count: 2 }
    ]
  },
  {
    path: 'aa.bb.dd',
    field: 'val',
    types: [ { fieldtype: 'int', count: 4, min: 66, max: 333 } ]
  },
  {
    path: 'aa.bb.dd.children',
    field: 'bob',
    types: [ { fieldtype: 'array', count: 1 } ]
  },
  {
    path: 'aa.bb.dd.children',
    field: 'eee',
    types: [ { fieldtype: 'string', count: 1, min: 'qqq', max: 'qqq' } ]
  },
  {
    path: 'aa.bb.dd.children',
    field: 'vvv',
    types: [ { fieldtype: 'array', count: 1 } ]
  },
  {
    path: 'aa.bb.dd.children',
    field: 'ww',
    types: [ { fieldtype: 'string', count: 1, min: 'rrr', max: 'rrr' } ]
  },
  {
    path: 'myobj',
    field: 's',
    types: [ { fieldtype: 'int', count: 1, min: 6, max: 6 } ]
  },
  {
    path: 'myobj',
    field: 't',
    types: [ { fieldtype: 'int', count: 1, min: 7, max: 7 } ]
  },
  {
    path: 'simples',
    field: '<arrayitem>',
    types: [ { fieldtype: 'int', count: 3, min: 7, max: 9 } ]
  },
  {
    path: 'stuff',
    field: 'children',
    types: [ { fieldtype: 'array', count: 1 } ]
  },
  {
    path: 'stuff',
    field: 'otherval',
    types: [ { fieldtype: 'bool', count: 1, min: true, max: true } ]
  },
  {
    path: 'stuff',
    field: 'val',
    types: [ { fieldtype: 'string', count: 1, min: 'xyz', max: 'xyz' } ]
  },
  {
    path: 'tt',
    field: 'a',
    types: [ { fieldtype: 'int', count: 1, min: 1, max: 1 } ]
  },
  {
    path: 'tt',
    field: 'b',
    types: [ { fieldtype: 'int', count: 1, min: 2, max: 2 } ]
  },
  {
    path: 'tt',
    field: 'c',
    types: [ { fieldtype: 'array', count: 1 } ]
  },
  {
    path: 'tt.c',
    field: 'bob',
    types: [ { fieldtype: 'array', count: 1 } ]
  },
  {
    path: 'tt.c',
    field: 'eee',
    types: [ { fieldtype: 'string', count: 1, min: 'qqq', max: 'qqq' } ]
  },
  {
    path: 'xx',
    field: 'stuff',
    types: [ { fieldtype: 'array', count: 1 } ]
  },
  {
    path: 'xx',
    field: 'val',
    types: [ { fieldtype: 'int', count: 2, min: 111, max: 222 } ]
  },
  {
    path: 'xx',
    field: 'yyy',
    types: [ { fieldtype: 'array', count: 1 } ]
  },
  {
    path: 'xx.stuff',
    field: 'children',
    types: [
      { fieldtype: 'object', count: 1 },
      { fieldtype: 'null', count: 1 },
      { fieldtype: 'array', count: 1 }
    ]
  },
  {
    path: 'xx.stuff',
    field: 'val',
    types: [ { fieldtype: 'int', count: 3, min: 22, max: 789 } ]
  },
  {
    path: 'xx.stuff.children',
    field: 'x',
    types: [ { fieldtype: 'int', count: 1, min: 1, max: 1 } ]
  },
  {
    path: 'xx.yyy',
    field: 'children',
    types: [
      { fieldtype: 'array', count: 1 },
      { fieldtype: 'int', count: 1, min: 5, max: 5 }
    ]
  },
  {
    path: 'xx.yyy',
    field: 'val',
    types: [ { fieldtype: 'int', count: 3, min: 123, max: 456 } ]
  },
  {
    path: 'xx.yyy.children',
    field: 'children',
    types: [
      { fieldtype: 'array', count: 1 },
      { fieldtype: 'string', count: 1, min: 'it', max: 'it' }
    ]
  },
  {
    path: 'xx.yyy.children',
    field: 'val',
    types: [ { fieldtype: 'int', count: 3, min: 111, max: 333 } ]
  }
]

```

&nbsp;


## Running The 'Raw' Extract Schema Function

If you only want to execute the `extractSchema()` function with a minimal aggregation pipeline to just see the function's raw output, run:

```javascript
var pipeline = [
  {"$replaceWith":
    extractSchema()
  },
];

db.mydata.aggregate(pipeline);

```

&nbsp;


## Execute The Unit Tests

To run the unit tests, execute the following from a terminal, ensuring you first export the variable `URL` to match the location of your running MongoDB deployment:

```console
export URL="mongodb+srv://myuser:mypasswd@mycluster.abc.mongodb.net/test"
./run-tests.sh
```

> _Note, these tests only work on MongoDB version 5.1 or greater due to the use of the newer [$documents](https://www.mongodb.com/docs/v6.0/reference/operator/aggregation/documents/) stage_

