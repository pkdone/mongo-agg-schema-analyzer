/**
 * Macro to generate the MongoDB Aggregation expressions to introspect a collection of documents
 * and infer its schema. The generated aggregation expression will construct the outline schema by
 * inspecting a collection's documents, even where some or all are composed of a complex nested
 * hierarchy of sub-documents. It descends through each document's nested fields collecting each
 * sub-document and associated metadata into a flattened array of elements in the result.
 * 
 * The function only supports MongoDB version 5+ due to the use of the new $getField operator.
 * However, for earlier versions of MongoDB you can replace $getField in this function's code with
 * @asya999's getField() function which performs the equivalent, at:
 * https://github.com/asya999/bits-n-pieces/blob/master/scripts/getField.js
 *
 * @param {Number} [maxElements=500]   [OPTIONAL] The maximum number of sub-documents to flatten
 *                                     per document (the resulting aggregation expression issues a
 *                                     warning in the aggregation's output if this number isn't
 *                                     sufficient to allow a document's hierarchy to be fully
 *                                     descended)
 * @return {Object}                    The generated MongoDB Aggregation JSON expression object
 *                                     which can be used by $set, $project, $replaceWith or other
 *                                     stage/operator in an aggregation pipeline, to construct the
 *                                     flattened array representation of a document's schema
 */
function extractSchema(maxElements=500) {
  return {
    // Loop an abitrary number of times, hoping that there's enough iterations to traverse the document's full hierarchy
    "$reduce": {        
      "input": {
        // Add buffer of an additional 1 item to be able to optionally provide an 'overrun' warning at the end of result array, if needed
        "$range": [0, {"$add": [maxElements, 1]}]
       },      
      "initialValue": {
        // Final result array to accumulate
        "content": [],        
        // Queue array of sub-docs still to be inspected
        "objectsToProcessQueue": [{"depth": 0, "index": "0", "subdocpath": "", "subdoc": "$$ROOT"}],        
      },      
      "in": {       
        // Add current sub-doc's metadata to result array 
        "content": captureCurrentObjectSchema("$$value.content", "$$value.objectsToProcessQueue", "$$this", maxElements),        
        // Add child objects of current sub-doc to the queue of array of items to be inspected later on (and remove the sub-doc just inspected)
        "objectsToProcessQueue": addChildrenOfCurrentObjectToQueue("$$value.objectsToProcessQueue"),
      }          
    }
  };
}


///////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////// SUPPORTING FUNCTIONS //////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////


/**
 * Macro to generate the aggregation expressions to get the next object (if any) from the start of
 * the queue and capture its schema metadata including path, data type and relative position data
 */
function captureCurrentObjectSchema(currentResultsArray, objectsToProcessQueue, currentResultPosition, maxElements) {
  return {
    "$let": {
      "vars": { 
        // Get current object from the front of the queue
        "currentObject": {"$first": objectsToProcessQueue},        
      },
      "in": {
        // Concatenate current result array with current object's data, returning this new combined array as the result
        "$concatArrays": [
          currentResultsArray,
          {"$cond": [
            // Stop accumulating array elements if we have now reached the end of the list of nested sub-document objects
            {"$ifNull": ["$$currentObject", false]},
            {"$cond": [                     
              {"$gte": [currentResultPosition, maxElements]},
              [{"WARNING": "The 'maxElements' parameter wasn't set to a large enough value to fully traverse the document's nested content"}],
              [{"$arrayToObject": [
                {"$concatArrays": buildArrayOfSchemaMetadataFields("$$currentObject", currentResultPosition)}
              ]}],                  
            ]},                         
            [], 
          ]},                         
        ]
      }
    }
  };
}


/**
 * Macro to generate the aggregation expressions to trim the first element, just inspected, from
 * the front of the queue and then add its direct children (if any) to the end of the queue, ready
 * to be processed in the future
 */
function addChildrenOfCurrentObjectToQueue(objectsToProcessQueue) {
  return {
    "$let": {
      "vars": { 
        // Get current object from the front of the queue      
        "currentObject": {"$first": objectsToProcessQueue},
      },
      "in": {
        "$let": {
          "vars": { 
            // Get current object's metadata
            "queueSize": {"$size": objectsToProcessQueue},
            "currentObjectChildren": getNestedChildrenOfSubdoc({"$getField": {"field": "subdoc", "input": "$$currentObject"}}),
            "currentObjectIdx": {"$getField": {"field": "index", "input": "$$currentObject"}},
            "currentSubdocPath": {"$getField": {"field": "subdocpath", "input": "$$currentObject"}},
            "newDepthNumber": {"$add": [{"$getField": {"field": "depth", "input": "$$currentObject"}}, 1]},
          },
          "in": {
            // Concatenate current queue array (minus its first object) with new child objects, returning this new combined array as the new version of the queue
            "$concatArrays": [
              // Chop off the first object of the queue of objects to inspect, because further below it will be decomposed into child objects
              {"$cond": [
                {"$gt": ["$$queueSize", 0]},
                {"$slice": [objectsToProcessQueue, 1, {"$add": ["$$queueSize", 1]}]},
                [],
              ]},             
              // Push the content of each field which is a child object or array to the end of the queue of elements to inspect
              {"$cond": [
                // MongoDB supports "100 levels of nesting for BSON documents" so no point in gong beyond that
                {"$and": [{"$isArray": "$$currentObjectChildren"}, {"$lte": ["$$newDepthNumber", 100]}]},
                // Loop through each field which is either a chold object or array of objects, adding each object to the queue
                constructQueueMember("$$currentObjectChildren", "$$currentObjectIdx", "$$currentSubdocPath", "$$newDepthNumber"),                
                [],
              ]},             
            ]            
          }
        }
      }      
    }
  };
}


/**
 * Macro to generate the aggregations expression to assembling all the fields for the schema
 * metadata for an object (which will be either the root document or one of potentially many
 * sub-documents
 */
function buildArrayOfSchemaMetadataFields(object, currentResultPosition) {
  return [
    // Capture the top level metadata for the object
    [{"k": "id", "v": currentResultPosition}],
    [{"k": "depth", "v": {"$getField": {"field": "depth", "input": object}}}],
    [{"k": "index", "v": {"$getField": {"field": "index", "input": object}}}],
    [{"k": "subdocpath", "v": {"$getField": {"field": "subdocpath", "input": object}}}],                  
    
    // Capture the schema for each field for this new object with its name, value and type
    [{"k": "schema", "v": {
      "$map": {
        "input": {"$objectToArray": {"$getField": {"field": "subdoc", "input": object}}},
        "as": "field",
        "in": {
          "fieldname": "$$field.k",
          "fieldvalue": {"$switch": {
                          "branches": [
                            {"case": {"$eq": [{"$type": "$$field.v"}, "array"]}, "then": "<array>"},
                            {"case": {"$eq": [{"$type": "$$field.v"}, "object"]}, "then": "<object>"},
                          ],
                          "default": "$$field.v",
                        }},                                                                                                     
          "fieldtype": {"$type": "$$field.v"},                                
        }
      }
    }}], 
  ];
}


/**
 * Macro to generate the aggregation expressions to find each field of the given object that maps
 * to a value which is an array or a sub-document (object) and return only those fields, collected
 * together in an array
 */
function getNestedChildrenOfSubdoc(subdoc) {
  return {
    // Loop through each field in the current object
    "$reduce": {
      "input": {"$objectToArray": subdoc},
      "initialValue": [],
      "in": {
        "$concatArrays": [
          "$$value",
          {"$switch": {
            "branches": [
              // If there's one object hanging off this field, just add that with additional metadata
              {"case": {"$eq": [{"$type": "$$this.v"}, "object"]}, "then": [{
                "key" : "$$this.k", 
                "value" : "$$this.v",
              }]},
              // If there's an array hanging off this field, unpack and add each array object with additional metadata
              {"case": {"$eq": [{"$type": "$$this.v"}, "array"]}, "then": {
                "$map": {
                  "input": "$$this.v",
                  "as": "element",
                  "in": {
                    "key" : "$$this.k",                                                                
                    "value" : {"$cond": [{"$eq": [{"$type": "$$element"}, "object"]}, "$$element", {"<arrayitem>": "$$element"}]},
                  }
                }
              }},
            ],
            // Otherwise this field does not correspond to an object or array so don't add the field in the result array
            "default": [], 
          }}            
        ]
      }
    }
  };
}


/**
 * Macro to generate the MongoDB Aggregation expressions to create a wrapper object for adding to
 * the queue capturing its depth, index, path and, hanging off a 'subdoc' field, the content itself
 */
function constructQueueMember(currentObjectChildren, currentObjectIdx, currentSubdocPath, newDepthNumber) {
  return {
    "$reduce": { 
      "input": {"$range": [0, {"$size": currentObjectChildren}]},
      "initialValue": [],
      "in": {
        "$let": {
          "vars": { 
            "childObject": {"$arrayElemAt": [currentObjectChildren, "$$this"]},
            "subdocPathSeperator": {"$cond": [{"$gt": [{"$strLenCP": currentSubdocPath}, 0]}, ".", ""]},                        
          },
          "in": {              
            "$concatArrays": [                            
              "$$value",
              [{
                // Add metadata to the object being added to the list including the actual object itself (keyed 'subdoc')
                "depth": newDepthNumber,
                "index": {"$concat": [currentObjectIdx, "_", {"$toString": "$$this"}]},
                "subdocpath": {"$concat": [currentSubdocPath, "$$subdocPathSeperator", {"$getField": {"field": "key", "input": "$$childObject"}}]},
                "subdoc": {"$getField": {"field": "value", "input": "$$childObject"}},                    
              }],
            ]                
          }
        }                                
      }
    },                  
  };
}


///////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////// TESTS //////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////


/**
 * Run an aggregation pipeline and compare its result with the expected result, throwing an error
 * if different
 */ 
function runAggPipelineAndErrorIfDifferent(funcName, pipeline, expectedResult) {
  const result = db.aggregate(pipeline).toArray();
  print(`\n\n----- ${funcName} ------\n`);
  print("EXPECTED RESULT:");
  print(expectedResult);
  print("ACTUAL RESULT:");
  print(result);
  print();

  if (JSON.stringify(result) != JSON.stringify(expectedResult)) {
    throw `${funcName} - TEST FAILED`;  
  }
}


/**
 * TEST: getNestedChildrenOfSubdoc
 * Requires MongoDB version 5.1+
 */ 
function test_getNestedChildrenOfSubdoc_1() {
  const expectedResult = [
    {
      children: [
        { key: 'b', value: { '<arrayitem>': 1 } },
        { key: 'b', value: { '<arrayitem>': 2 } },
        { key: 'b', value: { '<arrayitem>': 3 } },
        { key: 'c', value: { a: 1, b: 2, c: 3 } },
        { key: 'd', value: { x: 1 } },
        { key: 'd', value: { y: 2 } },
        { key: 'd', value: { z: 3 } },
        {
          key: 'e',
          value: { p: [ { q: 1 }, { r: 2 } ] }
        }
      ]
    },
    { children: [] }
  ];

  const pipeline = [
    {"$documents": [
      {
        "a": 1,
        "b": [1,2,3],
        "c": {"a": 1, "b": 2, "c": 3},
        "d": [{"x": 1}, {"y": 2}, {"z": 3}],
        "e": [{"p": [{"q": 1}, {"r": 2}]}],
      },
      {
        "other": true,
      },
    ]},
    
    {"$project": {
      "_id": 0,
      "children": getNestedChildrenOfSubdoc("$$ROOT"),
    }},    
  ];

  runAggPipelineAndErrorIfDifferent(test_getNestedChildrenOfSubdoc_1.name, pipeline, expectedResult);
}


/**
 * TEST: buildObjectMetadata
 * Requires MongoDB version 5.1+
 */ 
function test_buildArrayOfSchemaMetadataFields_1() {
  const expectedResult = [
    {
      recordList: [
        [ { k: 'id', v: 1 } ],
        [ { k: 'depth', v: 1 } ],
        [ { k: 'index', v: '0' } ],
        [ { k: 'subdocpath', v: '' } ],
        [
          {
            k: 'schema',
            v: [
              { fieldname: 'a', fieldvalue: 1, fieldtype: 'int' },
              {
                fieldname: 'b',
                fieldvalue: '<array>',
                fieldtype: 'array'
              },
              {
                fieldname: 'c',
                fieldvalue: '<object>',
                fieldtype: 'object'
              },
              {
                fieldname: 'd',
                fieldvalue: '<array>',
                fieldtype: 'array'
              },
              {
                fieldname: 'e',
                fieldvalue: '<array>',
                fieldtype: 'array'
              }
            ]
          }
        ]
      ]
    },
    {
      recordList: [
        [ { k: 'id', v: 1 } ],
        [ { k: 'depth', v: 2 } ],
        [ { k: 'index', v: '0_1_2_3' } ],
        [ { k: 'subdocpath', v: 'x.y.z' } ],
        [
          {
            k: 'schema',
            v: [
              { fieldname: 'other', fieldvalue: true, fieldtype: 'bool' }
            ]
          }
        ]
      ]
    }
  ];
  
  const pipeline = [
    {"$documents": [
      {
        "depth": 1,
        "index": "0",
        "subdocpath": "",
        "subdoc": {                   
          "a": 1,
          "b": [1,2,3],
          "c": {"a": 1, "b": 2, "c": 3},
          "d": [{"x": 1}, {"y": 2}, {"z": 3}],
          "e": [{"p": [{"q": 1}, {"r": 2}]}],
        },
      },
      {
        "depth": 2,
        "index": "0_1_2_3",
        "subdocpath": "x.y.z",
        "subdoc": {                   
          "other": true,
        },
      },
    ]},
    
    {"$project": {
      "_id": 0,
      "recordList": buildArrayOfSchemaMetadataFields("$$ROOT", 1),
    }},    
  ];

  runAggPipelineAndErrorIfDifferent(test_buildArrayOfSchemaMetadataFields_1.name, pipeline, expectedResult);
}


/**
 * TEST: constructQueueMember
 * Requires MongoDB version 5.1+
 */ 
function test_constructQueueMember_1() {
  const expectedResult = [
    {
      recordList: [
        {
          depth: 1,
          index: '1_0',
          subdocpath: 'b',
          subdoc: { '<arrayitem>': 1 }
        },
        {
          depth: 1,
          index: '1_1',
          subdocpath: 'b',
          subdoc: { '<arrayitem>': 2 }
        },
        {
          depth: 1,
          index: '1_2',
          subdocpath: 'c',
          subdoc: { a: 1, b: 2, c: 3 }
        },
        { depth: 1, index: '1_3', subdocpath: 'd', subdoc: { z: 3 } },
        {
          depth: 1,
          index: '1_4',
          subdocpath: 'e',
          subdoc: { p: [ { q: 1 }, { r: 2 } ] }
        }
      ]
    },
    {
      recordList: [
        {
          depth: 1,
          index: '1_0',
          subdocpath: 'b',
          subdoc: { '<arrayitem>': 3 }
        },
        { depth: 1, index: '1_1', subdocpath: 'd', subdoc: { x: 1 } },
        { depth: 1, index: '1_2', subdocpath: 'd', subdoc: { y: 2 } }
      ]
    }
  ]

  const pipeline = [
    {"$documents": [
      {"children": [
          { key: 'b', value: { '<arrayitem>': 1 } },
          { key: 'b', value: { '<arrayitem>': 2 } },
          { key: 'c', value: { a: 1, b: 2, c: 3 } },
          { key: 'd', value: { z: 3 } },
          {
            key: 'e',
            value: { p: [ { q: 1 }, { r: 2 } ] }
          }
      ]},
      {"children": [
          { key: 'b', value: { '<arrayitem>': 3 } },
          { key: 'd', value: { x: 1 } },
          { key: 'd', value: { y: 2 } },
      ]},
    ]},

    {"$project": {
      "_id": 0,
      "recordList": constructQueueMember("$children", "1", "", 1)
    }},    
  ];

  runAggPipelineAndErrorIfDifferent(test_constructQueueMember_1.name, pipeline, expectedResult);  
}


/**
 * TEST: extractSchema
 * Requires MongoDB version 5.1+
 */ 
function test_extractSchema_1() {
  const expectedResult = [
    {
      content: [
        {
          id: 0,
          depth: 0,
          index: '0',
          subdocpath: '',
          schema: [
            { fieldname: 'a', fieldvalue: 1, fieldtype: 'int' },
            { fieldname: 'b', fieldvalue: '<array>', fieldtype: 'array' },
            {
              fieldname: 'c',
              fieldvalue: '<object>',
              fieldtype: 'object'
            },
            { fieldname: 'd', fieldvalue: '<array>', fieldtype: 'array' },
            { fieldname: 'e', fieldvalue: '<array>', fieldtype: 'array' }
          ]
        },
        {
          id: 1,
          depth: 1,
          index: '0_0',
          subdocpath: 'b',
          schema: [
            { fieldname: '<arrayitem>', fieldvalue: 1, fieldtype: 'int' }
          ]
        },
        {
          id: 2,
          depth: 1,
          index: '0_1',
          subdocpath: 'b',
          schema: [
            { fieldname: '<arrayitem>', fieldvalue: 2, fieldtype: 'int' }
          ]
        },
        {
          id: 3,
          depth: 1,
          index: '0_2',
          subdocpath: 'b',
          schema: [
            { fieldname: '<arrayitem>', fieldvalue: 3, fieldtype: 'int' }
          ]
        },
        {
          id: 4,
          depth: 1,
          index: '0_3',
          subdocpath: 'c',
          schema: [
            { fieldname: 'a', fieldvalue: 1, fieldtype: 'int' },
            { fieldname: 'b', fieldvalue: 2, fieldtype: 'int' },
            { fieldname: 'c', fieldvalue: 3, fieldtype: 'int' }
          ]
        },
        {
          id: 5,
          depth: 1,
          index: '0_4',
          subdocpath: 'd',
          schema: [ { fieldname: 'x', fieldvalue: 1, fieldtype: 'int' } ]
        },
        {
          id: 6,
          depth: 1,
          index: '0_5',
          subdocpath: 'd',
          schema: [ { fieldname: 'y', fieldvalue: 2, fieldtype: 'int' } ]
        },
        {
          id: 7,
          depth: 1,
          index: '0_6',
          subdocpath: 'd',
          schema: [ { fieldname: 'z', fieldvalue: 3, fieldtype: 'int' } ]
        },
        {
          id: 8,
          depth: 1,
          index: '0_7',
          subdocpath: 'e',
          schema: [
            { fieldname: 'p', fieldvalue: '<array>', fieldtype: 'array' }
          ]
        },
        {
          id: 9,
          depth: 2,
          index: '0_7_0',
          subdocpath: 'e.p',
          schema: [ { fieldname: 'q', fieldvalue: 1, fieldtype: 'int' } ]
        },
        {
          id: 10,
          depth: 2,
          index: '0_7_1',
          subdocpath: 'e.p',
          schema: [ { fieldname: 'r', fieldvalue: 2, fieldtype: 'int' } ]
        }
      ],
      objectsToProcessQueue: []
    },
    {
      content: [
        {
          id: 0,
          depth: 0,
          index: '0',
          subdocpath: '',
          schema: [ { fieldname: 'other', fieldvalue: true, fieldtype: 'bool' } ]
        }
      ],
      objectsToProcessQueue: []
    }
  ];

  const pipeline = [
    {"$documents": [
      {
        "a": 1,
        "b": [1,2,3],
        "c": {"a": 1, "b": 2, "c": 3},
        "d": [{"x": 1}, {"y": 2}, {"z": 3}],
        "e": [{"p": [{"q": 1}, {"r": 2}]}],
      },
      {
        "other": true,
      },
    ]},

    {"$replaceWith": 
      extractSchema()
    },
  ];

  runAggPipelineAndErrorIfDifferent(test_extractSchema_1.name, pipeline, expectedResult);    
}


/**
 * Run All Tests
 * Requires MongoDB version 5.1+
 */ 
function runAllTests() {
  test_getNestedChildrenOfSubdoc_1();
  test_buildArrayOfSchemaMetadataFields_1();
  test_constructQueueMember_1();
  test_extractSchema_1();
}

