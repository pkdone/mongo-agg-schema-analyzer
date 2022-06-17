//
// Macro to generate the aggregation expression to find each field for a given object that maps to
// a value which is an array or a sub-document (object) and return only those fields, collected
// together in an array
//
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


//
// Macro to generate the aggregation expression to get the next object (if any) from the start of
// the queue and capture its metadata including path, data type and relative position data
//
function captureCurrentObjectMetadata(currentResultsArray, objectsToProcessQueue, maxElements) {
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
              {"$gte": ["$$this", maxElements]},
              [{"WARNING": "The 'maxElements' parameter wasn't set to a large enough value to fully traverse the document's nested content"}],
              [{"$arrayToObject": [
                {"$concatArrays": [
                  // Start the build of the new object in the result array with its metadata
                  [{"k": "id", "v": "$$this"}],
                  [{"k": "depth", "v": {"$getField": {"field": "depth", "input": "$$currentObject"}}}],
                  [{"k": "index", "v": {"$getField": {"field": "index", "input": "$$currentObject"}}}],
                  [{"k": "subdocpath", "v": {"$getField": {"field": "subdocpath", "input": "$$currentObject"}}}],                  
                  // Capture the schema for each field for this new object with its name, value and type
                  [{"k": "schema", "v": {
                    "$map": {
                      "input": {"$objectToArray": {"$getField": {"field": "subdoc", "input": "$$currentObject"}}},
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
                ]}
              ]}],                  
            ]},                         
            [], 
          ]},                         
        ]
      }
    }
  };
}


//
// Macro to generate the aggregation expression to trim the first element, just inspected, from
// the front of the queue and then add its direct children (if any) to the end of the queue, ready
// to be processed in the future
//
function addCurrentObjectsChildrenToQueue(objectsToProcessQueue) {
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
                {"$reduce": { 
                  "input": {"$range": [0, {"$size": "$$currentObjectChildren"}]},
                  "initialValue": [],
                  "in": {
                    "$let": {
                      "vars": { 
                        "childObject": {"$arrayElemAt": ["$$currentObjectChildren", "$$this"]},
                        "subdocPathSeperator": {"$cond": [{"$gt": [{"$strLenCP": "$$currentSubdocPath"}, 0]}, ".", ""]},                        
                      },
                      "in": {              
                        "$concatArrays": [                            
                          "$$value",
                          [{
                            // Add metadata to the object being added to the list including the actual object itself (keyed 'subdoc')
                            "depth": "$$newDepthNumber",
                            "index": {"$concat": ["$$currentObjectIdx", "_", {"$toString": "$$this"}]},
                            "subdocpath": {"$concat": ["$$currentSubdocPath", "$$subdocPathSeperator", {"$getField": {"field": "key", "input": "$$childObject"}}]},
                            "subdoc": {"$getField": {"field": "value", "input": "$$childObject"}},                    
                          }],
                        ]                
                      }
                    }                                
                  }
                }},                  
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
 * Macro to generate a MongoDB Aggregation expression to introspect a collection of documents and 
 * infer its schema. The generated aggregation expression will construct the outline schema by
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
        "content": captureCurrentObjectMetadata("$$value.content", "$$value.objectsToProcessQueue", maxElements),        
        // Add child objects of current sub-doc to the queue of array of items to be inspected later on (and remove the sub-doc just inspected)
        "objectsToProcessQueue": addCurrentObjectsChildrenToQueue("$$value.objectsToProcessQueue"),
      }          
    }
  };
}

