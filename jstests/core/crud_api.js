var crudAPISpecTests = function crudAPISpecTests() {
    "use strict";

    // Get the collection
    var col = db.crud_tests;

    // Setup
    var createTestExecutor = function(instance, method, data, setup) {
        return function(args) {
          print("dfdfdsfsd")
            // Execute setup
            if(setup) {
                setup(instance, method, data);
            }

            // Execute the method with arguments
            var r = instance[method].apply(instance, args.params);
            // Assert equality
            assert.docEq(args.result, r);

            // Get all the results
            var results = instance.find({}).toArray();
            // Assert equality
            assert.docEq(args.db, results);
        }
    }

    // All the data used for the tests
    var data = [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }];
    var findOneData = [{ _id:1, x:11 }];
    var replaceOneData = [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }];
    var updateManyData = [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }];
    var updateOneData = [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }];
    var bulkWriteOrderedData = [{ _id: 1, c: 1 }, { _id: 2, c: 2 }, { _id: 3, c: 3 }];
    var bulkWriteUnOrderedData = [{ _id: 1, c: 1 }, { _id: 2, c: 2 }, { _id: 3, c: 3 }];
    var distinctData = [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }];

    // Setup method
    var setup = function(col, method, data) {
        col.remove({});
        col.insertMany(data);
    }

    // Setup executors
    var deleteManyExecutor = createTestExecutor(col, 'deleteMany', data, setup);
    var deleteOneExecutor = createTestExecutor(col, 'deleteOne', data, setup);
    var findOneAndDeleteExecutor = createTestExecutor(col, 'findOneAndDelete', data, setup);
    var findOneAndReplaceExecutor = createTestExecutor(col, 'findOneAndReplace', data, setup);
    var findOneAndUpdateExecutor = createTestExecutor(col, 'findOneAndUpdate', data, setup);
    var insertManyExecutor = createTestExecutor(col, 'insertMany', findOneData, setup);
    var insertOneExecutor = createTestExecutor(col, 'insertOne', findOneData, setup);
    var replaceOneExecutor = createTestExecutor(col, 'replaceOne', replaceOneData, setup);
    var updateManyExecutor = createTestExecutor(col, 'updateMany', updateManyData, setup);
    var updateOneExecutor = createTestExecutor(col, 'updateOne', updateOneData, setup);
    var bulkOrderedWriteExecutor = createTestExecutor(col, 'bulkWrite', bulkWriteOrderedData, setup);
    var bulkUnOrderedWriteExecutor = createTestExecutor(col, 'bulkWrite', bulkWriteUnOrderedData, setup);
    var countExecutor = createTestExecutor(col, 'count', data, setup);
    var distinctExecutor = createTestExecutor(col, 'distinct', distinctData, setup);

    //
    // BulkWrite
    //

    bulkOrderedWriteExecutor({
      params: [[
          { insertOne: { document: {_id: 4, a: 1 } } }
        , { updateOne: { filter: {_id: 5, a:2}, update: {$set: {a:2}}, upsert:true } }
        , { updateMany: { filter: {_id: 6,a:3}, update: {$set: {a:3}}, upsert:true } }
        , { deleteOne: { filter: {c:1} } }
        , { deleteMany: { filter: {c:2} } }
        , { replaceOne: { filter: {c:3}, replacement: {c:4}, upsert:true } }]],
      result: {
        acknowledged: true, insertedCount:1, matchedCount:1, deletedCount:2, upsertedCount:2, insertedIds : {'0' : 4 }, upsertedIds : { '1' : 5, '2' : 6 }
      },
      db: [{ "_id" : 3, "c" : 4 }, { "_id" : 4, "a" : 1 }, { "_id" : 5, "a" : 2 }, { "_id" : 6, "a" : 3 }]
    });

    bulkUnOrderedWriteExecutor({
      params: [[
          { insertOne: { document: { _id: 4, a: 1 } } }
        , { updateOne: { filter: {_id: 5, a:2}, update: {$set: {a:2}}, upsert:true } }
        , { updateMany: { filter: {_id: 6, a:3}, update: {$set: {a:3}}, upsert:true } }
        , { deleteOne: { filter: {c:1} } }
        , { deleteMany: { filter: {c:2} } }
        , { replaceOne: { filter: {c:3}, replacement: {c:4}, upsert:true } }], { ordered: false }],
      result: {
        acknowledged: true, insertedCount:1, matchedCount:1, deletedCount:2, upsertedCount:2, insertedIds : {'0' : 4 }, upsertedIds : { '1' : 5, '2' : 6 }
      },
      db: [{ "_id" : 3, "c" : 4 }, { "_id" : 4, "a" : 1 }, { "_id" : 5, "a" : 2 }, { "_id" : 6, "a" : 3 }]
    });

    //
    // DeleteMany
    //

    // DeleteMany when many documents match
    deleteManyExecutor({
      params: [
        { _id: { $gt: 1 } }
      ],
      result: {acknowledged: true, deletedCount:2},
      db: [{_id:1, x: 11}]
    });
    // DeleteMany when no document matches
    deleteManyExecutor({
      params: [
        { _id: 4 }
      ],
      result: {acknowledged: true, deletedCount:0},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
    });
    // DeleteMany when many documents match, no write concern
    deleteManyExecutor({
      params: [
        { _id: { $gt: 1 } }, { w : 0 }
      ],
      result: {acknowledged: false},
      db: [{_id:1, x: 11}]
    });

    //
    // DeleteOne
    //

    // DeleteOne when many documents match
    deleteOneExecutor({
      params: [
          { _id: { $gt: 1 } }
        ],
      result: {acknowledged: true, deletedCount:1},
      db: [{_id:1, x: 11}, {_id:3, x: 33}]
    });
    // DeleteOne when one document matches
    deleteOneExecutor({
      params: [
          { _id: 2 }
        ],
      result: {acknowledged: true, deletedCount:1},
      db: [{_id:1, x: 11}, {_id:3, x: 33}]
    });
    // DeleteOne when no documents match
    deleteOneExecutor({
      params: [
          { _id: 4 }
        ],
      result: {acknowledged: true, deletedCount:0},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
    });
    // DeleteOne when many documents match, no write concern
    deleteOneExecutor({
      params: [
        { _id: { $gt: 1 } }, {w:0}
      ],
      result: {acknowledged: false},
      db: [{_id:1, x: 11}, {_id:3, x: 33}]
    });

    //
    // FindOneAndDelete
    //

    // FindOneAndDelete when one document matches
    findOneAndDeleteExecutor({
      params: [
            { _id: { $gt: 1 } }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
        ],
      result: {x:22},
      db: [{_id:1, x: 11}, {_id:3, x: 33}]
    });
    // FindOneAndDelete when one document matches
    findOneAndDeleteExecutor({
      params: [
            { _id: 2 }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
        ],
      result: {x:22},
      db: [{_id:1, x: 11}, {_id:3, x: 33}]
    });
    // FindOneAndDelete when no documents match
    findOneAndDeleteExecutor({
      params: [
            { _id: 4 }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
        ],
      result: null,
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
    });

    //
    // FindOneAndReplace
    //

    // FindOneAndReplace when many documents match returning the document before modification
    findOneAndReplaceExecutor({
      params: [
            { _id: { $gt: 1 } }
          , { x: 32 }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
        ],
      result: {x:22},
      db: [{_id:1, x: 11}, {_id:2, x: 32}, {_id:3, x: 33}]
    });
    // FindOneAndReplace when many documents match returning the document after modification
    findOneAndReplaceExecutor({
      params: [
            { _id: { $gt: 1 } }
          , { x: 32 }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true }
        ],
      result: {x:32},
      db: [{_id:1, x: 11}, {_id:2, x: 32}, {_id:3, x: 33}]
    });
    // FindOneAndReplace when one document matches returning the document before modification
    findOneAndReplaceExecutor({
      params: [
            { _id: 2 }
          , { x: 32 }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
        ],
      result: {x:22},
      db: [{_id:1, x: 11}, {_id:2, x: 32}, {_id:3, x: 33}]
    });
    // FindOneAndReplace when one document matches returning the document after modification
    findOneAndReplaceExecutor({
      params: [
            { _id: 2 }
          , { x: 32 }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true }
        ],
      result: {x:32},
      db: [{_id:1, x: 11}, {_id:2, x: 32}, {_id:3, x: 33}]
    });
    // FindOneAndReplace when no documents match returning the document before modification
    findOneAndReplaceExecutor({
      params: [
            { _id: 4 }
          , { x: 44 }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
        ],
      result: null,
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
    });
    // FindOneAndReplace when no documents match with upsert returning the document before modification
    findOneAndReplaceExecutor({
      params: [
            { _id: 4 }
          , { x: 44 }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, upsert:true }
        ],
      result: null,
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x:44}]
    });
    // FindOneAndReplace when no documents match returning the document after modification
    findOneAndReplaceExecutor({
      params: [
            { _id: 4 }
          , { x: 44 }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true }
        ],
      result: null,
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
    });
    // FindOneAndReplace when no documents match with upsert returning the document after modification
    findOneAndReplaceExecutor({
      params: [
            { _id: 4 }
          , { x: 44 }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true, upsert:true }
        ],
      result: {x:44},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 44}]
    });

    //
    // FindOneAndUpdate
    //

    // FindOneAndUpdate when many documents match returning the document before modification
    findOneAndUpdateExecutor({
      params: [
            { _id: { $gt: 1 } }
          , { $inc: { x: 1 } }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
        ],
      result: {x:22},
      db: [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]
    });
    // FindOneAndUpdate when many documents match returning the document after modification
    findOneAndUpdateExecutor({
      params: [
            { _id: { $gt: 1 } }
          , { $inc: { x: 1 } }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument: true }
        ],
      result: {x:23},
      db: [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]
    });
    // FindOneAndUpdate when one document matches returning the document before modification
    findOneAndUpdateExecutor({
      params: [
            { _id: 2 }
          , { $inc: { x: 1 } }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
        ],
      result: {x:22},
      db: [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]
    });
    // FindOneAndUpdate when one document matches returning the document after modification
    findOneAndUpdateExecutor({
      params: [
            { _id: 2 }
          , { $inc: { x: 1 } }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument: true }
        ],
      result: {x:23},
      db: [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]
    });
    // FindOneAndUpdate when no documents match returning the document before modification
    findOneAndUpdateExecutor({
      params: [
            { _id: 4 }
          , { $inc: { x: 1 } }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
        ],
      result: null,
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
    });
    // FindOneAndUpdate when no documents match with upsert returning the document before modification
    findOneAndUpdateExecutor({
      params: [
            { _id: 4 }
          , { $inc: { x: 1 } }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, upsert:true }
        ],
      result: null,
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
    });
    // FindOneAndUpdate when no documents match returning the document after modification
    findOneAndUpdateExecutor({
      params: [
            { _id: 4 }
          , { $inc: { x: 1 } }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true }
        ],
      result: null,
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
    });
    // FindOneAndUpdate when no documents match with upsert returning the document after modification
    findOneAndUpdateExecutor({
          params: [
            { _id: 4 }
          , { $inc: { x: 1 } }
          , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true, upsert:true }
        ],
      result: {x:1},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
    });

    //
    // InsertMany
    //

    // InsertMany with non-existing documents
    insertManyExecutor({
      params: [
          [{_id: 2, x: 22}, {_id:3, x:33}]
        ],
      result: {acknowledged: true, insertedIds: [2, 3]},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
    });
    // InsertMany with non-existing documents, no write concern
    insertManyExecutor({
      params: [
            [{_id: 2, x: 22}, {_id:3, x:33}]
          , {w:0}
        ],
      result: {acknowledged: false},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
    });

    //
    // InsertOne
    //

    // InsertMany with non-existing documents
    insertOneExecutor({
      params: [
          {_id: 2, x: 22}
        ],
      result: {acknowledged: true, insertedId: 2},
      db: [{_id:1, x: 11}, {_id:2, x: 22}]
    });
    // InsertMany with non-existing documents, no write concern
    insertOneExecutor({
          params: [
          {_id: 2, x: 22}, {w:0}
        ],
      result: {acknowledged: false},
      db: [{_id:1, x: 11}, {_id:2, x: 22}]
    });

    //
    // ReplaceOne
    //

    // ReplaceOne when many documents match
    replaceOneExecutor({
      params: [{ _id: { $gt: 1 } }, { x: 111 }],
      result: {acknowledged:true, matchedCount:1, modifiedCount:1},
      db: [{_id:1, x: 11}, {_id:2, x: 111}, {_id:3, x: 33}]
    });
    // ReplaceOne when one document matches
    replaceOneExecutor({
      params: [{ _id: 1 }, { _id: 1, x: 111 }],
      result: {acknowledged:true, matchedCount:1, modifiedCount:1},
      db: [{_id:1, x: 111}, {_id:2, x: 22}, {_id:3, x: 33}]
    });
    // ReplaceOne when no documents match
    replaceOneExecutor({
      params: [{ _id: 4 }, { _id: 4, x: 1 }],
      result: {acknowledged:true, matchedCount:0, modifiedCount:0},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
    });
    // ReplaceOne with upsert when no documents match without an id specified
    replaceOneExecutor({
      params: [{ _id: 4 }, { x: 1 }, {upsert:true}],
      result: {acknowledged:true, matchedCount:0, modifiedCount:0, upsertedId: 4},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
    });
    // ReplaceOne with upsert when no documents match with an id specified
    replaceOneExecutor({
      params: [{ _id: 4 }, { _id: 4, x: 1 }, {upsert:true}],
      result: {acknowledged:true, matchedCount:0, modifiedCount:0, upsertedId: 4},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
    });
    // ReplaceOne with upsert when no documents match with an id specified, no write concern
    replaceOneExecutor({
      params: [{ _id: 4 }, { _id: 4, x: 1 }, {upsert:true, w:0}],
      result: {acknowledged:false},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
    });
    // ReplaceOne with upsert when no documents match with an id specified, no write concern
    replaceOneExecutor({
      params: [{ _id: 4 }, { _id: 4, x: 1 }, {upsert:true, writeConcern:{w:0}}],
      result: {acknowledged:false},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
    });

    //
    // UpdateMany
    //

    // UpdateMany when many documents match
    updateManyExecutor({
      params: [{ _id: { $gt: 1 } }, { $inc: { x: 1 } }],
      result: {acknowledged:true, matchedCount:2, modifiedCount:2},
      db: [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 34}]
    });
    // UpdateMany when one document matches
    updateManyExecutor({
      params: [{ _id: 1 }, { $inc: { x: 1 } }],
      result: {acknowledged:true, matchedCount:1, modifiedCount:1},
      db: [{_id:1, x: 12}, {_id:2, x: 22}, {_id:3, x: 33}]
    });
    // UpdateMany when no documents match
    updateManyExecutor({
      params: [{ _id: 4 }, { $inc: { x: 1 } }],
      result: {acknowledged:true, matchedCount:0, modifiedCount:0},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
    });
    // UpdateMany with upsert when no documents match
    updateManyExecutor({
      params: [{ _id: 4 }, { $inc: { x: 1 } }, { upsert: true }],
      result: {acknowledged:true, matchedCount:0, modifiedCount:0, upsertedId: 4},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
    });
    // UpdateMany with upsert when no documents match, no write concern
    updateManyExecutor({
      params: [{ _id: 4 }, { $inc: { x: 1 } }, { upsert: true, w: 0 }],
      result: {acknowledged:false},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
    });

    //
    // UpdateOne
    //

    // UpdateOne when many documents match
    updateOneExecutor({
      params: [{ _id: { $gt: 1 } }, { $inc: { x: 1 } }],
      result: {acknowledged:true, matchedCount:1, modifiedCount:1},
      db: [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]
    });
    // UpdateOne when one document matches
    updateOneExecutor({
      params: [{ _id: 1 }, { $inc: { x: 1 } }],
      result: {acknowledged:true, matchedCount:1, modifiedCount:1},
      db: [{_id:1, x: 12}, {_id:2, x: 22}, {_id:3, x: 33}]
    });
    // UpdateOne when no documents match
    updateOneExecutor({
      params: [{ _id: 4 }, { $inc: { x: 1 } }],
      result: {acknowledged:true, matchedCount:0, modifiedCount:0},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
    });

    // UpdateOne with upsert when no documents match
    updateOneExecutor({
      params: [{ _id: 4 }, { $inc: { x: 1 } }, {upsert:true}],
      result: {acknowledged:true, matchedCount:0, modifiedCount:0, upsertedId: 4},
      db: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id: 4, x: 1}]
    });
    // UpdateOne when many documents match, no write concern
    updateOneExecutor({
      params: [{ _id: { $gt: 1 } }, { $inc: { x: 1 } }, {w:0}],
      result: {acknowledged:false},
      db: [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]
    });

    //
    // Count
    //

    // Simple count of all elements
    countExecutor({
      params: [{}],
      result: 3,
      db: data
    });
    // Simple count no arguments
    countExecutor({
      params: [],
      result: 3,
      db: data
    });
    // Simple count filtered
    countExecutor({
      params: [{_id: {$gt: 1}}],
      result: 2,
      db: data
    });
    // Simple count of all elements, applying limit
    countExecutor({
      params: [{}, {limit:1}],
      result: 1,
      db: data
    });
    // Simple count of all elements, applying skip
    countExecutor({
      params: [{}, {skip:1}],
      result: 2,
      db: data
    });
    // Simple count no arguments, applying hint
    countExecutor({
      params: [{}, {hint: "_id"}],
      result: 3,
      db: data
    });

    //
    // Distinct
    //

    // Simple distinct of field x no filter
    distinctExecutor({
      params: ['x'],
      result: [11, 22, 33],
      db: data
    });
    // Simple distinct of field x
    distinctExecutor({
      params: ['x', {}],
      result: [11, 22, 33],
      db: data
    });
    // Simple distinct of field x filtered
    distinctExecutor({
      params: ['x', {x: { $gt: 11 }}],
      result: [22, 33],
      db: data
    });
    // Simple distinct of field x filtered with maxTimeMS
    distinctExecutor({
      params: ['x', {x: { $gt: 11 }}, {maxTimeMS:100}],
      result: [22, 33],
      db: data
    });

    //
    // Find
    //

    col.deleteMany({});
    // Insert all of them
    col.insertMany([{a:0, b:0}, {a:1, b:1}]);

    // Simple projection
    var result = col.find({}).sort({a:1}).limit(1).skip(1).projection({_id:0, a:1}).toArray();
    assert.docEq(result, [{a:1}]);

    // Simple tailable cursor, should fail
    var cursor = col.find({}).sort({a:1}).tailable();
    assert.eq(34, cursor._options);
    var cursor = col.find({}).sort({a:1}).tailable(false);
    assert.eq(2, cursor._options);

    // Check modifiers
    var cursor = col.find({}).modifiers({$hint:'a_1'});
    assert.eq('a_1', cursor._query['$hint']);

    // allowPartialResults
    var cursor = col.find({}).allowPartialResults();
    assert.eq(128, cursor._options);

    // noCursorTimeout
    var cursor = col.find({}).noCursorTimeout();
    assert.eq(16, cursor._options);

    // oplogReplay
    var cursor = col.find({}).oplogReplay();
    assert.eq(8, cursor._options);

    //
    // Aggregation
    //

    col.deleteMany({});
    // Insert all of them
    col.insertMany([{a:0, b:0}, {a:1, b:1}]);

    // Simple aggregation with useCursor
    var result = col.aggregate([{$match: {}}], {useCursor:true}).toArray();
    assert.eq(2, result.length);

    // Simple aggregation with batchSize
    var result = col.aggregate([{$match: {}}], {batchSize:2}).toArray();
    assert.eq(2, result.length);

    // Set the maxTimeMS and allowDiskUse on aggregation query
    var result = col.aggregate([{$match: {}}], {batchSize:2, maxTimeMS:100, allowDiskUse:true}).toArray();
    assert.eq(2, result.length);
}

crudAPISpecTests();
