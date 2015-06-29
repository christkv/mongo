(function() {
    "use strict";

    var crudAPISpecTests = function crudAPISpecTests() {
        "use strict";

        // Get the colllection
        var coll = db.crud_tests;

        // Setup
        function createTestExecutor(coll, method) {
            return function(args) {
                // Drop collection
                coll.drop();
                // Insert test data
                var r = coll.insertMany(args.insert);
                assert.eq(args.insert.length, r.insertedIds.length);

                // Execute the method with arguments
                var r = coll[method].apply(coll, args.params);
                assert.docEq(args.result, r);

                // Get all the results
                var results = coll.find({}).toArray();
                assert.docEq(args.expected, results);
            }
        }

        // Setup executors
        var deleteManyExecutor = createTestExecutor(coll, 'deleteMany');
        var deleteOneExecutor = createTestExecutor(coll, 'deleteOne');
        var bulkOrderedWriteExecutor = createTestExecutor(coll, 'bulkWrite');
        var bulkUnOrderedWriteExecutor = createTestExecutor(coll, 'bulkWrite');
        var findOneAndDeleteExecutor = createTestExecutor(coll, 'findOneAndDelete');
        var findOneAndReplaceExecutor = createTestExecutor(coll, 'findOneAndReplace');
        var findOneAndUpdateExecutor = createTestExecutor(coll, 'findOneAndUpdate');
        var insertManyExecutor = createTestExecutor(coll, 'insertMany');
        var insertOneExecutor = createTestExecutor(coll, 'insertOne');
        var replaceOneExecutor = createTestExecutor(coll, 'replaceOne');
        var updateManyExecutor = createTestExecutor(coll, 'updateMany');
        var updateOneExecutor = createTestExecutor(coll, 'updateOne');
        var countExecutor = createTestExecutor(coll, 'count');
        var distinctExecutor = createTestExecutor(coll, 'distinct');

        //
        // BulkWrite
        //

        bulkOrderedWriteExecutor({
          insert: [{ _id: 1, c: 1 }, { _id: 2, c: 2 }, { _id: 3, c: 3 }],
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
          expected: [{ "_id" : 3, "c" : 4 }, { "_id" : 4, "a" : 1 }, { "_id" : 5, "a" : 2 }, { "_id" : 6, "a" : 3 }]
        });

        bulkUnOrderedWriteExecutor({
          insert: [{ _id: 1, c: 1 }, { _id: 2, c: 2 }, { _id: 3, c: 3 }],
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
          expected: [{ "_id" : 3, "c" : 4 }, { "_id" : 4, "a" : 1 }, { "_id" : 5, "a" : 2 }, { "_id" : 6, "a" : 3 }]
        });

        // DeleteMany
        //

        // DeleteMany when many documents match
        deleteManyExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
            { _id: { $gt: 1 } }
          ],
          result: {acknowledged: true, deletedCount:2},
          expected: [{_id:1, x: 11}]
        });
        // DeleteMany when no document matches
        deleteManyExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
            { _id: 4 }
          ],
          result: {acknowledged: true, deletedCount:0},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
        });
        // DeleteMany when many documents match, no write concern
        deleteManyExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
            { _id: { $gt: 1 } }, { w : 0 }
          ],
          result: {acknowledged: false},
          expected: [{_id:1, x: 11}]
        });

        //
        // DeleteOne
        //

        // DeleteOne when many documents match
        deleteOneExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
              { _id: { $gt: 1 } }
            ],
          result: {acknowledged: true, deletedCount:1},
          expected: [{_id:1, x: 11}, {_id:3, x: 33}]
        });
        // DeleteOne when one document matches
        deleteOneExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
              { _id: 2 }
            ],
          result: {acknowledged: true, deletedCount:1},
          expected: [{_id:1, x: 11}, {_id:3, x: 33}]
        });
        // DeleteOne when no documents match
        deleteOneExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
              { _id: 4 }
            ],
          result: {acknowledged: true, deletedCount:0},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
        });
        // DeleteOne when many documents match, no write concern
        deleteOneExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
            { _id: { $gt: 1 } }, {w:0}
          ],
          result: {acknowledged: false},
          expected: [{_id:1, x: 11}, {_id:3, x: 33}]
        });

        //
        // FindOneAndDelete
        //

        // FindOneAndDelete when one document matches
        findOneAndDeleteExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: { $eq: 2 } }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
            ],
          result: {x:22},
          expected: [{_id:1, x: 11}, {_id:3, x: 33}]
        });
        // FindOneAndDelete when one document matches
        findOneAndDeleteExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: 2 }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
            ],
          result: {x:22},
          expected: [{_id:1, x: 11}, {_id:3, x: 33}]
        });
        // FindOneAndDelete when no documents match
        findOneAndDeleteExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: 4 }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
            ],
          result: null,
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
        });

        //
        // FindOneAndReplace
        //

        // FindOneAndReplace when many documents match returning the document before modification
        findOneAndReplaceExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: { $gt: 1 } }
              , { x: 32 }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
            ],
          result: {x:22},
          expected: [{_id:1, x: 11}, {_id:2, x: 32}, {_id:3, x: 33}]
        });
        // FindOneAndReplace when many documents match returning the document after modification
        findOneAndReplaceExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: { $gt: 1 } }
              , { x: 32 }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true }
            ],
          result: {x:32},
          expected: [{_id:1, x: 11}, {_id:2, x: 32}, {_id:3, x: 33}]
        });
        // FindOneAndReplace when one document matches returning the document before modification
        findOneAndReplaceExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: 2 }
              , { x: 32 }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
            ],
          result: {x:22},
          expected: [{_id:1, x: 11}, {_id:2, x: 32}, {_id:3, x: 33}]
        });
        // FindOneAndReplace when one document matches returning the document after modification
        findOneAndReplaceExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: 2 }
              , { x: 32 }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true }
            ],
          result: {x:32},
          expected: [{_id:1, x: 11}, {_id:2, x: 32}, {_id:3, x: 33}]
        });
        // FindOneAndReplace when no documents match returning the document before modification
        findOneAndReplaceExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: 4 }
              , { x: 44 }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
            ],
          result: null,
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
        });
        // FindOneAndReplace when no documents match with upsert returning the document before modification
        findOneAndReplaceExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: 4 }
              , { x: 44 }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, upsert:true }
            ],
          result: null,
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x:44}]
        });
        // FindOneAndReplace when no documents match returning the document after modification
        findOneAndReplaceExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: 4 }
              , { x: 44 }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true }
            ],
          result: null,
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
        });
        // FindOneAndReplace when no documents match with upsert returning the document after modification
        findOneAndReplaceExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: 4 }
              , { x: 44 }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true, upsert:true }
            ],
          result: {x:44},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 44}]
        });

        //
        // FindOneAndUpdate
        //

        // FindOneAndUpdate when many documents match returning the document before modification
        findOneAndUpdateExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: { $gt: 1 } }
              , { $inc: { x: 1 } }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
            ],
          result: {x:22},
          expected: [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]
        });
        // FindOneAndUpdate when many documents match returning the document after modification
        findOneAndUpdateExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: { $gt: 1 } }
              , { $inc: { x: 1 } }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument: true }
            ],
          result: {x:23},
          expected: [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]
        });
        // FindOneAndUpdate when one document matches returning the document before modification
        findOneAndUpdateExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: 2 }
              , { $inc: { x: 1 } }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
            ],
          result: {x:22},
          expected: [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]
        });
        // FindOneAndUpdate when one document matches returning the document after modification
        findOneAndUpdateExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: 2 }
              , { $inc: { x: 1 } }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument: true }
            ],
          result: {x:23},
          expected: [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]
        });
        // FindOneAndUpdate when no documents match returning the document before modification
        findOneAndUpdateExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: 4 }
              , { $inc: { x: 1 } }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 } }
            ],
          result: null,
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
        });
        // FindOneAndUpdate when no documents match with upsert returning the document before modification
        findOneAndUpdateExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: 4 }
              , { $inc: { x: 1 } }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, upsert:true }
            ],
          result: null,
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
        });
        // FindOneAndUpdate when no documents match returning the document after modification
        findOneAndUpdateExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: 4 }
              , { $inc: { x: 1 } }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true }
            ],
          result: null,
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
        });
        // FindOneAndUpdate when no documents match with upsert returning the document after modification
        findOneAndUpdateExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [
                { _id: 4 }
              , { $inc: { x: 1 } }
              , { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true, upsert:true }
            ],
          result: {x:1},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
        });

        //
        // InsertMany
        //

        // InsertMany with non-existing documents
        insertManyExecutor({
          insert: [{ _id:1, x:11 }],
          params: [
              [{_id: 2, x: 22}, {_id:3, x:33}]
            ],
          result: {acknowledged: true, insertedIds: [2, 3]},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
        });
        // InsertMany with non-existing documents, no write concern
        insertManyExecutor({
          insert: [{ _id:1, x:11 }],
          params: [
                [{_id: 2, x: 22}, {_id:3, x:33}]
              , {w:0}
            ],
          result: {acknowledged: false},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
        });

        //
        // InsertOne
        //

        // InsertOne with non-existing documents
        insertOneExecutor({
          insert: [{ _id:1, x:11 }],
          params: [
              {_id: 2, x: 22}
            ],
          result: {acknowledged: true, insertedId: 2},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}]
        });
        // InsertOne with non-existing documents, no write concern
        insertOneExecutor({
          insert: [{ _id:1, x:11 }],
          params: [
              {_id: 2, x: 22}, {w:0}
            ],
          result: {acknowledged: false},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}]
        });

        //
        // ReplaceOne
        //

        // ReplaceOne when many documents match
        replaceOneExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: { $gt: 1 } }, { x: 111 }],
          result: {acknowledged:true, matchedCount:1, modifiedCount:1},
          expected: [{_id:1, x: 11}, {_id:2, x: 111}, {_id:3, x: 33}]
        });
        // ReplaceOne when one document matches
        replaceOneExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: 1 }, { _id: 1, x: 111 }],
          result: {acknowledged:true, matchedCount:1, modifiedCount:1},
          expected: [{_id:1, x: 111}, {_id:2, x: 22}, {_id:3, x: 33}]
        });
        // ReplaceOne when no documents match
        replaceOneExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: 4 }, { _id: 4, x: 1 }],
          result: {acknowledged:true, matchedCount:0, modifiedCount:0},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
        });
        // ReplaceOne with upsert when no documents match without an id specified
        replaceOneExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: 4 }, { x: 1 }, {upsert:true}],
          result: {acknowledged:true, matchedCount:0, modifiedCount:0, upsertedId: 4},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
        });
        // ReplaceOne with upsert when no documents match with an id specified
        replaceOneExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: 4 }, { _id: 4, x: 1 }, {upsert:true}],
          result: {acknowledged:true, matchedCount:0, modifiedCount:0, upsertedId: 4},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
        });
        // ReplaceOne with upsert when no documents match with an id specified, no write concern
        replaceOneExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: 4 }, { _id: 4, x: 1 }, {upsert:true, w:0}],
          result: {acknowledged:false},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
        });
        // ReplaceOne with upsert when no documents match with an id specified, no write concern
        replaceOneExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: 4 }, { _id: 4, x: 1 }, {upsert:true, writeConcern:{w:0}}],
          result: {acknowledged:false},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
        });

        //
        // UpdateMany
        //

        // UpdateMany when many documents match
        updateManyExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: { $gt: 1 } }, { $inc: { x: 1 } }],
          result: {acknowledged:true, matchedCount:2, modifiedCount:2},
          expected: [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 34}]
        });
        // UpdateMany when one document matches
        updateManyExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: 1 }, { $inc: { x: 1 } }],
          result: {acknowledged:true, matchedCount:1, modifiedCount:1},
          expected: [{_id:1, x: 12}, {_id:2, x: 22}, {_id:3, x: 33}]
        });
        // UpdateMany when no documents match
        updateManyExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: 4 }, { $inc: { x: 1 } }],
          result: {acknowledged:true, matchedCount:0, modifiedCount:0},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
        });
        // UpdateMany with upsert when no documents match
        updateManyExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: 4 }, { $inc: { x: 1 } }, { upsert: true }],
          result: {acknowledged:true, matchedCount:0, modifiedCount:0, upsertedId: 4},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
        });
        // UpdateMany with upsert when no documents match, no write concern
        updateManyExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: 4 }, { $inc: { x: 1 } }, { upsert: true, w: 0 }],
          result: {acknowledged:false},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]
        });

        //
        // UpdateOne
        //

        // UpdateOne when many documents match
        updateOneExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: { $gt: 1 } }, { $inc: { x: 1 } }],
          result: {acknowledged:true, matchedCount:1, modifiedCount:1},
          expected: [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]
        });
        // UpdateOne when one document matches
        updateOneExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: 1 }, { $inc: { x: 1 } }],
          result: {acknowledged:true, matchedCount:1, modifiedCount:1},
          expected: [{_id:1, x: 12}, {_id:2, x: 22}, {_id:3, x: 33}]
        });
        // UpdateOne when no documents match
        updateOneExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: 4 }, { $inc: { x: 1 } }],
          result: {acknowledged:true, matchedCount:0, modifiedCount:0},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]
        });

        // UpdateOne with upsert when no documents match
        updateOneExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: 4 }, { $inc: { x: 1 } }, {upsert:true}],
          result: {acknowledged:true, matchedCount:0, modifiedCount:0, upsertedId: 4},
          expected: [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id: 4, x: 1}]
        });
        // UpdateOne when many documents match, no write concern
        updateOneExecutor({
          insert: [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }],
          params: [{ _id: { $gt: 1 } }, { $inc: { x: 1 } }, {w:0}],
          result: {acknowledged:false},
          expected: [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]
        });

        //
        // Count
        //

        // Simple count of all elements
        countExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [{}],
          result: 3,
          expected: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }]
        });
        // Simple count no arguments
        countExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [],
          result: 3,
          expected: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }]
        });
        // Simple count filtered
        countExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [{_id: {$gt: 1}}],
          result: 2,
          expected: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }]
        });
        // Simple count of all elements, applying limit
        countExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [{}, {limit:1}],
          result: 1,
          expected: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }]
        });
        // Simple count of all elements, applying skip
        countExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [{}, {skip:1}],
          result: 2,
          expected: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }]
        });
        // Simple count no arguments, applying hint
        countExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: [{}, {hint: "_id"}],
          result: 3,
          expected: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }]
        });

        //
        // Distinct
        //

        // Simple distinct of field x no filter
        distinctExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: ['x'],
          result: [11, 22, 33],
          expected: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }]
        });
        // Simple distinct of field x
        distinctExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: ['x', {}],
          result: [11, 22, 33],
          expected: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }]
        });
        // Simple distinct of field x filtered
        distinctExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: ['x', {x: { $gt: 11 }}],
          result: [22, 33],
          expected: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }]
        });
        // Simple distinct of field x filtered with maxTimeMS
        distinctExecutor({
          insert: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }],
          params: ['x', {x: { $gt: 11 }}, {maxTimeMS:100}],
          result: [22, 33],
          expected: [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }]
        });

        //
        // Find
        //

        coll.deleteMany({});
        // Insert all of them
        coll.insertMany([{a:0, b:0}, {a:1, b:1}]);

        // Simple projection
        var result = coll.find({}).sort({a:1}).limit(1).skip(1).projection({_id:0, a:1}).toArray();
        assert.docEq(result, [{a:1}]);

        // Simple tailable cursor
        var cursor = coll.find({}).sort({a:1}).tailable();
        assert.eq(34, cursor._options);
        var cursor = coll.find({}).sort({a:1}).tailable(false);
        assert.eq(2, cursor._options);

        // Check modifiers
        var cursor = coll.find({}).modifiers({$hint:'a_1'});
        assert.eq('a_1', cursor._query['$hint']);

        // allowPartialResults
        var cursor = coll.find({}).allowPartialResults();
        assert.eq(128, cursor._options);

        // noCursorTimeout
        var cursor = coll.find({}).noCursorTimeout();
        assert.eq(16, cursor._options);

        // oplogReplay
        var cursor = coll.find({}).oplogReplay();
        assert.eq(8, cursor._options);

        //
        // Aggregation
        //

        coll.deleteMany({});
        // Insert all of them
        coll.insertMany([{a:0, b:0}, {a:1, b:1}]);

        // Simple aggregation with useCursor
        var result = coll.aggregate([{$match: {}}], {useCursor:true}).toArray();
        assert.eq(2, result.length);

        // Simple aggregation with batchSize
        var result = coll.aggregate([{$match: {}}], {batchSize:2}).toArray();
        assert.eq(2, result.length);

        // Set the maxTimeMS and allowDiskUse on aggregation query
        var result = coll.aggregate([{$match: {}}], {batchSize:2, maxTimeMS:100, allowDiskUse:true}).toArray();
        assert.eq(2, result.length);
    }

    crudAPISpecTests();
})();
