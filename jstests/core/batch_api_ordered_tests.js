var collectionName = "batch_write_protocol";
var coll = db.getCollection(collectionName);

jsTest.log("Starting unordered batch tests...");

var request;
var result;

/********************************************************
 *
 * Unordered tests should return same results for write command as
 * well as for the legacy operations
 *
 *******************************************************/
var executeTests = function() {
	/**
	 * Single successful ordered batch operation
	 */
	var batch = coll.initializeOrderedBulkOp();
	batch.insert({a:1});
	batch.find({a:1}).updateOne({$set: {b:1}});
	batch.find({a:2}).upsert().updateOne({$set: {b:2}});
	batch.insert({a:3});
	batch.find({a:3}).remove({a:3});
	var result = batch.execute();
	assert.eq(5, result.n);
	var upserts = result.getUpsertedIds();
	assert.eq(1, upserts.length);
	assert.eq(2, upserts[0].index);
	assert(upserts[0]._id != null);
	var upsert = result.getUpsertedIdAt(0);
	assert.eq(2, upsert.index);
	assert(upsert._id != null);

	// Create unique index
	coll.remove({});
	coll.ensureIndex({a : 1}, {unique : true});

	/**
	 * Single error ordered batch operation
	 */
	var batch = coll.initializeOrderedBulkOp();
	batch.insert({b:1, a:1});
	batch.find({b:2}).upsert().updateOne({$set: {a:1}});
	batch.insert({b:3, a:2});
	var result = batch.execute();

	// Basic properties check
	assert.eq(1, result.n);
	assert.eq(true, result.hasErrors());
	assert.eq(1, result.getErrorCount());

	// Get the top level error
	var error = result.getSingleError();
	assert.eq(65, error.code);
	assert(error.errmsg != null);

	// Get the first error
	var error = result.getErrorAt(0);
	assert.eq(11000, error.code);
	assert(error.errmsg != null);

	// Get the operation that caused the error
	var op = error.getOperation();
	assert.eq(2, op.q.b);
	assert.eq(1, op.u['$set'].a);
	assert.eq(false, op.multi);
	assert.eq(true, op.upsert);

	// Get the first error
	var error = result.getErrorAt(1);
	assert.eq(null, error);

	// Create unique index
	coll.dropIndexes();
	coll.remove({});
	coll.ensureIndex({a : 1}, {unique : true});

	/**
	 * Multiple error ordered batch operation
	 */
	var batch = coll.initializeOrderedBulkOp();
	batch.insert({b:1, a:1});
	batch.find({b:2}).upsert().updateOne({$set: {a:1}});
	batch.find({b:3}).upsert().updateOne({$set: {a:2}});
	batch.find({b:2}).upsert().updateOne({$set: {a:1}});
	batch.insert({b:4, a:3});
	batch.insert({b:5, a:1});
	var result = batch.execute();

	// Basic properties check
	assert.eq(1, result.n);
	assert.eq(true, result.hasErrors());
	assert(1, result.getErrorCount());

	// Individual error checking
	var error = result.getErrorAt(0);
	assert.eq(1, error.index);
	assert.eq(11000, error.code);
	assert(error.errmsg != null);
	assert.eq(2, error.getOperation().q.b);
	assert.eq(1, error.getOperation().u['$set'].a);
	assert.eq(false, error.getOperation().multi);
	assert.eq(true, error.getOperation().upsert);

	// Create unique index
	coll.dropIndexes();
	coll.remove({});
	coll.ensureIndex({a : 1}, {unique : true});

	/**
	 * Fail during batch construction due to single document > maxBSONSize
	 */
	// Set up a giant string to blow through the max message size
	var hugeString = "";
	// Create it bigger than 16MB
	for(var i = 0; i < (1024 * 1100); i++) {
		hugeString = hugeString + "1234567890123456"
	}

	// Set up the batch
	var batch = coll.initializeOrderedBulkOp();
	batch.insert({b:1, a:1});
	// Should fail on insert due to string being to big
	try {
		batch.insert({string: hugeString});
		assert(false);
	} catch(err) {}

	// Create unique index
	coll.dropIndexes();
	coll.remove({});

	/**
	 * Check that batch is split when documents overflow the BSON size
	 */
	// Set up a giant string to blow through the max message size
	var hugeString = "";
	// Create it bigger than 16MB
	for(var i = 0; i < (1024 * 256); i++) {
		hugeString = hugeString + "1234567890123456"
	}

	// Insert the string a couple of times, should force split into multiple batches
	var batch = coll.initializeOrderedBulkOp();
	batch.insert({a:1, b: hugeString});
	batch.insert({a:2, b: hugeString});
	batch.insert({a:3, b: hugeString});
	batch.insert({a:4, b: hugeString});
	batch.insert({a:5, b: hugeString});
	batch.insert({a:6, b: hugeString});
	var result = batch.execute();

	// Basic properties check
	assert.eq(6, result.n);
	assert.eq(false, result.hasErrors());

	// Create unique index
	coll.dropIndexes();
	coll.remove({});
}

// Save the existing useWriteCommands function
var _useWriteCommands = coll._mongo.useWriteCommands;

// Force the use of useWriteCommands
coll._mongo.useWriteCommands = function() {
	return true;
}

// Execute tests using legacy operations
executeTests();

// Force the use of legacy commands
coll._mongo.useWriteCommands = function() {
	return false;
}

// Execute tests using legacy operations
executeTests();

// Reset the function
coll._mongo.useWriteCommands = _useWriteCommands;