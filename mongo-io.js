/* jshint node:true */
'use strict';


// or more concisely
// var sys = require('sys');
var Logger = require('logb').getLogger(module.filename),
	MongoClient = require('mongodb').MongoClient;

// var byline = require('byline'),
// 	through2 = require('through2'),
// 	EJSON = require('mongodb-extended-json'),
// 	fs = require('fs');

var exec = require('child_process').exec,
	Logger = require('logb').getLogger(module.filename),
	MongoClient = require('mongodb').MongoClient;



// module.exports.mongoimport = function(opts, cb) {
// 	var host, port, dbName, collectionName, path;

// 	host = opts.host;
// 	port = opts.port;
// 	dbName = opts.db;
// 	collectionName = opts.collection;
// 	path = opts.path;

// 	Logger.debug('Connecting to database...');



// 	var url = 'mongodb://' + host + ':' + port + '/' + dbName;
// 	// console.log('url', url)
// 	MongoClient.connect(url, function(err, db) {
// 		var collection = db.collection(collectionName);


// 		byline(fs.createReadStream(path, {
// 				encoding: 'utf8'
// 			}))
// 			.pipe(through2.obj(function(line, encoding, done) {
// 				var mongoObject;
// 				var newObj = {};

// 				mongoObject = EJSON.parse(line);
// 				if (mongoObject.reviewTitle2sku) {
// 					Object.keys(mongoObject.reviewTitle2sku).forEach(function(key) {
// 						var newKey;
// 						newKey = key.replace(/\./gi, 'U+FF0E');
// 						newObj[newKey] = mongoObject.reviewTitle2sku[key];
// 					});
// 					mongoObject.reviewTitle2sku = newObj;
// 				}

// 				this.push(mongoObject);
// 				done();
// 			}))
// 			.pipe(through2.obj(function(mongoObject, encoding, done) {
// 				var self = this;
// 				var same;

// 				// console.log(JSON.stringify(mongoObject, null, 4))
// 				console.log(typeof mongoObject._id)

// 				// same = JSON.parse(JSON.stringify(mongoObject));
// 				// delete same._id;
// 				// console.log('inserting', JSON.stringify(mongoObject).substr(0, 100))
// 				// collection.update({
// 				// 	_id: mongoObject._id
// 				// }, same, {
// 				// 	upsert: true
// 				// }, function(err, result) {
// 				// 	if (err) {
// 				// 		err.mongoObject = mongoObject;
// 				// 		return self.emit('error', err);
// 				// 	}
// 				// 	Logger.debug('Inserted document into', '"' + collection.s.name + '"');
// 				// 	self.push(result);
// 				// 	done();
// 				// });
// 				collection.insert(mongoObject, function(err, result) {
// 					if (err) {
// 						err.mongoObject = mongoObject;
// 						return self.emit('error', err);
// 					}
// 					Logger.debug('Inserted document into', '"' + collection.s.name + '"');
// 					self.push(result);
// 					done();
// 				});
// 			}))
// 			.on('error', function(err) {
// 				console.log('Error', JSON.stringify(err, null, 4));
// 				process.exit();
// 			})
// 			.on('data', function() {})
// 			.on('end', function() {
// 				// console.log('end!')
// 				db.close();
// 				cb();
// 			});
// 	});

// };

function getCb(cb) {
	return function(error, stdout, stderr) {


		if (stdout && stdout !== '') {
			Logger.info('MongoIO >>>: ' + stdout);
		}
		if (stderr && stderr !== '') {
			Logger.info('MongoIO >>>: ' + stderr);
		}
		if (error !== null) {
			// console.log('exec error: ' + error);
			return cb(error);
		}
		cb(null, null);

	};
	// if (error) {
	// 	console.log('Error', err);
	// }
	// sys.puts(stdout);
	// sys.puts(stderr);

}

module.exports.mongoimport = function(opts, cb) {
	var host, port, dbName, collection, dataPath;

	host = opts.host;
	port = opts.port;
	dbName = opts.db;
	collection = opts.collection;
	dataPath = opts.path;


	var cmd;

	cmd = ['mongoimport', '--host', host, '--port', port, '-d', dbName, '-c', collection, '<', dataPath].join(' ');

	Logger.info('Running following cmd:', cmd);

	exec(cmd, getCb(cb));

};
module.exports.emptyCollection = function(opts, cb) {
	var host, port, dbName, collectionName;

	host = opts.host;
	port = opts.port;
	dbName = opts.db;
	collectionName = opts.collection;



	// Connection URL
	var url = 'mongodb://' + host + ':' + port + '/' + dbName;
	// Use connect method to connect to the Server
	MongoClient.connect(url, function(err, db) {

		// Get the documents collection
		var collection = db.collection(collectionName);
		// Insert some documents
		collection.drop(function(err, result) {
			if (err) {
				return cb(err);
			}
			Logger.info('Removed all documents in collection', '"' + collection.s.name + '"');
			cb(null, result);
			db.close();
		});
	});
};