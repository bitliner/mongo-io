/* jshint node:true */
'use strict';


// or more concisely
// var sys = require('sys');
var Logger = require('logb').getLogger(module.filename),
	MongoClient = require('mongodb').MongoClient;

var byline = require('byline'),
	through2 = require('through2'),
	EJSON = require('mongodb-extended-json'),
	fs = require('fs');



module.exports.mongoimport = function(opts, cb) {
	var host, port, dbName, collectionName, path;

	host = opts.host;
	port = opts.port;
	dbName = opts.db;
	collectionName = opts.collection;
	path = opts.path;

	Logger.debug('Connecting to database...');



	var url = 'mongodb://' + host + ':' + port + '/' + dbName;
	MongoClient.connect(url, function(err, db) {
		var collection = db.collection(collectionName);


		byline(fs.createReadStream(path, {
				encoding: 'utf8'
			}))
			.pipe(through2.obj(function(line, encoding, done) {

				this.push(EJSON.parse(line));
				done();
			}))
			.pipe(through2.obj(function(mongoObject, encoding, done) {
				var self = this;

				collection.insert(mongoObject, function(err, result) {
					if (err) {
						return self.emit('error', err);
					}
					Logger.debug('Inserted document into', '"' + collection.s.name + '"');
					self.push(result);
					done();
				});
			}))
			.on('data', function() {})
			.on('end', function() {
				db.close();
				cb();
			});
	});

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
		collection.remove({}, function(err, result) {
			if (err) {
				return cb(err);
			}
			Logger.info('Removed all documents in collection', '"' + collection.s.name + '"');
			cb(null, result);
			db.close();


		});


	});

};