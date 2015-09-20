/* jshint node:true */
'use strict';


// or more concisely
// var sys = require('sys');
var exec = require('child_process').exec,
	Logger = require('logb').getLogger(module.filename),
	MongoClient = require('mongodb').MongoClient;

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