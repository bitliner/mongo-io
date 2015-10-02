/* globals describe, it,beforeEach */
/* jshint node:true */
'use strict';

var MongoIo = require('../'),
	MongoClient = require('mongodb').MongoClient,
	path = require('path'),
	expect = require('chai').expect,
	Logger = require('logb').getLogger(module.filename);

describe('mongo.io', function() {
	var url;

	var host, port, dbName, collectionName;



	beforeEach(function() {
		host = '127.0.0.1';
		port = '27017';
		dbName = 'test-mongo-io';

		url = 'mongodb://' + host + ':' + port + '/' + dbName;
		collectionName = 'greetings';
	});


	describe('emptyCollection', function() {

		beforeEach(function(done) {


			MongoClient.connect(url, function(err, db) {
				var collection = db.collection(collectionName);
				collection.drop(function(err) {
					if (err) {
						return console.log('Error', err);
					}
					Logger.info('Removed all documents in collection "', '"' + collection.s.name + '"');

					collection.insert([{
						a: 1
					}, {
						a: 2
					}, {
						a: 3
					}], function(err) {
						if (err) {
							return console.log('Error', err);
						}
						Logger.info('Inserted documents into mongodb...');
						done();
					});
				});
			});

		});
		it('emptyCollection should work fine', function(done) {



			MongoClient.connect(url, function(err, db) {
				var collection = db.collection(collectionName);
				collection.find({}).toArray(function(err, docs) {
					expect(docs.length).to.be.eql(3);


					MongoIo.emptyCollection({
						host: host,
						port: port,
						db: dbName,
						collection: collectionName,
					}, function(err) {

						if (err) {
							console.log('err', err);
							return;
						}

						// Find some documents
						collection.find({}).toArray(function(err, docs) {
							expect(docs.length).to.be.eql(0);
							db.close();
							done();
						});
					});
				});
			});
		});
	});

	describe('mongoimport', function() {
		beforeEach(function(done) {
			MongoIo.emptyCollection({
				host: host,
				port: port,
				db: dbName,
				collection: collectionName,
			}, function() {
				done();
			});
		});
		it('mongoimport should work fine', function(done) {


			var url = 'mongodb://' + host + ':' + port + '/' + dbName;



			MongoIo.mongoimport({
				host: host,
				port: port,
				db: dbName,
				collection: collectionName,
				path: path.resolve(__dirname, './data/greetings.json')
			}, function(err) {

				if (err) {
					console.log('err', err);
					return;
				}

				MongoClient.connect(url, function(err, db) {
					// Get the documents collection
					var collection = db.collection(collectionName);
					// Find some documents
					collection.find({}).toArray(function(err, docs) {
						docs = docs.map(function(el) {
							delete el._id;
							return el;
						});
						expect(docs.length).to.be.eql(3);
						expect(docs).to.contain({
							name: 'hello',
							lang: 'en'
						});
						expect(docs).to.contain({
							name: 'ciao',
							lang: 'it'
						});
						expect(docs).to.contain({
							name: 'hi',
							lang: 'en'
						});
						db.close();
						done();
					});
				});


			});
		});
	});


});