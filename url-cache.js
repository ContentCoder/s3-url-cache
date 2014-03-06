/*
 * url-cache.js
 *  
 * Cache URL resource to S3.
 * 
 * version: 0.0.1
 * create date: 2014-2-21
 * update date: 2014-2-21
 */

var util		= require('util'), 
		path		= require('path'), 
		request = require('request'),
		uuid    = require('node-uuid'),
		aws     = require('aws-sdk');

// load configuration
var config  = require(path.join(__dirname, 'config.json'));

// aws init
aws.config.loadFromPath(path.join(__dirname, 'awsconfig.json'));
var s3 = new aws.S3();
var dynamodb = new aws.DynamoDB();

/* 
 * Cache url.
 * 
 * Parameters: 
 *	url: (String) http url string
 *	
 * Callback: 
 *	callback: (Function) function(err, item) {} 
 *		err: (Object) error object
 *		item: (object) cached DynamoDB url item
 */
function cache(url, callback) {
	var item = {};
	item.TableName	= config.TABLE;
	item.Key				= {};
	item.Key.Url		= {S: url};
	dynamodb.getItem(item, function(err, cachedItem) {
		if (err) {
			callback(err, null);
			return;
		}

		if (!cachedItem.Item) {
			var bucket	= config.BUCKET,
					key			= uuid.v1();
			url2s3(url, bucket, key, function(err, addedItem) {
				if (err) {
					callback(err, null);
				} else {
					addedItem.Status = {S: 'added'};
					callback(null, addedItem);
				}
			});		// url2s3
			return;
		}

		request.head(url, function(err, res, body) {
			if (err) {
				callback(err, null);
				return;
			}
			if (res.statusCode != 200) {
				var err = {};
				err.message = 'request url status error: ' + res.statusCode;
				callback(err, null);
				return;
			}

			if ((cachedItem.Item.ETag && cachedItem.Item.ETag.S == res.headers['etag']) || 
					(cachedItem.Item.LastModified && cachedItem.Item.LastModified.S == res.headers['last-modified'])) {
				cachedItem.Item.Status = {S: 'cached'};
				callback(null, cachedItem.Item);
			} else {
				var bucket	= cachedItem.Item.Bucket.S, 
						key			= cachedItem.Item.Key.S;
				url2s3(url, bucket, key, function(err, updatedItem) {
					if (err) {
						callback(err, null);
						return;
					}

					updatedItem.Status = {S: 'updated'};
					callback(null, updatedItem);
					return;
				});		// url2s3
			}
		});		// request.head
	});		// dynamodb.getItem
}

/* 
 * Cache url to S3.
 * 
 * Parameters: 
 *	url: (String) http url string
 *	bucket: (String) S3 bucket
 *	key: (String) S3 key
 * 
 * Callback: 
 *	callback: (Function) function(err, item) {} 
 *		err: (Object) error object
 *		item: (Obejct) cached DynamoDB url item		
 */
function url2s3(url, bucket, key, callback) {
	request({uri: url, encoding: null}, function(err, res, body) {
		if (err) {
			callback(err, null);
			return;
		}
		if (res.statusCode != 200) {
			var err = {};
			err.message = 'request url status error: ' + res.statusCode;
			callback(err, null);
			return;
		}

		var s3p = {};
		s3p.Bucket			= bucket;
		s3p.Key					= key;
		s3p.Body				= body;
		s3p.ContentType = res.headers['content-type'];
		s3.putObject(s3p, function(err, data) {
			if (err) {
				callback(err, null);
				return;
			}

			var dbp = {};
			dbp.TableName		= config.TABLE;
			dbp.Item				= {};
			dbp.Item.Url		= {S: url};
			dbp.Item.Bucket = {S: bucket};
			dbp.Item.Key		= {S: key};
			if (res.headers['age']) 
				dbp.Item.Age							= {S: res.headers['age']};
			if (res.headers['cache-control']) 
				dbp.Item.CacheControl			= {S: res.headers['cache-control']};
			if (res.headers['content-encoding']) 
				dbp.Item.ContentEncoding	= {S: res.headers['content-encoding']};
			if (res.headers['content-language']) 
				dbp.Item.ContentLanguage	= {S: res.headers['content-language']};
			if (res.headers['content-length']) 
				dbp.Item.ContentLength		= {S: res.headers['content-length']};
			if (res.headers['content-location']) 
				dbp.Item.ContentLocation	= {S: res.headers['content-location']};
			if (res.headers['content-md5']) 
				dbp.Item.ContentMD5				= {S: res.headers['content-md5']};
			if (res.headers['content-type']) 
				dbp.Item.ContentType			= {S: res.headers['content-type']};
			if (res.headers['date']) 
				dbp.Item.Date							= {S: res.headers['date']};
			if (res.headers['etag']) 
				dbp.Item.ETag							= {S: res.headers['etag']};
			if (res.headers['expires']) 
				dbp.Item.Expires					= {S: res.headers['expires']};
			if (res.headers['last-modified']) 
				dbp.Item.LastModified			= {S: res.headers['last-modified']};
			dynamodb.putItem(dbp, function(err, data) {
				if (err) {
					callback(err, null);
				} else {
					callback(null, dbp.Item);
				}
			});		// dynamodb.putItem
		});		// s3.putObject
	});		// request
}

exports.cache = cache;
	
