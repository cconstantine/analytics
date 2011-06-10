
var Connect = require('connect');
var dispatch = require('dispatch');

var Db = require('mongodb').Db,
    Connection = require('mongodb').Connection,
    Server = require('mongodb').Server,
    BSON = require('mongodb').BSONNative,
    URL = require('url');

var host = process.env['MONGO_NODE_DRIVER_HOST'] != null ? process.env['MONGO_NODE_DRIVER_HOST'] : 'localhost';
var port = process.env['MONGO_NODE_DRIVER_PORT'] != null ? process.env['MONGO_NODE_DRIVER_PORT'] : Connection.DEFAULT_PORT;
require('url').parse('/status?name=ryan', true)

function RunServer(col) {

    function register_event(req, res, event) {
	doc = {event: event, time: new Date()};
	
	var context = URL.parse(req.url, true).query
	    if (Object.keys(context).length > 0) {
		doc.context = context;
	    }
	col.insert(doc);
	
	res.end(JSON.stringify(doc)+'\n');
    }

    function graph_counts(req, res, event) {
	var params = URL.parse(req.url, true).query;

	var window = 60*60*1000;
	var now = (new Date()).getTime();

	var totalPoints = parseInt(params.totalPoints);
	var updateInterval = parseInt(params.updateInterval);


	var begin = now - (totalPoints * updateInterval);

	var data = [];
	
	var i = 0;

	function grab_point() {
	    if ( i < totalPoints && begin <= now ) {
		i+=1;
		var selector = {
		    'time' : {
			'$gt' : new Date(begin - window),
			'$lte': new Date(begin)
		    }
		};
		col.count(selector, function(err, count) {
			data.push([begin, count]);
			begin += updateInterval;
			grab_point();
		    });
	    } else {
		result = {};
		result[event] = data;
		
		res.end(JSON.stringify(result) + '\n');
	    }
	}

	grab_point();
    }

    Connect.createServer(
			 dispatch({
				 '/':function(req, res) { res.end("hi");},
				     '/register/(\\w+)': register_event,
					 '/graph/counts/(\\w+)': graph_counts,
					 
					 })
			 ).listen(3000);
}
console.log("Connecting to " + host + ":" + port);
var db = new Db('analytics', new Server(host, port, {}), {native_parser:true});
db.open(function(err, db) {
	db.collection('test', function(err, collection) {      
		RunServer(collection);
	    });
    });


		/*		for(var i = 0; i < 3; i++) {
		    collection.insert({'a':i});
		    }*/
			
