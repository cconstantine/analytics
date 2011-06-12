
var Connect = require('connect');
var dispatch = require('dispatch');

var Db = require('mongodb').Db,
    Connection = require('mongodb').Connection,
    Server = require('mongodb').Server,
    BSON = require('mongodb').BSONNative,
    URL = require('url');

sys = require("sys");

var host = process.env['MONGO_NODE_DRIVER_HOST'] != null ? process.env['MONGO_NODE_DRIVER_HOST'] : 'localhost';
var port = process.env['MONGO_NODE_DRIVER_PORT'] != null ? process.env['MONGO_NODE_DRIVER_PORT'] : Connection.DEFAULT_PORT;

function RunServer(events_col, mov_col) {
    var i = [['time', 1], ['context', 1], ['event', 1]];
    events_col.ensureIndex(i, function(err, indexName) {
	    console.log("created index: " + indexName);      
	});

    var i = [['_id', 1], ['time', -1], ['context', 1]];
    events_col.ensureIndex(i, function(err, indexName) {
	    console.log("created index: " + indexName);      
	});



    function movement(req, res) {

    }

    function register_event(req, res, event) {
	doc = {event: event, time: new Date()};
	
	var context = URL.parse(req.url, true).query;
	if (Object.keys(context).length > 0) {
	    doc.context = context;
	} 
	res.end(JSON.stringify(doc)+'\n');

	function register_movement(err, docs) {
	    if (Object.keys(context).length > 0) {
		var selector = {
		    context : context
		};
		var opts = {
		    sort : [['time', 'desc']],
		    limit : 1,
		    skip: 1
		};
		events_col.find(selector, opts, function(err, results) {
			results.each(function(err, val) {
				if (val) {
				    var movement = {
					from_event : val,
					to_event : docs[0],
					time: new Date(),
					delta: docs[0].time - val.time
				    };
				    mov_col.insert(movement);
				    console.log(movement);
				}
			    });
		    });
	    }
	}

	events_col.insert(doc,register_movement);
    }

    function graph_counts(req, res, event) {
	var params = URL.parse(req.url, true).query;

	//	var window = 60*60*1000;
	var now = (new Date()).getTime();

	var totalPoints = parseInt(params.totalPoints);
	var updateInterval = parseInt(params.updateInterval);
	var window = parseInt(params.window || updateInterval);
		
	var begin = now - (totalPoints * updateInterval);

	var data = [];
	
	var i = 0;

	function grab_point() {
	    if ( i < totalPoints && begin <= now ) {
		i+=1;
		var selector = {
		    'event': event,
		    'time' : {
			'$gt' : new Date(begin - window),
			'$lte': new Date(begin)
		    }
		};
		events_col.count(selector, function(err, count) {
			data.push([begin, count]);
			begin += updateInterval;
			grab_point();
		    });
	    } else {
		result = {};
		result[event] = data;
		if(params.callback) {
		    res.writeHead(200, {'Content-Type': 'application/javascript'});
		    res.end(params.callback + '(' + JSON.stringify(result) + ')\n'); 
		} else {
		    res.end(JSON.stringify(result) + '\n'); 
		}
		    
		
	    }
	}

	grab_point();
    }

    Connect.createServer(
			 dispatch({
				 '/':function(req, res) { res.end("hi");},
				 '/register/(\\w+)': register_event,
				 '/graph/counts/(\\w+)': graph_counts,
				 '/graph/movements/(\\w+)': movement,
					 
					 })
			 ).listen(3000);
}
console.log("Connecting to " + host + ":" + port);
var db = new Db('analytics', new Server(host, port, {}), {native_parser:true});
db.open(function(err, db) {
	db.collection('events', function(err, events_col) {
		if (err) {
		    console.log('*****************');
		    console.log(err.stack);
		    console.log('*****************');
		} else {

		    db.collection('movements', function(err, mov_col) {
			    if (err) {
				console.log('*****************');
				console.log(err.stack);
				console.log('*****************');
			    } else {
				RunServer(events_col, mov_col);
			    }
			});
		}
	    });
    });


		/*		for(var i = 0; i < 3; i++) {
		    collection.insert({'a':i});
		    }*/
			
