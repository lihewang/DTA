//Starting node
var events = require('events');
var redis = require('redis');
var request = require('request');

var eventEmitter = new events.EventEmitter();
var redisClient = redis.createClient({url:"http://127.0.0.1:6379"});

var startHandler = function StartRun() {
    console.log('Started Model!');
    for (var i = 1; i <= 2; i++) {
        request.post('http://localhost:8080',
            {json:{'work':'sp','zone':i,'mode':'sov','zonenum':3}},
                function(error,response,body){
                console.log(body);
            }); 
    }
};

eventEmitter.on('Start', startHandler);
eventEmitter.emit('Start');





