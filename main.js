//Starting node
var events = require('events');
var redis = require('redis');
var request = require('request');

var eventEmitter = new events.EventEmitter();
var redisClient = redis.createClient({url:"redis://127.0.0.1:6379"});
redisClient.select(6);  //task db
var startHandler = function StartRun() {
    console.log('Started Model!');
    redisClient.flushdb();
    for (var i = 1; i <= 3; i++) {
        redisClient.rpush('to-do',i);      
    }
    request.post('http://localhost:8080',
            {json:{'task':'sp','mode':'sov','zonenum':3}},
                function(error,response,body){
                console.log(body);
            }); 
};

eventEmitter.on('Start', startHandler);
eventEmitter.emit('Start');





