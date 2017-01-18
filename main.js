//Starting node
var events = require('events');
var redis = require('redis');
var request = require('request');

var eventEmitter = new events.EventEmitter();

var startHandler = function StartRun() {
    console.log('Started Model!');
    request.post('http://localhost:8080',
        {json:{'work':'sp','zone':1,'mode':'sov'}},
        function(error,response,body){
            console.log(response.statusCode);
            }); 
};

eventEmitter.on('Start', startHandler);
eventEmitter.emit('Start');





