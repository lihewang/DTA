//Starting node
var events = require('events');
var hashtable = require('hashtable');
var fs = require('fs');
var csv = require('fast-csv');
var redis = require('redis');

var timeHash = new hashtable();
var nodeHash = new hashtable();
var redisClient = redis.createClient({url:"http://127.0.0.1:6379"});

var eventEmitter = new events.EventEmitter();

//csv reader
var csvHandler = function Readcsv() {
  var stream = fs.createReadStream("./Data/links.csv")
  var csvStream = csv({headers : true})
      .on("data", function(data){
         timeHash.put(data['ID'],data['T']);
         //network topology
         var abnode = data['ID'].split('-');   
         if (nodeHash.has(abnode[0])){          
           var value = nodeHash.get(abnode[0]);
           value.push(abnode[1]);
           nodeHash.put(abnode[0],value);
           console.log(abnode[0] + ",[" + nodeHash.get(abnode[0]) + "]");
         }else{
           nodeHash.put(abnode[0],[abnode[1]]);
         }            
      })
      .on("end", function(){
         console.log("read links done"); 
         //Start listening to http
        eventEmitter.on('http', httpHandler);
        eventEmitter.emit('http');       
      });     
  stream.pipe(csvStream);
};

//Write shortest path to redis
var spHandler = function ShortestPath(body) {

      console.log("SP for zone " + body.zone);
      redisClient.flushdb();
      redisClient.set('1-2','1 2 3 4',function(err,reply) {
          console.log(reply + ' sp set');
      });
      //call volHandler
      eventEmitter.on('vol', volHandler);
      eventEmitter.emit('vol');      
};

//http listener
var httpHandler = function StartListenhttp(){
  var http = require('http');
  var handleRequest = function (req, res) {
    req.on('data',function(data){
        var body = JSON.parse(data);
        if (body.work == 'sp'){
          eventEmitter.on('sp', spHandler);
          eventEmitter.emit('sp',body); 
        }        
    });
    res.writeHead(200);
    res.end('Hello Kubernetes!');
  };
  var www = http.createServer(handleRequest);
  www.listen(8080);
  //process.exit(0); //End server 
};

//Write vol to redis
var volHandler = function vol() {
    console.log("write to vol redis");
    redisClient.select(1);
    redisClient.flushdb();
    var keyvalue = '1-3';
    redisClient.set(keyvalue,10);
    redisClient.exists(keyvalue,function(err,reply) {
          if(reply == 1){
            //keyvalue++
            console.log("exist");
            strEval = 'redis.call("set","' + keyvalue + '",redis.call("get","' + keyvalue + '")+100)';
            redisClient.eval(strEval,0);
          }else{
            console.log("not exist");
            redisClient.set(keyvalue,30);
          };
    });    
};

//start point
var startHandler = function StartRun() {
  //Read in links
  console.log('Started Worker!');
  eventEmitter.on('csv', csvHandler);
  eventEmitter.emit('csv');  
};

eventEmitter.on('Start', startHandler);
eventEmitter.emit('Start');


