//Starting node
var events = require('events');
var hashtable = require('hashtable');
var fs = require('fs');
var csv = require('fast-csv');
var redis = require('redis');
var pq = require('priorityqueuejs');

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
           console.log(abnode[0] + ",[" + nodeHash.get(abnode[0]) + "]");
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
      //Priority queue for frontier nodes
      var pqNode = new pq(function(a, b) {
          return a.t - b.t;
      });
      var cnt = 1;    //count visited zones
      var visitedNodes = new hashtable();       //track visited nodes, {node,time}
      var frontierNodes = new hashtable();      //track frontier nodes, {node, time}
      var parentNodes = new hashtable();        //track parent nodes, {node, parent node}      
      var currNode = body.zone;
      visitedNodes.put(body.zone,0);            //root node
      do {
        //Explore frontier node
        if (nodeHash.has(currNode)) {            //currNode has downsteam nodes
          var dnNodes = nodeHash.get(currNode);  //get new frontier nodes
          //Update time on new nodes
          dnNodes.forEach(function(dnNode) {

              if (!visitedNodes.has(dnNode)) {
                  var tempTime = visitedNodes.get(currNode) + timeHash.get(currNode + '-' + dnNode);

                  if (frontierNodes.has(dnNode)){
                      //update time
                      if (tempTime < frontierNodes.get(dnNode)) {                       
                          frontierNodes.put(dnNode, tempTime);
                          parentNodes.put(dnNode,currNode);
                      }                    
                  }else{
                      //first visited node
                      frontierNodes.put(dnNode, tempTime);
                      parentNodes.put(dnNode,currNode);
                  }

                //put to priority queue
                pqNode.enq({t:frontierNodes.get(dnNode),nd:dnNode}); 
              }  //end if

          }); //end forEach
        }  //end if
    
        var tempNd = pqNode.deq();  //get next node
        visitedNodes.put(tempNd.nd,tempNd.t);
        currNode = tempNd.nd;
        frontierNodes.remove(currNode);
        if(currNode<=body.zonenum){
          cnt++;
        }
      }
      while (visitedNodes.size() < nodeHash.size());
      
      //Construct path
      for (var i = 1; i <= body.zonenum; i++) {
        var zonePair = body.zone + '-' + i;
        var path = [i];
        var pNode = i;
        if (parentNodes.has(pNode)) {
          do {        
           pNode = parentNodes.get(pNode);
           path.unshift(pNode);      
          }
          while (pNode != body.zone);
        } 
        console.log(zonePair + ', ' + path);     
      }

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
  //debug
  eventEmitter.on('sp', spHandler);
  eventEmitter.emit('sp',{'work':'sp','zone':1,'mode':'sov','zonenum':2}); 

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


