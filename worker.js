//Starting node
var events = require('events');
var hashtable = require('hashtable');
var fs = require('fs');
var csv = require('fast-csv');
var redis = require('redis');
var pq = require('priorityqueuejs');
var express = require('express');
var bodyParser = require('body-parser');

var timeHash = new hashtable();
var nodeHash = new hashtable(); //network topology

var redisClient = redis.createClient({url:"http://127.0.0.1:6379"});
var jsonParser = bodyParser.json();
var eventEmitter = new events.EventEmitter();

//csv reader
var csvReader = function Readcsv(req,res,next) {
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
        next();  
      });     
  stream.pipe(csvStream);      
}

//Write shortest path to redis
var sp = function ShortestPath(req,res,next) {
      var bdy = req.body;
      console.log("*** Find path for zone " + bdy.zone);
      //prepare network - remove links going out of other zones
      for (var i = 1; i <= bdy.zonenum; i++) {
        if(i != bdy.zone){
          nodeHash.remove(i);
        }
      }
      //apply turn penalty


      //Priority queue for frontier nodes
      var pqNodes = new pq(function(a, b) {
          return b.t - a.t;
      });
      var cnt = 1;    //count visited zones
      var visitedNodes = new hashtable();       //track visited nodes, {node,time}
      var settledNodes = new hashtable();
      var parentNodes = new hashtable();        //track parent nodes, {node, parent node}      
      var currNode = bdy.zone;
      visitedNodes.put(bdy.zone,0);            //root node
      pqNodes.enq({t:0,nd:currNode}); 
      do {
        //Explore frontier node
        var tpNode = pqNodes.deq();
        currNode = tpNode.nd;
        settledNodes.put(currNode,1);
        //console.log('settled node add ' + currNode);
        if (nodeHash.has(currNode)) {            //currNode has downsteam nodes
          var dnNodes = nodeHash.get(currNode);  //get new frontier nodes
          //Update time on new nodes
          dnNodes.forEach(function(dnNode) {

              if (!settledNodes.has(dnNode)) {    //exclude settled nodes
                  //get time of dnNode
                  var tempTime = parseFloat(visitedNodes.get(currNode)) + parseFloat(timeHash.get(currNode + '-' + dnNode));

                  if (visitedNodes.has(dnNode)){
                      //dnNode has been checked before
                      if (tempTime < visitedNodes.get(dnNode)) {    //update time when the path is shorter                   
                          pqNodes.enq({t:tempTime,nd:dnNode});
                          parentNodes.put(dnNode,currNode);
                          visitedNodes.put(dnNode,tempTime);
                          //console.log('visit again ' + dnNode + ', ' + tempTime);
                      }                    
                  }else{
                      //first time checked node
                      pqNodes.enq({t:tempTime,nd:dnNode});
                      parentNodes.put(dnNode,currNode);
                      visitedNodes.put(dnNode,tempTime);
                      //console.log('visit first time ' + dnNode + ', ' + tempTime);
                  }

              }  //end if
             
          }); //end forEach
        }  //end if
        //console.log('pqNode size = ' + pqNodes.size());
      }
      while (pqNodes.size() > 0);
      
      //Construct path string and write to redis db
      redisClient.flushdb();
      for (var i = 1; i <= bdy.zonenum; i++) {
        var zonePair = bdy.zone + '-' + i;
        var path = i.toString();
        var pNode = i;
        if (parentNodes.has(pNode)) {
          do {        
           pNode = parentNodes.get(pNode);
           path = pNode.toString() + ',' + path;      
          }
          while (pNode != bdy.zone);
        } 
        console.log(zonePair + ', ' + path); 
        redisClient.set(zonePair,path);       //write to redis db  
      }
      next();      
}

//http listener
var app = express();
var router = express.Router();
router.use(bodyParser.json());
router.use(csvReader);
router.use(sp);
router.all('/', function(req,res,next){
  var bdy =req.body;
  console.log('Start zone ' + bdy.zone);
  res.send('csv read');
})
app.use('/', router);
var server = app.listen(8080);

//process.exit(0); //End server 

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
}



