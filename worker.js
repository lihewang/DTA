//Starting node
var events = require('events');
var hashtable = require('hashtable');
var fs = require('fs');
var csv = require('fast-csv');
var redis = require('redis');
var pq = require('priorityqueuejs');
var express = require('express');
var bodyParser = require('body-parser');
var async = require('async');
var math = require('mathjs');
var Scripto = require('redis-scripto');
    
var timeHash = new hashtable();     //link time
var distHash = new hashtable();     //link distance
var nodeHash = new hashtable();     //network topology
var tollHash = new hashtable();     //toll on the link
var timeFFHash = new hashtable();   //free flow time on link
var redisClient = redis.createClient({url:"redis://127.0.0.1:6379"}),multi;
var scriptManager = new Scripto(redisClient);
scriptManager.loadFromFile('task','./task.lua');
var jsonParser = bodyParser.json();
var eventEmitter = new events.EventEmitter();
var par = JSON.parse(fs.readFileSync("./Data/parameters.json"));

//********csv reader********
var rdcsv = function Readcsv(mode,pType,spZone,callback) {
  nodeHash.clear();
  //get banning facility type
  if(mode=="HOV"){
    var ftypeBan = par.pathban.HOV;
  }else{
    var ftypeBan = par.pathban.TRK;
  }

  var stream = fs.createReadStream("./Data/" + par.linkfilename);
  var csvStream = csv({headers : true})
      .on("data", function(data){
        //create time hash table
        for (var i = 1; i <= par.timesteps; i++) { 
          timeHash.put(data['ID'] + ':' + i, data['T' + i]);
          tollHash.put(data['ID'] + ':' + i, data['TR' + i]);        
        }
        distHash.put(data['ID'], data['Dist']);
        timeFFHash.put(data['ID'],data['Dist']/data['Spd']*60);
         //network topology
         var banLink = false;
         if (ftypeBan.indexOf(parseInt(data['Ftype']))==-1){  //if a link is not banned
            var abnode = data['ID'].split('-'); 
            //for decision point 
            if(pType != 'zone' && abnode[0] == spZone){
              if(pType == 'tf' && (parseInt(data['Ftype']) == par.ftypeexonrp||parseInt(data['Ftype']) == par.ftypeex)){
                banLink = true;
                console.log("dp ban link " + data['ID']);
              }
              if(pType == 'tl' && (parseInt(data['Ftype']) == par.ftypegp||parseInt(data['Ftype']) == par.ftypeexoffrp)){
                banLink = true;
              }
            } 
            if(banLink != true){
              if (nodeHash.has(abnode[0])){          
                var value = nodeHash.get(abnode[0]);
                value.push(abnode[1]);
                nodeHash.put(abnode[0],value);
                console.log(abnode[0] + ",[" + nodeHash.get(abnode[0]) + "], pathType:" + pType);
              }else{
                nodeHash.put(abnode[0],[abnode[1]]);
                console.log(abnode[0] + ",[" + nodeHash.get(abnode[0]) + "], pathType:" + pType);
              }
            }
         }           
      })
      .on("end", function(){      
        callback(null,'read links done');
      });     
  stream.pipe(csvStream);      
}

//********find time dependent shortest path and write results to redis********
var sp = function ShortestPath(zone,zonenum,tp,mode,pathType,callback) {
      //prepare network - remove links going out of other zones
      for (var i = 1; i <= zonenum; i++) {
        if(i != zone){
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
      var currNode = zone;
      visitedNodes.put(zone,0);            //root node
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
                  var timeCurrNode = parseFloat(visitedNodes.get(currNode));
                  var timePeriod = math.min(par.timesteps, math.floor(timeCurrNode/15) + parseInt(tp));
                  var tempTime = timeCurrNode + parseFloat(timeHash.get(currNode + '-' + dnNode + ':' + timePeriod));

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
      redisClient.select(1);  //path db
      multi = redisClient.multi();
      //zones
      for (var i = 1; i <= zonenum; i++) {
        var zonePair = zone + '-' + i;
        var path = i.toString();
        var pNode = i;
        if (parentNodes.has(pNode)) {
          do {        
           pNode = parentNodes.get(pNode);
           path = pNode.toString() + ',' + path;      
          }
          while (pNode != zone);
        }else{
          path = null
        }       
        if(path != null){
          multi.set(tp + ":" + zonePair + ":" + mode + ":" + pathType, path);       //write to redis db, example 3:7-2:SOV:zone
          //decision point path skims
          if (par.dcpnt.indexOf(parseInt(zone))!=-1){
            var dpPath = path.split(',');
            var skimDist = 0;
            var skimToll = 0;
            var skimFFtime = 0;
            for (var j = 0; j <= dpPath.length-2; j++) {
                skimDist = skimDist + parseFloat(distHash.get(dpPath[j] + '-' + dpPath[j+1]));
                skimToll = skimToll + parseFloat(tollHash.get(dpPath[j] + '-' + dpPath[j+1]));
                skimFFtime = skimFFtime + parseFloat(timeFFHash.get(dpPath[j] + '-' + dpPath[j+1]));
            }
            multi.set(tp + ":" + zonePair + ":" + mode + ":" + pathType + ":time", visitedNodes.get(i));
            multi.set(tp + ":" + zonePair + ":" + mode + ":" + pathType + ":dist", skimDist);
            multi.set(tp + ":" + zonePair + ":" + mode + ":" + pathType + ":toll", skimToll);
            multi.set(tp + ":" + zonePair + ":" + mode + ":" + pathType + ":fftime", skimFFtime);
          }
          console.log(tp + ":" + zonePair + ":" + pathType + ', ' + path); 
        }
      } 
      multi.exec(function(err,results){      
        callback(null,zone);
      });         
}

//********move vehicle and write results to redis********
var mv = function MoveVehicle(tp,zi,zj,pthTp,mode,vol,path,cb) {
  var arrPath = path.split(',');
  var totTime = 0;
  var tpNew = tp;
  var keyValue = ''; 
  multi = redisClient.multi();
  var j = 0;
  var breakloop = false;
  //loop links in the path
  async.during(
    //test function 
    function(cb){
      console.log('loop start ' + j);
      return cb(null,j <= arrPath.length-2 && !breakloop);
    },
    function(callback){
      if (par.dcpnt.indexOf(parseInt(arrPath[j]))!=-1 && j>0){
        //decision point (not the start node in path)
        var zonePair  = arrPath[j] + "-" + zj;      
        async.waterfall([
            //choice model
            function(callback){
              var timeTl = 0;
              var timeTf = 0;
              var distTl = 0;
              var distTf = 0;
              var Toll = 0;
              var timeFFTl = 0;
              var timeFFTf = 0;  
              var probility = 0;            
              multi.select(1);
              multi.get(tp + ":" + zonePair + ":" + mode + ":tl:time",function(err,result){
                timeTl = result;
              });
              multi.get(tp + ":" + zonePair + ":" + mode + ":tf:time",function(err,result){
                timeTf = result;
              });
              multi.get(tp + ":" + zonePair + ":" + mode + ":tl:dist",function(err,result){
                distTl = result;
              });
              multi.get(tp + ":" + zonePair + ":" + mode + ":tf:dist",function(err,result){
                distTf = result;
              });
              multi.get(tp + ":" + zonePair  + ":" + mode + ":tl:toll",function(err,result){
                Toll = result;
              });
              multi.get(tp + ":" + zonePair + ":" + mode + ":tl:fftime",function(err,result){
                timeFFTl = result;
              });
              multi.get(tp + ":" + zonePair + ":" + mode + ":tf:fftime",function(err,result){
                timeFFTf = result;
              });
              multi.exec(function(err,results){                                
                if(distTl > distTf * parseFloat(par.distmaxfactor)){
                  probility = 0;
                }else if(distTf > distTl * parseFloat(par.distmaxfactor)){
                  probility = 1;
                }else{
                  var utility = -1 * par.choicemodel.tollconst - (par.choicemodel.scalestdlen/distTl)  
                  ^ par.choicemodel.scalealpha * (par.choicemodel.timecoeff * (timeTl - timeTf)) 
                  + par.choicemodel.tollcoeff * Toll + par.choicemodel.timecoeff * par.choicemodel.reliacoeffratio
                  * par.choicemodel.reliacoefftime * ((timeFFTf - timeTf) * distTf ^ (-1 * par.choicemodel.reliacoeffdist)
                  - (timeFFTl -timeTl) * distTl ^ (-1 * par.choicemodel.reliacoeffdist));
                  
                  probility = 1 / (1 + math.exp(utility));
                }
                callback(null,probility);
              })
            },
            function(probility, callback){
              //recursive call mv
              var newVol = [];
              if(probility == 0){
                newVol[0] = vol;
                redisClient.get(tp + ":" + arrPath[j] + "-" + zj + ":" + mode + ":tf",function(err,result){
                  console.log('move at decision point prob=' + probility + " " + tp + ":" + arrPath[j] + "-" + zj + ":" + mode + ":tf:" + newVol[0]);
                  mv(tp,arrPath[j],zj,'tf',mode,newVol[0],result,function(err,result){
                    console.log('move at decision point ' + result);                 
                  });
                });
              }else if(probility == 1){
                newVol[0] = vol;
                redisClient.get(tp + ":" + arrPath[j] + "-" + zj + ":" + mode + ":tl",function(err,result){
                  console.log('move at decision point prob=' + probility + " " + tp + ":" + arrPath[j] + "-" + zj + ":" + mode + ":tl:" + newVol[0]);
                  mv(tp,arrPath[j],zj,'tl',mode,newVol[1],result,function(err,result){
                    console.log('move at decision point ' + result);                 
                  });
                });
              }else{
                newVol[0] = vol * (1 - probility);
                newVol[1] = vol * probility;

                redisClient.get(tp + ":" + arrPath[j] + "-" + zj + ":" + mode + ":tf",function(err,result){
                  console.log('move at decision point prob=' + probility + " " + tp + ":" + arrPath[j] + "-" + zj + ":" + mode + ":tf:" + newVol[0]);
                  mv(tp,arrPath[j],zj,'tf',mode,newVol[0],result,function(err,result){
                    console.log('move at decision point ' + result);                 
                  });
                });
                redisClient.get(tp + ":" + arrPath[j] + "-" + zj + ":" + mode + ":tl",function(err,result){
                  console.log('move at decision point prob=' + probility + " " + tp + ":" + arrPath[j] + "-" + zj + ":" + mode + ":tl:" + newVol[1]);
                  mv(tp,arrPath[j],zj,'tl',mode,newVol[1],result,function(err,result){
                    console.log('move at decision point ' + result);                 
                  });
                });
              } 
              callback(null,'done');           
            }],
            function(err,results){ 
              breakloop = true;           
              callback();                 
            })       
      }else{
        //not a decision point
        async.series([
            function(callback){ 
              var linkID = arrPath[j] + '-' + arrPath[j+1] + ':' + tp; 
              totTime = totTime + parseFloat(timeHash.get(linkID));
              tpNew = parseInt(tp) + math.floor(totTime/15);
              keyValue = arrPath[j] + '-' + arrPath[j+1] + ':' + tpNew + ':' + mode;
              console.log(keyValue + ',totTime=' + totTime + ',tpNew=' + tpNew);
              callback();
            },
            function(callback){
              redisClient.select(2);
              async.series([
                function(callback){
                  redisClient.exists(keyValue,function(err,reply) {
                    if(reply == 1){
                      //keyvalue++
                      console.log("exist");
                      strEval = 'redis.call("set","' + keyValue + '",redis.call("get","' + keyValue + '")+' + vol + ')';
                      redisClient.eval(strEval,0, function(err,reply){
                        console.log(err);
                        callback();
                      });
                    }else{
                      console.log('set ' + keyValue + ':' + vol);
                      redisClient.set(keyValue,vol,function(err,reply){
                        callback();
                      });
                    };                   
                  });
                }],
                function(err,results){
                  callback(null,results);
                })            
          }],
          function(err,results){
            console.log('loop end ' + j); 
            j = j + 1;             
            callback();                   
          })
        }
  },
  function(err){
    cb(null,keyValue); 
  }); 
}

//********http listener********
var app = express();
var router = express.Router();
router.use(bodyParser.json());
router.get('/', function(req,res){
  var bdy =req.body;
  console.log('task ' + bdy.task);
  //create shortest path
  if (bdy.task == 'sp'){ 
    var spZone = 0;
    var timeStep = 0; 
    var mode = '';
    var pathType = 'zone';  
    async.during(
      //loop until to-do list is null
      //test function 
      function(cb){ 
          console.log('***begin shortest path loop***');
          spZone = 0;
          timeStep = 0;
          redisClient.select(6);       //task db 
          async.series([
            function(callback){
                //get job
                redisClient.lpop('to-do', function(err, result) {  
                if(result != null){
                  var ts = result.split('-'); //tp-zone-SOV-tl
                  timeStep = ts[0];             
                  spZone = ts[1]; 
                  mode = ts[2];
                  if (ts.length > 3){
                    //decision point
                    pathType = ts[3];   //tl,tf
                  }else{
                    pathType = 'zone';  //zone
                  }
                }
                callback();     
              });              
            }],
            function(err,results){
              console.log('find sp for time step ' + timeStep + ', sp zone ' + spZone);
              return cb(null,spZone>0);
          });                           
      },
      //function 
      function(callback){
          async.series([
          function(cb){
            //read network
            rdcsv(mode,pathType,spZone,function(err,result){
                cb(null,result);
            });          
          },
          function(cb){
            //create shortest path
            sp(spZone,par.zonenum,timeStep,mode,pathType,function(err,result){
                console.log('run sp ' + result);
                cb(null,result);
            });
          }       
          ],function(err,results){
              callback(); 
          }); 
      },
      //whilst callback
      function(err,results){
          console.log('end loop ' + results);
          res.send('sp finished');
      }    
    );
  }
  //move vehicles
  else if(bdy.task == 'mv'){ 
    var zi = 0;  
    var zj = 0;
    var tp = 0;
    var mode = 0;
    var vol = 0;
    var pathType = '';
    console.log('***begin moving vehicles loop***');
    async.during(
      //loop until to-do list is null
      //test function 
      function(callback){               
        async.series([
          function(callback){
            zi = 0;
            redisClient.select(7);       //task db 
            redisClient.lpop('to-do', function(err, result) {
              if(result != null){
                var rs = result.split('-');
                zi = rs[0];
                zj = rs[1];
                tp = rs[2];
                mode = rs[3];
                vol = rs[4];
                pathType = rs[5];
              }
              callback();
            });
          }],
          function(err,results){
              return callback(null,zi>0);
          });
      },
      //function 
      function(callback){
        redisClient.select(1); //sp db
        if (par.dcpnt.indexOf(parseInt(zi))!=-1){
          //decision point
          //get path
          redisClient.get(tp + ":" + zi + "-" + zj + ":" + mode + ":" + pathType,function(err,result){
            //move vehicle
            console.log('decision point ' + tp + ":" + zi + ":" + zj  + ":" +  mode + ":" + pathType + " " + result);
            mv(tp,zi,zj,pathType,mode,vol,result,function(err,result){
                console.log('run mv ' + result);
                callback();
            });            
          });

        }else{
          redisClient.get(tp + ":" + zi + "-" + zj + ":" + mode + ":" + pathType,function(err,result){
            //move vehicle
            console.log(tp + ":" + zi + ":" + zj  + ":" +  mode + ":" + pathType + " " + result);
            mv(tp,zi,zj,pathType,mode,vol,result,function(err,result){
                console.log('run mv ' + result);
                callback();
            });            
          });
        }       
      },
      //whilst callback
      function(err,results){
          console.log('end moving vehicle loop ' + results);
          res.send('mv finished');
      }
    );
  }
});

app.use('/', router);
var server = app.listen(8080);

//process.exit(0); //End server 

//Write vol to redis
var volHandler = function vol() {
    console.log("write to vol redis");
    redisClient.select(2);
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



