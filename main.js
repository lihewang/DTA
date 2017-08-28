//Starting node
var redis = require('redis');
var request = require('request');
var async = require('async');
var fs = require('fs');
var csv = require('fast-csv');
var hashMap = require('hashmap');
var math = require('mathjs');
var Scripto = require('redis-scripto');

// redis db list - 1.shortest path  2.volume by mode and time step 3.congested time 4.VHT
//  6.shortest path task 7.move vehicle task

var currTime = new Date()
console.log("/***Main start " + currTime + " ***/"); 


//local test
var redisIP = "redis://127.0.0.1:6379";
var workerIP = "http://localhost:8080"

var appFolder = "./app";
var paraFile = appFolder + "/parameters.json";
var luaScript = appFolder + '/msa.lua';
var outputFile = "./output/vol.csv"
var redisClient = redis.createClient({url:redisIP}),multi;
var arrLink = [];
var par = null;
var timeFFHash = new hashMap();
var alphaHash = new hashMap();
var betaHash = new hashMap();
var capHash = new hashMap();
var iter = 0;
var gap = 1;

//create storage sink
//var storageName = "dta-model"; 
//var bucket = gcs.bucket(storageName);

//load redis lua script
var scriptManager = new Scripto(redisClient);
scriptManager.loadFromFile('msa',luaScript);

async.series([
    //read parameters
    function(callback){
        console.log("begin read parameters");
        par = JSON.parse(fs.readFileSync(paraFile));
        console.log("end read parameters");
        //clear link db
        multi = redisClient.multi();
        multi.select(2);  
        multi.flushdb(); 
        multi.exec(function(){
            callback();
        });
    },
    function(callback){
        async.during(
            function(callback){
                //check convergence
                iter = iter + 1;
                if(iter == 1){
                    return callback(null, true);
                }else{
                    return callback(null, iter <= par.maxiter && gap > par.gap)
                }
            },
            function(callback){
                async.series([                   
                    //read link file
                    function(callback){
                        arrLink = [];
                        var stream = fs.createReadStream(appFolder + "/" + par.linkfilename);
                        var csvStream = csv({headers : true})
                            .on("data", function (data) {
                                var anode = data['A'];
                                var bnode = data['B'];
                                anode = anode.trim();
                                bnode = bnode.trim();
                                var data_id = anode + '-' + bnode;
                                for (var i = 1; i <= par.timesteps; i++) { 
                                    arrLink.push(data_id + ':' + i); 
                                }
                                timeFFHash.set(data_id,data['Dist']/data['Spd']*60);
                                alphaHash.set(data_id,data['Alpha']);
                                betaHash.set(data_id,data['Beta']);
                                capHash.set(data_id,data['Cap']);
                            })
                            .on("end", function(){      
                                callback();
                            });     
                        stream.pipe(csvStream);
                    },
                    //create shortest paths for all time steps and modes               
                    function(callback){
                        console.log('start building sp'); 
                        //create job queue in redis
                        multi = redisClient.multi();
                        multi.select(6);
                        multi.flushdb(); 
                        par.modes.forEach(function (md) {   //loop modes
                            for (var i = 1; i <= par.timesteps; i++) {  //loop time steps
                                //zones
                                for (var j = 1; j <= par.zonenum; j++) {
                                    multi.RPUSH('task', 'sp-' + iter + '-' + i + '-' + j + '-' + md + '-ct');
                                }
                                //decision point
                                par.dcpnt.forEach(function (dcp) {
                                    //console.log("push decison point " + dcp + " to to-do list");
                                    var t2 = ['tl', 'tf'];
                                    t2.forEach(function (t) {
                                        multi.RPUSH('task', 'sp-' + iter + '-' + i + '-' + j + '-' + md + '-' + t);
                                    });
                                 });
                            }
                        });
                        multi.exec(function () {
                            callback();
                        });
                    },
                    //publish sp job
                    function (callback) {
                        redisClient.publish('job', 'sp', function (err, result) {
                            console.log('sp job created in redis ' + result);                           
                            callback();
                        }); 
                    },
                    //move vehicles
                    function(callback){
                        console.log('***moving vehicles***');
                        multi = redisClient.multi();
                        multi.select(7);
                        multi.flushdb(); 
                        async.series([
                            //set task to db
                            function(callback){               
                                async.series([
                                    function(callback){
                                        //read trip table inset
                                        var stream = fs.createReadStream(appFolder + "/" + par.triptablefilename);
                                        var csvStream = csv({headers : true})
                                            .on("data", function(data){ 
                                                if(parseInt(data['I'])>0){                            
                                                    multi.RPUSH('task', 'mv-' + iter + '-' + data['I'] + '-' + data['J'] + '-' + data['TP'] + '-' + data['Mode'] + '-' + data['Vol'] + '-zone');
                                                }
                                            })
                                            .on("end", function(){  
                                                multi.exec(function () {
                                                    callback();
                                                });
                                            });                                                      
                                        stream.pipe(csvStream); 
                                    }],
                                    function (err, results) {
                                        console.log(results);
                                    }); 
                            }],
                        function(err,results){
                            callback(); //move vehicles callback
                        }); 
                    },
                    //Moving average volume
                    function(callback){
                        
                        callback();
                    },
                    //Update time
                    function(callback){
                        multi = redisClient.multi();
                        var VHT_square = 0;
                        var VHT_tot = 0;      
                        async.eachSeries(arrLink,
                            function(item,callback){    //loop links (96 time steps)
                                var vol = 0;    //vol of all modes        
                                multi.select(2);
                                //total vol of all modes
                                async.eachSeries(par.modes, function(md,callback){
                                    if (iter>=2){   //MSA Volume
                                        var lastIter = iter - 1;
                                        var key1 = item + ":" + md + ":" + iter;
                                        var key2 = item + ":" + md + ":" + lastIter;
                                        scriptManager.run('msa', [key1,key2,iter], [], function(err, result) {
                                            //console.log('lua err=' + err + "," + result);
                                        });
                                    }
                                    multi.get(item + ":" + md + ":" + iter, function(err,result){
                                        if(result != null){
                                            vol = vol + result;
                                        }                                
                                    });
                                    
                                    callback();
                                })
                                multi.exec(function(){                                             
                                    var linkID = item.split(':')[0];
                                    //BPR function
                                    var cgTime = timeFFHash.get(linkID)*(1+alphaHash.get(linkID)*math.pow(vol*4/capHash.get(linkID),betaHash.get(linkID)));
                                    var vht = vol*cgTime;
                                    if(vht>0){
                                        console.log('iter=' + iter + ',item='+item+',VHT='+vht+',vol='+vol+',cgtime='+cgTime);
                                    }
                                    multi.select(3);
                                    multi.set(item, cgTime);
                                    multi.exec(function(){
                                    });   
                                    //VHT
                                    multi = redisClient.multi();
                                    multi.select(4);
                                    if(iter>=2){
                                        multi.get(linkID + ":" + item.split(':')[1], function(err, result){                                          
                                            VHT_square = VHT_square + math.pow((vht - result)/1000,2);
                                            VHT_tot = VHT_tot + result/1000;                                          
                                        });             
                                    }  
                                    multi.set(linkID + ":" + item.split(':')[1], vht);
                                    multi.exec(function(){
                                        callback(null,'VHT done');
                                    });             
                                });           
                            },
                            function(){
                                //calculate gap
                                if(iter>=2){
                                    if(VHT_tot != 0) {
                                        gap = math.pow(VHT_square/arrLink.length,0.5)*(arrLink.length/(VHT_tot));
                                    }else{
                                        gap = 0;
                                    }
                                }
                                console.log('gap='+gap+',VHT_square='+VHT_square+',VHT_tot='+VHT_tot+',linknum='+arrLink.length);                               
                                callback();
                            });
                        
                    }],
                    function(err,results){
                        callback();
                });
            },
            //whilst callback
            function(err,results){
                    //write csv file                  
                    var rcd = [];
                    async.series([
                        function(callback){
                            rcd.push(["iter","linkid","tp","mode","vol"]);
                            redisClient.select(2);
                            redisClient.keys('*',function(err,results){
                                multi = redisClient.multi();
                                results.forEach(function(key){
                                    var arrKey = key.split(":");                                      
                                    multi.get(key,function(err,result){                          
                                        rcd.push([arrKey[3],arrKey[0],arrKey[1],arrKey[2],result]);                                      
                                    })                        
                                })
                                multi.exec(function(){                                  
                                    callback();
                                });
                            });                           
                        },
                        function(callback){ 
                            //console.log(rcd);           
                            csv.writeToStream(fs.createWriteStream(outputFile), rcd, {headers: true})
                                .on("finish",function(){
                                    console.log('end');                                    
                                    callback();
                                });                                                   
                        }],
                        function(err,results){  
                            /*bucket.upload('/outset/*', function(err, file) {
                                console.log(file);
                            }); */                        
                            callback(); 
                        });          
            });
        }],
    function () {
        console.log("/***Main end " + currTime + " ***/");
        //process.exit(0); //End server
    });