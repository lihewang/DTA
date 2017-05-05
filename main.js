//Starting node
var redis = require('redis');
var request = require('request');
var async = require('async');
var fs = require('fs');
var csv = require('fast-csv');
var hashtable = require('hashtable');
var math = require('mathjs');

// redis db list - 1.shortest path  2.volume by mode and time step 3.congested time 4.vHT
//  6.shortest path task 7.move vehicle task
var redisClient = redis.createClient({url:"redis://127.0.0.1:6379"}),multi;
var arrLink = [];
var par = null;
var timeFFHash = new hashtable();
var alphaHash = new hashtable();
var betaHash = new hashtable();
var capHash = new hashtable();
var iter = 0;
var gap = 1;
async.series([
    //read parameters
    function(callback){
        par = JSON.parse(fs.readFileSync("./Data/parameters.json"));
        callback();
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
                        var stream = fs.createReadStream("./Data/" + par.linkfilename);
                        var csvStream = csv({headers : true})
                            .on("data", function(data){
                                for (var i = 1; i <= par.timesteps; i++) { 
                                    arrLink.push(data['ID'] + ':' + i); 
                                }
                                timeFFHash.put(data['ID'],data['Dist']/data['Spd']*60);
                                alphaHash.put(data['ID'],data['Alpha']);
                                betaHash.put(data['ID'],data['Beta']);
                                capHash.put(data['ID'],data['Cap']);
                            })
                            .on("end", function(){      
                                callback();
                            });     
                        stream.pipe(csvStream);
                    },
                    //create shortest path
                    function(callback){
                        console.log('start of sp');
                        async.series([
                            //put task to db
                            function(callback){               
                                async.series([
                                    function(callback){
                                        multi = redisClient.multi();
                                        //clear shortest path db
                                        multi.select(1);  
                                        multi.flushdb();
                                        //clear task db
                                        multi.select(6);  
                                        multi.flushdb();                     
                                        multi.exec(function(){
                                            callback();
                                        });
                                    },
                                    function(callback){
                                        multi = redisClient.multi();
                                        //mode, timestep and start zone
                                        par.modes.forEach(function(md){                       
                                            for (var i = 1; i <= par.timesteps; i++) {
                                                for (var j = 1; j <= par.zonenum; j++) {                          
                                                    multi.rpush('to-do',i + '-' + j + '-' + md);  //tp-zone-SOV    
                                                }
                                                //decision point
                                                par.dcpnt.forEach(function(dcp){
                                                    multi.rpush('to-do',i + '-' + dcp + '-' + md + '-tl'); //tp-zone-SOV-tl
                                                    multi.rpush('to-do',i + '-' + dcp + '-' + md + '-tf'); //tp-zone-SOV-tf
                                                });
                                            }
                                        });                                              
                                        multi.exec(function(){
                                            callback();
                                        });                    
                                    }],
                                    function(err,results){
                                        callback(); 
                                    }
                                );                               
                            },
                            //make call
                            function(callback){
                                request.get('http://localhost:8080',
                                {json:{'task':'sp','iter':iter}},
                                function(error,response,body){
                                    console.log(body);
                                    callback();
                                });
                            }],
                            function(err,results){
                                callback();
                            }
                        );      
                    },
                    //move vehicles
                    function(callback){
                        console.log('***moving vehicles***');
                        async.series([
                            //put task to db
                            function(callback){               
                                async.series([
                                    function(callback){
                                        multi = redisClient.multi();
                                        //clear link db
                                        multi.select(2);  
                                        multi.flushdb(); 
                                        //clear task db
                                        multi.select(7);  
                                        multi.flushdb();                                         
                                        multi.exec(function(){
                                            callback();
                                        });
                                    },
                                    function(callback){
                                        //read trip table input
                                        var stream = fs.createReadStream("./Data/" + par.triptablefilename);
                                        multi = redisClient.multi();
                                        multi.select(7); 
                                        var csvStream = csv({headers : true})
                                            .on("data", function(data){ 
                                                if(parseInt(data['I'])>0){                            
                                                    multi.rpush('to-do', data['I']+'-'+data['J']+'-'+data['TP']+'-'+data['Mode']+'-'+data['Vol']+'-zone');
                                                }
                                            })
                                            .on("end", function(){  
                                                multi.exec(function(){
                                                    callback(null,'read trip table done');
                                                });                                                  
                                            });    
                                        stream.pipe(csvStream); 
                                    }],
                                    function(err,results){
                                        callback(); 
                                    }); 
                            },
                            //make call
                            function(callback){
                                console.log('call mv');
                                request.get('http://localhost:8080',
                                {json:{'task':'mv','iter':iter}},
                                function(error,response,body){
                                    console.log('end of move vehicles ' + body);
                                    callback();
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
                                var vol = 0;
                                multi.select(2);
                                //total vol of all modes
                                async.eachSeries(par.modes, function(md,callback){
                                    multi.get(item + ":" + md, function(err,result){
                                        if(result != null){
                                            vol = vol + parseFloat(result);
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
                                        console.log('item='+item+',VHT='+vht+',vol='+vol+',cgtime='+cgTime);
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
                            rcd.push(["linkid","tp","mode","vol"]);
                            redisClient.select(2);
                            redisClient.keys('*',function(err,results){
                                multi = redisClient.multi();
                                results.forEach(function(key){
                                    var arrKey = key.split(":");                                      
                                    multi.get(key,function(err,result){                          
                                        rcd.push([arrKey[0],arrKey[1],arrKey[2],result]);                                      
                                    })                        
                                })
                                multi.exec(function(){                                  
                                    callback();
                                });
                            });                           
                        },
                        function(callback){ 
                            //console.log(rcd);           
                            csv.writeToStream(fs.createWriteStream("vol.csv"), rcd, {headers: true})
                                .on("finish",function(){
                                    console.log('end');
                                    callback();
                                });                                                   
                        }],
                        function(err,results){                            
                            callback(); 
                        });          
            });
        }],
    function(){
        process.exit(0); //End server
    });