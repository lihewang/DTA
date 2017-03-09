//Starting node
var redis = require('redis');
var request = require('request');
//var gapi = require('./google_api.js');
var async = require('async');
var fs = require('fs');
var csv = require('fast-csv');

var redisClient = redis.createClient({url:"redis://127.0.0.1:6379"}),multi;

var par = null;
async.series([
    //read parameters
    function(callback){
        par = JSON.parse(fs.readFileSync("./Data/parameters.json"));
        callback();
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
                {json:{'task':'sp'}},
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
                                multi.rpush('to-do', data['I']+'-'+data['J']+'-'+data['TP']+'-'+data['Mode']+'-'+data['Vol']);
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
                {json:{'task':'mv'}},
                function(error,response,body){
                    console.log('end of move vehicles ' + body);
                    callback();
                });
            }],
        function(err,results){
            callback(); //move vehicles callback
        }); 
    }],
    function(err,results){

});