//Starting node
var redis = require('redis');
var request = require('request');
//var gapi = require('./google_api.js');
var async = require('async');
var fs = require('fs');

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
                        //timestep and zone
                        for (var i = 1; i <= par.timesteps; i++) {
                            for (var j = 1; j <= par.zonenum; j++) {                          
                                multi.rpush('to-do',i + '-' + j);      
                            }
                            //decision point
                            par.dcpnt.forEach(function(value){
                                multi.rpush('to-do',i + '-' + value);
                            });
                        }                       
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
                request.post('http://localhost:8080',
                {json:{'task':'sp','mode':'TRK','zonenum':3}},
                function(error,response,body){
                    console.log('end of sp ' + body);
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
        console.log('start of moving vehicles');
    }

],
    function(err,results){

    });







