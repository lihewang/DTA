//Starting node
var redis = require('redis');
var request = require('request');
//var gapi = require('./google_api.js');
var async = require('async');
var fs = require('fs');

var redisClient = redis.createClient({url:"redis://127.0.0.1:6379"});

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
                redisClient.select(6);  
                redisClient.flushdb();
                for (var i = 1; i <= par.zonenum; i++) {
                    redisClient.rpush('to-do',i);      
                }
                callback();
            },
            //make call
            function(callback){
                request.post('http://localhost:8080',
                {json:{'task':'sp','mode':'sov','zonenum':3}},
                function(error,response,body){
                    console.log('end of sp ' + body);
                    callback();
                });
            }]
        );
        callback();
    },
    //move vehicles
    function(callback){
        console.log('start of moving vehicles');
    }

],
    function(err,results){

    });







