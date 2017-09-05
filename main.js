//main model controls

// redis db list - 1.shortest path  2.volume by mode and time step 3.congested time 4.VHT
//  6.shortest path task 7.move vehicle task 8.link task

var redis = require('redis');
var request = require('request');
var async = require('async');
var fs = require('fs');
var csv = require('fast-csv');
var hashMap = require('hashmap');
var math = require('mathjs');
var events = require('events');
var log4js = require('log4js');

log4js.configure({
    appenders: {
        everything: { type: 'file', filename: 'main.log', backups: 1 },
        console: { type: 'console' }
    },
    categories: {
        default: { appenders: ['everything', 'console'], level: 'all' }
    }
});
var logger = log4js.getLogger();
//clear log file
fs.truncate('main.log', 0, function () {
    logger.info(`[${process.pid}] clear main log file`)
});

//Desktop deployment
var redisIP = "redis://127.0.0.1:6379";
var appFolder = "./app";
var paraFile = appFolder + "/parameters.json";

var outputFile = "./output/vol.csv"
var redisClient = redis.createClient({ url: redisIP }), multi;
var redisJob = redis.createClient({ url: redisIP }), multi;
var arrLink = [];
var par = null;
var timeFFHash = new hashMap();
var alphaHash = new hashMap();
var betaHash = new hashMap();
var capHash = new hashMap();
var iter = 1;
var gap = 1;
var eventEmitter = new events.EventEmitter();

//subscribe to job channel
redisJob.subscribe("job");

//read in files
async.series([   
    function (callback) { 
        //read parameters
        par = JSON.parse(fs.readFileSync(paraFile));
        logger.info("read parameters");
        //clear link db
        multi = redisClient.multi();
        multi.select(2);  
        multi.flushdb(); 
        multi.exec(function (err, result) {   
            callback();
        });
    },
    //read link file
    function (callback) {
        arrLink = [];   //link A-link B:tp
        var stream = fs.createReadStream(appFolder + "/" + par.linkfilename);
        var csvStream = csv({ headers: true })
            .on("data", function (data) {
                var anode = data['A'];
                var bnode = data['B'];
                anode = anode.trim();
                bnode = bnode.trim();
                var data_id = anode + '-' + bnode;
                for (var i = 1; i <= par.timesteps; i++) {
                    arrLink.push(data_id + ':' + i);
                    multi.RPUSH(data_id + ':' + i);
                }
                timeFFHash.set(data_id, data['Dist'] / data['Spd'] * 60);
                alphaHash.set(data_id, data['Alpha']);
                betaHash.set(data_id, data['Beta']);
                capHash.set(data_id, data['Cap']);
            })
            .on("end", function (result) {
                multi.exec(function () {
                });
                logger.info("read network total of " + result + " links");
                callback();
            });
        stream.pipe(csvStream);
    }],
    function () {
        eventEmitter.emit('next_iter');
    });

//Loop iterations
var model_Loop = function () {
    async.series([
        //create shortest paths for all time steps and modes               
        function (callback) {
            //create job queue in redis
            multi = redisClient.multi();
            //flush sp db
            multi.select(1);
            multi.flushdb();
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
                redisClient.publish('job', 'sp', function (err, result) {
                    logger.info('iter' + iter + ' sp job created in redis ' + err + ', ' + result);
                    callback();
                });
            });
        },
        //put mv to redis
        function (callback) {
            multi = redisClient.multi();
            multi.select(7);
            multi.flushdb();
                //set task to db
                async.series([
                    function (callback) {
                        //read trip table inset
                        var stream = fs.createReadStream(appFolder + "/" + par.triptablefilename);
                        var csvStream = csv({ headers: true })
                                .on("data", function (data) {
                                    if (parseInt(data['I']) > 0) {
                                        multi.RPUSH('task', 'mv-' + iter + '-' + data['I'] + '-' + data['J'] + '-' + data['TP'] + '-' + data['Mode'] + '-' + data['Vol'] + '-ct');
                                    }
                                })
                                .on("end", function () {
                                    callback();
                                });
                            stream.pipe(csvStream);
                    }],
                    function (err, results) {
                        multi.exec(function (err, result) {
                            logger.info('iter' + iter + ' put mv tasks to redis ' + result);
                            callback();
                        });
                    });
        },
        //link task
        function (callback) {
            multi = redisClient.multi();
            multi.select(8);
            multi.flushdb();
            arrLink.forEach(function (link) {       //A-B:tp:iter
                multi.RPUSH('task', link + ":" + iter);
                //logger.info('link task push ' + link);
            });
            multi.exec(function (err, result) {
                logger.info('iter' + iter + ' link task pushed ' + !err);
                callback();
            });
        }],
        function (err, results) {

        });
}

eventEmitter.on('next_iter', model_Loop);

//log redis error
redisClient.on("error", function (err) {
    logger.debug('redis error ' + err);
});

redisJob.on("error", function (err) {
    logger.debug('redis error ' + err);
});

redisJob.on("message", function (channel, message) {
    if (message == 'sp_done') {
        //publish mv when sp is done
        logger.info('iter' + iter + ' sp done');
        redisClient.publish('job', 'mv', function (err, result) {
            //logger.info('mv published ' + result);
        }); 
    }
    if (message == 'mv_done') {
        //publish link update when mv is done
        logger.info('iter' + iter + ' mv done');
        redisClient.publish('job', 'linkupdate', function (err, result) {
            //logger.info('link update published ' + result);
        });
    }
    if (message == 'linkupdate_done') {
        logger.info('iter' + iter + ' link update done');
        var VHT_square = 0;
        var VHT_tot = 0;
        async.series([
            function (callback) {
                //calculate gap
                if (iter >= 2) {
                    if (VHT_tot != 0) {
                        gap = math.pow(VHT_square / arrLink.length, 0.5) * (arrLink.length / (VHT_tot));
                    } else {
                        gap = 0;
                    }
                }
                logger.debug('iter' + iter + ' gap=' + gap + ',VHT_square=' + VHT_square + ',VHT_tot=' + VHT_tot + ',linknum=' + arrLink.length);
                callback();
            },
            function (callback) {           
                //check convergence
                if (iter < par.maxiter && gap > par.gap) {
                    eventEmitter.emit('next_iter');
                    iter = iter + 1;
                    callback();
                } else {
                    //write csv file                  
                    var rcd = [];
                    async.series([
                        function (callback) {
                            rcd.push(["iter", "linkid", "tp", "mode", "vol"]);
                            redisClient.select(2);
                            redisClient.keys('*', function (err, results) {
                                multi = redisClient.multi();
                                results.forEach(function (key) {
                                    var arrKey = key.split(":");
                                    multi.get(key, function (err, result) {
                                        rcd.push([arrKey[3], arrKey[0], arrKey[1], arrKey[2], result]);
                                    })
                                })
                                multi.exec(function () {
                                    callback();
                                });
                            });
                        },
                        function (callback) {
                            //console.log(rcd);           
                            csv.writeToStream(fs.createWriteStream(outputFile), rcd, { headers: true })
                                .on("finish", function () {
                                    redisClient.flushall();
                                    logger.debug('end writing output');
                                    callback();
                                });
                        }],
                        function (err, results) {
                            callback(null, "End of program");
                        }); 
                }
                
            }],
            function () {
                //console.log("/***Main end " + currTime + " ***/");
                //process.exit(0); //End server
            });
    }
});