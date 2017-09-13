//main model controls

// redis db list - 1.shortest path  2.volume by mode and time step 3.congested time 4.VHT 5.trip table 
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
var progressBar = require('progress');

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
process.stdout.write('\033c');  //clear console

//clear log file
fs.truncate('main.log', 0, function () {
    logger.info('clear main log file');
});
fs.truncate('worker.log', 0, function () {
    logger.info('clear worker log file');
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
var spmvNum = 0;
var linkupdateNum = 0;
var refresh = true;
var eventEmitter = new events.EventEmitter();
var bar = new progressBar('[:bar] [:percent]', {width: 80, total: 100 });

//subscribe to job channel
redisJob.subscribe("job_status");
redisClient.flushall();

//read in files
async.series([   
    function (callback) { 
        //read parameters
        par = JSON.parse(fs.readFileSync(paraFile));
        logger.info("read parameters");
        callback();
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
                }
                timeFFHash.set(data_id, data['Dist'] / data['Spd'] * 60);
                alphaHash.set(data_id, data['Alpha']);
                betaHash.set(data_id, data['Beta']);
                capHash.set(data_id, data['Cap']);
            })
            .on("end", function (result) {
                logger.info("read network total of " + result + " links");
                callback();
            });
        stream.pipe(csvStream);
    },
    //read trip table
    function (callback) {
        logger.info('reading trip table file');
        var stream = fs.createReadStream(appFolder + "/" + par.triptablefilename);
        var key = '';
        var cnt = 0;
        var tot = 0;
        tot = math.pow(par.zonenum, 2) * 96 * par.modes.length
        bar.update(0);
        redisClient.select(5);
        var csvStream = csv({ headers: true })
            .on("data", function (data) {
                if (parseInt(data['TP']) <= par.timesteps) {
                    key = data['I'] + ':' + data['TP'] + ':' + data['Mode'] + ':ct';
                    redisClient.get(key, function (err, result) {
                        if (result == null) {
                            redisClient.set(key, data['J'] + ':' + data['Vol']);
                        } else {
                            redisClient.set(key, result + ',' + data['J'] + ':' + data['Vol']);
                        }
                    });
                }
                cnt = cnt + 1;
                bar.update(cnt / tot);
            })
            .on("end", function (result) {
                bar.update(1);
                logger.info("read trip tabel total of " + result + " zone pairs");
                callback(null, result);
            });
        stream.pipe(csvStream);
    }],
    function () {        
        eventEmitter.emit('next_iter');
    });

//Loop iterations
var model_Loop = function () {
    refresh = true;
    spmvNum = 0;
    async.series([
        //create shortest paths for all time steps and modes               
        function (callback) {
            //create job queue in redis
            multi = redisClient.multi();
            multi.select(6);
            multi.flushdb();
            par.modes.forEach(function (md) {   //loop modes
                for (var i = 1; i <= par.timesteps; i++) {  //loop time steps
                    //zones
                    for (var j = 1; j <= par.zonenum; j++) {
                        multi.RPUSH('task', 'sp-' + iter + '-' + i + '-' + j + '-' + md + '-ct');
                        spmvNum = spmvNum + 1;
                    }
                    //decision point
                    par.dcpnt.forEach(function (dcp) {
                        //console.log("push decison point " + dcp + " to to-do list");
                        var t2 = ['tl', 'tf'];
                        t2.forEach(function (t) {
                            multi.RPUSH('task', 'sp-' + iter + '-' + i + '-' + j + '-' + md + '-' + t);
                            spmvNum = spmvNum + 1;
                        });
                    });
                }
            });
            multi.exec(function (err, result) {
                logger.info('iter' + iter + ' put ' + spmvNum + ' sp tasks in redis');
                callback();
            });
        },
        //link task
        function (callback) {
            linkupdateNum = arrLink.length;
            multi = redisClient.multi();
            multi.select(8);
            multi.flushdb();
            arrLink.forEach(function (link) {       //A-B:tp:iter
                multi.RPUSH('task', link + ":" + iter);
                //logger.info('link task push ' + link);
            });
            multi.exec(function (err, result) {
                logger.info('iter' + iter + ' put link task in redis');
                callback();
            });
        }],
        function (err, results) {
            redisClient.publish('job', 'sp_mv', function (err, result) {
                logger.info('iter' + iter + ' running path building and moving vehicles');
                bar.update(0);
            });
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
    if (message.split(':')[0] == 'bar_tick') {
        redisClient.select(message.split(':')[1]);
        if (message.split(':')[1] == '6') { //sp_mv
            var tot = spmvNum;
        } else if (message.split(':')[1] == '8') {  //link update
            var tot = parseInt(linkupdateNum);
        }
        redisClient.LLEN('task', function (err, result) {
            var v = 1 - parseInt(result) / tot;
            if (refresh) {
                if (v == 1) {
                    refresh = false;
                }
                bar.update(v);
            }
        });       
    } else if (message == 'linkvolredis') {
        logger.info('iter' + iter + ' writting link volume to redis');
    }else if (message == 'sp_mv_done') {
        redisClient.publish('job', 'linkupdate', function (err, result) {
            logger.info('iter' + iter + ' running link attributes update');   
            bar.update(0);
            refresh = true;
        }); 
    } else if (message == 'linkupdate_done') {
        logger.info('iter' + iter + ' link update done');
        var VHT_square = 0;
        var VHT_tot = 0;
        async.series([
            function (callback) {
                //calculate gap
                if (iter > 1) {
                    var cntVHT = 0;
                    redisClient.select(4);
                    redisClient.keys('*', function (err, results) {
                        multi = redisClient.multi();
                        results.forEach(function (key) {
                            multi.get(key, function (err, result) {
                                var r = result.split(',');
                                VHT_square = VHT_square + math.pow(parseFloat(r[2]), 2);
                                VHT_tot = VHT_tot + parseFloat(r[1]);
                                if (parseFloat(r[0]) != 0 || parseFloat(r[1]) != 0) {
                                    logger.debug('iter' + iter + ' link ' + key + ' vht ' + r);
                                    cntVHT = cntVHT + 1;
                                }
                            })
                        })
                        multi.exec(function () {
                            if (VHT_tot != 0) {
                                gap = math.pow(VHT_square / cntVHT, 0.5) * cntVHT / VHT_tot;
                            } else {
                                gap = 0;
                            }
                            logger.info('iter' + iter + ' gap=' + math.round(gap, 2) + ',VHT_square=' + math.round(VHT_square, 2) + ',VHT_tot=' + math.round(VHT_tot, 2) + ',linknum=' + cntVHT);
                            callback();
                        });
                    });
                } else {
                    callback();
                }   
            },
            function (callback) {           
                //check convergence
                if (iter < par.maxiter && gap > par.gap) {
                    iter = iter + 1;
                    eventEmitter.emit('next_iter');
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
                                    var arrKey = key.split(':');
                                    if (arrKey != 'cntNode' && parseInt(arrKey[3]) == iter) {
                                        var arrKey = key.split(":");
                                        multi.get(key, function (err, result) {
                                            rcd.push([arrKey[3], arrKey[0], arrKey[1], arrKey[2], result]);
                                        })
                                    }
                                })
                                multi.exec(function () {
                                    callback();
                                });
                            });
                        },
                        function (callback) {       
                            csv.writeToStream(fs.createWriteStream(outputFile), rcd, { headers: true })
                                .on("finish", function () {
                                    redisClient.flushall();
                                    logger.info('iter' + iter + ' end writing output');
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