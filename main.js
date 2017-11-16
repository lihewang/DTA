//main model controls

// redis db list - 1.shortest path  2.volume by mode and time step 3.congested time, volume 4.VHT 5.trip table 
//  6.shortest path task for TAZs 7.toll 8.link task 9.iter 10.VHT_Diff 11.dp probability

var redis = require('redis');
var async = require('async');
var fs = require('fs');
var csv = require('fast-csv');
var hashMap = require('hashmap');
var math = require('mathjs');
var events = require('events');
var log4js = require('log4js');
var progressBar = require('progress');
var os = require('os');

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
var paraFile = appFolder + "/parameters_95.json";
var outsetFile = "./output/vol.csv"
var redisClient = redis.createClient({ url: redisIP }), multi;
var redisJob = redis.createClient({ url: redisIP }), multi;
var arrLink = [];
var par = null;
var iter = 1;
var gap = 1;
var spmvNum = 0;
var linkupdateNum = 0;
var refresh = true;
var startTimeStep = 1;
var endTimeStep = 96;
var eventEmitter = new events.EventEmitter();
var bar = new progressBar('[:bar] [:percent]', { width: 80, total: 100 });
var cvgStream = null;

//set global iter
redisClient.select(9);
redisClient.set('iter', iter);

//subscribe to job channel
redisJob.subscribe("job_status");
redisClient.flushall();

//read in files
async.series([  
    function (callback) {
        //set global iter
        redisClient.select(9);
        redisClient.set('iter', iter, function (err, result) {
            callback();
        });
    },
    function (callback) { 
        //read parameters
        par = JSON.parse(fs.readFileSync(paraFile));
        logger.info("read parameters");
        callback();
    },
    function (callback) {
        //create logToll file
        var logTollFile = './output' + '/' + par.log.tollfilename;
        fs.unlink(logTollFile, function (err, result) {
            var tollStream = fs.createWriteStream(logTollFile);
            tollStream.write("iter,tStep,A,B,vol,cap,vc,toll,MSAtoll,exp,mintoll,maxtoll" + os.EOL);
            tollStream.end();
            callback();
        });       
    },
    function (callback) {
        //create logChoice file
        var logChoiceFile = './output' + '/' + par.log.dpnodefilename;
        fs.unlink(logChoiceFile, function (err, result) {
            var arrLog = [];
            arrLog.push(["iter", "tStep", "dpID", "destID", "distTl", "distTf", "timeTl", "timeTf", "timeFFTl", "timeFFTf", "TollEL", "TollGP", "utility", "TlShare"]);
            csv.writeToStream(fs.createWriteStream(logChoiceFile, { 'flags': 'a' }), arrLog, { headers: true });
            callback();
        });       
    },
    function (callback) {
        //create logConverge file
        var logCvgFile = './output' + '/' + par.log.convergefilename;
        fs.unlink(logCvgFile, function (err, result) { 
            cvgStream = fs.createWriteStream(logCvgFile);
            cvgStream.write("iter, tStep, RgapVHT" + os.EOL);
            callback();
        });
    },
    //read node file
    function (callback) {
        var stream = fs.createReadStream(appFolder + "/" + par.nodefilename);
        var csvStream = csv({ headers: true })
            .on("data", function (data) {
                var id = data['N'];
                id = id.trim();
            })
            .on("end", function (result) {
                logger.info("read node total of " + result + " nodes");
                callback();
            });
        stream.pipe(csvStream);
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
                arrLink.push(data_id);    //anode-bnode
                //Check network for errors
                var attributes = ['TIME', 'Dist', 'Spd', 'Cap', 'Ftype', 'TOLL', 'TOLLTYPE', 'Alpha', 'Beta'];
                attributes.forEach(function (a) {
                    if (data[a] == '') {
                        logger.debug(`[${process.pid}]` + ' warning!!! network attribute ' + a + ' has null value!!!');
                    }
                });
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
        var tot = math.pow(par.zonenum, 2) * 96 * par.modes.length;
        var TT = new hashMap();
        bar.update(0);
        multi = redisClient.multi();
        multi.select(5);
        var csvStream = csv({ headers: true })
            .on("data", function (data) {
                if (parseInt(data['TP']) <= par.timesteps) {    //each record is an origin
                    key = data['I'] + ':' + data['TP'] + ':' + data['Mode'];
                    var v = TT.get(key);
                    if (v == null) {
                        TT.set(key, data['J'] + ':' + data['Vol']);
                    } else {
                        TT.set(key, v + ',' + data['J'] + ':' + data['Vol']);
                    }
                }
                cnt = cnt + 1;
                bar.update(cnt / tot);
            })
            .on("end", function (result) {
                bar.update(1);
                logger.info('writing trip table to redis');
                TT.forEach(function (value, key) {
                     multi.set(key, value, function (err, result) {
                     });
                });
                multi.exec(function (err, result) {
                    logger.info("read trip tabel total of " + cnt + " records");
                    callback(null, result);
                });    
            });
        stream.pipe(csvStream);
    }],
    function () {   
        endTimeStep = par.timesteps;
        eventEmitter.emit('next_iter');
    });

//Loop iterations
var model_Loop = function () {
    logger.info('iter' + iter + ' create spmv tasks');
    refresh = true;
    spmvNum = 0;
    async.series([
        function (callback) {
            //delete vht list
            redisClient.select(10);
            redisClient.flushdb();
            callback();
        },
        //create shortest paths for all time steps and modes               
        function (callback) {
            //create job queue in redis
            multi = redisClient.multi();
            multi.select(6);
            multi.flushdb(); 
            par.modes.forEach(function (md) {   //loop modes
                for (var i = startTimeStep; i <= endTimeStep; i++) {  //loop time steps
                    //zones (db6)
                    for (var j = 1; j <= par.zonenum; j++) {
                        //multi.select(6);
                        multi.RPUSH('task', iter + ',' + i + ',' + j + ',' + md);   //iter-timestep-zone-mode
                        spmvNum = spmvNum + 1;
                    }
                }
            });
            multi.exec(function (err, result) {
                logger.info('iter' + iter + ' set ' + spmvNum + ' sp tasks in redis');
                callback();
            });
        },
        //link task
        function (callback) {
            bar.update(0);
            redisClient.select(2);
            redisClient.set('cntNode', 0);
            redisClient.select(6);
            redisClient.set('cnt', 0);
            linkupdateNum = arrLink.length;
            multi = redisClient.multi();
            multi.select(8);
            multi.flushdb();
            var cnt = 0;
            arrLink.forEach(function (link) {       //anode-bnode
                multi.RPUSH('task', link);
                cnt = cnt + 1;
                bar.update(0.5);
            });
            multi.exec(function (err, result) {
                bar.update(1);
                logger.info('iter' + iter + ' set ' + linkupdateNum + ' link tasks in redis');
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
    } else if (message == 'sp_mv_done') {
        redisClient.publish('job', 'linkupdate', function (err, result) {
            logger.info('iter' + iter + ' running link attributes update');   
            bar.update(0);
            refresh = true;
        }); 
    } else if (message == 'linkupdate_done') {
        logger.info('iter' + iter + ' link update done');
        var gap = [];
        gap.push(0);
        var arrCvgLog = [];
        var maxGap = 0;
        async.series([
            function (callback) {
                //calculate gap
                if (iter > 1) {
                    var cntVHT = 0;
                    var arrvht = [];
                    var arrTs = [];
                    for (var i = startTimeStep; i <= endTimeStep; i++) {
                        arrTs.push(i);
                    }
                    logger.info('iter' + iter + ' startTimeStep=' + startTimeStep + ' endTimeStep=' + endTimeStep);
                    async.each(arrTs,
                        function (ts, callback) {
                            multi = redisClient.multi();
                            multi.select(10);
                            multi.LRANGE('vht' + ts, '0', '-1', function (err, result) {
                                arrvht = result;                               
                            });
                            multi.exec(function (err, result) {
                                var vht_tot = 0;
                                var vht_square = 0;
                                arrvht.forEach(function (vht) {
                                    var v = vht.split(',');                                   
                                    vht_tot = vht_tot + parseFloat(v[0]);
                                    vht_square = vht_square + math.pow(parseFloat(v[1]), 2);
                                    //if (ts == 71) {
                                    //    logger.info('iter' + iter + ' timestep=' + ts + ' vht=' + v[0] + ' vht_diff=' + v[1]);
                                    //}
                                });
                                if (arrvht.length != 0) {
                                    if (vht_tot == 0) {
                                        var gp = 0;
                                    } else {
                                        var gp = math.pow((vht_square / arrvht.length), 0.5) / (vht_tot / arrvht.length);
                                    }
                                    arrCvgLog.push([iter, ts, math.round(gp, 4)]);
                                    //logger.info('iter' + iter + ' arrCvgLog add' + '[' + iter + ',' + ts + ',' + math.round(gp, 4) + ']');
                                    gap.push(gp);
                                    maxGap = math.max(gp, maxGap);
                                } else {
                                    arrCvgLog.push([iter, ts, 0]);
                                    gap.push(0);
                                }
                                //if (ts == 71) {
                                //    logger.info('iter' + iter + ' timestep=' + ts + ' gap=' + gp + ' vht_square=' + math.round(vht_square, 0)
                                //        + ' vht_tot=' + math.round(vht_tot, 0) + ' arrvht length=' + arrvht.length);
                                //}
                                callback();
                            });
                        },
                        function (err) {
                            //start from timestep 1, find the boudary of time steps that meet the threshold
                            //logger.info('iter' + iter + ' this iter startTimeStep=' + startTimeStep + ' endTimeStep=' + endTimeStep + ' arrCvgLog length=' + arrCvgLog.length);
                            for (var i = 0; i < arrCvgLog.length; i++) {
                                //logger.info('iter' + iter + ' ts=' + arrCvgLog[i][1]);
                                if (arrCvgLog[i][2] >= par.timestepgap) {
                                    startTimeStep = arrCvgLog[i][1];
                                    break;
                                }                                
                            }
                            for (var i = arrCvgLog.length - 1; i >= 0; i--) {
                                var ts = arrCvgLog[i][1];
                                if (arrCvgLog[i][2] >= par.timestepgap) {
                                    endTimeStep = ts
                                    break;
                                }
                            }
                            for (var i = 0; i < arrCvgLog.length; i++) {
                                cvgStream.write(arrCvgLog[i][0] + ',' + arrCvgLog[i][1] + ',' + arrCvgLog[i][2] + os.EOL);
                            }                           
                            //csv.writeToStream(fs.createWriteStream('./output' + '/' + par.log.convergefilename, { 'flags': 'a' }), arrCvgLog, { headers: true })
                            logger.info('iter' + iter + ' gap=' + maxGap + ', next iter start ts=' + startTimeStep + ', end ts=' + endTimeStep);
                         }
                    );                    
                } 
                redisClient.select(9);
                redisClient.set('startTimeStep', startTimeStep);
                redisClient.set('endTimeStep', endTimeStep, function (err, result) {
                    callback();
                });
            },
            function (callback) {           
                //check convergence
                if (iter == 1 || (iter < par.maxiter && maxGap > par.gap)) {
                    iter = iter + 1;
                    redisClient.select(9);
                    redisClient.set('iter', iter, function (err, result) {
                        eventEmitter.emit('next_iter');
                    });
                    callback();
                } else {
                    //write csv file                  
                    var rcd = [];
                    async.series([
                        function (callback) {
                            rcd.push(["iter", "A", "B", "tp", "mode", "vol"]);
                            redisClient.select(2);
                            redisClient.keys('*', function (err, results) {
                                multi = redisClient.multi();
                                results.forEach(function (key) {
                                    var arrKey = key.split(':');
                                    if (arrKey != 'cntNode' && parseInt(arrKey[3]) == iter) { //&& parseInt(arrKey[3]) == iter
                                        var arrKey = key.split(":");
                                        var nd = arrKey[0].split('-');
                                        multi.get(key, function (err, result) {
                                            rcd.push([arrKey[3], nd[0], nd[1], arrKey[1], arrKey[2], math.round(result, 2)]);
                                        });
                                    }
                                })
                                multi.exec(function () {
                                    callback();
                                });
                            });
                        },
                        function (callback) {       
                            csv.writeToStream(fs.createWriteStream(outsetFile), rcd, { headers: true })
                                .on("finish", function () {
                                    redisClient.flushall();
                                    logger.info('iter' + iter + ' end writing output');
                                    callback();
                                });
                        }],
                        function (err, results) {
                            cvgStream.end();
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