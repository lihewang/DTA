//main model controls

// redis db list - 1.shortest path  2.volume by mode and time step 3.congested time, volume 4.VHT 5.trip table 
//  6.shortest path task for TAZs 7.toll 8.link task 9.iter 

var redis = require('redis');
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
var eventEmitter = new events.EventEmitter();
var bar = new progressBar('[:bar] [:percent]', { width: 80, total: 100 });

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
            redisClient.select(4);
            redisClient.del('vht', function (err, result) {
                callback();
            });
        },
        //create shortest paths for all time steps and modes               
        function (callback) {
            //create job queue in redis
            multi = redisClient.multi();
            multi.select(6);
            multi.flushdb(); 
            par.modes.forEach(function (md) {   //loop modes
                for (var i = 1; i <= par.timesteps; i++) {  //loop time steps
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
        var VHT_square = 0;
        var VHT_tot = 0;
        var gap = 1;
        async.series([
            function (callback) {
                //calculate gap
                if (iter > 1) {
                    var cntVHT = 0;
                    var arrvht = [];
                    multi = redisClient.multi();
                    multi.select(4);
                    multi.LRANGE('vht', '0', '-1', function (err, result) {
                        arrvht = result;
                    });
                    multi.exec(function (err, result) {
                        var vht_tot = 0;
                        var vht_square = 0;
                        arrvht.forEach(function (vht) {
                            var v = vht.split(',');
                            vht_tot = vht_tot + parseFloat(v[0]);
                            vht_square = vht_square + math.pow(parseFloat(v[1]), 2);
                        })
                        if (arrvht.length != 0) {
                            gap = math.pow(vht_square / arrvht.length, 0.5) * arrvht.length / vht_tot;
                        } else {
                            gap = 0;
                        }
                        logger.info('iter' + iter + ' gap=' + math.round(gap, 4) + ' arrvht=' + arrvht.length + ' vht_tot=' + vht_tot + ' vht_square=' + vht_square);
                        callback();
                    });
                } else {
                    callback();
                }   
            },
            function (callback) {           
                //check convergence
                if (iter < par.maxiter && gap > par.gap) {
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
                            csv.writeToStream(fs.createWriteStream(outsetFile), rcd, { headers: true })
                                .on("finish", function () {
                                    //redisClient.flushall();
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