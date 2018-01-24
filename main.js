//main model controls

// redis db list - 1.  2.volume by mode and time step 3.congested time, volume 4.VHT 5.trip table 
//  6.shortest path task for TAZs 7.link toll 8.link task 9.iter 10.VHT_Diff 11.dp probability

var redis = require('ioredis');
var async = require('async');
var fs = require('fs');
var readline = require('readline');
var csv = require('fast-csv');
var hashMap = require('hashmap');
var events = require('events');
var log4js = require('log4js');
var progressBar = require('progress');
var os = require('os');

var appFolder = "./app";
var paraFile = appFolder + "/parameters_sf.json";
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
//fs.truncate('worker.log', 0, function () {
//    logger.info('clear worker log file');
//});

//Desktop deployment
var redisIP = "redis://127.0.0.1:6379";

var outsetFile = "./output/vol.csv"
var redisClient = new redis(redisIP); 
var redisJob = new redis(redisIP); 
var arrLink = [];
var par = null;
var iter = 1;
var gap = 1;
var spmvNum = 0;
var linkupdateNum = 0;
var workercnt = 0;
var refresh = true;
var startTimeStep = 1;
var endTimeStep = 96;
var eventEmitter = new events.EventEmitter();
var bar = new progressBar('[:bar] [:percent]', { width: 80, total: 100 });
var cvgStream = null;
var pathStream = null;
var LinkAttributes = new hashMap(); 
var NodeNewtoOld = new hashMap();
var NodeOldtoNew = new hashMap();

//set global iter
redisClient.select(9);
redisClient.set('iter', iter);

//subscribe to job channel
redisJob.subscribe("job_status");
redisClient.flushall();

//read in files
async.series([  
    function (callback) {
        //start worker nodes
        var exec = require('child_process');
        //var child = exec.spawn('powershell.exe', ['-command', 'node worker.js'], { shell: true, detached: true });
        callback();
    },
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
        //create logConverge file
        var logCvgFile = './output' + '/' + par.log.convergefilename;
        fs.unlink(logCvgFile, function (err, result) { 
            cvgStream = fs.createWriteStream(logCvgFile);
            cvgStream.write("iter, tStep, GAP_VOL, RMSE_VHT" + os.EOL);
            callback();
        });
    },
    function (callback) {
        //create logPath file
        var logPathFile = './output' + '/' + par.log.pathfilename;
        fs.unlink(logPathFile, function (err, result) {
            pathStream = fs.createWriteStream(logPathFile, { 'flags': 'a' });
            pathStream.write("A, B, Vol, iter, tStep, Mode" + os.EOL);
            callback();
        });
    },
    function (callback) {
        //create link output file
        var logvolFile = './output' + '/' + 'vol.csv';
        fs.unlink(logvolFile, function (err, result) {
            volStream = fs.createWriteStream(logvolFile, { 'flags': 'a' });
            var s = 'A,B,TIMESTEP';
            for (var j = 0; j < par.modes.length; j++) {
                s = s + ',' + par.modes[j];
            }
            s = s + ',speed,toll,vc';
            volStream.write(s + os.EOL);
            callback();
        });
    },
    //read node file
    function (callback) {
        var stream = fs.createReadStream(appFolder + "/" + par.nodefilename);
        var sId = par.zonenum + 1;
        var csvStream = csv({ headers: true })
            .on("data", function (data) {
                var id = data['N'];
                id = parseInt(id.trim());
                if (id <= par.zonenum) {
                    NodeNewtoOld.set(id, id);
                    NodeOldtoNew.set(id, id);
                } else {
                    NodeNewtoOld.set(sId, id);
                    NodeOldtoNew.set(id, sId);
                    sId = sId + 1;
                }  
            })
            .on("end", function (result) {
                logger.info("read nodes total of " + par.zonenum + " zones");
                callback();
            });
        stream.pipe(csvStream);
    },
    //read link file
    function (callback) {
        arrLink = [];   //link A-link B
        var stream = fs.createReadStream(appFolder + "/" + par.linkfilename);
        var csvStream = csv({ headers: true })
            .on("data", function (data) {
                var anode = data['A'];
                var bnode = data['B'];
                anode = parseInt(anode.trim());
                bnode = parseInt(bnode.trim());
                var data_id = NodeOldtoNew.get(anode) + '-' + NodeOldtoNew.get(bnode);
                arrLink.push(data_id);    //anode-bnode
                LinkAttributes.set(data_id, {
                    dist: parseFloat(data['Dist']), cap: parseFloat(data['Cap'])
                });
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
        var myInterface = readline.createInterface({
            input: stream
        });
        var key = '';
        var keyPre = '';
        var v = '';
        var cnt = 0;
        var tot = Math.pow(par.zonenum, 2) * 96 * par.modes.length;
        bar.update(0);
        redisClient.select(5);
        myInterface.on("line", function (line) {           
                if (cnt > 0) {              
                    var keys = line.split(',');
                    key = keys[0] + ':' + keys[2] + ':' + keys[3]; 
                    if (keyPre != key && cnt > 1) {
                        redisClient.set(keyPre, v);
                        //logger.info("read trip tabel set key=" + keyPre + ' v=' + v);
                        v = keys[1] + ':' + keys[4];                   
                    } else {
                        if (cnt == 1) {
                            v = keys[1] + ':' + keys[4];
                        } else {
                            v = v + ',' + keys[1] + ':' + keys[4];
                        }                    
                    }  
                    keyPre = key;
                }
                cnt = cnt + 1;
                bar.update(cnt / tot);
            })
            .on("close", function (result) {
                redisClient.set(keyPre, v);
                //logger.info("read trip tabel set key=" + keyPre + ' v=' + v);
                bar.update(1);
                logger.info("read trip tabel total of " + cnt + " records");
                callback();
            });
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
                    for (var j = 1; j <= par.zonenum; j++) {
                        multi.rpush('task', iter + ',' + i + ',' + j + ',' + md);   //iter,timestep,zone,mode
                        //if (iter == 3 && i == 1) {
                        //    logger.info('iter' + iter + ' create spmv tasks' + iter + ',' + i + ',' + j + ',' + md);
                        //}
                        spmvNum = spmvNum + 1;
                    }
                }
            });
            multi.exec(function (err, result) {
                logger.info('iter' + iter + ' set ' + spmvNum + ' single source shortest path tasks in redis');
                callback();
            });
        },
        //link task
        function (callback) {
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
                multi.rpush('task', link);
                //logger.info('iter' + iter + ' set redis 8 ' + link);
                cnt = cnt + 1;
            });
            multi.exec(function (err, result) {
                logger.info('iter' + iter + ' set ' + linkupdateNum + ' link tasks in redis');
                callback();
            });
        },
        function (callback) {
            var logChoiceFile = './output' + '/' + par.log.dpnodefilename;
            fs.truncate(logChoiceFile, 0);  //log only last iteration
            choiceStream = fs.createWriteStream(logChoiceFile, { 'flags': 'a' });
            choiceStream.write("iter, orgID, destID, startTS, tStep, dpID, cmnNd, distTl, distTf, timeTl, timeTf, timeFFTl, timeFFTf, TollEL, TollGP, utility, TlShare" + os.EOL);
            callback();
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
        redisClient.llen('task', function (err, result) {
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
    } else if (message == 'worker_ready') {
        //workercnt = workercnt + 1;      
        //if (workercnt == par.numprocesses) {
        //    logger.info('worker nodes are ready. Total of ' + workercnt);
        //    eventEmitter.emit('next_iter');
        //}
    }
    else if (message == 'sp_mv_done') {
        redisClient.publish('job', 'linkupdate', function (err, result) {
            logger.info('iter' + iter + ' running link attributes update');   
            bar.update(0);
            refresh = true;
        }); 
    } else if (message == 'linkupdate_done') {
        logger.info('iter' + iter + ' link update done');
        //var gap = [];
        //gap.push(0);
        var arrCvgLog = [];        
        var maxGap = 0;
        var maxRMSE = 0;
        if (iter == 1) {
            maxGap = 99;
        }
        async.series([
            function (callback) {
                //calculate gap
                if (iter > 1) {
                    var cntVHT = 0;
                    var arrvht = [];
                    var arrTs = [];
                    for (var i = 0; i < par.timesteps; i++) {
                        arrTs.push(i);
                    }
                    async.each(arrTs,
                        function (ts, callback) {
                            var multi = redisClient.multi();
                            redisClient.select(10);
                            redisClient.lrange('vht' + ts, '0', '-1', function (err, result) {  
                                var vht_tot = 0;
                                var vht_square = 0;
                                var vol_tot = 0;
                                var vol_totdiff = 0;
                                arrvht = result;
                                arrvht.forEach(function (vht) {
                                    var v = vht.split(',');
                                    vht_tot = vht_tot + parseFloat(v[0]);
                                    vht_square = vht_square + Math.pow(parseFloat(v[1]), 2);
                                    vol_tot = vol_tot + parseFloat(v[2]);
                                    vol_totdiff = vol_totdiff + parseFloat(v[3]);
                                    //if (ts == 0) {
                                    //    logger.info('iter' + iter + ' timestep=' + ts + ' vht=' + v[0] + ' vht_diff=' + v[1]);
                                    //}
                                });
                                if (arrvht.length != 0) {
                                    if (vht_tot == 0) {
                                        var gp = 0;
                                        var rmse = 0;
                                    } else {
                                        var gp = vol_totdiff / vol_tot;
                                        var rmse = Math.pow((vht_square / arrvht.length), 0.5) / (vht_tot / arrvht.length);
                                    }
                                    arrCvgLog.push([iter, ts, Math.round(gp * 10000) / 10000, Math.round(rmse * 10000) / 10000]);
                                    //logger.debug('iter' + iter + ' vol diff=' + vol_totdiff + ' vol tot=' + vol_tot);
                                    //gap.push(rmse);
                                    maxGap = Math.max(gp, maxGap);
                                    maxRMSE = Math.max(rmse, maxRMSE);
                                } else {
                                    arrCvgLog.push([iter, ts, 0, 0]);
                                    //gap.push(0);
                                }
                                //if (ts == 71) {
                                    //logger.info('iter' + iter + ' timestep=' + ts + ' gap=' + gp + ' vht_square=' + math.round(vht_square, 0)
                                    //    + ' vht_tot=' + math.round(vht_tot, 0) + ' arrvht length=' + arrvht.length);
                                //}
                                callback();                                  
                            });                           
                        },
                        function (err) {
                            //start from timestep 1, find the boudary of time steps that meet the threshold
                            //logger.info('iter' + iter + ' this iter startTimeStep=' + startTimeStep + ' endTimeStep=' + endTimeStep + ' arrCvgLog length=' + arrCvgLog.length);
                            //for (var i = 0; i < arrCvgLog.length; i++) {
                            //    //logger.info('iter' + iter + ' ts=' + arrCvgLog[i][1]);
                            //    if (arrCvgLog[i][2] >= par.timestepgap) {
                            //        startTimeStep = arrCvgLog[i][1] + 1;
                            //        break;
                            //    }                                
                            //}
                            //for (var i = arrCvgLog.length - 1; i >= 0; i--) {
                            //    var ts = arrCvgLog[i][1] + 1;
                            //    if (arrCvgLog[i][2] >= par.timestepgap) {
                            //        endTimeStep = ts
                            //        break;
                            //    }
                            //}
                            startTimeStep = 1;
                            endTimeStep = par.timesteps
                            for (var i = 0; i < arrCvgLog.length; i++) {
                                cvgStream.write(arrCvgLog[i][0] + ',' + (arrCvgLog[i][1] + 1) + ',' + arrCvgLog[i][2] + ',' + arrCvgLog[i][3] + os.EOL);
                            }                                                     
                            logger.info('iter' + iter + ' RGAP=' + Math.round(maxGap*10000)/10000 + ' VHT_RMSE=' + Math.round(maxRMSE*10000)/10000);
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
                if (iter < par.maxiter && maxGap > par.gap) { //continue
                    iter = iter + 1;
                    redisClient.select(9);
                    redisClient.set('iter', iter, function (err, result) {
                        eventEmitter.emit('next_iter');
                    });
                    callback();
                } else {
                    //write csv file 
                    //logger.info('iter' + iter + ' start write csv');
                    async.series([
                        function (callback) {
                            redisClient.select(2);
                            redisClient.keys('*', function (err, results) {
                                async.eachSeries(results, function (key, callback) {
                                    var arrKey = key.split(':');
                                    var speed = [];
                                    var vc = [];
                                    var toll = [];
                                    //logger.info('iter' + iter + ' key=' + key);
                                    if (arrKey != 'cntNode' && parseInt(arrKey[1]) == iter) { //&& parseInt(arrKey[3]) == iter
                                        var arrKey = key.split(":");
                                        var nd = arrKey[0].split('-');
                                        var link = LinkAttributes.get(arrKey[0]);
                                        //get speed, vc
                                        redisClient.select(3);
                                        redisClient.get(arrKey[0], function (err, result) {
                                            var arrResult = result.split(':');
                                            //logger.info(`[${process.pid}]` + ' iter' + iter + ' link=' + linkid + ' result=' + arrResult.length);
                                            speed.push(0);
                                            vc.push(0);
                                            for (var j = 0; j < par.timesteps; j++) {
                                                var time = arrResult[j].split(',')[0];
                                                if (time == 0) {
                                                    speed.push(70); //ff speed
                                                } else {
                                                    speed.push(link.dist / time * 60);
                                                }
                                                var vol = arrResult[j].split(',')[1];
                                                vc.push(vol * 4 / link.cap);
                                            }
                                        });
                                        //get toll
                                        redisClient.select(7);
                                        redisClient.get(arrKey[0], function (err, result) {
                                            toll.push(0);
                                            if (result == null) {                                                
                                                for (var j = 0; j < par.timesteps; j++) {
                                                    toll.push(0);
                                                }
                                            } else {
                                                var arrResult = result.split(',');
                                                for (var j = 0; j < par.timesteps; j++) {
                                                    toll.push(arrResult[j]);
                                                }
                                            }
                                        });
                                        redisClient.select(2);
                                        redisClient.get(key, function (err, result) {
                                            //logger.info('iter' + iter + ' key=' + key + ' result=' + result);
                                            var modes = par.modes;
                                            var v = result.split(',');
                                            for (var j = 1; j <= par.timesteps; j++) {
                                                var s = '';
                                                for (var i = 0; i < modes.length; i++) {
                                                    if (s == '') {
                                                        s = v[i * par.timesteps + j - 1];
                                                    } else {
                                                        s = s + ',' + v[i * par.timesteps + j - 1];
                                                    }
                                                }
                                                volStream.write(NodeNewtoOld.get(parseInt(nd[0])) + ',' + NodeNewtoOld.get(parseInt(nd[1])) + ',' + j + ',' + s + ',' + speed[j] + ',' + toll[j] + ',' +vc[j] + os.EOL);
                                            }
                                            callback();
                                        });
                                    } else {
                                        callback();
                                    }
                                }, function (err) {
                                    logger.info('iter' + iter + ' write to vol file');
                                    callback();
                                });
                            });
                        }],
                        function (err, results) {
                            volStream.end();
                            cvgStream.end();
                            pathStream.end();
                            redisClient.publish('job', 'end', function (err, result) {
                                logger.info('iter' + iter + ' end of program');
                                process.exit(0); //End server
                                callback(null, "End of program");
                            });
                        });
                }
                
            }],
            function () {
            });
    }
});