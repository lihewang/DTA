//main model controls

// redis db list - 1.  2.volume by mode and time step 3.congested time, volume 4.VHT 5.trip table 
//  6.shortest path task for TAZs 7.link toll 8.link task 9.iter 10.VHT_Diff 

var redis = require('ioredis');
var async = require('async');
var fs = require('fs');
var csv = require('fast-csv');
var os = require('os');
var storage = require('@google-cloud/storage');

var cloud_prjID = 'dta-01';
var cloud_bucketName = 'eltod';
var runlistfilename = 'runlist.json';
var redisIP = "redis://127.0.0.1:6379";
//var redisIP = "redis.default.svc.cluster.local:6379";

var gcs = storage({
    projectId: cloud_prjID,
    keyFilename: 'dta-01-1e8b82b8f33c.json'   //needed to run locally; not needed for Google Cloud
});
var bucket = gcs.bucket(cloud_bucketName);

var outsetFile = "/output/vol.csv"
var arrLink = [];
var zoneNodes = [];
var iter = 1;
var gap = 1;
var spmvNum = 0;
var linkupdateNum = 0;
var workercnt = 0;
var startTimeStep = 1;
var endTimeStep = 96;
var cvgStream = null;
var pathStream = null;
var LinkAttributes = new Map(); 
var NodeNewtoOld = new Map();
var NodeOldtoNew = new Map();
var scenRuns = [];
var scenIndex = 0;
var runListPar = '';
var par = null;
var volStream = null;
var loadedLinkFile = '';
var redisClient = new redis(redisIP);
var redisJob = new redis(redisIP);
redisJob.subscribe("job_status");

//read run list file
async.series([
    function (callback) {
        console.log('read run list file');
        var runlistFile = bucket.file(runlistfilename);
        runlistFile.download(function (err, result) {
            runListPar = JSON.parse(result);         
            callback();
        });
    }],
    function () {        
        scen_Loop();
    });

var scen_Loop = function () {
    //read in files
    console.log('--- Start Scenario ' + runListPar.runs[scenIndex] + ' ---');
    async.series([
        function (callback) {
            //read parameters for the scenario
            var scenario = runListPar.runs[scenIndex];
            var parFile = runListPar[scenario].parameterfile;
            var overwrite = runListPar[scenario].overwrite;
            bucket.file(parFile).download(function (err, result) {
                if (err) {
                    console.log('parameter file not found ' + parFile);
                }
                par = JSON.parse(result);
                Object.keys(overwrite).forEach(function (k) {
                    par[k] = overwrite[k];
                });
                callback();
            });
        },
        function (callback) {
            //set global iter
            redisClient.select(9);
            redisClient.set('iter', iter, function (err, result) {
                callback();
            });
        },
        //function (callback) {
        //    //create logToll file
        //    var logTollFile = './output' + '/' + par.log.tollfilename;
        //    fs.unlink(logTollFile, function (err, result) {
        //        var tollStream = fs.createWriteStream(logTollFile);
        //        tollStream.write("iter,tStep,A,B,vol,cap,vc,toll,MSAtoll,exp,mintoll,maxtoll" + os.EOL);
        //        tollStream.end();
        //        callback();
        //    });
        //},
        //function (callback) {
        //    //create logConverge file
        //    var logCvgFile = './output' + '/' + par.log.convergefilename;
        //    fs.unlink(logCvgFile, function (err, result) {
        //        cvgStream = fs.createWriteStream(logCvgFile);
        //        cvgStream.write("iter, tStep, GAP_VOL, RMSE_VHT" + os.EOL);
        //        callback();
        //    });
        //},
        //function (callback) {
        //    //create logPath file
        //    var logPathFile = './output' + '/' + par.log.pathfilename;
        //    fs.unlink(logPathFile, function (err, result) {
        //        pathStream = fs.createWriteStream(logPathFile, { 'flags': 'a' });
        //        pathStream.write("A, B, Vol, iter, tStep, Mode" + os.EOL);
        //        callback();
        //    });
        //},
        function (callback) {
            //create link output file
            console.log('create link output file');
            loadedLinkFile = './output/' + runListPar.runs[scenIndex] + '/vol.csv';
            volStream = fs.createWriteStream(loadedLinkFile);        
            var s = 'A, B, TIMESTEP';
            for (var j = 0; j < par.modes.length; j++) {
                s = s + ',' + par.modes[j];
            }
            s = s + ',speed,toll,vc' + os.EOL;
            volStream.write(s);
            callback();                                  
        },
        //read node file
        function (callback) {
            //console.log('read node file ' + par.nodefilename);
            var stream = bucket.file(par.nodefilename).createReadStream();
            var sId = par.zonenum + 1;
            var csvStream = csv({ headers: true })
                .on("error", function (err) {
                    console.log('node file not found ' + par.nodefilename);
                })
                .on("data", function (data) {
                    var id = data['N'];
                    id = parseInt(id.trim());
                    if (id <= par.zonenum) {
                        zoneNodes.push(id);
                        NodeNewtoOld.set(id, id);
                        NodeOldtoNew.set(id, id);
                    } else {
                        NodeNewtoOld.set(sId, id);
                        NodeOldtoNew.set(id, sId);
                        sId = sId + 1;
                    }
                })
                .on("end", function (result) {
                    console.log("read nodes total of " + par.zonenum + " zones");
                    callback();
                });
            stream.pipe(csvStream);
        },
        //read link file
        function (callback) {
            arrLink = [];   //link A-link B
            var stream = bucket.file(par.linkfilename).createReadStream();
            var csvStream = csv({ headers: true })
                .on("error", function (err) {
                    console.log('link file not found ' + par.linkfilename);
                })
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
                            console.log(`[${process.pid}]` + ' warning!!! network attribute ' + a + ' has null value!!!');
                        }
                    });
                })
                .on("end", function (result) {
                    console.log("read network total of " + result + " links");
                    callback();
                });
            stream.pipe(csvStream);
        },
        //read trip table
        function (callback) {           
            var stream = bucket.file(par.triptablefilename).createReadStream();
            var key = '';
            var keyPre = '';
            var v = '';
            var vol = '';
            var newRcd = true;
            redisClient.select(5);
            var csvStream = csv({ headers: true })
                .on("error", function (err) {
                    console.log('trip table file not found ' + par.triptablefilename);
                })
                .on("data", function (data) {
                    vol = data['Vol'];  
                    key = data['I'] + ':' + data['TP'] + ':' + data['Mode'];
                    if (keyPre != key) {
                        if (v != '') {
                            redisClient.set(keyPre, v);
                            //console.log("read trip tabel set key=" + keyPre + ' v=' + v);
                        }
                        newRcd = true;
                        v = '';
                        if (vol != '0' && data['I'] != data['J']) {
                            v = data['J'] + ':' + vol;
                            newRcd = false;
                        }
                    } else {
                        if (vol != '0' && data['I'] != data['J']) {
                            if (newRcd) {
                                v = data['J'] + ':' + vol;
                                newRcd = false;
                            } else {
                                v = v + ',' + data['J'] + ':' + vol;
                            }
                        }
                    }
                    keyPre = key;
                })
                .on("end", function (result) {
                    console.log("read trip tabel total of " + result + " records");
                    callback();
                });
            stream.pipe(csvStream);            
        }],
        function () {
            finishreadfiles = true;
            endTimeStep = par.timesteps;
            model_Loop();                   
        });
}
//Loop iterations
var model_Loop = function () {
    console.log('iter' + iter + ' create spmv tasks');
    //refresh = true;
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
                    for (var j = 0; j < zoneNodes.length; j++) {
                        multi.rpush('task', iter + ',' + i + ',' + zoneNodes[j] + ',' + md);   //iter,timestep,zone,mode
                        //if (iter == 3 && i == 1) {
                        //    console.log('iter' + iter + ' create spmv tasks' + iter + ',' + i + ',' + j + ',' + md);
                        //}
                        spmvNum = spmvNum + 1;
                    }
                }
            });
            multi.exec(function (err, result) {
                console.log('iter' + iter + ' set ' + spmvNum + ' single source shortest path tasks in redis');
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
                //console.log('iter' + iter + ' set redis 8 ' + link);
                cnt = cnt + 1;
            });
            multi.exec(function (err, result) {
                console.log('iter' + iter + ' set ' + linkupdateNum + ' link tasks in redis');
                callback();
            });
        },
        function (callback) {
            //var logChoiceFile = './output' + '/' + par.log.dpnodefilename;
            //fs.truncate(logChoiceFile, 0);  //log only last iteration
            //choiceStream = fs.createWriteStream(logChoiceFile, { 'flags': 'a' });
            //choiceStream.write("iter, orgID, destID, startTS, tStep, dpID, cmnNd, distTl, distTf, timeTl, timeTf, timeFFTl, timeFFTf, TollEL, TollGP, utility, TlShare" + os.EOL);
            callback();
        }],
        function (err, results) {
            redisClient.publish('job', 'sp_mv', function (err, result) {
                console.log('iter' + iter + ' running path building and moving vehicles');
            });
        });
}

redisJob.on("error", function (err) {
    console.log('redis error ' + err);
});


redisJob.on("message", function (channel, message) {
    if (message == 'linkvolredis') {
        console.log('iter' + iter + ' writting link volume to redis');
    }
    else if (message == 'sp_mv_done') {
        redisClient.publish('job', 'linkupdate', function (err, result) {
            console.log('iter' + iter + ' running link attributes update');   
            //bar.update(0);
            //refresh = true;
        }); 
    } else if (message == 'linkupdate_done') {
        console.log('iter' + iter + ' link update done');
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
                                    //    console.log('iter' + iter + ' timestep=' + ts + ' vht=' + v[0] + ' vht_diff=' + v[1]);
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
                                    //console.log('iter' + iter + ' vol diff=' + vol_totdiff + ' vol tot=' + vol_tot);
                                    //gap.push(rmse);
                                    maxGap = Math.max(gp, maxGap);
                                    maxRMSE = Math.max(rmse, maxRMSE);
                                } else {
                                    arrCvgLog.push([iter, ts, 0, 0]);
                                    //gap.push(0);
                                }
                                //if (ts == 71) {
                                    //console.log('iter' + iter + ' timestep=' + ts + ' gap=' + gp + ' vht_square=' + Math.round(vht_square, 0)
                                    //    + ' vht_tot=' + Math.round(vht_tot, 0) + ' arrvht length=' + arrvht.length);
                                //}
                                callback();                                  
                            });                           
                        },
                        function (err) {
                            //start from timestep 1, find the boudary of time steps that meet the threshold
                            console.log('iter' + iter + ' startTimeStep=' + startTimeStep + ' endTimeStep=' + endTimeStep);
                            for (var i = startTimeStep; i <= endTimeStep; i++) {
                                if (arrCvgLog[i - 1][2] >= par.timestepgap) {
                                    startTimeStep = i;
                                    break;
                                }                                
                            }
                            for (var i = endTimeStep; i >= startTimeStep; i--) {                               
                                if (arrCvgLog[i - 1][2] >= par.timestepgap) {
                                    endTimeStep = i;
                                    break;
                                }
                            }
                            //startTimeStep = 1;
                            //endTimeStep = par.timesteps
                            //for (var i = 0; i < arrCvgLog.length; i++) {
                            //    cvgStream.write(arrCvgLog[i][0] + ',' + (arrCvgLog[i][1] + 1) + ',' + arrCvgLog[i][2] + ',' + arrCvgLog[i][3] + os.EOL);
                            //} 
                            console.log('iter' + iter + ' next iter startTimeStep=' + startTimeStep + ' endTimeStep=' + endTimeStep)
                            console.log('iter' + iter + ' RGAP=' + Math.round(maxGap * 10000) / 10000 + ' VHT_RMSE=' + Math.round(maxRMSE * 10000) / 10000);
                            callback();
                         }
                    );                    
                } else {
                    callback();
                }                 
            },
            function (callback) { 
                redisClient.select(9);
                redisClient.set('startTimeStep', startTimeStep);
                redisClient.set('endTimeStep', endTimeStep);
                //check convergence
                if (iter < par.maxiter && maxGap > par.gap) { //continue
                    iter = iter + 1;
                    redisClient.select(9);
                    redisClient.set('iter', iter, function (err, result) {
                        model_Loop();
                    });
                    callback();
                } else {
                    //write csv file 
                    //console.log('iter' + iter + ' start write csv');
                    async.series([
                        function (callback) {
                            redisClient.select(2);
                            redisClient.keys('*', function (err, results) {
                                async.eachSeries(results, function (key, callback) {
                                    var arrKey = key.split(':');
                                    var speed = [];
                                    var vc = [];
                                    var toll = [];
                                    //console.log('iter' + iter + ' key=' + key);
                                    if (arrKey != 'cntNode' && parseInt(arrKey[1]) == iter) { //&& parseInt(arrKey[3]) == iter
                                        var arrKey = key.split(":");
                                        var nd = arrKey[0].split('-');
                                        var link = LinkAttributes.get(arrKey[0]);
                                        //get speed, vc
                                        redisClient.select(3);
                                        redisClient.get(arrKey[0], function (err, result) {
                                            var arrResult = result.split(':');
                                            //console.log(`[${process.pid}]` + ' iter' + iter + ' link=' + linkid + ' result=' + arrResult.length);
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
                                            //console.log('iter' + iter + ' key=' + key + ' result=' + result);
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
                                                volStream.write(NodeNewtoOld.get(parseInt(nd[0])) + ',' + NodeNewtoOld.get(parseInt(nd[1])) + ',' + j + ',' + s + ',' + speed[j] + ',' + toll[j] + ',' + vc[j] + os.EOL);
                                            }
                                            callback();
                                        });
                                    } else {
                                        callback();
                                    }
                                }, function (err) {
                                    console.log('iter' + iter + ' write to vol file');
                                    callback();
                                });
                            });
                        },
                        function (callback) {
                            //upload to bucket
                            bucket.upload(loadedLinkFile, {destination: '/result/' + runListPar.runs[scenIndex] + '/vol.csv' }, function (err) {
                                callback();
                            });                                                                             
                        }],
                        function (err, results) {
                            volStream.end();
                            //cvgStream.end();
                            //pathStream.end();
                            if (scenIndex == runListPar.runs.length - 1) {
                                console.log('iter' + iter + ' end of program');
                                process.exit(0); //End server
                                callback(null, "End of program");
                            } else {
                                iter = 1;
                                scenIndex = scenIndex + 1;
                                scen_Loop();
                            }                            
                        });
                }
                
            }],
            function () {
            });
    }
});