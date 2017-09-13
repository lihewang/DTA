//Starting node
var hashMap = require('hashmap');
var fs = require('fs');
var csv = require('fast-csv');
var redis = require('redis');
var pq = require('priorityqueuejs');
var async = require('async');
var math = require('mathjs');
var Scripto = require('redis-scripto');
var cluster = require('cluster');
var currTime = new Date();
var log4js = require('log4js');

log4js.configure({
    appenders: {
        everything: { type: 'file', filename: 'worker.log', backups: 1 },
        console: { type: 'console' }
    },
    categories: {
        default: { appenders: ['everything', 'console'], level: 'all' }
    }
});
var logger = log4js.getLogger();

var timeHash = new hashMap();     //link time
var distHash = new hashMap();     //link distance
var typeHash = new hashMap();     //link type
var nodeHash_all = new hashMap(); //network topology of all links
var nodeHash = new hashMap();
var tollHash = new hashMap();     //toll on the link
var timeFFHash = new hashMap();   //free flow time on link
var alphaHash = new hashMap();
var betaHash = new hashMap();
var capHash = new hashMap();
var pathHash = new hashMap();       //paths
var volHash = new hashMap();        //volume
var ttHash = new hashMap();         //trip table
var arrLink = [];
var spZones = [];                   //sp zones processed by the worker
var arrTT = [];
var iter = 0;

//deploy to cluster
//var redisIP = 'redis://redis.default.svc.cluster.local:6379';
//local test
var redisIP = "redis://127.0.0.1:6379";
var appFolder = "./app";
var paraFile = appFolder + "/parameters.json";
var luaScript_msa = appFolder + '/msa.lua';

var redisClient = redis.createClient({ url: redisIP }), multi;
var redisJob = redis.createClient({ url: redisIP }), multi;
var par = JSON.parse(fs.readFileSync(paraFile));
var linkFile = appFolder + '/' + par.linkfilename;

//load redis lua script
var scriptManager = new Scripto(redisClient);
scriptManager.loadFromFile('msa', luaScript_msa);

//********csv reader********
var rdcsv = function Readcsv(callback) {
    nodeHash_all.clear();    
    timeHash.clear();
    distHash.clear();
    tollHash.clear();
    timeFFHash.clear();
    pathHash.clear();
    //logger.debug(`Node ${process.pid} read links start`);
    arrLink.length = 0;
    spZones.length = 0;
    var stream = fs.createReadStream(linkFile);
    var csvStream = csv({ headers: true })
        .on("data", function (data) {
            var anode = data['A'];
            var bnode = data['B'];
            anode = anode.trim();
            bnode = bnode.trim();
            //create time, toll, dist, and free flow time hash table  
            var data_id = anode + '-' + bnode;
            for (var i = 1; i <= par.timesteps; i++) {
                timeHash.set(data_id + ':' + i, data['TIME']);
                tollHash.set(data_id + ':' + i, parseFloat(data['TR' + i]) * parseFloat(data['Dist']));
                arrLink.push(data_id + ':' + i);
            }
            distHash.set(data_id, data['Dist']);
            typeHash.set(data_id, data['Ftype']);
            timeFFHash.set(data_id, data['Dist'] / data['Spd'] * 60);
            alphaHash.set(data_id, data['Alpha']);
            betaHash.set(data_id, data['Beta']);
            capHash.set(data_id, data['Cap']);

            //network topology
            var banLink = false;
            //console.log("read link " + data_id);
                if (nodeHash_all.has(anode)) {
                    var value = [];
                    value = nodeHash_all.get(anode.toString());    //get existing value
                    value.push(bnode);                                       
                    nodeHash_all.set(anode, value);
                    //logger.debug('a node already added. ' + anode + ",[" + nodeHash.get(anode) + "], pathType:" + pType);
                } else {                       
                    nodeHash_all.set(anode, [bnode]);
                    //logger.debug('new a node. ' + anode + ",[" + nodeHash.get(anode) + "], pathType:" + pType);
                }
        })
        .on("end", function (result) {
            callback(null, result);
        });
    stream.pipe(csvStream);
}
//********ban links********
var ban = function banLinks(mode, pType, spZone, callback) {
    nodeHash.copy(nodeHash_all);
    if (mode == "HOV") {
        var ftypeBan = par.pathban.HOV;
    } else if (mode == "TRK") {
        var ftypeBan = par.pathban.TRK;
    } else {
        var ftypeBan = [];
    }
    //loop each link
    nodeHash.forEach(function (value, key) {
        //ban centroids
        if (key <= par.zonenum && key != spZone){
            nodeHash.set(key, []);
        }
        var bNds = nodeHash.get(key);
        bNds.forEach(function (bNd) {
            var tp = typeHash.get(key + '-' + bNd);
            //mode banning
            if (ftypeBan.indexOf(tp) != -1) {
                bNds.remove(bNd);
                logger.debug(`[${process.pid }]` + " ban link remove " + key + "-" + bNd);
            //decision point
            if (pType != 'zone' && key == spZone) {
            }
                if (pType == 'tf' && (tp == par.ftypeexonrp || tp == par.ftypeex)) {
                    bNds.remove(bNd);
                    logger.debug(`[${process.pid}]` + "decision point remove " + key + "-" + bNd);
                }
                if (pType == 'tl' && (tp == par.ftypegp || tp == par.ftypeexoffrp)) {
                    bNds.remove(bNd);
                    logger.debug(`[${process.pid}]` + "decision point remove " + key + "-" + bNd);
                }
            }
        });
    });
    callback(null, "ban links end for zone " + spZone);
}
//********find time dependent shortest path and write results to redis********
var sp = function ShortestPath(zone, zonenum, tp, mode, pathType, iter, callback) {
    //logger.debug(`[${process.pid}]` + ' SP for zone ' + zone + ', tp ' + tp + ', mode ' + mode + ', pathType ' + pathType);
    //single node shortest path
    spZones.push(zone + ':' + tp + ':' + mode + ':' + pathType);
    async.series([
        function (callback) {
            //prepare network - remove links going out of other zones
            for (var i = 1; i <= zonenum; i++) {
                if (i != zone) {
                    nodeHash.remove(i);
                }
            }
            callback();
        }],
        //apply turn penalty

        function (err, result) {
            //Priority queue for frontier nodes
            var pqNodes = new pq(function (a, b) {
                return b.t - a.t;
            });
            var cnt = 1;    //count visited zones
            var visitedNodes = new hashMap();       //track visited nodes, {node,time}
            var settledNodes = new hashMap();
            var parentNodes = new hashMap();        //track parent nodes, {node, parent node}    
            var currNode = zone;
            visitedNodes.set(zone, 0);            //root node
            pqNodes.enq({ t: 0, nd: currNode });
            do {
                //Explore frontier node
                var tpNode = pqNodes.deq();
                currNode = tpNode.nd;
                settledNodes.set(currNode, 1);
                //if (zone == '93') {logger.debug(`[${process.pid}]` + ' settled node add ' + currNode);}
                
                if (nodeHash.has(currNode)) {            //currNode has downsteam nodes
                    var dnNodes = nodeHash.get(currNode.toString());  //get new frontier nodes
                    //Update time on new nodes
                    if (dnNodes != []){
                        dnNodes.forEach(function (dnNode) {
                            //logger.debug(`[${process.pid}]` + ' node ' + currNode + ' <-- ' + 'node ' + dnNode);
                            if (!settledNodes.has(dnNode)) {    //exclude settled nodes
                                //get time of dnNode
                                var timeCurrNode = parseFloat(visitedNodes.get(currNode.toString()));
                                var timePeriod = math.min(par.timesteps, math.floor(timeCurrNode / 15) + parseInt(tp));
                                var tempTime = timeCurrNode + parseFloat(timeHash.get(currNode + '-' + dnNode + ':' + timePeriod));

                                if (visitedNodes.has(dnNode)) {
                                    //dnNode has been checked before
                                    if (tempTime < visitedNodes.get(dnNode.toString())) {    //update time when the path is shorter                   
                                        pqNodes.enq({ t: tempTime, nd: dnNode });
                                        parentNodes.set(dnNode, currNode);
                                        //console.log("set node " + dnNode + "'s parent as " + parentNodes.get(dnNode.toString()));
                                        visitedNodes.set(dnNode, tempTime);
                                        //logger.debug('visit again ' + dnNode + ', ' + tempTime);
                                    }
                                } else {
                                    //first time checked node
                                    pqNodes.enq({ t: tempTime, nd: dnNode });
                                    parentNodes.set(dnNode, currNode);
                                    //logger.debug("set node " + dnNode + "'s parent as " + parentNodes.get(dnNode.toString()));
                                    visitedNodes.set(dnNode, tempTime);
                                    //logger.debug('visit first time ' + dnNode + ', ' + tempTime);
                                }
                            }  //end if
                        });    //end forEach
                    }
                }  //end if
                //logger.debug('pqNode size = ' + pqNodes.size());
            }
            while (pqNodes.size() > 0);
            //Construct path string and write to redis db
            redisClient.select(1);  //path db
            multi = redisClient.multi();
            //zones
            for (var i = 1; i <= zonenum; i++) {
                var zonePair = zone + '-' + i;
                var path = i.toString();
                var pNode = i;
                if (parentNodes.has(pNode.toString())) {
                    //logger.debug(`[${process.pid}]` + " node " + pNode + " has parent nodes");
                    do {
                        pNode = parentNodes.get(pNode.toString());
                        path = pNode.toString() + ',' + path;
                    }
                    while (pNode != zone);
                } else {
                    path = null
                }
                //if (zone == '93') { logger.debug(`[${process.pid}]` + tp + ":" + zonePair + ":" + mode + ":" + pathType + " " + path);}
                if (path != null) {
                    //multi.set(tp + ":" + zonePair + ":" + mode + ":" + pathType, path);       //write to redis db, example 3:7-2:SOV:zone
                    pathHash.set(tp + ":" + zonePair + ":" + mode + ":" + pathType, path);
                    //logger.debug(`[${process.pid}]` + ' sp ' + tp + ":" + zonePair + ":" + mode + ":" + pathType + ' ' + path);
                    //decision point path skims
                    if (par.dcpnt.indexOf(parseInt(zone)) != -1) {
                        var dpPath = path.split(',');
                        var skimDist = 0;
                        var skimToll = 0;
                        var skimFFtime = 0;
                        for (var j = 0; j <= dpPath.length - 2; j++) {
                            skimDist = skimDist + parseFloat(distHash.get(dpPath[j] + '-' + dpPath[j + 1]));
                            var tempTl = parseFloat(tollHash.get(dpPath[j] + '-' + dpPath[j + 1] + ':' + tp));
                            if (typeof tempTl == 'undefined' || isNaN(tempTl)) {
                                tempTl = 0;
                            }
                            skimToll = skimToll + tempTl;
                            skimFFtime = skimFFtime + parseFloat(timeFFHash.get(dpPath[j] + '-' + dpPath[j + 1]));
                        }
                        multi.set(tp + ":" + zonePair + ":" + mode + ":" + pathType + ":time", visitedNodes.get(i.toString()));
                        multi.set(tp + ":" + zonePair + ":" + mode + ":" + pathType + ":dist", skimDist);
                        multi.set(tp + ":" + zonePair + ":" + mode + ":" + pathType + ":toll", skimToll);
                        multi.set(tp + ":" + zonePair + ":" + mode + ":" + pathType + ":fftime", skimFFtime);
                    }
                    //logger.debug(`[${process.pid}]` + ' Iter=' + iter + ' ' + tp + ":" + zonePair + ":" + pathType + ', ' + path + ' Toll=' + skimToll);
                }
            }
            multi.exec(function (err, results) {
                callback(null, 'SP for zone ' + zone + ', tp ' + tp + ', mode ' + mode + ', pathType ' + pathType);
            });
        });
}

//********move vehicle and write results to redis********
var mv = function MoveVehicle(tp, zi, zj, pthTp, mode, vol, path, iter, callback) {
    if (path == null) {
        callback(null, zi + '-' + zj + ':' + tp + ':' + mode);
    } else {
        var arrPath = path.split(',');
        var totTime = 0;
        var tpNew = tp;
        var keyValue = '';
        multi = redisClient.multi();
        var j = 0;
        var breakloop = false;
        //loop links in the path
        async.during(
            //test function 
            function (cb) {
                //logger.debug(`[${process.pid}]` + 'loop start ' + j + ', iter=' + iter + ',zi=' + zi + ',zj=' + zj + ',vol=' + vol);
                return cb(null, j <= arrPath.length - 2 && !breakloop);
            },
            function (callback) {
                if (par.dcpnt.indexOf(parseInt(arrPath[j])) != -1 && j > 0 && mode == "SOV") {
                    //decision point (not the start node in path)
                    var zonePair = arrPath[j] + "-" + zj;
                    async.waterfall([
                        //choice model
                        function (callback) {
                            var timeTl = 0;
                            var timeTf = 0;
                            var distTl = 0;
                            var distTf = 0;
                            var Toll = 0;
                            var timeFFTl = 0;
                            var timeFFTf = 0;
                            var probility = 0;
                            multi.select(1);
                            multi.get(tp + ":" + zonePair + ":" + mode + ":tl:time", function (err, result) {
                                timeTl = result;
                            });
                            multi.get(tp + ":" + zonePair + ":" + mode + ":tf:time", function (err, result) {
                                timeTf = result;
                            });
                            multi.get(tp + ":" + zonePair + ":" + mode + ":tl:dist", function (err, result) {
                                distTl = result;
                            });
                            multi.get(tp + ":" + zonePair + ":" + mode + ":tf:dist", function (err, result) {
                                distTf = result;
                            });
                            multi.get(tp + ":" + zonePair + ":" + mode + ":tl:toll", function (err, result) {
                                Toll = result;
                            });
                            multi.get(tp + ":" + zonePair + ":" + mode + ":tl:fftime", function (err, result) {
                                timeFFTl = result;
                            });
                            multi.get(tp + ":" + zonePair + ":" + mode + ":tf:fftime", function (err, result) {
                                timeFFTf = result;
                            });
                            multi.exec(function (err, results) {
                                if (distTl > distTf * parseFloat(par.distmaxfactor)) {
                                    probility = 0;
                                } else if (distTf > distTl * parseFloat(par.distmaxfactor)) {
                                    probility = 1;
                                } else {
                                    var utility = -1 * par.choicemodel.tollconst[tp - 1] - math.pow((par.choicemodel.scalestdlen / distTl)
                                        , par.choicemodel.scalealpha) * (par.choicemodel.timecoeff * (timeTl - timeTf))
                                        + par.choicemodel.tollcoeff * Toll + par.choicemodel.timecoeff * par.choicemodel.reliacoeffratio
                                        * par.choicemodel.reliacoefftime * ((timeFFTf - timeTf) * math.pow(distTf, (-1 * par.choicemodel.reliacoeffdist))
                                            - (timeFFTl - timeTl) * math.pow(distTl, (-1 * par.choicemodel.reliacoeffdist)));

                                    probility = 1 / (1 + math.exp(utility));

                                    /*logger.debug('probility calculation: tollconst=' + par.choicemodel.tollconst[tp - 1] + ',scalesdlen=' + par.choicemodel.scalestdlen
                                        + ',scalealpha=' + par.choicemodel.scalealpha + ',timecoeff=' + par.choicemodel.timecoeff
                                        + ',tollcoeff=' + par.choicemodel.tollcoeff + ',reliacoeffratio=' + par.choicemodel.reliacoeffratio
                                        + ',reliacoefftime=' + par.choicemodel.reliacoefftime + ',reliacoeffdist=' + par.choicemodel.reliacoeffdist);
                                    logger.debug('distTl=' + distTl + ',distTf=' + distTf + ',timeTl=' + timeTl + ',timeTf=' + timeTf
                                        + ',timeFFTl=' + timeFFTl + ',timeFFTf=' + timeFFTf + ',Toll=' + Toll + ',utility=' + utility + ',probility=' + probility);*/
                                }
                                callback(null, probility);
                            })
                        },
                        function (probility, callback) {
                            //set to to-do list
                            var ptype = [];
                            var splitVol = [];
                            if (probility == 0) {
                                ptype[0] = 'tf';
                                splitVol[0] = vol;
                            } else if (probility == 1) {
                                ptype[0] = 'tl';
                                splitVol[0] = vol;
                            } else {
                                ptype[0] = 'tf';
                                splitVol[0] = vol * (1 - probility);
                                ptype[1] = 'tl';
                                splitVol[1] = vol * probility;
                            }
                            var i = 0;
                            async.eachSeries(ptype,
                                function (ptp, callback) {
                                    multi.select(7);  //to-do db
                                    multi.rpush('task', 'mv-' + iter + '-' + arrPath[j] + '-' + zj + '-' + tpNew + '-' + mode + '-' + splitVol[i] + '-' + ptp);
                                    logger.debug(`[${process.pid}]` + ' decision pnt added task ' + 'mv-' + iter + '-' + arrPath[j] + '-' + zj + '-' + tpNew + '-' + mode + '-' + splitVol[i] + '-' + ptp);
                                    multi.exec(function (err, results) {
                                        i = i + 1;
                                        logger.debug(`[${process.pid}]` + 'decision pnt added to-do ' + results);
                                        callback();
                                    });
                                },
                                function (err) {
                                    callback();
                                });
                        }],
                        function (err, results) {
                            logger.debug(`[${process.pid}]` + ' decision point loop end ' + j);
                            breakloop = true;
                            callback(null, results);
                        });
                } else {
                    //not a decision point
                    async.series([
                        function (callback) {
                            tpNew = parseInt(tp) + math.floor(totTime / 15);
                            if (tpNew > parseInt(par.timesteps)) {
                                tpNew = parseInt(par.timesteps);
                            }
                            var linkID = arrPath[j] + '-' + arrPath[j + 1] + ':' + tpNew;
                            totTime = totTime + parseFloat(timeHash.get(linkID));
                            //logger.debug(`[${process.pid}] ` + linkID + ' time=' + parseFloat(timeHash.get(linkID.toString())) + ',iter=' + iter);
                            keyValue = arrPath[j] + '-' + arrPath[j + 1] + ':' + tpNew + ':' + mode + ':' + iter;
                            //logger.debug(`[${process.pid}] ` + keyValue + ',tp=' + tp + ',totTime=' + totTime + ',tpNew=' + tpNew);
                            callback();
                        },
                        function (callback) {
                            //save to array
                            if (volHash.has(keyValue)) {
                                volHash.set(keyValue, volHash.get(keyValue) + vol);
                            } else {
                                volHash.set(keyValue, vol);
                            }
                            callback();
                        }],
                        function (err, results) {
                            //logger.debug(`[${process.pid}]` + ' non decision point loop end ' + j);
                            j = j + 1;
                            callback();
                        });
                }
            },
            function (err) {
                callback(null, zi + '-' + zj + ':' + tp + ':' + mode);
            });
    }
}

//log redis error
redisClient.on("error", function (err) {
    logger.info(`[${process.pid}] redis error ` + err);
});

//********subscriber********
if (cluster.isMaster) {
    //create master
    process.stdout.write('\033c');
    //clear redis db
    redisClient.FLUSHALL;
    fs.truncate('worker.log', 0, function () {
        logger.info(`[${process.pid}] clear worker log file`)
    });
    logger.info(`[${process.pid}] cluster master node [${process.pid}] is running`);
    for (var i = 0; i < par.numprocesses; i++) {
        var worker = cluster.fork();
    }
    cluster.on('death', function (worker) {
        logger.info(`Worker node [${process.pid}] died`);
    });
} else { 
    //create worker
    logger.info(`[${process.pid}] cluster worker node [${process.pid}] is running`);
    //read network
    rdcsv(function (err, result) {
        logger.info(`[${process.pid}] read network ` + result + ' links');
    });
    
    redisJob.subscribe("job");

    //process jobs
    redisJob.on("message", function (channel, message) {
        //sp and mv
        if (message == 'sp_mv') {
            spZones = [];
            var spZone = 0;
            var timeStep = 0;
            var mode = '';
            var pathType = '';  //ct,tl,tf
            var hasTask = true;
            var cnt = 0;
            //update time
            if (iter >= 2) {
                timeHash.clear();
                redisClient.select(3);
                redisClient.mget(arrLink, function (err, result) {
                    for (var i = 0; i < arrLink.length; i++) {
                        timeHash.set(arrLink[i], result[i]);
                    }
                });
                /*
                multi = redisClient.multi();
                multi.select(3);
                arrLink.forEach(function (link) {
                    multi.get(link, function (err, result) {
                        timeHash.set(link, result);
                    });
                });
                multi.exec(function () {
                    //console.log('arrLink size=' + arrLink.length + ' zone=' + zone + ' tp=' + tp + 
                    //' 7-4cgTime=' + timeHash.get('7-4:1') + ' 7-5cgTime=' + timeHash.get('7-5:1'));   
                    callback();
                });*/
            } 
            var tot = 0;
            async.during(           //loop until jobs are done
                //test function
                function (callback) {
                    spZone = 0;
                    redisClient.select(6);
                    redisClient.lpop('task', function (err, result) {
                        //logger.debug(`[${process.pid}]` + ' get sp task err=' + err + ", result=" + result);
                        if (result == null) {
                            redisClient.INCR('cnt', function (err, result) {
                                if (result == par.numprocesses) {
                                    logger.info(`[${process.pid}]` + ' iter' + iter + ' publish sp_done');
                                    redisClient.publish('job', 'linkvolredis');
                                    redisClient.publish('job_status', 'linkvolredis');
                                }
                            });
                        } else {
                            var tsk = result.split('-'); //sp-iter-tp-zone-SOV-ct/tl/tf
                            iter = tsk[1];
                            timeStep = tsk[2];
                            spZone = tsk[3];
                            mode = tsk[4];
                            pathType = tsk[5];   //ct,tl,tf                           
                        }
                        redisClient.publish('job_status', 'bar_tick:6');
                        return callback(null, result != null);
                    });
                },
                //create shortest path and move vehicles
                function (callback) {
                    async.series([
                        function (callback) {
                            //banning
                            ban(mode, pathType, spZone, function (err, result) {
                                //logger.info(`[${process.pid}] ` + result);
                                callback();
                            });
                        },
                        function (callback) {
                            sp(spZone, par.zonenum, timeStep, mode, pathType, iter, function (err, result) {                               
                                //tot = 0; 
                                var mvTasks = [];
                                redisClient.select(5);
                                redisClient.get(spZone + ':' + timeStep + ':' + mode + ':' + pathType, function (err, result) {
                                    if (result != null) {
                                        mvTasks = result.split(',');
                                        mvTasks.forEach(function (item) {
                                            var tsk = item.split(':');   //I-J-tp-mode-vol-ct
                                            var zj = tsk[0];
                                            var vol = tsk[1];
                                            //vehicle trips from this zone
                                            var path = pathHash.get(timeStep + ":" + spZone + "-" + zj + ":" + mode + ":" + pathType);
                                            mv(timeStep, spZone, zj, pathType, mode, parseFloat(vol), path, iter, function (err, result) {
                                                //tot = tot + 1;
                                                //logger.info(`[${process.pid}] ` + ' zone ' + spZone + ' moved ' + tot);
                                            });
                                        });                                       
                                    } 
                                    callback(null, result);
                                });
                                //mvTasks = ttHash.get(spZone + ':' + timeStep + ':' + mode + ':' + pathType);                              
                            })
                        }],
                        function () {
                            pathHash.clear();
                            cnt = cnt + 1;
                            //logger.info(`[${process.pid}] ` + ' spZone ' + spZone + ' tp ' + timeStep + ' mode ' + mode + ' total moved pair ' + tot);
                            callback();
                        });
                },
                //whilst callback
                function (err, results) {
                    logger.info(`[${process.pid}]` + ' iter' + iter + ' sp processed total of ' + cnt);
                });
        }
        //write vol to redis
        else if (message == 'linkvolredis') {
            redisClient.select(2);
            var cnt = 0;
            var tot = volHash.count();
            if (tot == 0) {
                redisClient.INCR('cntNode', function (err, result) {
                    logger.info(`[${process.pid}] ` + ' iter' + iter + ' node ' + result + ' no link vol written to redis');
                    if (parseInt(result) == par.numprocesses) {
                        redisClient.set('cntNode', 0);
                        logger.info(`[${process.pid}]` + ' iter' + iter + ' publish sp_mv_done');
                        redisClient.publish('job_status', 'sp_mv_done');
                    }
                });
            }
            volHash.forEach(function (value, key) {
                cnt = cnt + 1;
                if (parseInt(key.split(':')[3]) == iter) { 
                    redisClient.INCRBYFLOAT(key, math.round(value, 3), function (err, result) {
                        if (cnt == tot) {
                            redisClient.INCR('cntNode', function (err, result) {
                                logger.info(`[${process.pid}] ` + ' iter' + iter + ' node ' + result + ' write link vol to redis');
                                if (parseInt(result) == par.numprocesses) {
                                    redisClient.set('cntNode', 0);
                                    logger.info(`[${process.pid}]` + ' iter' + iter + ' publish sp_mv_done');
                                    redisClient.publish('job_status', 'sp_mv_done');
                                }
                            });
                        }
                    });
                }
            });   
        }
        //update link volume and time
        else if (message = "linkupdate") {
            iter = 0;
            var linktp = '';
            var cnt = 0;
            async.during(
                //test function
                function (callback) {
                    redisClient.select(8);
                    redisClient.lpop('task', function (err, result) {
                        //logger.debug(`[${process.pid}] link update get task ` + result);
                        if (result == null) {
                            redisClient.INCR('cnt', function (err, result) {
                                if (result == par.numprocesses) {
                                    logger.info(`[${process.pid}]` + ' iter' + iter + ' publish linkupdate_done');
                                    redisClient.publish('job_status', 'linkupdate_done');
                                }
                            });
                        } else {
                            linktp = result.split(':')[0] + ':' + result.split(':')[1];
                            iter = result.split(':')[2]; 
                        }
                        return callback(null, result != null);
                    });
                    redisClient.publish('job_status', 'bar_tick:8');
                },
                function (callback) {
                    var vol = 0;
                    //loop modes
                    async.series([
                        //MSA Volume
                        function (callback) {
                            async.every(par.modes, function (md, callback) {         
                                    var lastIter = iter - 1;
                                    var key1 = linktp + ":" + md + ":" + iter;
                                    var key2 = linktp + ":" + md + ":" + lastIter;
                                    //logger.debug(`[${process.pid}] ` + key1 + ", " + key2);
                                    scriptManager.run('msa', [key1, key2, iter], [], function (err, result) { //return v_current, v_previous, v_msa
                                        if (err) {
                                            logger.debug('mas lua err=' + err + ", result=" + result);
                                        }
                                        vol = vol + parseFloat(result.split(',')[2]);
                                        if (vol > 0) {
                                            logger.debug(`[${process.pid}] ` + ' iter=' + iter + ' link ' + key1 + ' MSA result ' + result);
                                        }
                                        callback(null, true);
                                    });   
                                },
                                function (err, result) {
                                    callback();
                                });
                        },
                        //moving average volume

                        //congested time
                        function (callback) {
                            var linkID = linktp.split(':')[0];
                            var cgTime = timeFFHash.get(linkID) * (1 + alphaHash.get(linkID) * math.pow(vol * 4 / capHash.get(linkID), betaHash.get(linkID)));
                            var vht = vol * cgTime / 60;
                            if (vol > 0) {
                                logger.debug(`[${process.pid}] ` + 'iter=' + iter + ', link=' + linkID + ', tp=' + linktp.split(':')[1] + ', VHT=' + math.round(vht, 2) +
                                    ', vol=' + parseFloat(vol) + ', cgtime=' + math.round(cgTime, 3));
                            }
                            //set congested time to redis
                            multi = redisClient.multi();
                            multi.select(3);
                            multi.set(linktp, cgTime);
                            multi.exec(function (err, result) {
                            });
                            //VHT
                            redisClient.select(4);
                            redisClient.get(linktp, function (err, result) {
                                if (result == null) {   //current vht, previous vht, vht diff
                                    redisClient.set(linktp, vht + ',0,0', function (err, result) {
                                        //logger.debug(`[${process.pid}]` + 'iter=' + iter + ' err ' + err + ' result ' + result);
                                        callback();  
                                    });
                                    
                                } else {
                                    var v = result;
                                    var vht_pre = parseFloat(v.split(',')[0]);
                                    redisClient.set(linktp, vht + ',' + vht_pre + ',' + (vht - vht_pre), function (err, result) {
                                        //logger.debug(`[${process.pid}]` + 'iter=' + iter + ' linktp ' + linktp + ' vht ' + vht + ',' +
                                        //    ' previous ' + v + ' ' + (vht - parseFloat(v.split(',')[0])));
                                        callback(); 
                                    });      
                                }       
                            });
                             
                        }],
                        function (err, result) {
                            cnt = cnt + 1;
                            callback();
                        });
                },
                //whilst callback
                function (err) {
                    logger.info(`[${process.pid}]` + ' iter' + iter + ' link update processed total of ' + cnt);
                });
        }
    });
}