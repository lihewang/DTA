//Starting node
var hashMap = require('hashmap');
var fs = require('fs');
var csv = require('fast-csv');
var redis = require('redis');
var pq = require('priorityqueuejs');
//var http = require('http');
var bodyParser = require('body-parser');
var async = require('async');
var math = require('mathjs');
var Scripto = require('redis-scripto');
var cluster = require('cluster');
//var url = require('url');
var currTime = new Date();
var log4js = require('log4js');

log4js.configure({
    appenders: {
        everything: { type: 'file', filename: 'worker_logs.log', backups: 1 },
        console: { type: 'console' }
    },
    categories: {
        default: { appenders: ['everything', 'console'], level: 'all' }
    }
});
var logger = log4js.getLogger();

logger.info(`[${process.pid}] Program worker.js start`);

var timeHash = new hashMap();     //link time
var distHash = new hashMap();     //link distance
var typeHash = new hashMap();     //link type
var nodeHash_all = new hashMap(); //network topology of all links
var nodeHash = new hashMap();
var tollHash = new hashMap();     //toll on the link
var timeFFHash = new hashMap();   //free flow time on link
var arrLink = [];

//deploy to cluster
//var redisIP = 'redis://redis.default.svc.cluster.local:6379';
//local test
var redisIP = "redis://127.0.0.1:6379";
var appFolder = "./app";
var paraFile = appFolder + "/parameters.json";
var luaScript = appFolder + '/task.lua';

var redisClient = redis.createClient({ url: redisIP }), multi;
var redisJob = redis.createClient({ url: redisIP }), multi;
var jsonParser = bodyParser.json();
var par = JSON.parse(fs.readFileSync(paraFile));
var linkFile = appFolder + '/' + par.linkfilename;

//load redis lua script
var scriptManager = new Scripto(redisClient);
scriptManager.loadFromFile('task', luaScript);

//********csv reader********
var rdcsv = function Readcsv(callback) {
    nodeHash_all.clear();    
    timeHash.clear();
    distHash.clear();
    tollHash.clear();
    timeFFHash.clear();
    //logger.debug(`Node ${process.pid} read links start`);
    arrLink.length = 0;
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
        .on("end", function () {
            callback(null, 'done');
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
        var bNds = nodeHash.get(key);
        bNds.forEach(function (bNd) {
            var tp = typeHash.get(key + '-' + bNd);
            //mode banning
            if (ftypeBan.indexOf(tp) != -1) {
                bNds.remove(bNd);
                logger.debug(`[${process.pid }]` + " ban link remove " + key + "-" + bNd);
            }
            //decision point
            if (pType != 'zone' && key == spZone) {
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
    callback(null, "ban links end");
}
//********find time dependent shortest path and write results to redis********
var sp = function ShortestPath(zone, zonenum, tp, mode, pathType, iter, callback) {
    //logger.debug(`[${process.pid}]` + ' SP for zone ' + zone + ', tp ' + tp + ', mode ' + mode + ', pathType ' + pathType);
    //single node shortest path
    async.series([
        function (callback) {
            //prepare network - remove links going out of other zones
            for (var i = 1; i <= zonenum; i++) {
                if (i != zone) {
                    nodeHash.remove(i);
                }
            }
            callback();
        },
        function (callback) {
            //read in congested time
            if (iter >= 2) {
                timeHash.clear();
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
                });
            } else {
                callback();
            }
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
                //logger.debug('settled node add ' + currNode);
                if (nodeHash.has(currNode)) {            //currNode has downsteam nodes
                    var dnNodes = nodeHash.get(currNode.toString());  //get new frontier nodes
                    //Update time on new nodes
                    dnNodes.forEach(function (dnNode) {
                        //logger.debug(`[processor ${process.pid}]` + ' node ' + currNode + ' <-- ' + 'node ' + dnNode);
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

                    }); //end forEach
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
                    //console.log("node " + pNode + " has parent nodes");
                    do {
                        pNode = parentNodes.get(pNode.toString());
                        path = pNode.toString() + ',' + path;
                    }
                    while (pNode != zone);
                } else {
                    path = null
                }
                //logger.debug(`[${process.pid}]` + " SP from " + zone + " to " + i + " : " + path);
                if (path != null) {
                    multi.set(tp + ":" + zonePair + ":" + mode + ":" + pathType, path);       //write to redis db, example 3:7-2:SOV:zone
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
            //console.log('loop start ' + j + ', iter=' + iter + ',zi=' + zi + ',zj=' + zj + ',vol=' + vol);
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

                                /*console.log('probility calculation: tollconst=' + par.choicemodel.tollconst[tp - 1] + ',scalesdlen=' + par.choicemodel.scalestdlen
                                    + ',scalealpha=' + par.choicemodel.scalealpha + ',timecoeff=' + par.choicemodel.timecoeff
                                    + ',tollcoeff=' + par.choicemodel.tollcoeff + ',reliacoeffratio=' + par.choicemodel.reliacoeffratio
                                    + ',reliacoefftime=' + par.choicemodel.reliacoefftime + ',reliacoeffdist=' + par.choicemodel.reliacoeffdist);
                                console.log('distTl=' + distTl + ',distTf=' + distTf + ',timeTl=' + timeTl + ',timeTf=' + timeTf
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
                               /* multi.select(7);  //to-do db
                                multi.rpush('to-do', arrPath[j] + '-' + zj + '-' + tpNew + '-' + mode + '-' + splitVol[i] + '-' + ptp);
                                console.log('decision pnt added to-do ' + arrPath[j] + '-' + zj + '-' + tpNew + '-' + mode + '-' + splitVol[i] + '-' + ptp);
                                multi.exec(function (err, results) {
                                    i = i + 1;
                                    console.log('decision pnt added to-do ' + results);
                                    callback();
                                });*/
                                request.get(workerIP + '/?task=sp&iter=' + iter + '&item=' + arrPath[j] + '-' + zj + '-' + tpNew + '-' + mode + '-' + splitVol[i] + '-' + ptp,
                                    function (error, response, body) {
                                        console.log(body);
                                        callback();
                                    });
                            },
                            function (err) {
                                callback();
                            });
                    }],
                    function (err, results) {
                        console.log('loop end ' + j);
                        breakloop = true;
                        callback(null, results);
                    });
            } else {
                //not a decision point
                async.series([
                    function (callback) {
                        tpNew = parseInt(tp) + math.floor(totTime / 15);
                        var linkID = arrPath[j] + '-' + arrPath[j + 1] + ':' + tpNew;
                        totTime = totTime + parseFloat(timeHash.get(linkID.toString()));
                        console.log(linkID + ' time=' + parseFloat(timeHash.get(linkID.toString())) + ',iter=' + iter);

                        keyValue = arrPath[j] + '-' + arrPath[j + 1] + ':' + tpNew + ':' + mode + ':' + iter;
                        console.log(keyValue + ',tp=' + tp + ',totTime=' + totTime + ',tpNew=' + tpNew);
                        callback();
                    },
                    function (callback) {
                        redisClient.select(2);
                        async.series([
                            function (callback) {
                                scriptManager.run('task', [keyValue, vol], [], function (err, result) {
                                    //console.log('lua err=' + err + "," + result);
                                    callback();
                                });
                            }],
                            function (err, results) {
                                callback(null, results);
                            })
                    }],
                    function (err, results) {
                        console.log('loop end ' + j);
                        j = j + 1;
                        callback();
                    })
            }
        },
        function (err) {
            callback(null, keyValue);
        });
}

//log redis error
redisClient.on("error", function (err) {
    logger.info(`[${process.pid}] redis error ` + err);
});

//********subscriber********
if (cluster.isMaster) {
    //create master
    fs.truncate('worker_logs.log', 0, function () {
        logger.info(`[${process.pid}] clear worker log file`)
    });
    logger.info(`[${process.pid}] master node [${process.pid}] is running`);
    for (var i = 0; i < par.numprocesses; i++) {
        var worker = cluster.fork();
    }
    cluster.on('death', function (worker) {
        logger.info(`Worker node [${process.pid}] died`);
    });
} else { 
    //create worker
    logger.info(`[${process.pid}] worker node [${process.pid}] is running`);
    //read network
    rdcsv(function (err, result) {
        logger.info(`[${process.pid}] read network ` + result);
    });
    redisJob.subscribe("job");

    //process jobs
    redisJob.on("message", function (channel, message) {
        logger.info(`[${process.pid}] Get job message ` + message);    //job message is sp or mv
        //sp
        if (message == 'sp') {
            var iter = 0;
            var spZone = 0;
            var timeStep = 0;
            var mode = '';
            var pathType = '';  //ct,tl,tf
            var hasTask = true;
            var cnt = 0;
            async.during(           //loop until jobs are done
                //test function
                function (callback) {
                    spZone = 0;
                    redisClient.select(6);
                    redisClient.LPOP('task', function (err, result) {
                        //logger.debug(`[${process.pid}] get task ` + result);
                        if (result != null) {
                            var tsk = result.split('-'); //sp-tp-zone-SOV-ct/tl/tf
                            iter = tsk[1];
                            timeStep = tsk[2];
                            spZone = tsk[3];
                            mode = tsk[4];
                            pathType = tsk[5];   //ct,tl,tf
                        } 
                        return callback(null, result != null);
                    });
                },
                //create shortest path
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
                                //logger.info(`[${process.pid}] ` + result);
                                callback(null, result);
                            })
                        }],
                        function () {
                            cnt = cnt + 1;
                            callback();
                        });
                },
                //whilst callback
                function (err, results) {
                    logger.info(`[${process.pid}] sp processed total of ` + cnt);
                });
        }
        //move vehicles
        else if (bdy.task == 'mv') {
            var rs = bdy.item.split('-');
            var zi = rs[0];
            var zj = rs[1];
            var tp = rs[2];
            var mode = rs[3];
            var vol = rs[4];
            var pathType = rs[5];;
            console.log('***begin moving vehicles***');
            async.series([
                function (callback) {
                    //banning
                    ban(mode, pathType, zi, function (err, result) {
                        callback();
                    });
                },
                function (callback) {
                    redisClient.select(1); //sp db
                    if (par.dcpnt.indexOf(parseInt(zi)) != -1) {
                        //decision point
                        //get path
                        redisClient.get(tp + ":" + zi + "-" + zj + ":" + mode + ":" + pathType, function (err, result) {
                            //move vehicle
                            logger.debug('mv - decision point ' + tp + ":" + zi + ":" + zj + ":" + mode + ":" + pathType + " " + result);
                            mv(tp, zi, zj, pathType, mode, vol, result, bdy.iter, function (err, result) {
                                console.log('run mv ' + result);
                                callback();
                            });
                        });
                    } else {
                        redisClient.get(tp + ":" + zi + "-" + zj + ":" + mode + ":" + pathType, function (err, result) {
                            //move vehicle
                            logger.debug('mv - zone node ' + tp + ":" + zi + ":" + zj + ":" + mode + ":" + pathType + " " + result);
                            mv(tp, zi, zj, pathType, mode, vol, result, bdy.iter, function (err, result) {
                                logger.debug('run mv ' + result);
                                callback();
                            });
                        });
                    }
                }],
                function (err, results) {
                    return callback(null, results);
                });
        } else {
            logger.debug(`[${process.pid}] job not found`);
        }
    });
}