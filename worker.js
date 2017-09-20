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
var nodeNet = [];
var tollHash = new hashMap();     //toll on the link
var timeFFHash = new hashMap();   //free flow time on link
var alphaHash = new hashMap();
var betaHash = new hashMap();
var capHash = new hashMap();
var pathHash = new hashMap();       //paths
var volHash = new hashMap();        //volume
var ttHash = new hashMap();         //trip table
var newNode = [];                   //new node id to network node id
var arrLink = [];
var networkLink = [];
var arrTT = [];
var iter = 0;

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
            networkLink.push([anode, bnode]);
            //logger.debug(`[${process.pid}]` + ' csv anode= ' + anode + ", bnode=" + bnode);
        })
        .on("end", function (result) {
            callback(null, result);
        });
    stream.pipe(csvStream);
}

//********build network********
var bldNet = function build_net (callback) {   
    //Read network topology
    newNode = [];
    newLink = [];  
    //logger.debug(`[${process.pid}]` + ' network link ' + networkLink);
    //create new node number, array index is the new node number
    for (var i = 0; i <= par.zonenum; i++) {
        newNode.push(i.toString());    //zone number
    }   
    //renumber nodes
    networkLink.forEach(function (ab) {
        var anode = ab[0];
        var bnode = ab[1];
        if (newNode.indexOf(anode) == -1) {
            newNode.push(anode);
        }
        if (newNode.indexOf(bnode) == -1) {
            newNode.push(bnode);
        }
        newLink.push([newNode.indexOf(anode), newNode.indexOf(bnode)]);     //convert to new node number
    });
    logger.debug(`[${process.pid}]` + ' renumbered nodes ' + newNode);
    //sort by anode
    newLink.sort(function (a, b) {
        return a[0] - b[0];
    })
    //build network
    var pre = 0;
    newLink.forEach(function (ab) {
        var a_new = ab[0];  
        var b_new = ab[1];
        if (a_new == pre) {         //a_new exist
            var value = nodeNet[a_new];
            value.push(b_new);
            nodeNet[a_new] = value;
            logger.debug(`[${process.pid}]` + ' anode already added. ' + a_new + ",[" + nodeNet[a_new] + "]");
        } else {
            nodeNet[a_new] = [b_new] ;
            logger.debug(`[${process.pid}]` + ' new anode. ' + a_new + typeof (a_new) + ",[" + b_new + typeof (b_new) + "]");
        }
        pre = a_new;
    });
    
    //loop each link
    //var i = 0
    //nodeNet.forEach(function(bNds) {
    //    //logger.debug(`[${process.pid}]` + " network " + key + "[" + value + ']');
    //    bNds.forEach(function (bNd) {
    //        var tp = typeHash.get(newNode(key) + '-' + newNode(bNd));
    //        //logger.debug(`[${process.pid}]` + " link " + key + '-' + bNd + ' type ' + tp + ' ban type ' + ftypeBan + ' find ' + ftypeBan.indexOf(parseInt(tp)));
    //        //mode banning
    //        if (ftypeBan.indexOf(parseInt(tp)) != -1) {
    //            bNds.splice(bNds.indexOf(bNd),1);
    //            //logger.debug(`[${process.pid}]` + " ban link remove " + key + "-" + bNd + ' mode ' + mode);
    //        }
    //        //decision point
    //        if (pType != 'ct' && key == spZone) {
    //            if (pType == 'tf' && (tp == par.ftypeexonrp || tp == par.ftypeex)) {
    //                bNds.splice(bNds.indexOf(bNd), 1);
    //                //logger.debug(`[${process.pid}]` + "decision point remove " + key + "-" + bNd);
    //            }
    //            if (pType == 'tl' && (tp == par.ftypegp || tp == par.ftypeexoffrp)) {
    //                bNds.splice(bNds.indexOf(bNd), 1);
    //                //logger.debug(`[${process.pid}]` + "decision point remove " + key + "-" + bNd);
    //            }
    //        }
    //    });
        
    //});
    callback(null, "build network");
}

//********find time dependent shortest path and write results to redis********
var sp = function ShortestPath(zone, zonenum, tp, mode, pathType, iter, callback) {
    logger.debug(`[${process.pid}]` + ' SP for zone ' + zone + ', tp ' + tp + ', mode ' + mode + ', pathType ' + pathType);
    //single node shortest path
    var ftypeBan = [];
    async.series([
        function (callback) {
            //get mode banning ftype          
            if (mode == "HOV") {
                ftypeBan = par.pathban.HOV;
            } else if (mode == "TRK") {
                ftypeBan = par.pathban.TRK;
            } 
            callback();
        }],
        //apply turn penalty

        function (err, result) {
            //Priority queue for frontier nodes
            var pqNodes = new pq(function (a, b) {
                return b.t - a.t;
            });         
            var visitedNodes = new hashMap();       //track visited nodes, {node,time}
            var settledNodes = [];
            var parentNodes = new hashMap();        //track parent nodes, {node, parent node}    
            var currNode = newNode.indexOf(zone.toString());
            visitedNodes.set(currNode, 0);            //root node
            pqNodes.enq({ t: 0, nd: currNode });
            var cnt = 1;    //count visited zones
            //if (zone == '122') { logger.debug(`[${process.pid}]` + ' zone ' + zone + ' sp loop nodes starts '); }
            do {
                //Explore frontier node
                //var tpNode = pqNodes.deq();
                //if (currNode == '122') { logger.debug(`[${process.pid}]` + ' pq size ' + pqNodes.size());}
                currNode = pqNodes.deq().nd;
                settledNodes.push(currNode);

                //skip centriod 
                if (currNode <= zonenum && currNode != zone) {
                    //logger.debug(`[${process.pid}]` + ' skip zone node ' + currNode);
                    continue;
                }

                var dnNodes = nodeNet[currNode];  //get new frontier nodes
                //logger.debug(`[${process.pid}]` + ' dnNodes ' + dnNodes);
                if (dnNodes != null) { 
                    dnNodes.forEach(function (dnNode) {
                        //ban links for dp
                        if (cnt == 1) {
                            if (pathType == 'tf') {
                                var tp = parseInt(typeHash.get(newNode[currNode] + '-' + newNode[dnNode]));
                                if (tp == par.ftypeexonrp || tp == par.ftypeex) {
                                    //logger.debug(`[${process.pid}]` + " decision point tf skip " + newNode[currNode] + "-" + newNode[dnNode]);
                                    cnt = cnt + 1;
                                    return;
                                }

                            }
                            if (pathType == 'tl') {
                                var tp = parseInt(typeHash.get(newNode[currNode] + '-' + newNode[dnNode]));
                                if (tp == par.ftypegp || tp == par.ftypeexoffrp) {
                                    //logger.debug(`[${process.pid}]` + " decision point tl skip " + newNode[currNode] + "-" + newNode[dnNode]);
                                    cnt = cnt + 1;
                                    return;
                                }
                            }
                        }
                        
                        if (settledNodes.indexOf(dnNode) == -1) {    //exclude settled nodes
                            //get time of dnNode
                            var timeCurrNode = parseFloat(visitedNodes.get(currNode));
                            var timePeriod = math.min(par.timesteps, math.floor(timeCurrNode / 15) + parseInt(tp));
                            var tempTime = timeCurrNode + parseFloat(timeHash.get(currNode + '-' + dnNode + ':' + timePeriod));

                            var preTime = visitedNodes.get(dnNode);
                            if (preTime == null || tempTime < preTime) {
                                //dnNode has not been checked before or update time when the path is shorter                   
                                pqNodes.enq({ t: tempTime, nd: dnNode });
                                if (dnNode <= zonenum) {
                                    cnt = cnt + 1;
                                }
                                parentNodes.set(dnNode, currNode);
                                logger.debug(`[${process.pid}]` + " set node " + newNode[dnNode] + "'s parent as " + newNode[parentNodes.get(dnNode)]);
                                visitedNodes.set(dnNode, tempTime);
                                //logger.debug('visit again ' + dnNode + ', ' + tempTime);
                            }
                        }
                        //logger.debug(`[${process.pid}]` + ' pqNode size = ' + pqNodes.size());
                    });
                }
            }
            while (pqNodes.size() > 0 && cnt <= zonenum);
            //if (zone == '122') { logger.debug(`[${process.pid}]` + ' zone ' + zone + ' sp loop nodes ends '); }
            //Construct path string and write dp path to redis db, zone path to local hash table
            redisClient.select(1);  //path db
            multi = redisClient.multi();
            //zones
            for (var i = 1; i <= zonenum; i++) {
                var zonePair = zone + '-' + i;
                var path = i;
                var pNode = i;
                if (parentNodes.has(pNode)) {
                    //logger.debug(`[${process.pid}]` + " node " + pNode + " has parent nodes");
                    var j = 0;
                    do {
                        pNode = parentNodes.get(pNode);
                        path = pNode + ',' + path;
                        //logger.debug(`[${process.pid}]` + " node " + pNode + " path " + path);
                        j = j + 1;
                    }
                    while (pNode != newNode.indexOf(zone));
                } else {
                    path = null
                }
                
                if (path != null) { 
                    //if (zonePair == '122-80') {
                        logger.debug(`[${process.pid}]` + ' ' + tp + ":" + zonePair + ":" + mode + ":" + pathType + " " + path);
                    //}
                    var key = tp + ":" + zonePair + ":" + mode + ":" + pathType;
                    if (par.dcpnt.indexOf(parseInt(zone)) != -1) {
                        //decision point path
                        multi.set(key, path);       //write to redis db, example 1:7-2:SOV:tl
                        //logger.debug(`[${process.pid}]` + ' dp sp ' + key + ' ' + path); 
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
                        multi.set(key + ":time", visitedNodes.get(i.toString()));
                        multi.set(key + ":dist", skimDist);
                        multi.set(key + ":toll", skimToll);
                        multi.set(key + ":fftime", skimFFtime);
                    } else {
                        //zone path
                        pathHash.set(key, path);
                        //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' set sp ct path ' + key + ' ' + path);
                    }
              
                    //logger.debug(`[${process.pid}]` + ' Iter=' + iter + ' ' + tp + ":" + zonePair + ":" + pathType + ', ' + path + ' Toll=' + skimToll);
                }
            }
            multi.exec(function (err, results) {
                //logger.debug(`[${process.pid}]` + ' Iter=' + iter + ' sp multi exec ' + result);
                callback(null, 'SP for zone ' + zone + ', tp ' + tp + ', mode ' + mode + ', pathType ' + pathType);
            });
            //if (zone == '122') { logger.debug(`[${process.pid}]` + ' zone ' + zone + ' write to hash ends'); }
        });
}

//********move vehicle and write results to redis********
var mv = function MoveVehicle(tp, zi, zj, pthTp, mode, vol, path, iter, callback) {
    if (path == null) {
        logger.debug(`[${process.pid}]` + ' iter ' + iter + ' move finished ' + zi + '-' + zj + ':' + tp + ':' + mode + ' path ' + path);
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
            function (callback) {
                //logger.debug(`[${process.pid}]` + ' iter' + iter + ' link loop' + j +  ', zonepair=' +arrPath[j] + "-" + zj + ',vol=' + vol + ',path=' + path);
                return callback(null, j <= arrPath.length - 2 && !breakloop);
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

                                    //logger.debug('probility calculation: tollconst=' + par.choicemodel.tollconst[tp - 1] + ',scalesdlen=' + par.choicemodel.scalestdlen
                                    //    + ',scalealpha=' + par.choicemodel.scalealpha + ',timecoeff=' + par.choicemodel.timecoeff
                                    //    + ',tollcoeff=' + par.choicemodel.tollcoeff + ',reliacoeffratio=' + par.choicemodel.reliacoeffratio
                                    //    + ',reliacoefftime=' + par.choicemodel.reliacoefftime + ',reliacoeffdist=' + par.choicemodel.reliacoeffdist);
                                    //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' distTl=' + distTl + ',distTf=' + distTf + ',timeTl=' + timeTl + ',timeTf=' + timeTf
                                    //    + ',timeFFTl=' + timeFFTl + ',timeFFTf=' + timeFFTf + ',Toll=' + Toll + ',utility=' + utility + ',probility=' + probility);
                                }
                                callback(null, probility);
                            })
                        },
                        function (probility, callback) {
                            //split and mv
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
                                    //get path
                                    redisClient.select(1);
                                    redisClient.get(tpNew + ":" + arrPath[j] + '-' + zj + ":" + mode + ":" + ptp, function (err, result) {
                                        //tp + ":" + zonePair + ":" + mode + ":" + pathType
                                        var path = result;
                                        //move vehicle
                                        mv(tpNew, arrPath[j], zj, ptp, mode, splitVol[i], path, iter, function (err, result) {  
                                            //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' dp mv ' + tpNew + ':' + arrPath[j] + '-' + zj + ':'
                                            //             + mode + ':' + ptp + ' path ' +  path + ' vol ' + splitVol[i]);
                                            i = i + 1
                                            callback();
                                        });
                                    });    
                                },
                                function (err) {
                                    callback();
                                });
                        }],
                        function (err, results) {
                            //logger.debug(`[${process.pid}]` + ' decision point loop end ' + j);
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
                            //logger.debug(`[${process.pid}]` + ' load to link ' + keyValue + ' vol ' + vol);
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
                //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' move finished ' + tp + ':' + zi + '-' + zj + ':' + mode + ' path ' + path + ' pthType ' + pthTp);
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
    async.series([
        function (callback) {
            rdcsv(function (err, result) {
                logger.info(`[${process.pid}] read network ` + result + ' links');
                callback();
            });
        },
        function (callback) {
            bldNet(function (err, result) {
                callback();
            });
        }
    ]);

    redisJob.subscribe("job");

    //process jobs
    redisJob.on("message", function (channel, message) {
        //sp and mv
        if (message == 'sp_mv') {
            var spZone = 0;
            var timeStep = 0;
            var mode = '';
            var pathType = '';  //ct,tl,tf
            var hasTask = true; 
            var tot = 0;
            async.series([
                function (callback) {
                    //update time
                    redisClient.select(9);
                    redisClient.get('iter', function (err, result) {
                        if (parseInt(result) >= 2) {
                            timeHash.clear();
                            redisClient.select(3);
                            redisClient.mget(arrLink, function (err, result) {
                                for (var i = 0; i < arrLink.length; i++) {
                                    timeHash.set(arrLink[i], result[i]);
                                }
                                //console.log(`[${process.pid}]` + ' iter' + iter + ' arrLink size=' + arrLink.length + ' 7-4cgTime=' + timeHash.get('7-4:1') + ' 7-5cgTime=' + timeHash.get('7-5:1'));
                                callback();
                            });
                        } else {
                            callback();
                        }
                        //logger.debug(`[${process.pid}]` + ' iter ' + result);
                    })
                },
                function (callback) {
                    //sp for decision points
                    async.during(
                        //test function
                        function (callback) {
                            redisClient.select(7);
                            redisClient.lpop('task', function (err, result) {
                                //logger.info(`[${process.pid}]` + ' iter' + iter + ' dp sp task ' + result);
                                if (result == null) {
                                    redisClient.INCR('cnt', function (err, result) {
                                        if (result == par.numprocesses) {
                                            return callback(null, false);        
                                        }
                                    });
                                } else {
                                    var tsk = result.split('-'); //sp-iter-tp-zone-SOV-ct/tl/tf
                                    iter = tsk[1];
                                    timeStep = tsk[2];
                                    spZone = tsk[3];
                                    mode = tsk[4];
                                    pathType = tsk[5];   //ct,tl,tf  
                                    return callback(null, true);
                                }                             
                            });
                        },
                        function (callback) {
                            sp(spZone, par.zonenum, timeStep, mode, pathType, iter, function (err, result) {
                                //logger.info(`[${process.pid}] ` + ' iter' + iter + ' sp for dp ' + spZone + ' ' + timeStep + ' ' + mode + ' ' + pathType);
                                callback();
                            });       
                        },
                        //whilst callback
                        function (err, results) {
                            logger.info(`[${process.pid}]` + ' iter' + iter + ' publish decison points sp_done');
                            redisClient.publish('job', 'sp_mv_zones');
                            redisClient.publish('job_status', 'sp_mv_zones'); 
                            callback();
                        });
                }
            ]);
        } else if (message == 'sp_mv_zones') {  
            var cnt = 0;
            //logger.info(`[${process.pid}]` + ' iter' + iter + ' start sp_mv_zones');
            async.during(           //loop until jobs are done
                //test function
                function (callback) {
                    spZone = 0;
                    redisClient.select(6);
                    redisClient.lpop('task', function (err, result) {
                        //logger.debug(`[${process.pid}]` + ' iter' + iter + ' get sp task ' + result);
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
                            sp(spZone, par.zonenum, timeStep, mode, pathType, iter, function (err, result) {
                                //tot = 0; 
                                var mvTasks = [];
                                redisClient.select(5);
                                redisClient.get(spZone + ':' + timeStep + ':' + mode + ':' + pathType, function (err, result) {
                                    //logger.info(`[${process.pid}]` + ' iter' + iter + ' get mv to nodes ' + spZone + ':' + timeStep + ':' + mode + ':' + pathType + ' ' + result);
                                    if (result != null) {
                                        mvTasks = result.split(',');
                                        mvTasks.forEach(function (item) {
                                            var tsk = item.split(':');   //I-J-tp-mode-vol-ct
                                            var zj = tsk[0];
                                            var vol = tsk[1];
                                            //vehicle trips from this zone
                                            var path = pathHash.get(timeStep + ":" + spZone + "-" + zj + ":" + mode + ":" + pathType);
                                            //logger.info(`[${process.pid}]` + ' iter' + iter + ' get path ' + timeStep + ":" + spZone + "-" + zj + ":" + mode + ":" + pathType);
                                            mv(timeStep, spZone, zj, pathType, mode, parseFloat(vol), path, iter, function (err, result) {
                                            //    //tot = tot + 1;
                                            //    //logger.info(`[${process.pid}]` + ' iter' + iter + ' moved ' + spZone + '-' + zj);
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
            multi = redisClient.multi();
            multi.select(2);
            var cnt = 0;
            var tot = volHash.count();
            if (tot == 0) {
                redisClient.INCR('cntNode', function (err, result) {
                    logger.info(`[${process.pid}]` + ' iter' + iter + ' node ' + result + ' no link vol written to redis');
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
                    multi.INCRBYFLOAT(key, math.round(value, 3), function (err, result) {    
                    });
                }
            });
            multi.exec(function (err, results) {
                if (cnt == tot) {
                    redisClient.INCR('cntNode', function (err, result) {
                        logger.info(`[${process.pid}]` + ' iter' + iter + ' node ' + result + ' write link vol to redis');
                        if (parseInt(result) == par.numprocesses) {
                            redisClient.set('cntNode', 0);
                            logger.info(`[${process.pid}]` + ' iter' + iter + ' publish sp_mv_done');
                            redisClient.publish('job_status', 'sp_mv_done');
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
                        return callback(null, result != null); //loop all tasks
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
                                            //logger.debug(`[${process.pid}] ` + ' iter=' + iter + ' link ' + key1 + ' MSA result ' + result);
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
                            //if (vol > 0) {
                            //    logger.debug(`[${process.pid}] ` + 'iter=' + iter + ', link=' + linkID + ', tp=' + linktp.split(':')[1] + ', VHT=' + math.round(vht, 2) +
                            //        ', vol=' + parseFloat(vol) + ', cgtime=' + math.round(cgTime, 3));
                            //}
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