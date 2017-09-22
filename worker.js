//Starting node
var hashMap = require('hashmap');
var fs = require('fs');
var csv = require('fast-csv');
var redis = require('redis');
var pq = require('priorityqueue');
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

var linkHash = new hashMap();     //link
var allNodes = [];
var pathHash = new hashMap();       //paths
var volHash = new hashMap();        //volume
var newNode = [];                   //new node id to network node id
var arrLink = [];
var networkLink = [];
var iter = 0;

var redisIP = "redis://127.0.0.1:6379";
var appFolder = "./app";
var paraFile = appFolder + "/parameters_95.json";
var luaScript_msa = appFolder + '/msa.lua';

var redisClient = redis.createClient({ url: redisIP }), multi;
var redisJob = redis.createClient({ url: redisIP }), multi;
var par = JSON.parse(fs.readFileSync(paraFile));
var linkFile = appFolder + '/' + par.linkfilename;
var nodeFile = appFolder + '/' + par.nodefilename;
//load redis lua script
var scriptManager = new Scripto(redisClient);
scriptManager.loadFromFile('msa', luaScript_msa);

//********node reader********
var rdnd = function Readcsv(callback) {
    var stream = fs.createReadStream(nodeFile);
    var csvStream = csv({ headers: true })
        .on("data", function (data) {
            var id = data['N'];
            id = id.trim();
            var nd = { id: parseInt(id), dnd: [], vsted: -1, stled: 0, pnd: nd, type: data['DTA_Type'], segnum: data['SEGNUM'], prohiblnk: data['Prohibit_Links'] };
            allNodes.push(nd);
        })
        .on("end", function (result) {
            callback(null, result);
        });
    stream.pipe(csvStream);
}
//********link reader********
var rdlnk = function Readcsv(callback) {
    //logger.debug(`Node ${process.pid} read links start`);
    arrLink.length = 0;
    var stream = fs.createReadStream(linkFile);
    var csvStream = csv({ headers: true })
        .on("data", function (data) {
            var arrTime = [];
            var arrToll = [];
            arrTime[0] = 0;
            arrToll[0] = 0;
            for (var i = 1; i <= par.timesteps; i++) {
                arrTime.push(parseFloat(data['TIME']));
                arrToll.push(parseFloat(data['TOLL']));
            }
            var aNode = allNodes.find(function (nd) {
                return nd.id == parseInt(data['A']);
            });
            var bNode = allNodes.find(function (nd) {
                return nd.id == parseInt(data['B']);
            });
            var a = data['A'];
            a = a.trim();
            var b = data['B'];
            b = b.trim();
            linkHash.set(a + '-' + b, {
                aNd: aNode, bNd: bNode, time: arrTime, toll: arrToll, dist: parseFloat(['Dist']), type: parseInt(data['Ftype']), cap: parseFloat(data['Cap']),
                alpha: parseFloat(data['Alpha']), beta: parseFloat(data['Beta'])
            });
            networkLink.push([aNode.id, bNode.id]);
        })
        .on("end", function (result) {
            callback(null, result);
        });
    stream.pipe(csvStream);
}
//********build network********
var bldNet = function build_net (callback) {   
    //build network topology
    linkHash.forEach(function (value, key) { 
        value.aNd.dnd.push(value.bNd);
    });
    callback(null, "build network");
}

//********find time dependent shortest path and write results to redis********
var sp = function ShortestPath(zone, zonenum, tp, mode, pathType, iter, callback) {
    //logger.debug(`[${process.pid}]` + ' SP for zone ' + zone + ', tp ' + tp + ', mode ' + mode + ', pathType ' + pathType);
    //single node shortest path
    var ftypeBan = [];
    async.series([
        function (callback) {
            //get mode banning ftype          
            if (mode == "HOV") {
                ftypeBan = parseInt(par.pathban.HOV);
            } else if (mode == "TRK") {
                ftypeBan = parseInt(par.pathban.TRK);
            } 
            callback();
        }],
        //apply turn penalty

        function (err, result) {
            //Priority queue for frontier nodes
            var pqNodes = new pq(function(a,b){
                comparator: (a, b)=> b.vsted != a.vsted ? b.vsted - a.vsted : b.vsted - a.vsted;
            });         
            var currNode = allNodes.find(function (nd) {
                return nd.id == zone;
            });
            currNode.vsted = 0;                      //track visited nodes and store time
            pqNodes.enqueue(currNode);
            var cnt = 1;    //count visited zones

            do {
                //Explore frontier node
                currNode = pqNodes.dequeue();
                currNode.stled = 1;
                //logger.debug(`[${process.pid}]` + ' currNode ' + currNode.id);
                //skip centriod 
                if (currNode.id <= zonenum && currNode.id != zone) {
                    //logger.debug(`[${process.pid}]` + ' skip zone node ' + currNode.id);
                    continue;
                }

                var dnNodes = currNode.dnd;  //get new frontier nodes
                if (dnNodes.length != 0) { 
                    async.each(dnNodes, function (dnNode, callback) {                       
                        var lnk = linkHash.get(currNode.id + '-' + dnNode.id);
                        if (lnk == null) { logger.debug(`[${process.pid}]` + ' null link ' + currNode.id + '-' + dnNode.id);}
                        var tp = lnk.type;
                        //ban links for dp
                        if (cnt == 1) {
                            if (pathType == 'tf' && (tp == par.ftypeexonrp || tp == par.ftypeex)) {
                                //logger.debug(`[${process.pid}]` + " decision point tf skip " + newNode[currNode] + "-" + newNode[dnNode]);
                                cnt = cnt + 1;
                                return;
                            }
                            if (pathType == 'tl' && (tp == par.ftypegp || tp == par.ftypeexoffrp)) {
                                //logger.debug(`[${process.pid}]` + " decision point tl skip " + newNode[currNode] + "-" + newNode[dnNode]);
                                cnt = cnt + 1;
                                return;
                            }
                        }
                        //ban links for mode
                        if (tp == ftypeBan) {
                            cnt = cnt + 1;
                            return;
                        }
                        if (dnNode.stled != 1) {    //exclude settled nodes
                            //get time of dnNode
                            var timeCurrNode = currNode.vsted;
                            var timePeriod = math.min(par.timesteps, math.floor(timeCurrNode / 15) + parseInt(tp));
                            var tempTime = timeCurrNode + linkHash.get(currNode.id + '-' + dnNode.id).time[timePeriod];

                            var preTime = dnNode.vsted;
                            if (preTime == -1 || tempTime < preTime) {
                                //dnNode has not been checked before or update time when the path is shorter                   
                                pqNodes.enqueue(dnNode);
                                if (dnNode.id <= zonenum) {
                                    cnt = cnt + 1;
                                }
                                dnNode.pnd = currNode;
                                //logger.debug(`[${process.pid}]` + " set node " + dnNode.id + "'s parent as " + dnNode.pnd.id);
                                dnNode.vsted = tempTime;
                                //logger.debug(`[${process.pid}]` + ' visit ' + dnNode + ', ' + tempTime);
                            }
                        }
                        callback();
                        //logger.debug(`[${process.pid}]` + ' pqNode size = ' + pqNodes.size());
                    });
                }
            }
            while (pqNodes.size() > 0 && cnt <= zonenum);
            
            //Construct path string and write dp path to redis db, zone path to local hash table
            redisClient.select(1);  //path db
            multi = redisClient.multi();
            //zones
            for (var i = 1; i <= zonenum; i++) {
                var zonePair = zone + '-' + i;
                var path = i;
                var pNode = allNodes.find(function (nd) {
                    return nd.id == i;
                });
                var zNode = pNode;
                if (pNode.pnd != null) {
                    //logger.debug(`[${process.pid}]` + " node " + pNode.id + " has parent nodes " + pNode.pnd.id);
                    async.during(
                        function (callback) {
                            return callback(null, pNode.id != zone);
                        },
                        function (callback) {
                            pNode = pNode.pnd;
                            path = pNode.id + ',' + path;
                            //logger.debug(`[${process.pid}]` + " node " + pNode.id + " path " + path);
                        }    
                    );
                } else {
                    path = null
                }
                
                if (path != null) {
                    //log path
                    if (zonePair == '122-80') {
                        logger.debug(`[${process.pid}]` + ' path for ' + tp + ":" + zonePair + ":" + mode + ":" + pathType + " " + path);
                    }
                    var key = tp + ":" + zonePair + ":" + mode + ":" + pathType;
                    if (zNode.type == par.dcpnttype) {
                        //decision point path
                        multi.set(key, path);       //write to redis db, example 1:7-2:SOV:tl
                        //logger.debug(`[${process.pid}]` + ' dp sp ' + key + ' ' + path); 
                        var dpPath = path.split(',');
                        var skimDist = 0;
                        var skimToll = 0;
                        var skimFFtime = 0;
                        for (var j = 0; j <= dpPath.length - 2; j++) {
                            var link = dpPath[j] + '-' + dpPath[j + 1];
                            skimDist = skimDist + parseFloat(linkHash.get(link).dist);
                            var tempTl = linkHash.get(link)[tp];
                            skimToll = skimToll + tempTl;
                            skimFFtime = skimFFtime + linkHash.get(link).time[tp];
                        }
                        multi.set(key + ":time", zNode.vsted);
                        multi.set(key + ":dist", skimDist);
                        multi.set(key + ":toll", skimToll);
                        multi.set(key + ":fftime", skimFFtime);
                        //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' sp ct skim time ' + visitedNodes[i] +
                        //    ' dist ' + skimDist + ' toll ' + skimToll + ' fftime ' + skimFFtime + ' path ' + path);
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
        //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' move finished ' + zi + '-' + zj + ':' + tp + ':' + mode + ' path ' + path);
        callback(null, zi + '-' + zj + ':' + tp + ':' + mode);
    } else {
        var arrPath = path.split(',');
        var totTime = 0;
        var tpNew = tp;
        var keyValue = '';
        multi = redisClient.multi();
        var j = 0;
        var breakloop = false;
        var anode = {};
        //loop links in the path
        async.during(
            //test function 
            function (callback) {
                //logger.debug(`[${process.pid}]` + ' iter' + iter + ' link loop' + j +  ', zonepair=' +arrPath[j] + "-" + zj + ',vol=' + vol + ',path=' + path);
                anode = allNodes.find(function (nd) {
                    return nd.id == arrPath[j];
                });
                return callback(null, j <= arrPath.length - 2 && !breakloop);
            },
            function (callback) {
                if (anode.type == par.dcpnttyp && j > 0 && mode == "SOV") {
                    //decision point (not the start node in path)
                    var zonePair = anode.id + "-" + zj;
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
                                    redisClient.get(tpNew + ":" + anode.id + '-' + zj + ":" + mode + ":" + ptp, function (err, result) {
                                        //tp + ":" + zonePair + ":" + mode + ":" + pathType
                                        var path = result;
                                        //move vehicle
                                        mv(tpNew, anode.id, zj, ptp, mode, splitVol[i], path, iter, function (err, result) {  
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
                            var linkID = anode.id + '-' + arrPath[j + 1];
                            totTime = totTime + linkHash.get(linkID).time[tpNew];
                            //logger.debug(`[${process.pid}] ` + linkID + ' time=' + parseFloat(timeHash.get(linkID.toString())) + ',iter=' + iter);
                            keyValue = linkID + ':' + tpNew + ':' + mode + ':' + iter;
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
            rdnd(function (err, result) {
                logger.info(`[${process.pid}] read node ` + result + ' nodes');
                callback();
            });
        },
        function (callback) {
            rdlnk(function (err, result) {
                logger.info(`[${process.pid}] read link ` + result + ' links');
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
                                            //logger.info(`[${process.pid}]` + ' iter' + iter + ' get path ' + timeStep + ":" + spZone + "-" + zj + ":" + mode + ":" + pathType + ' ' + path);
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
                            var cgTime = linkHash.get(linkID).time * (1 + linkHash.get(linkID).alpha * math.pow(vol * 4 / linkHash.get(linkID).cap, linkHash.get(linkID).beta));
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