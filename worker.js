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

var allLinks = new hashMap();       //link
var allNodes = new hashMap();
var pathHash = new hashMap();       //zone paths
var dppathHash = new hashMap();     //decision point paths
var newNode = [];                   //new node id to network node id
var arrLink = [];
var mvTasks = [];
var dps = [];
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
            if (data['DTA_Type'] == par.dcpnttype) {
                dps.push(id);
            }
            var nd = { id: parseInt(id), dnd: [], vsted: 99, stled: 0, pnd: null, lknd: [], type: data['DTA_Type'], segnum: data['SEGNUM'], prohiblnk: data['Prohibit_Links'], dnlnk:[] };
            allNodes.set(nd.id, nd);
        })
        .on("end", function (result) {
            callback(null, result);
        });
    stream.pipe(csvStream);
}
//********link reader********
var rdlnk = function Readcsv(callback) {
    //logger.debug(`Node ${process.pid} read links start`);
    var stream = fs.createReadStream(linkFile);
    var csvStream = csv({ headers: true })
        .on("data", function (data) {
            var arrTime = [];
            var arrToll = [];
            arrTime[0] = 0;
            arrToll[0] = 0;
            var a = data['A'];
            a = a.trim();
            var b = data['B'];
            b = b.trim();
            for (var i = 1; i <= par.timesteps; i++) {
                arrTime.push(parseFloat(data['TIME']));
                arrToll.push(parseFloat(data['TOLL']));
                arrLink.push(a + '-' + b + ':' + i);
            }
            var aNode = allNodes.get(parseInt(data['A']));
            var bNode = allNodes.get(parseInt(data['B']));
           
            allLinks.set(a + '-' + b, {
                aNd: aNode, bNd: bNode, time: arrTime, toll: arrToll, dist: parseFloat(data['Dist']), type: parseInt(data['Ftype']), cap: parseFloat(data['Cap']),
                alpha: parseFloat(data['Alpha']), beta: parseFloat(data['Beta']), vol: []
            });
            
            //put link time to node
            var currNode = allNodes.get(bNode.id);
            currNode.lknd.push([aNode.id, arrTime]);
            //logger.debug(`[${process.pid}]` + ' bnode ' + currNode.id + ' link anode ' + aNode.id + ' time1 ' + arrTime[1]);
        })
        .on("end", function (result) {
            callback(null, result);
        });
    stream.pipe(csvStream);
}
//********build network********
var bldNet = function build_net (callback) {   
    //build network topology
    allLinks.forEach(function (value, key) {        
        value.aNd.dnd.push(value.bNd);
        value.aNd.dnlnk.push(value);
        //logger.debug(`[${process.pid}]` + ' add anode ' + value.aNd.id + ' dnNode as ' + value.bNd.id);
    });
    callback(null, "build network");
}
//********init node and link********
var initNd = function init_nodes(callback) {
    //init nodes
    allNodes.forEach(function (nd) {
        nd.vsted = 99;
        nd.stled = 0;
        nd.pnd = null;
        //logger.debug(`[${process.pid}]` + ' node ' + nd.id + ' num of dnNodes ' + nd.dnd.length);
    });
    callback(null, "init nodes");
}
var initLink = function init_links(callback) {
    //init links    
    allLinks.forEach(function (value, key) {
        var arrVol = [];
        for (var i = 0; i <= par.modes.length; i++) {
            var jarr = []
            for (var j = 0; j <= par.timesteps; j++) {
                jarr.push(0);
            }
            arrVol.push(jarr);
        }
        value.vol = arrVol;
    });
    callback(null, "init links");
}
//********find time dependent shortest path and write results to redis********
var sp = function ShortestPath(zone, zonenum, tp, mode, pathType, iter, callback) {
    //logger.debug(`[${process.pid}]` + ' ***SP for zone ' + zone + ', tp ' + tp + ', mode ' + mode + ', pathType ' + pathType);
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
            initNd(function (err, result) {
                //logger.info(`[${process.pid}] ` + result);
                callback();
            });
        },
        //apply turn penalty

        function (callback) {
            //Priority queue for frontier nodes
            var pqNodes = new pq(function (a, b) {
                //comparator: (a, b)=> b.vsted != a.vsted ? a.vsted - b.vsted : a.vsted - b.vsted;
                return a.vsted - b.vsted;
            });
            var currNode = allNodes.get(zone);
            var zNode = currNode;
            currNode.vsted = 0;                      //track visited nodes and store time
            pqNodes.enqueue(currNode);
            var cnt = 1;    //count visited zones
            //visit nodes
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
                for (i = 0; i < dnNodes.length; i++) {
                    dnNode = dnNodes[i];
                    //logger.debug(`[${process.pid}]` + " node " + currNode.id + " dnNode " + dnNode.id);
                    var lnks = currNode.dnlnk;
                    var Linktp = 0;
                    lnks.forEach(function (lnk) {
                        if (lnk.bNd.id == dnNode.id) {
                            Linktp = lnk.type;
                            return;
                        }
                    });
                    //ban links for dp
                    if (cnt == 1) {
                        if (pathType == 'tf' && (Linktp == par.ftypeexonrp || Linktp == par.ftypeex)) {
                            //logger.debug(`[${process.pid}]` + " decision point tf path skip " + currNode.id + "-" + dnNode.id);
                            cnt = cnt + 1;
                            continue;
                        }
                        if (pathType == 'tl' && (Linktp == par.ftypegp || Linktp == par.ftypeexoffrp)) {
                            //logger.debug(`[${process.pid}]` + " decision point tl path skip " + currNode.id + "-" + dnNode.id);
                            cnt = cnt + 1;
                            continue;
                        }
                    }
                    //ban links for mode
                    if (ftypeBan.indexOf(Linktp) != -1) {
                        continue;
                    }
                    //logger.debug(`[${process.pid}]` + " node " + currNode.id + " dnNode " + dnNode.id + ' settled ' + dnNode.stled);
                    if (dnNode.stled != 1) {    //exclude settled nodes
                        //get time of dnNode
                        var timeCurrNode = currNode.vsted;
                        var timePeriod = math.min(par.timesteps, math.floor(timeCurrNode / 15) + parseInt(tp));
                        var tempTime = 0;
                        var pNds = dnNode.lknd;         //link anodes
                        pNds.forEach(function (nd) {
                            if (nd[0] == currNode.id) {
                                tempTime = timeCurrNode + nd[1][timePeriod];
                                return;
                            }
                        });

                        var preTime = dnNode.vsted;
                        if (tempTime < preTime) {
                            //dnNode has not been checked before or update time when the path is shorter                   
                            pqNodes.enqueue(dnNode);
                            if (dnNode.id <= zonenum) {
                                cnt = cnt + 1;
                            }
                            //find link
                            var tpLnk = null;
                            currNode.dnlnk.forEach(function (lnk) {
                                if (lnk.bNd.id == dnNode.id) {
                                    tpLnk = lnk;
                                    return;
                                }
                            });
                            dnNode.pnd = [currNode, dnNode, tpLnk, tempTime, timePeriod];
                            //logger.debug(`[${process.pid}]` + " set currNode=" + dnNode.pnd[0].id + ' dnNode=' + dnNode.pnd[1].id + ' time=' + dnNode.pnd[3] + ' tp=' + dnNode.pnd[4]);
                            dnNode.vsted = tempTime;
                            //logger.debug(`[${process.pid}]` + ' visit ' + dnNode.id + ', ' + tempTime);
                        }
                    }
                    //logger.debug(`[${process.pid}]` + ' pqNode size = ' + pqNodes.size());
                }
            }
            while (pqNodes.size() > 0 && cnt <= zonenum);

            //put paths to hashtables
            for (var i = 1; i <= zonenum; i++) {
                var zonePair = zone + '-' + i;
                var path = [];
                var pNode = allNodes.get(i);
                var eNode = pNode;
                //create path
                while (pNode.pnd != null) {
                    path.push(pNode.pnd);
                    //logger.debug(`[${process.pid}]` + '  ' + zonePair + ' path add [' + pNode.pnd[0].id + ', ' + pNode.pnd[1].id + ']');
                    pNode = pNode.pnd[0];
                }
                if (path.length > 0) {
                    var key = tp + ":" + zonePair + ":" + mode + ":" + pathType;
                    if (pathType == 'tf' || pathType == 'tl') {
                        //**decision point path
                        var skimTime = path[0][3];
                        var skimDist = 0;
                        var skimToll = 0;
                        var skimFFtime = 0;
                        for (var j = 0; j <= path.length - 1; j++) {
                            var link = path[j][2];
                            skimDist = skimDist + parseFloat(link.dist);
                            var tempTl = link.toll[tp];
                            skimToll = skimToll + tempTl;
                            skimFFtime = skimFFtime + link.time[tp];
                            //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' sp dp ' + link.aNd.id + '-' + link.bNd.id);
                        }
                        pathHash.set(key, path);
                        var skim = [];
                        skim.push(skimTime);
                        skim.push(skimDist);
                        skim.push(skimToll);
                        skim.push(skimFFtime);
                        pathHash.set(key + ':skim', skim);
                        //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' sp dp ' + key + ' skim time ' + eNode.vsted +
                        //    ' dist ' + skimDist + ' toll ' + skimToll + ' fftime ' + skimFFtime + ' pathlength ' + path.length);
                    } else {
                        //**zone path
                        pathHash.set(key, path);
                        //start log
                        //if (zonePair == '1-37') {
                        //var strPath = '';
                        //for (var i = 0; i <= path.length - 1; i++) {
                        //    strPath = path[i][1].id + ',' + strPath;
                        //}
                        //strPath = path[i - 1][0].id + ',' + strPath;
                        //logger.debug(`[${process.pid}]` + ' zone sp ' + key + ' ' + strPath);
                        //} //end log
                    }
                } 
            }
            callback();
        }],
        function (err) {
            callback(null, 'SP for zone ' + zone + ', tp ' + tp + ', mode ' + mode + ', pathType ' + pathType);
        });
}

//********move vehicle********
var mv = function MoveVehicle(tp, zi, zj, pthTp, mode, vol, path, iter, callback) {
    //logger.debug(`[${process.pid}]` + ' ****iter ' + iter + ' mv start ' + zi + '-' + zj + ':' + tp + ':' + mode + ':' + pthTp + ' vol=' + vol + ' path ' + path.length);
    if (path == null) {
        callback(null, zi + '-' + zj + ':' + tp + ':' + mode);
    } else {
        var totTime = 0;
        var tpNew = tp;
        var keyValue = '';
        multi = redisClient.multi();
        var j = 0;
        var breakloop = false;
        var modenum = 0;
        for (var i = 0; i <= par.modes.length - 1; i++){
            if (mode == par.modes[i]) {
                modenum = i;
                break;
            }
        }
        //loop links in the path
        var pathItem= path.pop();
        while (pathItem != null && !breakloop){
            var aNode = pathItem[0];
            //logger.debug(`[${process.pid}]` + ' iter' + iter + ' mv link loop' + j + ' currnode=' + pathItem[0].id + ', zonepair=' + zi + "-" + zj + ',vol=' + vol + ' mode=' + mode + ' aNodeType=' + aNode.type);               
            if (aNode.type == par.dcpnttype && j > 0 && mode == "SOV") {
                //**decision point (not the start node in path)**
                var ptype = ['tl', 'tf'];
                var time = [0, 0]; 
                var dist = [0, 0];
                var toll = [0, 0];
                var timeFF = [0, 0];
                var probility = 0; 
                var tltfPath = [null, null];
                for (var i = 0; i <= 1; i++){       //tl and tf
                    var key = tpNew + ":" + aNode.id + '-' + zj + ":" + mode + ":" + ptype[i];
                    tltfPath[i] = pathHash.get(key);
                    if (tltfPath[i] == null) {
                        //build dp sp
                        sp(parseInt(aNode.id), par.zonenum, tpNew, mode, ptype[i], iter, function (err, result) {
                            tltfPath[i] = pathHash.get(key);
                            //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' get path key=' + key + ' path len=' + path.length);
                            var pathSkim = pathHash.get(key + ':skim');
                            time[i] = pathSkim[0];
                            dist[i] = pathSkim[1];
                            toll[i] = pathSkim[2];
                            timeFF[i] = pathSkim[3];
                        });     
                    } else {
                        var pathSkim = pathHash.get(key + ':skim');
                        time[i] = pathSkim[0];
                        dist[i] = pathSkim[1];
                        toll[i] = pathSkim[2];
                        timeFF[i] = pathSkim[3];
                    } 
                }
                if (dist[0] > dist[1] * parseFloat(par.distmaxfactor)) {
                    probility = 0;
                    //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' probility=0');
                } else if (dist[1] > dist[0] * parseFloat(par.distmaxfactor)) {
                    probility = 1;
                    //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' probility=1 distTf=' + distTf + ' distTl=' + distTl);
                } else {
                    var utility = -1 * par.choicemodel.tollconst[tp - 1] - math.pow((par.choicemodel.scalestdlen / dist[0])
                        , par.choicemodel.scalealpha) * (par.choicemodel.timecoeff * (time[0] - time[1]))
                        + par.choicemodel.tollcoeff * toll[0] + par.choicemodel.timecoeff * par.choicemodel.reliacoeffratio
                        * par.choicemodel.reliacoefftime * ((timeFF[1] - time[1]) * math.pow(dist[1], (-1 * par.choicemodel.reliacoeffdist))
                            - (timeFF[0] - time[0]) * math.pow(dist[0], (-1 * par.choicemodel.reliacoeffdist)));

                    probility = 1 / (1 + math.exp(utility));

                    logger.debug('probility calculation: tollconst=' + par.choicemodel.tollconst[tp - 1] + ',scalesdlen=' + par.choicemodel.scalestdlen
                        + ',scalealpha=' + par.choicemodel.scalealpha + ',timecoeff=' + par.choicemodel.timecoeff
                        + ',tollcoeff=' + par.choicemodel.tollcoeff + ',reliacoeffratio=' + par.choicemodel.reliacoeffratio
                        + ',reliacoefftime=' + par.choicemodel.reliacoefftime + ',reliacoeffdist=' + par.choicemodel.reliacoeffdist);
                    logger.debug(`[${process.pid}]` + ' iter ' + iter + ' distTl=' + dist[0] + ',distTf=' + dist[1] + ',timeTl=' + time[0] + ',timeTf=' + time[1]
                        + ',timeFFTl=' + timeFF[0] + ',timeFFTf=' + timeFF[1] + ',Toll=' + toll[0] + ',utility=' + utility + ',probility=' + probility);
                }
                var splitVol = [0, 0];
                if (probility == 0) {
                    splitVol[1] = vol;
                } else if (probility == 1) {
                    splitVol[0] = vol;
                } else {
                    splitVol[0] = vol * probility;
                    splitVol[1] = vol * (1 - probility);
                }
                for (var i = 0; i <= 1; i++) {
                    if (splitVol[i] > 0) {
                        mvTasks.push(aNode.id + ':' + zj + ':' + splitVol[i] + ':' + ptype[i]);
                    }
                }
                breakloop = true;
            } else {  
                //not decision point
                pathItem[2].vol[modenum][pathItem[4]] = pathItem[2].vol[modenum][pathItem[4]] + vol;
                j = j + 1;
                //logger.debug(`[${process.pid}]` + ' iter' + iter + ' load link ' + pathItem[2].aNd.id + '-' + pathItem[2].bNd.id + ' mode=' + modenum + ' tp=' + pathItem[4] + ' vol=' + vol);                      
            }
            pathItem = path.pop();
        } 
        callback(); 
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
    logger.info(`[${process.pid}]` + ' creating ' + par.numprocesses + ' worker nodes');
    for (var i = 0; i < par.numprocesses; i++) {
        var worker = cluster.fork();
    }
    cluster.on('death', function (worker) {
        logger.info(`Worker node [${process.pid}] died`);
    });
} else { //worker node   
    //read network
    async.series([
        function (callback) {
            rdnd(function (err, result) {
                //logger.info(`[${process.pid}] read node ` + result + ' nodes');
                callback();
            });
        },
        function (callback) {
            rdlnk(function (err, result) {
                //logger.info(`[${process.pid}] read link ` + result + ' links');
                callback();
            });
        },
        function (callback) {
            bldNet(function (err, result) {
                callback();
            });
        }
    ]);
    logger.info(`[${process.pid}] cluster worker node [${process.pid}] is ready`);
    redisJob.subscribe("job");

    //process jobs
    redisJob.on("message", function (channel, message) {
        //sp and mv       
        if (message == 'sp_mv') {
            initLink(function (err, result) {
                //logger.debug(`[${process.pid}]` + ' ##### ' + result);
            });
            var spZone = 0;
            var timeStep = 0;
            var mode = '';
            var pathType = '';  //ct,tl,tf
            var hasTask = true; 
            var tot = 0;
            async.series([
                function (callback) {
                    //update time if iter >= 2
                    redisClient.select(9);
                    redisClient.get('iter', function (err, result) {  
                        iter = result;
                        if (parseInt(result) >= 2) {  //iter>=2           
                            redisClient.select(3);
                            redisClient.mget(arrLink, function (err, result) {
                                //logger.debug(`[${process.pid}]` + ' iter' + iter + ' update time bnode num of linktp ' + result.length);
                                for (var i = 0; i < arrLink.length; i++) {
                                    var link = arrLink[i].split(':')[0]; //link id
                                    var bNode = allNodes.get(parseInt(link.split('-')[1])); //bNode id
                                    //logger.debug(`[${process.pid}]` + ' iter' + iter + ' bnode ' + bNode.id);
                                    var pNds = bNode.lknd
                                    pNds.forEach(function (nd) {
                                        if (nd[0] == link.split('-')[0]) {
                                            nd[1][arrLink[i].split(':')[1]] = parseFloat(result[i]);
                                            //logger.debug(`[${process.pid}]` + ' iter' + iter + ' update time bnode ' + bNode.id + ' from anode ' + nd[0] + ' old_time=' + nd[1][arrLink[i].split(':')[1]] + ' new_time=' + result[i]);
                                            return;
                                        }
                                    });
                                }                                
                                callback();
                            });
                        } else {
                            callback();
                        }
                        
                    })
                },
                function (callback) {
                    //sp for decision points 
                    logger.info(`[${process.pid}]` + ' iter' + iter + ' publish decison points sp_done');
                    redisClient.publish('job', 'sp_mv_zones');
                    redisClient.publish('job_status', 'sp_mv_zones');
                    callback();
                }
            ]);
        } else if (message == 'sp_mv_zones') {  
            var cnt = 0;
            var spZone = 0;
            //logger.info(`[${process.pid}]` + ' iter' + iter + ' start sp_mv_zones');
            async.during(           //loop until jobs are done
                //test function
                function (callback) {                   
                    redisClient.select(6);
                    redisClient.lpop('task', function (err, result) {
                        //logger.debug(`[${process.pid}]` + ' ***iter' + iter + ' get sp zone task ' + result);
                        if (result == null) {
                            redisClient.INCR('cnt', function (err, result) {
                                if (result == par.numprocesses) {
                                    logger.info(`[${process.pid}]` + ' iter' + iter + ' publish sp_done');
                                    redisClient.publish('job', 'linkvolredis');
                                    redisClient.publish('job_status', 'linkvolredis');
                                }
                            });
                        } else {
                            var tsk = result.split(','); //iter-tp-zone-mode
                            iter = tsk[0];
                            timeStep = tsk[1];
                            spZone = tsk[2];
                            mode = tsk[3];
                            pathType = 'ct';    //cost time
                        }
                        redisClient.publish('job_status', 'bar_tick:6');
                        return callback(null, result != null);
                    });
                },
                //create shortest path and move vehicles for an origin zone
                function (callback) {
                    //logger.info(`[${process.pid}]` + ' ***iter' + iter + ' sp and mv from node=' + spZone + ' timestep=' + timeStep + ', mode=' + mode + ', pathType=' + pathType);                                      
                    async.series([
                        function (callback) {
                            //**build sp
                            sp(parseInt(spZone), par.zonenum, timeStep, mode, pathType, iter, function (err, result) {
                                callback();
                            })
                        },
                        function (callback) {
                            //read tasks
                            redisClient.select(5);
                            redisClient.get(spZone + ':' + timeStep + ':' + mode, function (err, result) {
                                if (result != null) {
                                    var tsks = result.split(',');   //from spZone to all destination zones 'zj:vol'
                                    tsks.forEach(function (tsk) {                                        
                                        mvTasks.push(spZone+ ':' + tsk + ':ct');  //tsk = 'zi:zj:vol:ct'
                                    });
                                }
                                callback();
                            });
                        },
                        function (callback) {
                            //move all tasks
                            var task = ''; 
                            async.during(
                                function (callback) {
                                    task = mvTasks.pop();   //tsk = 'zj:vol:ptp'
                                    callback(null, task != null);
                                },
                                function (callback) {
                                    var arrTask = task.split(':');
                                    var zi = arrTask[0];
                                    var zj = arrTask[1];   //destination node
                                    var vol = arrTask[2];
                                    var ptp = arrTask[3];
                                    //zone node task                                       
                                    path = pathHash.get(timeStep + ":" + zi + "-" + zj + ":" + mode + ":" + ptp);   
                                    if (path == null) {
                                        //logger.info(`[${process.pid}]` + ' iter' + iter + ' mv path=null ' + timeStep + ":" + zi + "-" + zj + ":" + mode + ":" + ptp);
                                        callback();
                                    } else {
                                        //**move vehicles
                                        mv(timeStep, zi, zj, ptp, mode, parseFloat(vol), path, iter, function (err, result) {
                                            //logger.info(`[${process.pid}]` + ' iter' + iter + ' zone moved ' + spZone + '-' + zj + ' pathtype=' + ptp + ' vol=' + vol);
                                            callback();
                                        });
                                    }
                                },
                                function (err) {
                                    callback();
                                });                                    
                        }],
                        function () {
                            pathHash.clear();
                            cnt = cnt + 1;
                            //logger.info(`[${process.pid}] ` + ' spZone=' + spZone + ' tp=' + timeStep + ' mode=' + mode + ' total moved pair ' + cnt);
                            callback();
                        });
                },
                //whilst callback
                function (err, results) {                   
                    logger.info(`[${process.pid}]` + ' iter' + iter + ' sp processed total of ' + cnt);
                });
        }      
        else if (message == 'linkvolredis') {
            //write vol to redis
            redisClient.select(2);
            multi = redisClient.multi();
            multi.select(2);
            var cnt = 0;
            var tot = allLinks.count();
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
            allLinks.forEach(function (value, key) {
                //key in db=aNode.id + '-' + bNode.id + ':' + tpNew + ':' + mode + ':' + iter;                
                cnt = cnt + 1;
                for (var i = 0; i <= par.modes.length; i++) {
                    for (var j = 0; j <= par.timesteps; j++) {
                        var v = value.vol[i][j];
                        var tp = j + 1;                        
                        if (v > 0) {
                            var dbKey = key + ':' + tp + ':' + par.modes[i] + ':' + iter;
                            //logger.debug(`[${process.pid}]` + ' iter' + iter + ' write vol to db dbkey=' + dbKey + ' v=' + v);
                            multi.INCRBYFLOAT(dbKey, v, function (err, result) {                               
                            });                          
                        }
                    }
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
                            async.eachSeries(par.modes, function (md, callback) {
                                var lastIter = iter - 1;
                                var key1 = linktp + ":" + md + ":" + iter;                               
                                if (iter > 1) {
                                    var key2 = linktp + ":" + md + ":" + lastIter;                                  
                                    scriptManager.run('msa', [key1, key2, iter], [], function (err, result) { //return v_current, v_previous, v_msa                                       
                                        vol = vol + parseFloat(result.split(',')[2]);
                                        //logger.debug(`[${process.pid}] ` + ' iter=' + iter + ' link ' + key1 + ' MSA result ' + result + ' vol=' + vol);
                                        callback(null, true);
                                    });
                                } else {
                                    redisClient.select(2);
                                    redisClient.get(key1, function (err, result) {
                                        if (result != null) {
                                            vol = vol + parseFloat(result);
                                            //logger.debug(`[${process.pid}] ` + 'iter=' + iter + ' ' + key1 + ' vol=' + vol);
                                        }
                                        callback(null, true);
                                    });                                    
                                } 
                            },
                            function (err, result) {
                                callback();
                            });
                            
                        },
                        //moving average volume

                        //congested time
                        function (callback) {
                            var linkID = linktp.split(':')[0];
                            var lnk = allLinks.get(linkID);
                            var cgTime = lnk.time[linktp.split(':')[1]] * (1 + lnk.alpha * math.pow(vol * 4 / lnk.cap, lnk.beta));
                            var vht = vol * cgTime / 60;
                            //if (vol > 0) {
                            //    logger.debug(`[${process.pid}] ` + 'iter=' + iter + ', link=' + linkID + ', tp=' + linktp.split(':')[1] + ', VHT=' + math.round(vht, 2) +
                            //        ', vol=' + math.round(vol, 2) + ', cgtime=' + math.round(cgTime, 2));
                            //}
                            //set congested time to redis
                            multi = redisClient.multi();
                            multi.select(3);
                            //multi.get(linktp, function (err, result) {
                            //    logger.debug(`[${process.pid}] ` + 'iter=' + iter + ' linktp ' + linktp + ' old cgTime=' + result);
                            //})
                            //logger.debug(`[${process.pid}] ` + 'iter=' + iter + ' linktp ' + linktp + ' new cgtime=' + cgTime);
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