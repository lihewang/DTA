//Starting node
var hashMap = require('hashmap');
var fs = require('fs');
var csv = require('fast-csv');
var redis = require('redis');
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
var arrTollLink = [];
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
            var nd = { id: parseInt(id), dnd: [], dnlnk: [], vsted: [999,999,999], stled: 0, pqindex:0, pnd: null, type: data['DTA_Type'], segnum: data['SEGNUM'], prohiblnk: data['Prohibit_Links'] };
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
            var arrffTime = [];
            var arrToll = [];
            arrTime[0] = 0;
            arrffTime[0] = 0;
            arrToll[0] = 0;
            var a = data['A'];
            a = a.trim();
            var b = data['B'];
            b = b.trim();
            for (var i = 1; i <= par.timesteps; i++) {
                arrTime.push(parseFloat(data['TIME']));
                arrffTime.push(parseFloat(data['TIME']));
                arrToll.push(parseFloat(data['TOLL']));
                arrLink.push(a + '-' + b + ':' + i);
                if (parseInt(data['TOLLTYPE']) == 1) {
                    arrTollLink.push(a + '-' + b + ':' + i);
                }
            }
            var aNode = allNodes.get(parseInt(data['A']));
            var bNode = allNodes.get(parseInt(data['B']));
           
            allLinks.set(a + '-' + b, {
                aNd: aNode, bNd: bNode, time: arrTime, fftime: arrffTime, toll: arrToll, tolltype: parseInt(data['TOLLTYPE']), dist: parseFloat(data['Dist']), type: parseInt(data['Ftype']), cap: parseFloat(data['Cap']),
                alpha: parseFloat(data['Alpha']), beta: parseFloat(data['Beta']), vol: [], volmsa:[]
            });
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
        nd.vsted = [999, 999, 999];    //time, time+fixtoll, perceived time
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
    logger.debug(`[${process.pid}]` + ' iter' + iter + ' ***SP for zone ' + zone + ', tp ' + tp + ', mode ' + mode + ', pathType ' + pathType);
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
            //get impedance
            if (pathType == 'ct') {
                var Imp = 1;    //time + fixed toll + min EL toll
            } else {
                var Imp = 2;    //perceived time + fixed toll
            }
            var currNode = allNodes.get(zone);
            var zNode = currNode;
            //track visited nodes and store time
            currNode.vsted[0] = 0;
            currNode.vsted[Imp] = 0;  
            var pq = [];                        //priority queue with binary heap
            pq.push(currNode);
            var cnt = 1;    //count visited zones
            
            //visit nodes
            do {
                //Explore frontier node
                currNode = pq.splice(0, 1)[0];     //remove root
                logger.debug(`[${process.pid}]` + ' --sp settled currNode=' + currNode.id + ' pq length=' + pq.length);
                if (pq.length > 0) {
                    pq.unshift(pq.splice(pq.length - 1, 1)[0]);      //move last node to the root
                    pq[0].pqindex = 0;
                    logger.debug(`[${process.pid}]` + ' --pq root node=' + pq[0].id + ' pq length=' + pq.length);
                    //bubble down
                    var index = 0;

                    //loop has problem!!!!!!!!
                    while ((index + 1) * 2 <= pq.length) {          //left child exist
                        var switchNode = pq[(index + 1) * 2 - 1];   //left child
                        var switchIndex = (index + 1) * 2 - 1
                        if ((index + 1) * 2 + 1 <= pq.length) {     //right child exist
                            var rightChild = pq[(index + 1) * 2];
                            if (rightChild.vsted[Imp] < switchNode.vsted[Imp]) {
                                switchNode = rightChild;
                                switchIndex = (index + 1) * 2
                            }
                        }
                        if (switchNode.vsted[Imp] < pq[index].vsted[Imp]) {
                            //switch
                            pq[switchIndex] = pq[index];   //child = parent
                            pq[index] = switchNode;        //parent = child
                            switchNode.pqindex = index;     //child index
                            pq[switchIndex].pqindex = switchIndex;  //parent index
                            index = switchIndex;
                        }
                    }
                }                              
                
                //log pq
                var strpq = '';
                for (var i = 0; i < pq.length; i++) {
                    if (strpq == '') {
                        strpq = pq[i].id;
                    } else {
                        strpq = strpq + ',' + pq[i].id;
                    }
                }
                logger.debug(`[${process.pid}]` + ' after bubble down priority queue=' + strpq);
                //end log
                currNode.stled = 1;
                //skip centriod 
                if (currNode.id <= zonenum && currNode.id != zone) {
                    //logger.debug(`[${process.pid}]` + ' skip zone node ' + currNode.id);
                    continue;
                }

                var dnNodes = currNode.dnd;  //get new frontier nodes                 
                for (i = 0; i < dnNodes.length; i++) {
                    var dnNode = dnNodes[i];
                    var dnLink = currNode.dnlnk[i];
                    var Linktp = dnLink.type;
                    //logger.debug(`[${process.pid}]` + ' ---sp currNode=' + currNode.id + ' dnNodes Cnt=' + dnNodes.length + ' dnNode=' + dnNode.id + ' dnlink=' + dnLink.aNd.id + '-' + dnLink.bNd.id);
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
                    if (dnNode.stled != 1) {                            //exclude settled nodes                       
                        var impCurrNode = currNode.vsted[Imp];          //get impedance of dnNode
                        var timePeriod = math.min(par.timesteps, math.floor(currNode.vsted[0] / 15) + parseInt(tp));    //get time period
                        //get toll
                        var fixtoll = 0;    //fixed toll 
                        var mintoll = 0;    //min EL toll 
                        if (dnLink.tolltype == 2) {
                            fixtoll = dnLink.toll[timePeriod];
                        }
                        if (dnLink.tolltype == 1) {
                            mintoll = par.tollpolicy.mintoll;
                        }                       
                        if (pathType == 'ct') {
                            var alltoll = fixtoll + mintoll;                   
                        } else {
                            var alltoll = fixtoll;  //tl anf tf path exclude mintoll
                        }
                        //get perceived time factor
                        var perceiveFactor = 1;
                        if (pathType != 'ct' && iter>1) {
                            perceiveFactor = 1 + (par.choicemodel.perceivetimemaxvc - 1) / (1 + math.exp(-1 * par.choicemodel.perceivetimesteep *
                                (dnLink.volmsa[timePeriod] * 4 / dnLink.cap - par.choicemodel.perceivetimemidvc)));
                        }
                        var tempTime = currNode.vsted[0] + dnLink.time[timePeriod];               //time
                        var tempImp = currNode.vsted[Imp] + dnLink.time[timePeriod] * perceiveFactor + alltoll / (par.choicemodel.timecoeff / par.choicemodel.tollcoeff);        //impedance
                        //logger.debug(`[${process.pid}]` + ' tempTime=' + tempTime + ' tempImp=' + tempImp + ' toll=' + alltoll + ' dnNodeImp=' + dnNode.vsted[Imp] + ' perceivefct=' + perceiveFactor);

                        if (tempImp < dnNode.vsted[Imp]) {
                            var index = 0;
                            //dnNode has not been checked before or update time when the path is shorter                             
                            if (dnNode.vsted[Imp] == 999) {
                                pq.push(dnNode);    //not visited
                                index = pq.length - 1;
                            } else {
                                index = dnNode.pqindex
                            } 
                            dnNode.vsted[Imp] = tempImp;
                            dnNode.vsted[0] = tempTime;

                            //bubble up in priority queue  
                            var parentIndex = math.floor((index + 1) / 2 - 1);                           
                            while (parentIndex >= 0) {   //has parent
                                //logger.debug(`[${process.pid}]` + ' bubble up parentIndex=' + parentIndex + ' dnNodeImp=' + dnNode.vsted[Imp] + ' parentImp=' + pq[parentIndex].vsted[Imp]);
                                if (dnNode.vsted[Imp] < pq[parentIndex].vsted[Imp]) {
                                    //logger.debug(`[${process.pid}]` + ' bubble up switch before=' + pq[parentIndex].id + ',' + pq[index].id);
                                    pq[index] = pq[parentIndex];
                                    pq[parentIndex] = dnNode;
                                    dnNode.pqindex = parentIndex;
                                    pq[index].pqindex = index;
                                    //logger.debug(`[${process.pid}]` + ' bubble up switch after=' + pq[parentIndex].id + ',' + pq[index].id);
                                    index = parentIndex;
                                    parentIndex = math.floor((index + 1) / 2 - 1);                                   
                                } else {
                                    break;
                                }
                            }
                            //log pq
                            var strpq = '';
                            for (var j = 0; j < pq.length; j++) {
                                if (strpq == '') {
                                    strpq = pq[j].id;
                                } else {
                                    strpq = strpq + ',' + pq[j].id;
                                }
                            }
                            logger.debug(`[${process.pid}]` + ' after bubble up priority queue=[' + strpq + ']');
                            
                            if (dnNode.id <= zonenum) { //track centriod nodes
                                cnt = cnt + 1;
                            }
                            dnNode.pnd = [currNode, dnNode, dnLink, tempTime, timePeriod, tempImp];                           
                            //logger.debug(`[${process.pid}]` + " ---Node=" + dnNode.pnd[0].id + ' dnNode=' + dnNode.pnd[1].id + ' time=' + dnNode.pnd[3] + ' tp=' + dnNode.pnd[4] + ' imp=' + dnNode.pnd[5]);                          
                        }
                    }                    
                }
                logger.debug(`[${process.pid}]` + ' after bubble up pq length=' + pq.length);
            }while (pq.length > 0 && cnt <= zonenum);
            
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
                        var skimImp = path[0][5];
                        var skimDist = 0;
                        var skimToll = 0;
                        var skimFFtime = 0;
                        
                        for (var j = 0; j <= path.length - 1; j++) {
                            var link = path[j][2];
                            skimDist = skimDist + parseFloat(link.dist);
                            skimToll = skimToll + link.toll[tp];
                            skimFFtime = skimFFtime + link.fftime[tp];
                        }
                        pathHash.set(key, path);
                        var skim = [];
                        skim.push(skimTime);
                        skim.push(skimDist);
                        skim.push(skimToll);
                        skim.push(skimFFtime);
                        skim.push(skimImp);
                        pathHash.set(key + ':skim', skim);
                        //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' sp dp ' + key + ' skim time ' + eNode.vsted[0] + ' imp=' + eNode.vsted[Imp] +
                        //    ' dist ' + skimDist + ' toll ' + skimToll + ' fftime ' + skimFFtime + ' pathlength ' + path.length);
                    } else {
                        //**zone path
                        pathHash.set(key, path);
                        //start log
                        //if (zonePair == '1-37') {
                        var strPath = '';
                        for (var i = 0; i <= path.length - 1; i++) {
                            strPath = path[i][1].id + ',' + strPath;
                        }
                        strPath = path[i - 1][0].id + ',' + strPath;
                        //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' zone sp ' + key + ' ' + strPath);
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
                    //var setDist = 7.2;
                    var scale = math.pow((par.choicemodel.scalestdlen / dist[0]), par.choicemodel.scalealpha);
                    var relia = par.choicemodel.timecoeff * par.choicemodel.reliacoeffratio * par.choicemodel.reliacoefftime *
                            ((timeFF[1] - time[1]) * math.pow(dist[1], (-1 * par.choicemodel.reliacoeffdist))
                            - (timeFF[0] - time[0]) * math.pow(dist[0], (-1 * par.choicemodel.reliacoeffdist)));
                    var utility = -1 * par.choicemodel.tollconst[tp - 1] - scale * (par.choicemodel.timecoeff * (time[0] - time[1])
                        + par.choicemodel.tollcoeff * toll[0] + relia);

                    probility = 1 / (1 + math.exp(utility));

                    //logger.debug('probility calculation: tollconst=' + par.choicemodel.tollconst[tp - 1] + ',scalesdlen=' + par.choicemodel.scalestdlen
                    //    + ',scalealpha=' + par.choicemodel.scalealpha + ',timecoeff=' + par.choicemodel.timecoeff
                    //    + ',tollcoeff=' + par.choicemodel.tollcoeff + ',reliacoeffratio=' + par.choicemodel.reliacoeffratio
                    //    + ',reliacoefftime=' + par.choicemodel.reliacoefftime + ',reliacoeffdist=' + par.choicemodel.reliacoeffdist);
                    logger.debug(`[${process.pid}]` + ' iter ' + iter + ' distTl=' + math.round(dist[0], 2) + ',distTf=' + math.round(dist[1], 2) + ',timeTl=' + math.round(time[0], 2)
                        + ',timeTf=' + math.round(time[1], 2) + ',timeFFTl=' + timeFF[0] + ',timeFFTf=' + timeFF[1] + ',Toll=' + math.round(toll[0], 2)
                        + ',utility=' + math.round(utility, 2) + ',probility=' + math.round(probility, 4));
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
                //logger.debug(`[${process.pid}]` + ' iter' + iter + ' mv load link ' + pathItem[2].aNd.id + '-' + pathItem[2].bNd.id + ' mode=' + modenum + ' tp=' + pathItem[4] + ' vol=' + vol);                      
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
            redisClient.publish('job_status', 'sp_mv_zones');
            async.series([
                function (callback) {
                    //update time and volume if iter >= 2
                    redisClient.select(9);
                    redisClient.get('iter', function (err, result) {
                        iter = parseInt(result);
                        if (iter >= 2) {  //iter>=2           
                            redisClient.select(3);
                            redisClient.mget(arrLink, function (err, result) {
                                var i = 0;
                                async.during(function (callback) {
                                    return callback(null, i < arrLink.length);
                                },
                                    function (callback) {
                                        var time = [];
                                        var volmsa = [];
                                        var linkid = '';
                                        do {
                                            linkid = arrLink[i].split(':')[0];                                          
                                            time.push(parseFloat(result[i].split(',')[0]));
                                            volmsa.push(parseFloat(result[i].split(',')[1]));
                                            i = i + 1;
                                        } while (i < arrLink.length && linkid == arrLink[i].split(':')[0])
                                        var link = allLinks.get(linkid);
                                        link.time = time;
                                        link.volmsa = volmsa;
                                        //logger.info(`[${process.pid}]` + ' iter' + iter + ' update toll ' + arrTollLink[i] + ' toll=' + link.toll[i + 1]); 
                                        callback();
                                    },
                                    function (err) {
                                        callback();
                                    }); 
                            });
                        } else {
                            callback();
                        }
                    });
                },                
                function (callback) {
                    //update toll
                    redisClient.select(9);
                    redisClient.get('iter', function (err, result) {
                        iter = parseInt(result);
                        if (iter >= 2) {  //iter>=2
                            var i = 0;
                            redisClient.select(7);
                            redisClient.mget(arrTollLink, function (err, result) {  
                                async.during(function (callback) {
                                        return callback(null, i < arrTollLink.length);
                                    },
                                    function (callback) {
                                        var link = allLinks.get(arrTollLink[i].split(':')[0])
                                        link.toll[i + 1] = parseFloat(result[i]);
                                        //logger.info(`[${process.pid}]` + ' iter' + iter + ' update toll ' + arrTollLink[i] + ' toll=' + link.toll[i + 1]);
                                        i = i + 1;
                                        callback();
                                    },
                                    function (err) {
                                        callback();
                                    }
                                );                               
                            });
                        } else {
                            callback();
                        }
                    });
                },
                function (callback) {
                    //sp for decision points 
                    //logger.info(`[${process.pid}]` + ' iter' + iter + ' publish decison points sp_done');
                    var cnt = 0;
                    var spZone = 0;
                    var link = allLinks.get('4-2');
                    async.during(           //loop until jobs are done
                        //test function
                        function (callback) {
                            redisClient.select(6);
                            redisClient.lpop('task', function (err, result) {
                                //logger.debug(`[${process.pid}]` + ' ***iter' + iter + ' get sp zone task ' + result);
                                if (result == null) {
                                    redisClient.INCR('cnt', function (err, result) {
                                        if (result == par.numprocesses) {
                                            logger.info(`[${process.pid}]` + ' iter' + iter + ' --- sp and mv done ---');
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
                                                mvTasks.push(spZone + ':' + tsk + ':ct');  //tsk = 'vol:ct'
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
                            logger.info(`[${process.pid}]` + ' iter' + iter + ' sp and mv processed total of ' + cnt);
                            callback();
                        });
                }
            ]);
        }    
        else if (message == 'linkvolredis') {
            //write vol to redis
            redisClient.select(2);
            multi = redisClient.multi();
            multi.select(2);
            var cnt = 0;
            var tot = allLinks.count();
            if (tot == 0) {
               redisClient.publish('job_status', 'sp_mv_done');
            }
            allLinks.forEach(function (value, key) {
                //key in db=aNode.id + '-' + bNode.id + ':' + tpNew + ':' + mode + ':' + iter;                
                cnt = cnt + 1;
                for (var i = 0; i <= par.modes.length; i++) {
                    for (var j = 0; j <= par.timesteps; j++) {
                        var v = value.vol[i][j];
                        if (v > 0) {
                            var dbKey = key + ':' + j + ':' + par.modes[i] + ':' + iter;
                            //logger.debug(`[${process.pid}]` + ' iter' + iter + ' write vol to db dbkey=' + dbKey + ' v=' + v);
                            multi.INCRBYFLOAT(dbKey, v, function (err, result) {                               
                            });                          
                        }
                    }
                }
            });
            multi.exec(function (err, results) {
                redisClient.INCR('cntNode', function (err, result) {
                    if (parseInt(result) == par.numprocesses) {
                        redisClient.set('cntNode', 0);
                        logger.info(`[${process.pid}]` + ' iter' + iter + ' --- write vol to redis done ---');
                        redisClient.publish('job_status', 'sp_mv_done');
                    }
                });
            });
        }
        //update link volume and time
        else if (message = "linkupdate") {  //update one link for all time steps
            var link = '';
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
                                    logger.info(`[${process.pid}]` + ' iter' + iter + ' --- linkupdate done ---');
                                    redisClient.publish('job_status', 'linkupdate_done');
                                }
                            });
                        } else {
                            link = result;  //'anode-bnode'
                        }
                        return callback(null, result != null); //loop all tasks
                    });
                    redisClient.publish('job_status', 'bar_tick:8');
                },
                function (callback) {
                    var vol = [];
                    multi = redisClient.multi();
                    multi.select(2);
                    async.series([
                        //MSA Volume
                        function (callback) {
                            var i = 0;
                            async.during(   //loop time steps
                                function (callback) {
                                    i = i + 1;
                                    return callback(null, i <= par.timesteps);
                                },
                                function (callback) {
                                    var tempvol = 0;
                                    async.eachSeries(par.modes,
                                        function (md, callback) {   //loop modes
                                            var lastIter = iter - 1;
                                            var key1 = link + ":" + i + ":" + md + ":" + iter;
                                            if (iter > 1) {
                                                var key2 = link + ":" + i + ":" + md + ":" + lastIter;
                                                //logger.debug(`[${process.pid}] ` + ' iter=' + iter + ' link ' + key2);
                                                scriptManager.run('msa', [key1, key2, iter], [], function (err, result) { //return v_current, v_previous, v_msa                                       
                                                    //tempvol = tempvol + parseFloat(result.split(',')[2]);
                                                    //logger.debug(`[${process.pid}] ` + ' iter=' + iter + ' link ' + key1 + ' MSA result ' + result);
                                                });
                                            }
                                            multi.get(key1, function (err, result) {
                                                if (result != null) {
                                                    tempvol = tempvol + parseFloat(result);                                              
                                                    //logger.debug(`[${process.pid}] ` + 'iter=' + iter + ' get currIter vol after MSA ' + key1 + ' vol=' + tempvol);
                                                }
                                            });
                                            callback(null, true);
                                        },
                                        function (err, result) {
                                            vol.push(tempvol);
                                            callback();
                                        });
                                },
                                function (err) {
                                    multi.exec(function (err, result) {
                                        redisClient.select(2);
                                        redisClient.get(link + ':1:SOV:' + iter, function (err, result) {
                                            //logger.debug(`[${process.pid}] ` + 'iter=' + iter + ' MSA vol ' + link + ':1:SOV:' + iter + ' ' + result);
                                        });
                                        callback();
                                    });
                                });  //end loop time steps
                        },
                        //moving average volume

                        //congested time
                        function (callback) {
                            var lnk = allLinks.get(link);
                            var vht = [];
                            var vhtPre = [];
                            multi = redisClient.multi();
                            async.series([
                                //set congested time to redis
                                function (callback) {
                                    for (var i = 1; i <= par.timesteps; i++) {
                                        if (vol[i - 1] != null) {   //no traffic
                                            var cgTime = lnk.fftime[i] * (1 + lnk.alpha * math.pow(vol[i - 1] * 4 / lnk.cap, lnk.beta));
                                            vht.push(vol[i - 1] * cgTime / 60);
                                        } else {
                                            var cgTime = lnk.time[i];
                                            vht.push(0);
                                        }
                                        //logger.debug(`[${process.pid}] ` + 'iter=' + iter + ' set time ' + link + ':' + i + ' vol=' + vol[i - 1] + ' time=' + cgTime);
                                        multi.select(3);
                                        multi.set(link + ':' + i, cgTime + ',' + vol[i - 1]);                                      
                                    }
                                    multi.exec(function (err, result) {
                                        callback();
                                    });
                                },
                                //set vht to redis
                                function (callback) {
                                    for (var i = 1; i <= par.timesteps; i++) {
                                        multi.select(4);
                                        multi.getset(link + ':' + i, vht[i - 1], function (err, result) {
                                            if (result != null) {
                                                vhtPre.push(parseFloat(result));
                                            } else {
                                                vhtPre.push(0);
                                            }
                                        });
                                    }
                                    multi.exec(function (err, result) {
                                        callback();
                                    });
                                },
                                //set vht diff to redis
                                function (callback) {
                                    for (var i = 1; i <= par.timesteps; i++) {
                                        multi.select(4);
                                        var diff = vht[i - 1] - vhtPre[i - 1];
                                        multi.RPUSH('vht', vht[i - 1] + ',' + diff, function (err, result) {

                                        });
                                    }
                                    multi.exec(function (err, result) {
                                        callback();
                                    });
                                }],
                                function (err) {
                                    callback();
                                });
                        },
                        function(callback){
                            //update toll
                            var lnk = allLinks.get(link);
                            if (lnk.tolltype == 1) {    //EL
                                multi.select(7);
                                var toll = [];
                                for (var i = 1; i <= par.timesteps; i++) {
                                    var toll = par.tollpolicy.mintoll + (par.tollpolicy.maxtoll - par.tollpolicy.mintoll) *  math.pow(vol[i - 1] * 4 / lnk.cap, par.tollpolicy.exp);
                                    multi.set(link + ':' + i, toll);
                                    //logger.info(`[${process.pid}]` + ' iter' + iter + ' set toll vol=' + vol[i - 1] + ' toll=' + toll);
                                }
                                multi.exec(function (err, result) {
                                    callback();
                                });
                            } else {
                                callback();
                            }
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