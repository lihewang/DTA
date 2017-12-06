//Starting node
var hashMap = require('hashmap');
var fs = require('fs');
var csv = require('fast-csv');
var redis = require('ioredis');
var async = require('async');
var math = require('mathjs');
var cluster = require('cluster');
var log4js = require('log4js');
var os = require('os');

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
var pathLenHash = new hashMap();
var newNode = [];                   //new node id to network node id
var arrLink = [];
var arrTollLink = [];
var mvTasks = [];
var dps = [];
var tltfPath = [null, null];
var iter = 0;
var cntPackets = 0;
var pathStream = null;

var redisIP = "redis://127.0.0.1:6379";
var appFolder = "./app";
var paraFile = appFolder + "/parameters_95.json";

var redisClient = new redis(redisIP); 
var redisJob = new redis(redisIP);
var par = JSON.parse(fs.readFileSync(paraFile));
var linkFile = appFolder + '/' + par.linkfilename;
var nodeFile = appFolder + '/' + par.nodefilename;
var startTimeStep = 1;
var endTimeStep = par.timesteps;
var logTollFile = './output' + '/' + par.log.tollfilename;

//********node reader********
var rdnd = function Readcsv(callback) {
    var stream = fs.createReadStream(nodeFile);
    var csvStream = csv({ headers: true })
        .on("data", function (data) {
            var id = data['N'];
            id = id.trim();
            var pRcd = [];
            if (data['DTA_Type'] == par.dcpnttype) {
                dps.push(id);
                pRcd = new hashMap();
            }
            var nd = {
                id: parseInt(id), dnd: [], dnlnk: [], vsted: [999, 999, 999], stled: 0, pqindex: 0, pnd: null, type: data['DTA_Type'], segnum: data['SEGNUM'],
                prohiblnk: data['Prohibit_Links'], tollshare: pRcd, tollDist: 0
            };
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
            }           
            if (parseInt(data['TOLLTYPE']) == 1) {
                arrTollLink.push(a + '-' + b);
            }
            var aNode = allNodes.get(parseInt(data['A']));
            var bNode = allNodes.get(parseInt(data['B']));
            if (bNode == null) {
                logger.debug(`[${process.pid}]` + ' link aNode=' + data['A'] + ' link bNode=' + data['B']);
            }
            if (parseInt(data['TOLLTYPE']) == 0) {
                arrToll = [];
            }
            allLinks.set(a + '-' + b, {
                aNd: aNode, bNd: bNode, time: arrTime, fftime: arrffTime, toll: arrToll, tolltype: parseInt(data['TOLLTYPE']), dist: parseFloat(data['Dist']), type: parseInt(data['Ftype']), cap: parseFloat(data['Cap']),
                tollsegnum: parseInt(data['TOLLSEGNUM']), alpha: parseFloat(data['Alpha']), beta: parseFloat(data['Beta']), vol: [], volmsa:[]
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
        for (var i = 0; i < par.modes.length; i++) {    //0 based
            var jarr = []
            for (var j = 0; j <= par.timesteps; j++) {  //1 based
                jarr.push(0);
            }
            arrVol.push(jarr);
        }
        value.vol = arrVol;
        arrLink.push(key); 
    });
    callback(null, "init links");
}
//********find time dependent shortest path and write results to redis********
var sp = function ShortestPath(zone, zonenum, tp, mode, pathType, iter, zj, callback) {
    //if (tp == 96) {
    //    logger.debug(`[${process.pid}]` + ' iter' + iter + ' ***SP for zone ' + zone + ' zj=' + zj + ', tp ' + tp + ', mode ' + mode + ', pathType ' + pathType);
    //}
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
                //settle node
                currNode = pq.splice(0, 1)[0];     //remove root
                currNode.stled = 1; 
                //if (zj == 77) {
                //    logger.debug(`[${process.pid}]` + ' Iter' + iter + ' currNode id=' + currNode.id);
                //}
                if (pq.length > 0) {
                    pq.unshift(pq.splice(pq.length - 1, 1)[0]);      //move last node to the root
                    pq[0].pqindex = 0;

                    //bubble down
                    var index = 0;
                    while ((index + 1) * 2 <= pq.length) {          //left child exist
                        var switchIndex = (index + 1) * 2 - 1;
                        var switchNode = pq[switchIndex];   //left child                        
                        if ((index + 1) * 2 + 1 <= pq.length) {     //right child exist
                            var rightChild = pq[(index + 1) * 2];
                            if (rightChild.vsted[Imp] < switchNode.vsted[Imp]) {
                                switchNode = rightChild;
                                switchIndex = (index + 1) * 2;
                            }
                        }
                        if (switchNode.vsted[Imp] < pq[index].vsted[Imp]) {
                            //switch
                            pq[switchIndex] = pq[index];   //child = parent
                            pq[index] = switchNode;        //parent = child
                            switchNode.pqindex = index;     //child index
                            pq[switchIndex].pqindex = switchIndex;  //parent index
                            index = switchIndex;
                            //if (index > pq.length - 1) {
                            //    logger.debug(`[${process.pid}]` + ' 228 pq index error! zone=' + zone + ' zj=' + zj + ' pathType=' + pathType + ' index=' + index + ' pq length=' + pq.length + ' dnNode=' + dnNode.id)
                            //}
                        } else {
                            break;
                        }
                    }
                }                                              
                //log pq
                //if (zone == 21463 && zj == 13 && pathType == 'tf') {
                //    var strpq = '';
                //    for (var i = 0; i < pq.length; i++) {
                //        if (strpq == '') {
                //            strpq = pq[i].id;
                //        } else {
                //            strpq = strpq + ',' + pq[i].id;
                //        }
                //    }
                //    logger.debug(`[${process.pid}]` + ' Iter' + iter + ' root node=' + currNode.id + ' after bubble down priority queue=[' + strpq + ']');
                //}
                //end log

                //skip centriod 
                if (currNode.id <= zonenum && currNode.id != zone) {
                    continue;
                }
                //get new frontier nodes 
                var dnNodes = currNode.dnd;  
                for (i = 0; i < dnNodes.length; i++) {
                    var dnNode = dnNodes[i];
                    var dnLink = currNode.dnlnk[i];
                    var Linktp = dnLink.type;
                    //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' ---sp currNode=' + currNode.id + ' dnNodes Cnt=' + dnNodes.length + ' dnNode=' + dnNode.id + ' dnlink=' + dnLink.aNd.id + '-' + dnLink.bNd.id);
                    //ban links for dp
                    if (cnt == 1) {
                        if (pathType == 'tf' && (Linktp == par.ftypeex || Linktp == par.ftypeexonrp)) {
                            //logger.debug(`[${process.pid}]` + " decision point tf path skip " + currNode.id + "-" + dnNode.id);
                            cnt = cnt + 1;
                            continue;
                        }
                        if (pathType == 'tl' && !(Linktp == par.ftypeex || Linktp == par.ftypeexonrp)) {
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
                        var timePeriod = math.floor(currNode.vsted[0] / 15) + parseInt(tp);    //get time period
                        if (timePeriod > par.timesteps) {
                            timePeriod = timePeriod - par.timesteps;
                        }
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
                            var alltoll = fixtoll + mintoll;    //ct path toll               
                        } else {
                            var alltoll = fixtoll;              //tl and tf path use fixtoll only
                        }
                        //get perceived time factor
                        var perceiveFactor = 1;
                        if (pathType != 'ct' && iter>1) {
                            perceiveFactor = 1 + (par.choicemodel.perceivetimemaxvc - 1) / (1 + math.exp(-1 * par.choicemodel.perceivetimesteep *
                                (dnLink.volmsa[timePeriod] * 4 / dnLink.cap - par.choicemodel.perceivetimemidvc)));                           
                        }
                        var tempTime = currNode.vsted[0] + dnLink.time[timePeriod];               //time
                        var tempImp = currNode.vsted[Imp] + dnLink.time[timePeriod] * perceiveFactor + alltoll / (par.choicemodel.timecoeff / par.choicemodel.tollcoeff);        //impedance
                        //if (iter == 6 && tp == 65 && zone == 11) {
                        //    logger.debug(`[${process.pid}]` + ' Iter' + iter + ' dnLinkTime=' + dnLink.time[timePeriod] + ' timePeriod=' + timePeriod +
                        //        ' tempTime=' + tempTime + ' tempImp=' + tempImp + ' toll=' + alltoll + ' dnNodeImp=' + dnNode.vsted[Imp] + ' perceivefct=' + perceiveFactor);
                        //}
                        if (tempImp < dnNode.vsted[Imp]) {
                            var index = 0;
                            //dnNode has not been checked before or update time when the path is shorter                             
                            if (dnNode.vsted[Imp] == 999) {
                                pq.push(dnNode);    //not visited
                                index = pq.length - 1;
                                dnNode.pqindex = index;
                                //if (dnNode.id == 114) {
                                //    logger.debug(`[${process.pid}]` + ' push in pq id=' + dnNode.id + ' pq length=' + pq.length);
                                //}
                            } else {
                                index = dnNode.pqindex
                                if (index > pq.length - 1) {
                                    logger.debug(`[${process.pid}]` + ' 320 pq index error! zone=' + zone + ' zj=' + zj + ' pathType=' + pathType + ' index=' + index + ' pq length=' + pq.length + ' dnNode=' + dnNode.id)
                                }
                            } 
                            dnNode.vsted[Imp] = tempImp;
                            dnNode.vsted[0] = tempTime;
                            
                            //bubble up in priority queue  
                            var parentIndex = math.floor((index + 1) / 2 - 1);                           
                            while (parentIndex >= 0) {   //has parent
                                if (pq[parentIndex] == null) {
                                    logger.debug(`[${process.pid}]` + ' bubble up parentIndex=' + parentIndex + ' index=' + index+ ' dnNodeImp=' + dnNode.vsted[Imp]);
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
                                }
                                if (dnNode.vsted[Imp] < pq[parentIndex].vsted[Imp]) {
                                    //logger.debug(`[${process.pid}]` + ' bubble up switch before=' + pq[parentIndex].id + ',' + pq[index].id);
                                    pq[index] = pq[parentIndex];
                                    pq[parentIndex] = dnNode;
                                    dnNode.pqindex = parentIndex;
                                    pq[index].pqindex = index;
                                    //logger.debug(`[${process.pid}]` + ' bubble up switch after=' + pq[parentIndex].id + ',' + pq[index].id);
                                    index = parentIndex;
                                    //if (index > pq.length - 1) {
                                    //    logger.debug(`[${process.pid}]` + ' 354 pq index error! zone=' + zone + ' zj=' + zj + ' pathType=' + pathType + ' index=' + index + ' pq length=' + pq.length + ' dnNode=' + dnNode.id)
                                    //}
                                    parentIndex = math.floor((index + 1) / 2 - 1);                                   
                                } else {
                                    break;
                                }
                            }
                                                    
                            if (dnNode.id <= zonenum) { //track centriod nodes
                                cnt = cnt + 1;
                            }
                            dnNode.pnd = [currNode, dnNode, dnLink, tempTime, timePeriod, tempImp]; 
                            //if (zone == 42424 && dnNode.pnd[4] == 1) {
                            //    logger.debug(`[${process.pid}]` + " Node=" + dnNode.pnd[0].id + ' dnNode=' + dnNode.pnd[1].id + ' time=' + dnNode.pnd[3] + ' tp=' + dnNode.pnd[4] + ' imp=' + dnNode.pnd[5]);                          
                            //}
                        }
                    }                    
                }         
            } while (pq.length > 0 && cnt <= zonenum && dnNode.id != zj);

            //put paths to hashtables
            for (var i = 1; i <= zonenum; i++) {
                var zonePair = zone + '-' + i;
                var path = [];
                var pNode = allNodes.get(i);
                var eNode = pNode;
                //create path
                while (pNode.pnd != null) {
                    path.push(pNode.pnd);
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
                        for (var j = path.length - 1; j >= 0; j--) {
                            var link = path[j][2];
                            skimDist = skimDist + parseFloat(link.dist);
                            if (pathType == 'tl') {
                                path[j][1].tollDist = skimDist;
                                //if (zone == 14) {
                                //    logger.debug(`[${process.pid}]` + ' Iter' + iter + ' node=' + path[j][1].id + ' dist=' + skimDist);
                                //}
                            }
                            if (link.tolltype == par.tolltype.dynamic || link.tolltype == par.tolltype.fix) {
                                skimToll = skimToll + link.toll[tp];            //including EL and fixed toll                                
                            }
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
                        //    ' dist ' + math.round(skimDist, 2) + ' toll ' + skimToll + ' fftime ' + math.round(skimFFtime, 2) + ' pathlength ' + path.length);
                    } else {
                        //**zone path
                        pathHash.set(key, path);
                    }
                    //start log
                    //if (iter == 1 && tp == 1 && zone == 100299 && i == 21) {
                    //    var strPath = '';
                    //    for (var j = 0; j <= path.length - 1; j++) {
                    //        strPath = path[j][1].id + ',' + strPath;
                    //    }
                    //    strPath = path[j - 1][0].id + ',' + strPath;
                    //    logger.debug(`[${process.pid}]` + ' Iter' + iter + ' zj=' + zj + ' zone sp ' + key + ' ' + strPath);
                    //} //end log
                } 
            }            
            callback();
        }],
        function (err) {            
            callback(null, 'SP for zone ' + zone + ', tp ' + tp + ', mode ' + mode + ', pathType ' + pathType);
        });
}

//********move vehicle********
var mv = function MoveVehicle(tp, zi, zj, pthTp, mode, vol, path, iter, zint, tsint, callback) {
    //if (tp == 96) {
    //    logger.debug(`[${process.pid}]` + ' ****iter ' + iter + ' mv start ' + zi + '-' + zj + ':' + tp + ':' + mode + ':' + pthTp + ' vol=' + vol + ' path ' + path.length);
    //}
    if (path == null) {
        callback(null, zi + '-' + zj + ':' + tp + ':' + mode);
    } else {
        var totTime = 0;
        var tpNew = tp;
        var keyValue = '';
        var firstNode = true; 
        var breakloop = false;
        var modenum = 0;
        for (var i = 0; i <= par.modes.length - 1; i++){
            if (mode == par.modes[i]) {
                modenum = i;
                break;
            }
        }
        //decision point paths should be kept for other packets
        if (pthTp != 'ct') {
            var pathCopy = [];
            for (var i = 0; i < path.length; i++) {
                pathCopy.push(path[i]);
            }
        } else {
            var pathCopy = path;
        }
        //loop links in the path
        var pathItem = pathCopy.pop();
        while (pathItem != null && !breakloop) {
            var aNode = pathItem[0];
            if (aNode.type == par.dcpnttype && (!firstNode) && mode == "SOV" && vol >= par.minpacketsize) {
                //**decision point (not the start node in path)**
                var ptype = ['tl', 'tf'];
                var time = [0, 0]; 
                var dist = [0, 0];
                var toll = [0, 0];
                var timeFF = [0, 0];
                var probability = 0; 
                //get paths skims
                for (var i = 0; i <= 1; i++){       //tl (i=0) and tf (i=1)
                    var key = tpNew + ":" + aNode.id + '-' + zj + ":" + mode + ":" + ptype[i];
                    var log = false;                    
                    tltfPath[i] = pathHash.get(key);
                    if (tltfPath[i] == null) {  //tltf path is not built before
                        //build dp sp  
                        //logger.debug(`[${process.pid}]` + ' iter' + iter + ' dp build sp=' + key);
                        sp(parseInt(aNode.id), par.zonenum, tpNew, mode, ptype[i], iter, zj, function (err, result) {                            
                            log = true;
                            //tltfPath[i] = pathHash.get(key);
                            if (pathHash.get(key) == null) {                                
                                dist[i] = 9999;
                            } else {
                                var pathSkim = pathHash.get(key + ':skim');
                                time[i] = pathSkim[0];
                                dist[i] = pathSkim[1];
                                toll[i] = pathSkim[2];
                                timeFF[i] = pathSkim[3];
                            }
                        });     
                    } else {
                        var pathSkim = pathHash.get(key + ':skim');
                        time[i] = pathSkim[0];
                        dist[i] = pathSkim[1];
                        toll[i] = pathSkim[2];
                        timeFF[i] = pathSkim[3];
                        //if (aNode.id == 21463 && zj == 13 && ptype[i] == 'tf') {
                        //    logger.debug(`[${process.pid}]` + ' iter ' + iter + ' dp get path exist key=' + key + ' toll skim=' + toll[0] + ' ' + toll[1]);
                        //}
                    } 
                }
                if (dist[0] > dist[1] * parseFloat(par.distmaxfactor)) {
                    probability = 0;
                    //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' probability=0');
                } else if (dist[1] > dist[0] * parseFloat(par.distmaxfactor)) {
                    probability = 1;
                    //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' probability=1 distTf=' + distTf + ' distTl=' + distTl);
                } else {
                    //get the distance to the common node
                    var DistCommon = pathLenHash.get(tpNew + ":" + aNode.id + "-" + zj + ":" + mode + ":tl");
                    if (DistCommon == null) {
                        var pathTl = pathHash.get(tpNew + ":" + aNode.id + "-" + zj + ":" + mode + ":tl");
                        var pathTf = pathHash.get(tpNew + ":" + aNode.id + "-" + zj + ":" + mode + ":tf");
                        if (pathTl != null && pathTf != null) {                            
                            var idxTf = 1;
                            for (var idxTl = 1; idxTl < pathTl.length; idxTl++) {
                                //if (aNode.id == 14 && tpNew == 1) {
                                //    logger.debug(`[${process.pid}]` + ' iter ' + iter + ' find ' + tpNew + ":" + aNode.id + "-" + zj + ":" + mode +
                                //        ' tlNode=' + pathTl[idxTl][0].id + ' tfNode=' + pathTf[idxTf][0].id);
                                //}
                                if (pathTl[idxTl][0].id == pathTf[idxTf][0].id) {                                   
                                    idxTf = idxTf + 1;
                                    if (idxTf == (pathTf.length - 1)) {
                                        DistCommon = pathTl[idxTl][0].tollDist;
                                        break;
                                    }
                                } else {
                                    DistCommon = pathTl[idxTl - 1][0].tollDist;
                                    //if (aNode.id == 14) {
                                    //    logger.debug(`[${process.pid}]` + ' iter ' + iter + ' ' + tpNew + ":" + aNode.id + "-" + zj + ":" + mode + ' cmNode=' + pathTl[idxTl - 1][0].id + ' DistCommon=' + DistCommon);
                                    //}
                                    break;
                                }
                            }
                        }
                        pathLenHash.set(tpNew + ":" + aNode.id + "-" + zj + ":" + mode + ":tl", DistCommon);
                        //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' ' + tpNew + ":" + aNode.id + "-" + zj + ":" + mode + ' DistCommon=' + DistCommon);
                    }

                    var scale = math.pow((par.choicemodel.scalestdlen / DistCommon), par.choicemodel.scalealpha);
                    var relia = par.choicemodel.timecoeff * par.choicemodel.reliacoeffratio * par.choicemodel.reliacoefftime *
                            ((timeFF[1] - time[1]) * math.pow(dist[1], (-1 * par.choicemodel.reliacoeffdist))
                            - (timeFF[0] - time[0]) * math.pow(dist[0], (-1 * par.choicemodel.reliacoeffdist)));
                    var utility = -1 * par.choicemodel.tollconst[tp - 1] - scale * (par.choicemodel.timecoeff * (time[0] - time[1])
                        + par.choicemodel.tollcoeff * (toll[0] - toll[1]) + relia);

                    probability = 1 / (1 + math.exp(utility));
                    //log file
                    var pkey = zj + ':' + tpNew + ':' + mode;
                    var pRcd = aNode.tollshare;
                    if (!pRcd.has(iter + ':' + pkey)) {                             //hasn't been checked in this iteration  
                        if (log) {
                            var arrLog = [[]];
                            arrLog.push([iter, tpNew, aNode.id, zj, math.round(dist[0], 2), math.round(dist[1], 2), DistCommon, math.round(time[0], 2), math.round(time[1], 2), timeFF[0], timeFF[1], toll[0], toll[1],
                                math.round(utility, 2), math.round(probability, 4)]);
                            if (par.log.dpnode.findIndex(k => k == aNode.id) != -1) {
                                csv.writeToStream(fs.createWriteStream('./output' + '/' + par.log.dpnodefilename, { 'flags': 'a' }), arrLog, { headers: true });
                            }
                        }
                        pRcd.set(iter + ':' + pkey, probability);
                    }                    
                    //logger.debug('probability calculation: tollconst=' + par.choicemodel.tollconst[tp - 1] + ',scalesdlen=' + par.choicemodel.scalestdlen
                    //    + ',scalealpha=' + par.choicemodel.scalealpha + ',timecoeff=' + par.choicemodel.timecoeff
                    //    + ',tollcoeff=' + par.choicemodel.tollcoeff + ',reliacoeffratio=' + par.choicemodel.reliacoeffratio
                    //    + ',reliacoefftime=' + par.choicemodel.reliacoefftime + ',reliacoeffdist=' + par.choicemodel.reliacoeffdist);
                    //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' dp id=' + aNode.id + ' distTl=' + math.round(dist[0], 2) + ',distTf=' + math.round(dist[1], 2) + ',timeTl=' + math.round(time[0], 2)
                    //    + ',timeTf=' + math.round(time[1], 2) + ',timeFFTl=' + timeFF[0] + ',timeFFTf=' + timeFF[1] + ',Toll=' + math.round(toll[0], 2)
                    //    + ',utility=' + math.round(utility, 2) + ',probability=' + math.round(probability, 4));
                }
                var splitVol = [0, 0];
                if (probability == 0) {
                    splitVol[1] = vol;
                } else if (probability == 1) {
                    splitVol[0] = vol;
                } else {
                    splitVol[0] = vol * probability;
                    splitVol[1] = vol * (1 - probability);
                }  
                if (splitVol[0] < par.minpacketsize) {
                    splitVol[0] = 0;
                    splitVol[1] = vol;
                }
                if (splitVol[1] < par.minpacketsize) {  //if both less than min packet size, use toll path
                    splitVol[0] = vol;
                    splitVol[1] = 0;
                }
                for (var i = 0; i <= 1; i++) {
                    if (splitVol[i] > 0) {
                        mvTasks.push(aNode.id + ':' + zj + ':' + splitVol[i] + ':' + ptype[i] + ':' + tpNew + ':' + zint + ':' + tsint);
                        cntPackets = cntPackets + 1;
                        //if (iter == 2) {
                        //    logger.debug(`[${process.pid}]` + ' iter' + iter + ' add task ' + aNode.id + ':' + zj + ':' + splitVol[i] + ':' + ptype[i] + ':' + tpNew + ' cntPackets=' + cntPackets);
                        //}
                    }
                }
                cntPackets = cntPackets - 1;
                breakloop = true;
            } else {  
                //not decision point
                pathItem[2].vol[modenum][pathItem[4]] = pathItem[2].vol[modenum][pathItem[4]] + vol;
                firstNode = false;
                var paths = par.log.path.split(',');     // zi,zj,timestep          
                if (parseInt(paths[0]) == zint && parseInt(paths[1]) == zj && parseInt(paths[2]) == tsint) {
                    pathStream.write(iter + ',' + tpNew + ',' + pathItem[0].id + ',' + pathItem[1].id + ',' + vol + os.EOL);
                }
            }
            pathItem = pathCopy.pop();
        } 
        if (!breakloop) {
            cntPackets = cntPackets - 1;
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
    redisClient.publish('job_status', 'worker_ready');

    //process jobs
    redisJob.on('message', function (channel, message) {
        //***sp and mv***       
        if (message == 'sp_mv') {
            //log path file
            var logPathFile = './output' + '/' + par.log.pathfilename;
            pathStream = fs.createWriteStream(logPathFile, { 'flags': 'a' });

            initLink(function (err, result) {
                //logger.debug(`[${process.pid}]` + ' ##### ' + result);
            });
            var spZone = 0;
            var timeStep = 0;
            var mode = '';
            var pathType = '';  //ct,tl,tf
            var hasTask = true;

            //redisClient.publish('job_status', 'sp_mv_zones');
            async.series([
                function (callback) {
                    redisClient.select(9);
                    redisClient.get('iter', function (err, result) {
                        iter = parseInt(result);
                        if (iter >= 2) {  //iter>=2   
                            redisClient.get('startTimeStep', function (err, result) {
                                startTimeStep = parseInt(result);
                                redisClient.get('endTimeStep', function (err, result) {
                                    endTimeStep = parseInt(result);
                                    callback();
                                });
                            });
                        } else {
                            callback();
                        }
                    });
                },
                function (callback) {
                    //update time and volume if iter >= 2
                    redisClient.select(9);
                    redisClient.get('iter', function (err, result) {
                        iter = parseInt(result);
                        if (iter >= 2) {  //iter>=2           
                            redisClient.select(3);
                            async.each(arrLink, function (linkid, callback) {
                                redisClient.get(linkid, function (err, result) {                                   
                                    var link = allLinks.get(linkid);
                                    var arrResult = result.split(':');
                                    //logger.info(`[${process.pid}]` + ' iter' + iter + ' link=' + linkid + ' result=' + arrResult.length);
                                    for (var j = 0; j < par.timesteps; j++) {
                                        var time = parseFloat(arrResult[j].split(',')[0]);
                                        var volmsa = parseFloat(arrResult[j].split(',')[1]);
                                        link.time[j + 1] = time;
                                        link.volmsa[j + 1] = volmsa;
                                    }
                                    callback();
                                });
                            }, function (err) {
                                callback();
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
                            async.each(arrTollLink, function (linkid, callback) {
                                var link = allLinks.get(linkid);
                                redisClient.get(linkid, function (err, result) {
                                    var arrResult = result.split(',');
                                    //logger.info(`[${process.pid}]` + ' iter' + iter + ' link=' + linkid + ' result=' + arrResult.length);
                                    for (var j = 0; j < par.timesteps; j++) {
                                        link.toll[j + 1] = parseFloat(arrResult[j]);;
                                    }
                                    callback();
                                })                                
                            }, function (err) {
                                callback();
                            });                                            
                        } else {
                            callback();
                        }
                    });
                },
                function (callback) {
                    var cnt = 0;
                    var spZone = 0;
                    async.during(           //loop until jobs are done
                        //test function
                        function (callback) {
                            redisClient.select(6);
                            redisClient.lpop('task', function (err, result) {
                                //logger.debug(`[${process.pid}]` + ' ***iter' + iter + ' get sp zone task ' + result);
                                if (result == null) {
                                    redisClient.incr('cnt', function (err, result) {
                                        logger.info(`[${process.pid}]` + ' iter' + iter + ' processor ' + result + ' sp and mv processed total of ' + cnt);
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
                            async.series([
                                function (callback) {
                                    //**build sp
                                    sp(parseInt(spZone), par.zonenum, timeStep, mode, pathType, iter, 0, function (err, result) {
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
                                                mvTasks.push(spZone + ':' + tsk + ':ct' + ':' + timeStep + ':' + spZone + ':' + timeStep);  //tsk = 'zj:vol'
                                            });
                                            cntPackets = mvTasks.length;
                                        }
                                        callback();
                                    });
                                },
                                function (callback) {
                                    //move all tasks
                                    var task = '';
                                    async.during(
                                        function (callback) {
                                            task = mvTasks.pop();   //task = 'zi:zj:vol:pathtype:timestep'
                                            //logger.info(`[${process.pid}]` + ' iter' + iter + ' mv task=' + task);
                                            callback(null, task != null && cntPackets > 0);
                                        },
                                        function (callback) {
                                            var arrTask = task.split(':');
                                            var zi = arrTask[0];
                                            var zj = arrTask[1];   //destination node
                                            var vol = arrTask[2];
                                            var ptp = arrTask[3];
                                            var ts = arrTask[4];
                                            var zint = arrTask[5];
                                            var tsint = arrTask[6];
                                            //zone node task                                       
                                            var path = pathHash.get(ts + ":" + zi + "-" + zj + ":" + mode + ":" + ptp);
                                            if (path == null) {
                                                //logger.info(`[${process.pid}]` + ' iter' + iter + ' mv path=null ' + timeStep + ":" + zi + "-" + zj + ":" + mode + ":" + ptp);
                                                callback();
                                            } else {
                                                //**move vehicles                                                
                                                mv(ts, zi, zj, ptp, mode, parseFloat(vol), path, iter, zint, tsint, function (err, result) {
                                                    //logger.info(`[${process.pid}]` + ' iter' + iter + ' zone moved ' + task);
                                                    callback();
                                                });
                                            }
                                        },
                                        function (err) {
                                            //logger.info(`[${process.pid}]` + ' iter' + iter + ' end move task=' + task + ' cntPackets=' + cntPackets);
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
                            callback();
                        });
                }
            ],
                function (err, result) {
                    pathStream.end();
                });
        }
        //***write vol to redis***
        else if (message == 'linkvolredis') {            
            redisClient.select(2);
            var i = 0;           
            async.during(
                function (callback) {                   
                    return callback(null, arrLink.length > 0);
                },
                function (callback) {
                    var key = arrLink[i] + ':' + iter;
                    var lnk = allLinks.get(arrLink[i]);
                    var strVol = '';
                    redisClient.getset(key, 'lock', function (err, result) {
                        if (result != 'lock') {
                            if (result == null) {
                                for (var j = 0; j < par.modes.length; j++) {
                                    for (var k = 1; k <= par.timesteps; k++) {
                                        if (k >= startTimeStep && k <= endTimeStep){
                                            if (strVol == '') {
                                                strVol = lnk.vol[j][k].toString();
                                            } else {
                                                strVol = strVol + ',' + lnk.vol[j][k].toString();
                                            }
                                        } else {
                                            if (strVol == '') {
                                                strVol = '0';
                                            } else {
                                                strVol = strVol + ',0';
                                            }
                                        }
                                    }
                                } 
                                
                            } else {
                                var vp = result.split(',');
                                var index = 0;
                                for (var j = 0; j < par.modes.length; j++) {
                                    for (var k = 1; k <= par.timesteps; k++) {
                                        if (k >= startTimeStep && k <= endTimeStep) {
                                            if (strVol == '') {
                                                strVol = (lnk.vol[j][k] + parseFloat(vp[index])).toString();
                                            } else {
                                                strVol = strVol + ',' + (lnk.vol[j][k] + parseFloat(vp[index])).toString();
                                            }
                                        } else {
                                            if (strVol == '') {
                                                strVol = vp[index];
                                            } else {
                                                strVol = strVol + ',' + vp[index];
                                            }
                                        }
                                        index = index + 1;
                                    }
                                }                                 
                            }
                            redisClient.set(key, strVol, function (err, result) {
                                arrLink.splice(i, 1);
                                i = i + 1;
                                if (i >= arrLink.length) {
                                    i = 0;
                                }
                                callback();
                            });
                        } else {
                            i = i + 1;
                            if (i >= arrLink.length) {
                                i = 0;
                            }
                            callback();
                        }
                    });                   
                },
                function (err) {
                    redisClient.incr('cntNode', function (err, result) {
                        if (parseInt(result) == par.numprocesses) {                           
                            logger.info(`[${process.pid}]` + ' iter' + iter + ' --- write vol to redis done --- ');
                            redisClient.publish('job_status', 'sp_mv_done');
                        }
                    });
                }
            );    
        }
        //***calculate MSA link volume and time***
        else if (message = "linkupdate") {  //update one link each time
            var link = '';
            var cnt = 0;            
            var stepSize = 0;  //MSA step size
            var tollStream = fs.createWriteStream(logTollFile, { 'flags': 'a' });
            //logger.info(`[${process.pid}]` + ' iter' + iter + ' update link vol');
            iter = parseInt(iter);
            async.during(
                //test function
                function (callback) {
                    redisClient.select(8);
                    redisClient.lpop('task', function (err, result) {
                        //logger.debug(`[${process.pid}] link update get task ` + result);
                        if (result == null) {
                            redisClient.incr('cnt', function (err, result) {
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
                    var volMode = []; 
                    var vol = [];
                    redisClient.select(2);
                    async.series([
                        //MSA Volume
                        function (callback) {
                            if (iter > 1) {
                                //weight factor for MSA
                                var d = par.weightmsa;                                
                                if (d == 0) {
                                    stepSize = 1 / iter;
                                } else if (d == 1) {
                                    stepSize = 2 / (iter + 1);
                                } else if (d == 2) {
                                    stepSize = 6 * iter / ((iter + 1) * (2 * iter + 1));
                                } else if (d == 3) {
                                    stepSize = 4 * iter / ((iter + 1) * (iter + 1));
                                } else if (d == 4) {
                                    stepSize = 30 * math.pow(iter, 3) / ((iter + 1) * (2 * iter + 1) * (3 * math.pow(iter, 2) + 3 * iter - 1));
                                }
                                var lastIter = iter - 1;
                                var key1 = link + ":" + iter;
                                var key2 = link + ":" + lastIter;
                                redisClient.get(key1, function (err, result) {
                                    var arrV_cur = result.split(',');
                                    redisClient.get(key2, function (err, result) {
                                        var arrV_pre = result.split(',');
                                        var strVol = '';
                                        for (var i = 0; i <= par.timesteps * par.modes.length - 1; i++) {
                                            var msaVol = parseFloat(arrV_pre[i]) * (1 - stepSize) + parseFloat(arrV_cur[i]) * stepSize;
                                            if (strVol == '') {
                                                strVol = msaVol.toString();
                                            } else {
                                                strVol = strVol + ',' + msaVol.toString();
                                            }
                                            volMode.push(msaVol);                                           
                                        }
                                        redisClient.set(key1, strVol, function (err, result) {
                                            callback();
                                        });
                                    });
                                });
                            } else {
                                var key1 = link + ":" + iter;
                                redisClient.get(key1, function (err, result) {
                                    volMode = result.split(','); 
                                    callback();
                                });                                                             
                            }
                        },
                        //moving average volume

                        //congested time
                        function (callback) {
                            var lnk = allLinks.get(link);
                            var vht = [];
                            var vhtPre = [];  
                            //add up vol by time steps
                            for (var i = 0; i <= par.timesteps - 1; i++) {
                                var tVol = 0;
                                for (var j = 0; j < par.modes.length; j++) {
                                    tVol = tVol + parseFloat(volMode[i + par.timesteps * j]);
                                }
                                vol.push(tVol);
                            }                            
                            async.series([
                                //set congested time to redis
                                function (callback) {
                                    redisClient.select(3);
                                    var strTemp = '';
                                    for (var i = 0; i <= par.timesteps - 1; i++) {
                                        if (vol[i] != 0) {   
                                            var cgTime = lnk.fftime[i + 1] * (1 + lnk.alpha * math.pow(vol[i] * 4 / lnk.cap, lnk.beta));
                                            vht.push(vol[i] * cgTime / 60);
                                        } else {    //no traffic
                                            var cgTime = lnk.time[i];
                                            vht.push(0);
                                        }
                                        if (strTemp == '') {
                                            strTemp = cgTime + ',' + vol[i];
                                        } else {
                                            strTemp = strTemp + ':' + cgTime + ',' + vol[i];
                                        }
                                                                                                                   
                                    }
                                    redisClient.set(link, strTemp, function (err, result) {
                                        callback();
                                    });                                    
                                },
                                //set vht to redis
                                function (callback) {
                                    var multi = redisClient.multi();
                                    var lnktime = [];
                                    //logger.debug(`[${process.pid}] ` + 'iter=' + iter + ' stime=' + startTimeStep + ' etime=' + endTimeStep);
                                    for (var i = 0; i < par.timesteps; i++) {
                                        lnktime.push(link + ':' + i);
                                    }
                                    async.series([
                                        function (callback) {
                                            //get previous iter vht
                                            redisClient.select(4);
                                            if (iter > 1) { 
                                                redisClient.mget(lnktime, function (err, result) {
                                                    for (var i = 0; i < result.length; i++) {
                                                        vhtPre.push(parseFloat(result[i]));
                                                        //logger.debug(`[${process.pid}] ` + 'iter=' + iter + ' link=' + link + ':' + i + ' vhtPre=' + result[i]);
                                                    }
                                                    callback();
                                                });
                                            } else {
                                                callback();
                                            }
                                        },
                                        function (callback) {
                                            //set current iter vht
                                            multi.select(4);
                                            for (var i = 0; i < par.timesteps; i++) {
                                                multi.set(link + ':' + i, vht[i], function (err, result) { });
                                                if (iter == 1) {
                                                    vhtPre.push(0);
                                                }
                                            }                                   
                                            multi.exec(function (err, result) {
                                                callback();
                                            });
                                        }], function (err) {
                                            callback();
                                        });
                                },
                                //set vht diff to redis
                                function (callback) {
                                    var multi = redisClient.multi();
                                    multi.select(10);
                                    for (var i = 0; i < par.timesteps; i++) {                                       
                                        var diff = math.round(vht[i] - vhtPre[i], 4);
                                        //if (i == 0) {
                                        //    logger.info(`[${process.pid}]` + ' iter' + iter + ' set vht to db10 ts=' + i + ' link=' + link + ' vht=' +
                                        //        vht[i] + ' vhtPre=' + vhtPre[i] + ' diff=' + diff);
                                        //}
                                        multi.rpush('vht' + i, vht[i] + ',' + diff, function (err, result) {

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
                        function (callback) {
                            //update toll
                            var lnk = allLinks.get(link);
                            var preToll = [];
                            redisClient.select(7);
                            if (lnk.tolltype == par.tolltype.dynamic) {    //EL toll link
                                var strToll = '';
                                async.series([function (callback) {
                                    if (iter > 1) { //MSA toll
                                        redisClient.get(link, function (err, result) {
                                            preToll = result.split(',');
                                            callback();
                                        });
                                    } else {
                                        callback();
                                    }
                                },
                                function (callback) {
                                    for (var i = 0; i < par.timesteps; i++) {
                                        var vc = vol[i] * 4 / lnk.cap;
                                        var toll = par.tollpolicy.mintoll + (par.tollpolicy.maxtoll - par.tollpolicy.mintoll) * math.pow(vc, par.tollpolicy.exp);
                                        toll = math.round(math.min(toll, par.tollpolicy.maxtoll), 2);
                                        if (iter > 1) {
                                            var msaToll = preToll[i] * (1 - stepSize) + toll * stepSize;
                                        } else {
                                            var msaToll = toll;
                                        }
                                        if (strToll == '') {
                                            strToll = msaToll.toString();
                                        } else {
                                            strToll = strToll + ',' + msaToll.toString();
                                        }
                                        if (par.log.toll == 1) {
                                            tollStream.write(iter + ',' + (i + 1) + ',' + lnk.aNd.id + ',' + lnk.bNd.id + ',' + math.round(vol[i], 2) + ',' +
                                                lnk.cap + ',' + math.round(vc, 2) + ',' + toll + ',' + msaToll + ',' + par.tollpolicy.exp + ',' + par.tollpolicy.mintoll + ',' +
                                                par.tollpolicy.maxtoll + os.EOL);
                                        }
                                    }
                                    redisClient.set(link, strToll, function (err, result) {
                                        callback();
                                    });
                                }],
                                function (err) {
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
                    tollStream.end();
                    logger.info(`[${process.pid}]` + ' iter' + iter + ' link update processed total of ' + cnt);
                });
        } else if (message == 'end') {
            process.exit(0); //End server
        }
    });
}