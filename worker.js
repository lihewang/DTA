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
var appFolder = "./app";
var paraFile = appFolder + "/parameters_sf.json";
var allLinks = new hashMap();       //link
var allNodes = new hashMap();
var pathHash = new hashMap();       //zone paths
var pathSkimHash = new hashMap();  
var pathLenHash = new hashMap();
var newNode = [];                   //new node id to network node id
var arrLink = [];
var arrTollLink = [];
var mvTasks = [];
var nodeList = [];
var dps = [];
var tltfPath = [null, null];
var iter = 0;
//var cntPackets = 0;
var pathStream = null;
var choiceStream = null;
var stats = {zonepath:0, dppath:0, zonepacket: 0, dppacket: 0};
var redisIP = "redis://127.0.0.1:6379";
var redisClient = new redis(redisIP); 
var redisJob = new redis(redisIP);
var par = JSON.parse(fs.readFileSync(paraFile));
var linkFile = appFolder + '/' + par.linkfilename;
var nodeFile = appFolder + '/' + par.nodefilename;
var startTimeStep = 1;
var endTimeStep = par.timesteps;


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
                id: parseInt(id), dnd: [], dnlnk: [], vsted: [999, 999, 999, 999, 0, 999], stled: 0, pqindex: 0, pnd: null, type: data['DTA_Type'], segnum: data['SEGNUM'],
                prohiblnk: data['Prohibit_Links'], tollshare: pRcd
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
//********init link********
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
var sp = function ShortestPath(zone, zonenum, tp, mode, pathType, iter, zj, totTime, callback) {
    //if (tp == 1) {
    //var tm2 = 0;
    //var tm3 = 0;    
    //logger.debug(`[${process.pid}]` + ' iter' + iter + ' ***SP for ' + tp + ':' + zone + '-' + zj + ':' + mode + ':' + pathType);
    //}
    //single node shortest path
    var path2 = null;
    var pathType2 = '';
    var arrPath2 = [];
    
    if (pathType == 'ct'){
        stats.zonepath = stats.zonepath + 1;
    } else {
        stats.dppath = stats.dppath + 1;
        if (pathType == 'tf') {
            pathType2 = 'tl';
        } else {
            pathType2 = 'tf';
        }
        path2 = pathHash.get(tp + ":" + zone + "-" + zj + ":" + mode + ":" + pathType2); 
        for (var j = 0; j < path2.length - 1; j++) {
            arrPath2.push(path2[j][1].id);            
        }
    }
    var ftypeBan = [];
        //get mode banning ftype          
        if (mode == "HOV") {
            ftypeBan = par.pathban.HOV;
        } else if (mode == "TRK") {
            ftypeBan = par.pathban.TRK;
        }
        //var tm1 = process.hrtime();
        for (var i = 0; i < nodeList.length; i++) {
            nodeList[i].vsted = [0, 0, 999, 0, 0, 0];    //time, time+fixtoll, perceived time, dist, toll, fftime
            nodeList[i].stled = 0;
            nodeList[i].pnd = null;
        }
        //logger.debug(`[${process.pid}]` + ' nanosec=' + process.hrtime(tm1)[1] / 1000000 + ' -- init nd');
        nodeList = [];
        var currNode = allNodes.get(zone);
        //track visited nodes and store time
        currNode.vsted[0] = totTime;
        currNode.vsted[2] = 0;  
        currNode.vsted[3] = 0;
        currNode.vsted[4] = 0;
        currNode.vsted[5] = 0;
        var pq = [];                        //priority queue with binary heap
        pq.push(currNode);
        nodeList.push(currNode);
        var cnt = 0;    //count visited zones
        var commonNode = null;
        var commonNodeIndex = -1;
        var vot = par.choicemodel.timecoeff / par.choicemodel.tollcoeff;
        var loopcnt = 0;
            
        tm2 = process.hrtime();
        //visit nodes            
        do {               
            //settle node
            //currNode = pq.splice(0, 1)[0];     //remove root
            currNode = pq.shift();     //remove root
            currNode.stled = 1;                 
            if (currNode.id <= zonenum) { //track centriod nodes                    
                cnt = cnt + 1;
            }
            //if (tp == 1) {
            //    logger.debug(`[${process.pid}]` + ' Iter' + iter + ' centriodCnt=' + cnt + ' currnode=' + currNode.id + ' path2Length=' + arrPath2.length);
            //}
            if (pathType != 'ct' && path2 != null) { //dp paths: compare with other path   
                commonNodeIndex = arrPath2.indexOf(currNode.id);
                if (commonNodeIndex != -1) {
                    commonNode = currNode;
                    break;
                    //if (zone == 30332) {
                    //    logger.debug(`[${process.pid}]` + ' Iter' + iter + ' cnt=' + cnt + ' commonnode=' + currNode.id);
                    //}
                }                   
            }
            if (commonNode != null) {
                break;
            }
            if (pq.length > 0) {
                //pq.unshift(pq.splice(pq.length - 1, 1)[0]);      //move last node to the root
                pq.unshift(pq.pop());
                pq[0].pqindex = 0;
                //bubble down
                var index = 0;
                while ((index + 1) * 2 <= pq.length) {          //left child exist
                    var switchIndex = (index + 1) * 2 - 1;
                    var switchNode = pq[switchIndex];   //left child                        
                    if ((index + 1) * 2 + 1 <= pq.length) {     //right child exist
                        var rightChild = pq[(index + 1) * 2];
                        if (rightChild.vsted[2] < switchNode.vsted[2]) {
                            switchNode = rightChild;
                            switchIndex = (index + 1) * 2;
                        }
                    }
                    if (switchNode.vsted[2] < pq[index].vsted[2]) {
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
                }//end bubble down
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
                //ban links for dp
                if (currNode.id == zone) {
                    if (pathType == 'tf' && (Linktp == par.ftypeex || Linktp == par.ftypeexonrp)) {
                        continue;
                    }
                    if (pathType == 'tl' && !(Linktp == par.ftypeex || Linktp == par.ftypeexonrp)) {
                        continue;
                    }
                }
                //ban links for mode
                if (ftypeBan.indexOf(Linktp) != -1) {
                    continue;
                }
                if (dnNode.stled != 1) {                            //exclude settled nodes                                               
                    var timePeriod = math.floor(currNode.vsted[0] / 15) + tp;    //get time period
                    if (timePeriod > par.timesteps) {
                        timePeriod = timePeriod - par.timesteps;
                    }
                    //get toll
                    //var fixtoll = 0;    //fixed toll 
                    //var mintoll = 0;    //min EL toll 
                    //if (dnLink.tolltype == 2) {
                    //    fixtoll = dnLink.toll[timePeriod];
                    //}
                    //if (dnLink.tolltype == 1) {
                    //    mintoll = par.tollpolicy.mintoll;
                    //}                       
                    //if (pathType == 'ct') {
                    //    var alltoll = fixtoll + mintoll;    //ct path toll               
                    //} else {
                    //    var alltoll = fixtoll;              //tl and tf path use fixtoll only
                    //}
                    if (dnLink.tolltype > 0) {
                        alltoll = dnLink.toll[timePeriod];
                    } else {
                        alltoll = 0;
                    }
                    //get perceived time factor
                    var perceiveFactor = 1;
                    if (pathType != 'ct' && iter>1) {
                        perceiveFactor = 1 + (par.choicemodel.perceivetimemaxvc - 1) / (1 + math.exp(-1 * par.choicemodel.perceivetimesteep *
                            (dnLink.volmsa[timePeriod] * 4 / dnLink.cap - par.choicemodel.perceivetimemidvc)));                           
                    }
                    //vsted[] time, time+fixtoll, perceived time, dist, toll, fftime
                    var tempTime = currNode.vsted[0] + dnLink.time[timePeriod];               //time
                    var tempImp = currNode.vsted[2] + dnLink.time[timePeriod] * perceiveFactor + alltoll / vot;        //impedance
                    if (tempImp < dnNode.vsted[2]) {
                        var index = 0;
                        //dnNode has not been checked before or update time when the path is shorter                             
                        if (dnNode.vsted[2] == 999) {
                            pq.push(dnNode);    //not visited
                            nodeList.push(dnNode);
                            index = pq.length - 1;
                            dnNode.pqindex = index;
                                
                        } else {
                            index = dnNode.pqindex;
                        } 
                        dnNode.vsted[2] = tempImp;
                        dnNode.vsted[0] = tempTime;
                        dnNode.vsted[3] = currNode.vsted[3] + dnLink.dist;   //dist
                        if (dnLink.tolltype > 0) {
                            dnNode.vsted[4] = currNode.vsted[4] + dnLink.toll[timePeriod];   //toll
                            //logger.debug(`[${process.pid}]` + ' sp link=' + dnNode.id + ' toll=' + dnNode.vsted[4]);
                        } else {
                            dnNode.vsted[4] = currNode.vsted[4];
                        }
                        dnNode.vsted[5] = currNode.vsted[5] + dnLink.fftime[timePeriod];   //fftime
                        //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' sp ' + ' node=' + dnNode.id + ' preNode=' + currNode.id + ' preNdDist=' + currNode.vsted[3] + ' dnNdDist=' + dnNode.vsted[3]);
                        //bubble up in priority queue  
                        var parentIndex = math.floor((index + 1) / 2 - 1);                           
                        while (parentIndex >= 0) {   //has parent
                            //if (pq[parentIndex] == null) {
                            //    logger.debug(`[${process.pid}]` + ' bubble up parentIndex=' + parentIndex + ' index=' + index+ ' dnNodeImp=' + dnNode.vsted[Imp]);
                            //    //log pq
                            //    var strpq = '';
                            //    for (var j = 0; j < pq.length; j++) {
                            //        if (strpq == '') {
                            //            strpq = pq[j].id;
                            //        } else {
                            //            strpq = strpq + ',' + pq[j].id;
                            //        }
                            //    }
                            //    logger.debug(`[${process.pid}]` + ' after bubble up priority queue=[' + strpq + ']');
                            //}
                            if (dnNode.vsted[2] < pq[parentIndex].vsted[2]) {
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
                        }//end bubble up
                                                                                
                        //cumulative time and imp from origin zone to dnNode; currNode is parent node; dist, toll, fftime
                        dnNode.pnd = [currNode, dnNode, dnLink, tempTime, timePeriod, tempImp, dnNode.vsted[3], dnNode.vsted[4], dnNode.vsted[5]]; 
                        //logger.debug(`[${process.pid}]` + ' set pnd dnNode=' + dnNode.id + ' [' + currNode.id + ',' + dnNode.id + ',' + currNode.id +
                        //    '-' + dnNode.id + ',' + tempTime + ',' + timePeriod + ',' + tempImp + ',' + dnNode.vsted[3] + ',' + dnNode.vsted[4] + ',' + dnNode.vsted[5] + ']');
                    }
                }                   
            }  //end loop dnNodes 
            loopcnt = loopcnt + 1;
        } while (pq.length > 0 && cnt < zonenum);
        //if (tp == 1) {
            //tm3 = process.hrtime();
            //logger.debug(`[${process.pid}]` + ' nanosec=' + process.hrtime(tm2)[1] / 1000000 + ' Iter' + iter + ' end sp path tree building pqlength=' + pq.length + ' zonecnt=' + cnt + ' loops=' + loopcnt);
        //}
        //put paths to hashtables               
        //--- create path ---
        if (pathType != 'ct') { //dp tl and tf paths 
            var cPath = [];
            //use path from the other path from common node to destination zone
            if (commonNode != null) {
                for (var k = 0; k <= commonNodeIndex - 1; k++) {
                    cPath.push(path2[k]);
                }
                var tNd = commonNode;
                //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' commonNode=' + tNd.id + ' index=' + commonNodeIndex);
                //start log
                //if (zone == 30332) {
                    //var strPath = cPath[0][1].id;
                    //for (var j = 0; j < cPath.length; j++) {
                    //    strPath = cPath[j][0].id + ',' + strPath;
                    //}
                    //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' tltf copy other path ' + tp + ':' + zone + '-' + zj + ':' + mode + ':' + pathType + ' ' + strPath);
                //}
                //end log
                //use built path from start node to common node
                while (tNd.pnd != null) {
                    cPath.push(tNd.pnd);
                    //if (zone == 100296 && parseInt(zj) == 8) {
                    //    logger.debug(`[${process.pid}]` + ' Iter' + iter + ' dp pathHash ' + ' ' + tNd.id);
                    //}
                    if (tNd.type == par.dcpnttype && tNd.id != zone) {     //dp node                        
                        var link = cPath[cPath.length - 2][2];
                        //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' link=' + link.aNd.id + '-' + link.bNd.id + ' type=' + link.type);
                        if (link.type == par.ftypeex || link.type == par.ftypeexonrp) {  //current path is a toll path                   
                            var currPathType = 'tl';
                        } else {
                            var currPathType = 'tf';
                        }
                        if (tNd.id == zone) {
                            var tptemp = tp;
                        } else {
                            var tptemp = tNd.pnd[4];
                        }
                        var key = tptemp + ':' + tNd.pnd[1].id + '-' + zj + ':' + mode + ':' + currPathType;
                        if (!pathHash.has(key)) {
                            var dpPath = [];
                            for (var j = 0; j < cPath.length - 1; j++) {
                                dpPath.push(cPath[j]);
                            }                              
                            dpPath.push(tNd.pnd);
                            pathHash.set(key, dpPath);   //dp path
                            //start log
                            //if (zone == 100296 && parseInt(zj) == 8) {
                            //var strPath = dpPath[0][1].id;
                            //for (var j = 0; j <= dpPath.length - 1; j++) {
                            //    strPath = dpPath[j][0].id + ',' + strPath;
                            //}
                            //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' tltf set new dp pathHash ' + key + ' ' + strPath);
                            //}
                            //end log
                        }

                    }
                    tNd = tNd.pnd[0];
                }
                cPath.push([0, 0, 0, 0, 0, 0, 0, 0, 0]);
                var key = tp + ':' + zone + '-' + zj + ':' + mode + ':' + pathType;

                //start log
                //if (zone == 100296 && parseInt(zj) == 8) {
                    //var strPath = cPath[0][1].id;
                    //for (var j = 0; j <= cPath.length - 1; j++) {
                    //    strPath = cPath[j][0].id + ',' + strPath;
                    //}
                    //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' tltf set dp pathHash ' + key + ' ' + strPath);
                //}
                //end log
                //get skim
                var tskim = [];
                var dppnd = path2[path2.length - 1];
                //this path
                tskim.push(commonNode.pnd[3] - dppnd[3]);    //time
                tskim.push(commonNode.pnd[6]);    //dist
                tskim.push(commonNode.pnd[7]);    //toll
                tskim.push(commonNode.pnd[8]);    //fftime
                tskim.push(commonNode.pnd[5]);    //imp
                tskim.push(commonNode);
                pathSkimHash.set(key, tskim);
                //if (zone == 30332) {
                //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' path skim 1 ' + key + ' time=' + commonNode.pnd[3] + '-' + dppnd[3] + ' dist=' + tskim[1] + ' toll=' + tskim[2]
                //    + ' dpnode=' + dppnd[1].id + ' commonNode=' + commonNode.id);
                //}
                //other path
                var oskim = [];
                var pathItem = path2[commonNodeIndex];
                oskim.push(pathItem[3] - dppnd[3]);    //time
                oskim.push(pathItem[6] - dppnd[6]);    //dist
                oskim.push(pathItem[7] - dppnd[7]);    //toll
                oskim.push(pathItem[8] - dppnd[8]);    //fftime
                oskim.push(pathItem[5] - dppnd[5]);    //imp
                oskim.push(commonNode);
                pathSkimHash.set(tp + ':' + zone + '-' + zj + ':' + mode + ':' + pathType2, oskim);
                //if (zone == 30332) { 
                    //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' path skim 2 ' + tp + ':' + zone + '-' + zj + ':' + mode + ':' + pathType2 + ' dist=' + oskim[1]
                    //    + ' common node toll=' + pathItem[7] + ' dp node toll=' + path2[path2.length - 1][7]);
                //}
            }
            pathHash.set(tp + ':' + zone + '-' + zj + ':' + mode + ':' + pathType, cPath);
        } else { //ct path                
            for (var i = 1; i <= zonenum; i++) {
                var path = [];
                var pNode = allNodes.get(i);                  
                while (pNode.pnd != null) {
                    path.push(pNode.pnd);       //path array[0] is the destination 
                    var dpNode = pNode.pnd[0];
                    if (dpNode.type == par.dcpnttype) {     //dp node                        
                        var link = pNode.pnd[2];
                        if (link.type == par.ftypeex || link.type == par.ftypeexonrp) {  //current path is a toll path                   
                            var currPathType = 'tl';
                        } else {
                            var currPathType = 'tf';
                        }
                        if (dpNode.id == zone) {
                            var tptemp = tp;
                        } else {
                            var tptemp = dpNode.pnd[4];
                        }
                        var key = tptemp + ':' + pNode.pnd[0].id + '-' + i + ':' + mode + ':' + currPathType;
                        //logger.debug(`[${process.pid}]` + ' Iter' + iter + ' link=' + link.aNd.id + '-' + link.bNd.id + ' type=' + currPathType);
                        if (!pathHash.has(key)) {
                            var cPath = [];                               
                            for (var j = 0; j < path.length; j++) {
                                cPath.push(path[j]);
                            }
                            cPath.push(dpNode.pnd);
                            pathHash.set(key, cPath);   //dp path

                            //start log
                            //if (pNode.pnd[0].id == 30332 && i == 125) {
                            //    var strPath = cPath[0][1].id;
                            //    for (var j = 0; j < cPath.length; j++) {
                            //        strPath = cPath[j][0].id + ',' + strPath;
                            //    }
                            //    logger.debug(`[${process.pid}]` + ' Iter' + iter + ' ct set dp pathHash ' + key + ' ' + strPath);
                            //}
                            //end log
                        } 
                    }
                    pNode = pNode.pnd[0]; //parent node                    
                }                
                if (path.length > 0) {
                    pathHash.set(tp + ':' + zone + '-' + i + ':' + mode + ':' + pathType, path);
                    //start log
                    //if (zone == 130332 && i == 125) {
                    //    var strPath = path[0][1].id;
                    //    for (var j = 0; j < path.length; j++) {
                    //        strPath = path[j][0].id + ',' + strPath;
                    //    }
                    //    logger.debug(`[${process.pid}]` + ' Iter' + iter + ' zone sp ' + tp + ':' + zone + '-' + i + ':' + mode + ':' + pathType + ' ' + strPath);
                    //}
                    //end log
                } 
            }
        }
        //logger.debug(`[${process.pid}]` + ' nanosec=' + process.hrtime(tm3)[1]/1000000 + ' Iter' + iter + ' --end sp building');
        callback(null, 'SP for zone ' + zone + ', tp ' + tp + ', mode ' + mode + ', pathType ' + pathType);
}

//********move vehicle********
var mv = function MoveVehicle(tp, zi, zj, pthTp, mode, vol, path, iter, zint, tsint, splitCnt, totTime, callback) {
    //if (zi == 100296 && parseInt(zj) == 8) {
    //  logger.debug(`[${process.pid}]` + ' iter ' + iter + ' ***mv ' + tp + ':'+ zi + '-' + zj + ':' + mode + ':' + pthTp + ' vol=' + vol);
    //}
    if (path == null) {
        callback(null, zi + '-' + zj + ':' + tp + ':' + mode);
    } else {
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
            stats.dppacket = stats.dppacket + 1;
        } else {
            var pathCopy = path;
            stats.zonepacket = stats.zonepacket + 1;
        }
        //loop links in the path
        var pathItem = pathCopy.pop();   
        if (pthTp != 'ct') {
            pathItem = pathCopy.pop(); 
        }
        var logpaths = par.log.path.split(',');     // zi,zj,timestep  
        while (pathItem != null) {  
            //if (zi == 100214 && zj == 104) {
            //logger.debug(`[${process.pid}]` + ' iter' + iter + ' mv link=' + pathItem[0].id + '-' + pathItem[2].bNd.id + ' pathLength=' + pathCopy.length + ' vol=' + vol);
            //}
            pathItem[2].vol[modenum][pathItem[4]] = pathItem[2].vol[modenum][pathItem[4]] + vol;
            //if (tp == 1) {
            //    logger.debug(`[${process.pid}]` + ' iter' + iter + ' ' + tp + ':' + zi + '-' + zj + ':' + mode + ':' + pthTp + ' vol=' + vol + ' link=' + pathItem[2].aNd.id + '-' + pathItem[2].bNd.id + ' vol=' + vol);
            //}
            var aNode = pathItem[1];
            var currTp = pathItem[4];
            //log path file
            if (parseInt(logpaths[0]) == zint && parseInt(logpaths[1]) == zj && parseInt(logpaths[2]) == tsint) {
                pathStream.write(pathItem[0].id + ',' + pathItem[1].id + ',' + vol + ',' + iter + ',' + currTp + ',' + mode + os.EOL);
            } 
           
            if (aNode.type == par.dcpnttype && mode == "SOV" && vol >= par.minpacketsize && splitCnt <= par.maxsplitcount) {
                //--- decision point (not the start node in path) ---
                //if (zi == 100214 && zj == 104) {
                //logger.debug(`[${process.pid}]` + ' iter' + iter + ' decision point link=' + pathItem[0].id + '-' + pathItem[1].id + ' pathLength=' + pathCopy.length);
                //}
                totTime = pathItem[3];
                var ptype = ['tl', 'tf'];
                var time = [0, 0];
                var dist = [0, 0];
                var toll = [0, 0];
                var timeFF = [0, 0];
                var probability = 0;
                //var currTp = pathItem[4];
                var link = pathCopy[pathCopy.length - 1][2];
                //if (iter == 2) {
                //    logger.debug(`[${process.pid}]` + ' iter ' + iter + ' dp dn link=' + link.aNd.id + '-' + link.bNd.id);
                //}
                if (link.type == par.ftypeex || link.type == par.ftypeexonrp) {  //current path is a toll path                   
                    var currPathType = 0;
                    var buildPathType = 1;
                } else {
                    var currPathType = 1;
                    var buildPathType = 0;
                }
                
                //build second path                
                var key = currTp + ":" + aNode.id + '-' + zj + ":" + mode + ":" + ptype[buildPathType];
                //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' --build second path ' + key);    
                if (!pathHash.has(key)) {
                    sp(parseInt(aNode.id), par.zonenum, parseInt(currTp), mode, ptype[buildPathType], iter, zj, totTime, function (err, result) {
                        //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' --finish built second path ' + key);
                    });                   
                }  
                pathSkim = pathSkimHash.get(key);
                if (pathSkim == null) {
                    dist[buildPathType] = 9999;
                    //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' second path skim is null ' + key);
                }else{                
                    time[buildPathType] = pathSkim[0];
                    dist[buildPathType] = pathSkim[1];
                    toll[buildPathType] = pathSkim[2];
                    timeFF[buildPathType] = pathSkim[3];
                }  

                //current path skim
                var pathSkim = pathSkimHash.get(currTp + ":" + aNode.id + '-' + zj + ":" + mode + ":" + ptype[currPathType]);
                if (pathSkim == null) {
                    dist[currPathType] = 9999;
                    //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' first path skim is null key=' + currTp + ":" + aNode.id + '-' + zj + ":" + mode + ":" + ptype[currPathType]);
                } else {
                    time[currPathType] = pathSkim[0];
                    dist[currPathType] = pathSkim[1];
                    toll[currPathType] = pathSkim[2];
                    timeFF[currPathType] = pathSkim[3];
                }
                if (dist[currPathType] == 9999) {
                    if (currPathType == 0) {
                        probability = 0;
                    } else {
                        probability = 1;
                    }
                } else if (dist[0] > dist[1] * parseFloat(par.distmaxfactor)) {
                    probability = 0;
                    //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' ' + key + ' probability=0 distTf=' + dist[1] + ' distTl=' + dist[0]);
                } else if (dist[1] > dist[0] * parseFloat(par.distmaxfactor)) {
                    probability = 1;
                    //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' ' + key + ' probability=1 distTf=' + dist[1] + ' distTl=' + dist[0]);
                } else {                        
                    var scale = math.pow((par.choicemodel.scalestdlen / dist[0]), par.choicemodel.scalealpha);
                    var relia = par.choicemodel.timecoeff * par.choicemodel.reliacoeffratio * par.choicemodel.reliacoefftime *
                            ((timeFF[1] - time[1]) * math.pow(dist[1], (-1 * par.choicemodel.reliacoeffdist))
                            - (timeFF[0] - time[0]) * math.pow(dist[0], (-1 * par.choicemodel.reliacoeffdist)));
                    var utility = -1 * par.choicemodel.tollconst[currTp - 1] - scale * (par.choicemodel.timecoeff * (time[0] - time[1])
                        + par.choicemodel.tollcoeff * (toll[0] - toll[1]) + relia);

                    probability = 1 / (1 + math.exp(utility));
                    //log file                    
                    if (par.log.dpnode.indexOf(aNode.id) != -1) {                             //hasn't been checked in this iteration  
                        choiceStream.write(iter + ',' + zint + ',' + zj + ',' + tsint + ',' + currTp + ',' + aNode.id + ',' + pathSkim[5].id + ',' + math.round(dist[0], 2)
                            + ',' + math.round(dist[1], 2) + ',' + math.round(time[0], 2) + ',' + math.round(time[1], 2) + ',' + timeFF[0] + ',' + timeFF[1]
                            + ',' + toll[0] + ',' + toll[1] + ',' + math.round(utility, 2) + ',' + math.round(probability, 4) + os.EOL);                         
                    }                    
                    //logger.debug('probability calculation: tollconst=' + par.choicemodel.tollconst[tp - 1] + ',scalesdlen=' + par.choicemodel.scalestdlen
                    //    + ',scalealpha=' + par.choicemodel.scalealpha + ',timecoeff=' + par.choicemodel.timecoeff
                    //    + ',tollcoeff=' + par.choicemodel.tollcoeff + ',reliacoeffratio=' + par.choicemodel.reliacoeffratio
                    //    + ',reliacoefftime=' + par.choicemodel.reliacoefftime + ',reliacoeffdist=' + par.choicemodel.reliacoeffdist);
                    //if (isNaN(probability)) {
                        //logger.debug(`[${process.pid}]` + ' iter ' + iter + tp + ':' + zi + '-' + zj + ':' + mode + ':' + pthTp + ' dp id=' + aNode.id + ' distTl=' + math.round(dist[0], 2) + ',distTf=' + math.round(dist[1], 2) + ',timeTl=' + math.round(time[0], 2)
                        //    + ',timeTf=' + math.round(time[1], 2) + ',timeFFTl=' + timeFF[0] + ',timeFFTf=' + timeFF[1] + ',Toll=' + math.round(toll[0], 2)
                        //    + ',utility=' + math.round(utility, 2) + ',probability=' + math.round(probability, 4));
                    //}
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
                //if (splitVol[0] < par.minpacketsize) {
                //    splitVol[0] = 0;
                //    splitVol[1] = vol;
                //}
                //if (splitVol[1] < par.minpacketsize) {  //if both less than min packet size, use toll path
                //    splitVol[0] = vol;
                //    splitVol[1] = 0;
                //}
                //if (zj == 15) {
                //    logger.debug(`[${process.pid}]` + ' iter' + iter + ' key=' + aNode.id + ':' + zj + ':' + splitVol[buildPathType] + ':' + ptype[buildPathType] + ':' + currTp + ':' + zint + ':' + tsint + ' splitvol tl=' + splitVol[0]);
                //}
                if (splitVol[buildPathType] > 0) {                   
                    //tp, zi, zj, pthTp, mode, vol, path, iter, zint, tsint, callback
                    //cntPackets = cntPackets + 1; 
                    //
                    //spZone:zj:vol:pathtype:timeStep:spZone:timeStep
                    splitCnt = splitCnt + 1
                    mvTasks.push(aNode.id + ':' + zj + ':' + splitVol[buildPathType] + ':' + ptype[buildPathType] + ':' + currTp + ':' + zint + ':' + tsint + ':' + splitCnt + ':' + pathItem[3]);
                    //if (tp == 1) {
                    //    logger.debug(`[${process.pid}]` + ' iter' + iter + ' add dp task ' + aNode.id + ':' + zj + ':' + splitVol[buildPathType] + ':' + ptype[buildPathType] + ':' + currTp + ':' + zint + ':' + tsint);
                    //}
                }
                vol = splitVol[currPathType];
            } 
                 
            pathItem = pathCopy.pop();

        } //end loop
        callback(); 
    }
}

redisJob.subscribe("job");
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
    redisJob.on('message', function (channel, message) {
        if (message == 'end') {
            logger.info(`[${process.pid}]` + ' --- end cluster ---');
            process.exit(0); //End server
        }
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
            var logChoiceFile = './output' + '/' + par.log.dpnodefilename;
            fs.truncate(logChoiceFile, 0);  //log only last iteration
            choiceStream = fs.createWriteStream(logChoiceFile, { 'flags': 'a' });
            choiceStream.write("iter, orgID, destID, startTS, tStep, dpID, cmnNd, distTl, distTf, timeTl, timeTf, timeFFTl, timeFFTf, TollEL, TollGP, utility, TlShare" + os.EOL);
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
                    stats.zonepath = 0;
                    stats.dppath = 0;
                    stats.zonepacket = 0;
                    stats.dppacket = 0;
                    async.during(           //loop until jobs are done
                        //test function
                        function (callback) {
                            redisClient.select(6);
                            redisClient.lpop('task', function (err, result) {
                                //logger.debug(`[${process.pid}]` + ' ***iter' + iter + ' get sp zone task ' + result);
                                if (result == null) {
                                    redisClient.incr('cnt', function (err, result) {
                                        logger.info(`[${process.pid}]` + ' iter' + iter + ' processor ' + result + ' task=' + cnt + ' znpath=' + stats.zonepath + ' zonepkt=' + stats.zonepacket + 
                                            ' dppath=' + stats.dppath  + ' dppkt=' + stats.dppacket);
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
                                    sp(parseInt(spZone), par.zonenum, parseInt(timeStep), mode, pathType, iter, 0, 0, function (err, result) {
                                        //logger.info(`[${process.pid}]` + ' iter' + iter + ' sp zone=' + spZone);
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
                                                mvTasks.push(spZone + ':' + tsk + ':ct' + ':' + timeStep + ':' + spZone + ':' + timeStep + ':0:0');  //tsk = 'zj:vol'
                                            });                                            
                                            //cntPackets = mvTasks.length;
                                        }
                                        callback();
                                    });
                                },
                                function (callback) {
                                    //move all tasks
                                    var task = '';
                                    task = mvTasks.pop();
                                    while (task != null) {
                                        //logger.info(`[${process.pid}]` + ' iter' + iter + ' task num=' + mvTasks.length);
                                        var arrTask = task.split(':');
                                        var zi = arrTask[0];
                                        var zj = arrTask[1];   //destination node
                                        var vol = arrTask[2];
                                        var ptp = arrTask[3];
                                        var ts = arrTask[4];
                                        var zint = arrTask[5];
                                        var tsint = arrTask[6]; //time step when leave the origin zone
                                        var totTime = arrTask[7];
                                        var splitCnt = parseInt(arrTask[7]);
                                        //zone node task                                       
                                        var path = pathHash.get(ts + ":" + zi + "-" + zj + ":" + mode + ":" + ptp);
                                        if (path == null) {
                                            logger.info(`[${process.pid}]` + ' iter' + iter + ' mv path=null ' + timeStep + ":" + zi + "-" + zj + ":" + mode + ":" + ptp);
                                        } else {
                                            //**move vehicles
                                            //logger.info(`[${process.pid}]` + ' iter' + iter + ' move ' + task);                                               
                                            mv(ts, zi, zj, ptp, mode, parseFloat(vol), path, iter, zint, tsint, splitCnt, totTime, function (err, result) {
                                               //logger.info(`[${process.pid}]` + ' iter' + iter + ' moved ' + task);
                                                                                               
                                            });
                                        }
                                        task = mvTasks.pop();                                        
                                    }
                                    callback();
                                }],
                                function () { 
                                    pathHash.clear();
                                    pathSkimHash.clear();
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
                    choiceStream.end();
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
                                            //if (isNaN(lnk.vol[j][k])) {
                                            //    logger.debug(`[${process.pid}]` + ' iter' + iter + ' mode=' + j + ' ts=' + k + ' link =' + arrLink[i]);
                                            //}
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
                            //if (tp == 1) {
                            //    logger.debug(`[${process.pid}]` + ' iter' + iter + ' link=' + arrLink[i] + ' vol=' + strVol);
                            //}
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
            var logTollFile = './output' + '/' + par.log.tollfilename;
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
                    var volModeDiff = [];
                    var vol = [];
                    var volDiff = [];
                    
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
                                            volModeDiff.push(math.abs(msaVol - parseFloat(arrV_pre[i])));
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
                                var tVolDiff = 0;
                                for (var j = 0; j < par.modes.length; j++) {
                                    tVol = tVol + parseFloat(volMode[i + par.timesteps * j]);
                                    tVolDiff = tVolDiff + parseFloat(volModeDiff[i + par.timesteps * j]);
                                }
                                vol.push(tVol);
                                volDiff.push(tVolDiff);
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
                                        multi.rpush('vht' + i, vht[i] + ',' + diff + ',' + vol[i] + ',' + volDiff[i], function (err, result) {

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
                                        var tlLinks = par.log.toll;
                                        var tlLog = false;
                                        for (var j = 0; j < tlLinks.length; j++) {
                                            if (tlLinks[j] == link) {
                                                tlLog = true;
                                                break;
                                            }
                                        }
                                        if (tlLog) {
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
                    //logger.info(`[${process.pid}]` + ' iter' + iter + ' link update processed total of ' + cnt);
                });
        } else if (message == 'end') {
            process.exit(0); //End server
        }
    });
}