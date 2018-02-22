//Starting node
var fs = require('fs');
var csv = require('fast-csv');
var redis = require('ioredis');
var async = require('async');
var os = require('os');
var storage = require('@google-cloud/storage');

var cloud_prjID = 'dta-01';
var cloud_bucketName = 'eltod';
var runlistfilename = 'runlist.json';
//var redisIP = "redis://127.0.0.1:6379";
var redisIP = "redis.default.svc.cluster.local:6379";

var gcs = storage({
    projectId: cloud_prjID,
    //keyFilename: 'dta-01-1e8b82b8f33c.json'   //needed to run locally
});
var bucket = gcs.bucket(cloud_bucketName);

var allLinks = new Map();       //link
var allNodes = new Map();
var NodeNewtoOld = new Map();
var NodeOldtoNew = new Map();
var arrLink = [];
var arrTollLink = [];
var mvTasks = [];
var nodeList = [];
var iter = 0;
var pathStream = null;
var choiceStream = null;
var stats = {zonepath:0, dppath:0, zonepacket: 0, dppacket: 0};
var logStream = fs.createWriteStream('/output/log.txt');  
var redisClient = new redis(redisIP); 
var redisJob = new redis(redisIP);
var startTimeStep = 1;
var endTimeStep = 96;
var runListPar = '';
var scenIndex = 0;
var par = null;

//********node reader********
var rdnd = function Readcsv(callback) {
    var stream = bucket.file(par.nodefilename).createReadStream();
    var sId = par.zonenum + 1;
    var csvStream = csv({ headers: true })
        .on("data", function (data) {
            var id = data['N'];
            id = parseInt(id.trim());
            var newid = id;
            if (id <= par.zonenum) {
                NodeNewtoOld.set(id, id);
                NodeOldtoNew.set(id, id);
            } else {
                NodeNewtoOld.set(sId, id);
                NodeOldtoNew.set(id, sId);
                newid = sId;
                sId = sId + 1;              
            }  
            var dlk = new Map();
            var nd = {
                id: newid, dnd: [], dnlink: dlk, path: [], imp: 999, time: 0, upNd: [], skim: [], stled: 0, pqindex: 0, type: data['DTA_Type'], segnum: data['SEGNUM'],
                prohiblnk: data['Prohibit_Links']
            };
            allNodes.set(newid, nd);
        })
        .on("end", function (result) {
            callback(null, result);
        });
    stream.pipe(csvStream);
}
//********link reader********
var rdlnk = function Readcsv(callback) {
    var stream = bucket.file(par.linkfilename).createReadStream();
    var csvStream = csv({ headers: true })
        .on("data", function (data) {
            var arrTime = [];
            var arrffTime = [];
            var arrToll = [];
            arrTime[0] = 0;
            arrffTime[0] = 0;
            arrToll[0] = 0;
            var a = data['A'];
            a = parseInt(a.trim());
            var b = data['B'];
            b = parseInt(b.trim());
            for (var i = 1; i <= par.timesteps; i++) {
                arrTime.push(parseFloat(data['TIME']));
                arrffTime.push(parseFloat(data['TIME']));
                arrToll.push(parseFloat(data['TOLL']));                              
            }           
            if (parseInt(data['TOLLTYPE']) == 1) {
                arrTollLink.push(NodeOldtoNew.get(a) + '-' + NodeOldtoNew.get(b));
            }
            var aNode = allNodes.get(NodeOldtoNew.get(a));
            var bNode = allNodes.get(NodeOldtoNew.get(b));
            var type = parseInt(data['Ftype']);           
            var link = {
                aNd: aNode, bNd: bNode, time: arrTime, fftime: arrffTime, toll: arrToll, tolltype: parseInt(data['TOLLTYPE']),
                dist: parseFloat(data['Dist']), type: type, cap: parseFloat(data['Cap']),
                tollsegnum: parseInt(data['TOLLSEGNUM']), alpha: parseFloat(data['Alpha']), beta: parseFloat(data['Beta']), vol: [], volmsa: []
            };
            aNode.dnd.push(bNode);
            aNode.dnlink.set(bNode.id, link);
            allLinks.set(NodeOldtoNew.get(a) + '-' + NodeOldtoNew.get(b), link);           
        })
        .on("end", function (result) {
            callback(null, result);
        });
    stream.pipe(csvStream);
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
var sp = function ShortestPath(zone, zonenum, tp, mode, pathType, iter, zj, totTime, tltfcnt, mgNodes, callback) {
    //if (NodeNewtoOld.get(zone.id) == 40346) {
    //var tm1 = process.hrtime();    
    //    logger.debug(`[${process.pid}]` + ' iter' + iter + ' ***SP for ' + tp + ':' + NodeNewtoOld.get(zone.id) + '-' + zj + ':' + mode + ':' + pathType);
    //}
    //single node shortest path
    if (pathType == 'ct'){
        stats.zonepath = stats.zonepath + 1;
    } else {
        stats.dppath = stats.dppath + 1;      
    }
    var ftypeBan = [];
        //get mode banning ftype          
        if (mode == "HOV") {
            ftypeBan = par.pathban.HOV;
        } else if (mode == "TRK") {
            ftypeBan = par.pathban.TRK;
        }
        //init network nodes
        for (var i = 0; i < nodeList.length; i++) {
            nodeList[i].skim = [];    //time, time+fixtoll, perceived time, dist, toll, fftime
            nodeList[i].stled = 0;
            nodeList[i].imp = 999;
            nodeList[i].time = 0;
            nodeList[i].upNd = [];
        }
        
        nodeList = [];
        var currNode = zone;
        var zoneNode = currNode;
        //if (zoneNode == null) {
        //    logger.debug(`[${process.pid}]` + ' iter' + iter + ' ***SP for ' + tp + ':' + NodeNewtoOld.get(zone.id) + '-' + zj + ':' + mode + ':' + pathType);
        //}
        currNode.path = {};
        var path = currNode.path;
        //track visited nodes and store time
        currNode.time = totTime;
        currNode.imp = 0;  
        
        var pq = [];                        //priority queue with binary heap
        pq.push(currNode);
        nodeList.push(currNode);
        var cnt = 0;    //count visited zones
        var vot = par.choicemodel.timecoeff / par.choicemodel.tollcoeff;
        if (pathType != 'ct') {
            mgNodes.pop();
        }
        //visit nodes   
        var loopcnt = 0;
        do {          
            //settle node
            loopcnt = loopcnt + 1;
            currNode = pq.shift();     //remove root
            currNode.stled = 1; 
            path[currNode.id] = currNode.upNd;            
            
            //if (pathType != 'ct') { //tltf path
                for (var j = mgNodes.length - 1; j >= 0; j--) {                    
                    if (mgNodes[j].id == currNode.id) {  
                        //logger.debug(`[${process.pid}]` + ' iter' + iter + ' --- end SP nanosec=' + process.hrtime(tm1)[1] / 1000000 + ' sp mgNode=' + NodeNewtoOld.get(mgNodes[j].id) + ' loopcnt=' + loopcnt);
                        return callback(null, currNode); 
                    }
                }                               
            //} else { //ct path
            //    for (var j = 0; j < mgNodes.length; j++) {
            //        if (mgNodes[j].id == currNode.id) {
            //            mgNodes.splice(j, 1);
            //            if (mgNodes.length == 0) {
            //                return callback(null, null);
            //            }
            //        }
            //    }
            //}
            
            if (currNode.id <= zonenum) { //track centriod nodes                    
                cnt = cnt + 1;
            }
                        
            if (pq.length > 0) {    
                pq.unshift(pq.pop());           //move last node to the root
                pq[0].pqindex = 0;
                //bubble down
                var index = 0;
                while ((index + 1) * 2 <= pq.length) {          //left child exist
                    var switchIndex = (index + 1) * 2 - 1;
                    var switchNode = pq[switchIndex];   //left child                        
                    if ((index + 1) * 2 + 1 <= pq.length) {     //right child exist
                        var rightChild = pq[(index + 1) * 2];
                        if (rightChild.imp < switchNode.imp) {
                            switchNode = rightChild;
                            switchIndex = (index + 1) * 2;
                        }
                    }
                    if (switchNode.imp < pq[index].imp) {
                        //switch
                        pq[switchIndex] = pq[index];   //child = parent
                        pq[index] = switchNode;        //parent = child
                        switchNode.pqindex = index;     //child index
                        pq[switchIndex].pqindex = switchIndex;  //parent index
                        index = switchIndex;
                    } else {
                        break;
                    }
                }//end bubble down
            }                                              
            //skip centriod 
            if (currNode.id <= zonenum && currNode.id != zone.id) {
                continue;
            }
            //if (iter == 4 && tp == 61) {
            //    logger.debug(`[${process.pid}]` + ' iter' + iter + ' currNode=' + NodeNewtoOld.get(currNode.id));
            //}
            //get new frontier nodes 
            var dnNodes = currNode.dnd;                  
            for (i = 0; i < dnNodes.length; i++) {
                var dnNode = dnNodes[i];               
                var dnLink = currNode.dnlink.get(dnNode.id);
                var Linktp = dnLink.type;
                //ban links for dp
                if (currNode.id == zone.id && tltfcnt == 0) {
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
                    var timePeriod = Math.floor(currNode.time / 15) + tp;    //get time period                   
                    if (timePeriod > par.timesteps) {
                        timePeriod = timePeriod - par.timesteps;
                    }
                    //get perceived time factor
                    var perceiveFactor = 1;
                    if (pathType != 'ct' && iter>1) {
                        perceiveFactor = 1 + (par.choicemodel.perceivetimemaxvc - 1) / (1 + Math.exp(-1 * par.choicemodel.perceivetimesteep *
                            (dnLink.volmsa[timePeriod] * 4 / dnLink.cap - par.choicemodel.perceivetimemidvc)));                           
                    }
                    //vsted[] time, time+fixtoll, perceived time, dist, toll, fftime                  
                    var tempImp = currNode.imp + dnLink.time[timePeriod] * perceiveFactor + dnLink.toll[timePeriod] / vot;        //impedance                    
                    if (tempImp < dnNode.imp) {
                        var index = 0;
                        //dnNode has not been checked before or update time when the path is shorter                          
                        if (dnNode.imp == 999) {
                            pq.push(dnNode);    //not visited
                            nodeList.push(dnNode);
                            index = pq.length - 1;
                            dnNode.pqindex = index;                                
                        } else {
                            index = dnNode.pqindex;
                        } 
                        dnNode.imp = tempImp;
                        dnNode.time = currNode.time + dnLink.time[timePeriod];               //time
                        dnNode.upNd = currNode;
                       
                        //bubble up in priority queue
                        var parentIndex = Math.floor((index + 1) / 2 - 1);                           
                        while (parentIndex >= 0) {   //has parent                            
                            if (dnNode.imp < pq[parentIndex].imp) {
                                pq[index] = pq[parentIndex];
                                pq[parentIndex] = dnNode;
                                dnNode.pqindex = parentIndex;
                                pq[index].pqindex = index;
                                index = parentIndex;
                                parentIndex = Math.floor((index + 1) / 2 - 1);                                   
                            } else {
                                break;
                            }
                        }//end bubble up                                                                                                       
                    }
                }                   
            }  //end loop dnNodes 
            //if (tp == 61) {
            //    logger.debug(`[${process.pid}]` + ' iter' + iter + ' pq len=' + pq.length);
            //}
        } while (pq.length > 0 && cnt < zonenum);
        //logger.debug(`[${process.pid}]` + ' iter' + iter + ' --- end SP for ' + ' nanosec=' + process.hrtime(tm1)[1] / 1000000 + ' loopcnt=' + loopcnt);
        callback(null, null);   //return null if no merge node found
}

//********move vehicle********
var mv = function MoveVehicle(pkt, callback) {
    //if (pkt.zint == 192) {
    //    logger.debug(`[${process.pid}]` + ' iter' + iter + ' ***mv ' + pkt.ts + ':' + NodeNewtoOld.get(pkt.zi) + '-' + pkt.zj + ':' + pkt.pathType + ' vol=' + pkt.vol);
    //}
    if (pkt.pathType == 'ct') {
        stats.zonepacket = stats.zonepacket + 1;
    } else {
        stats.dppacket = stats.dppacket + 1;
    }
    var path = [];    //path from i to j
    //get path
    if (pkt.path.length == 0) { //packet starts at the zone
        var iNode = allNodes.get(pkt.zi);
        var jNode = allNodes.get(pkt.zj);
        var pathTree = iNode.path;        
        path.push(jNode);
        var upNode = pathTree[pkt.zj];        
        while (upNode.id != pkt.zi) {
            path.push(upNode);              
            upNode = pathTree[upNode.id];                               
        } 
        path.push(upNode);
        pkt.path = path;
    } else {
        path = pkt.path;
    }
    //log path
    //if (pkt.zi == 192 && pkt.zj == 213) {
        //var str = '';
        //for (var i = 0; i < path.length; i++) {
        //    str = NodeNewtoOld.get(path[i].id) + ',' + str;
        //}
        //logger.debug(`[${process.pid}]` + ' iter' + iter + ' mv ' + pkt.ts + ':' + NodeNewtoOld.get(pkt.zi) + '-' + pkt.zj + ':' + pkt.pathType + ' path=' + str);
    //}
    //end log

    if (path.length == 1) {
        callback(null, zi + '-' + zj + ':' + tp + ':' + mode);
    } else {
        var modenum = 0;
        for (var i = 0; i <= par.modes.length - 1; i++){
            if (pkt.mode == par.modes[i]) {
                modenum = i;
                break;
            }
        }
        var time = pkt.totTime;        
        var logpaths = par.log.path.split(',');     // zi,zj,timestep  
        var aNode = path.pop();
        while (path.length > 0) {  
            var bNode = path[path.length - 1];
            var link = aNode.dnlink.get(bNode.id);
            var currTp = Math.floor(time / 15) + pkt.tsint;
            if (currTp > par.timesteps) {
                currTp = currTp - par.timesteps;
            }
            
            time = pkt.totTime + link.time[currTp];            
            link.vol[modenum][currTp] = link.vol[modenum][currTp] + pkt.vol;
            //logger.debug(`[${process.pid}]` + ' iter' + iter + ' set vol on link=' + NodeNewtoOld.get(link.aNd.id) + '-' + NodeNewtoOld.get(link.bNd.id) + ' vol=' + pkt.vol);
            //log path file
            if (parseInt(logpaths[0]) == pkt.zint && parseInt(logpaths[1]) == pkt.zj && parseInt(logpaths[2]) == pkt.tsint) {
                pathStream.write(aNode.id + ',' + bNode.id + ',' + pkt.vol + ',' + iter + ',' + currTp + ',' + pkt.mode + os.EOL);
            } 

            //--- decision point (not the start node in path) ---
            if (bNode.type == par.dcpnttype && pkt.mode == "SOV" && pkt.vol >= par.minpacketsize && pkt.splitCnt <= par.maxsplitcount) {                               
                var ptype = ['tl', 'tf'];
                var timeskim = [0, 0];
                var dist = [0, 0];
                var toll = [0, 0];
                var timeFF = [0, 0];
                var probability = 0;
                var cnt = 0;
                var rpLink = bNode.dnlink.get(path[path.length - 2].id);
                if (rpLink.type == par.ftypeex || rpLink.type == par.ftypeexonrp) {  //current path is a toll path                   
                    var currPathType = 0;
                    var buildPathType = 1;
                } else {
                    var currPathType = 1;
                    var buildPathType = 0;
                }                 
                //build the second path 
                var mgNode = null;
                sp(bNode, par.zonenum, currTp, pkt.mode, ptype[buildPathType], iter, pkt.zj, time, cnt, path, function (err, result) {
                    mgNode = result;
                    //if (pkt.zint == 192) {
                        //logger.debug(`[${process.pid}]` + ' iter' + iter + ' second path mgNode=' + NodeNewtoOld.get(mgNode.id) + ' pathType=' + ptype[buildPathType] + 
                        //    ' dp node=' + NodeNewtoOld.get(bNode.id));
                    //}
                }); 
                if (mgNode == null) {
                    dist[buildPathType] = 9999;
                } else {
                    var bldPath = [];
                    //use the current path from merge node to destination node
                    var j = 0;
                    while (path[j].id != mgNode.id) {
                        bldPath.push(path[j]);
                        j = j + 1;
                    }
                    var pTree = bNode.path;
                    bldPath.push(mgNode);
                    var upNode = pTree[mgNode.id];

                    while (upNode.id != bNode.id) {
                        bldPath.push(upNode);
                        upNode = pTree[upNode.id];
                    }
                    bldPath.push(upNode);
                    //log path
                    //if (pkt.tsint == 198) {
                    //var str = '';
                    //for (var i = 0; i < bldPath.length; i++) {
                    //    str = NodeNewtoOld.get(bldPath[i].id) + ',' + str;
                    //}
                    //logger.debug(`[${process.pid}]` + ' iter' + iter + ' mv second path ' + pkt.ts + ':' + NodeNewtoOld.get(pkt.zi) + '-' + pkt.zj + ':' + ptype[buildPathType] + ' path=' + str);
                    //}
                    //end log
                    //var tm1 = process.hrtime(); 
                    //logger.debug(`[${process.pid}]` + ' iter' + iter + ' skim start');
                    //skim paths 
                    var ts = currTp;
                    for (var i = 0; i <= 1; i++) {
                        if (i == 0) {
                            var skimPath = path;
                            //logger.debug(`[${process.pid}]` + ' iter' + iter + ' path len=' + path.length);
                            var tptype = currPathType;
                        } else {
                            var skimPath = bldPath;
                            var tptype = buildPathType;
                        }

                        var j = skimPath.length - 1;
                        var aNd = skimPath[j];
                        while (aNd.id != mgNode.id && j > 0) {
                            var bNd = skimPath[j - 1];
                            var lnk = aNd.dnlink.get(bNd.id);
                            //logger.debug(`[${process.pid}]` + ' iter' + iter + ' pathType=' + ptype[tptype] + ' lnk=' + NodeNewtoOld.get(aNd.id) + '-' + NodeNewtoOld.get(lnk.bNd.id));
                            timeskim[tptype] = timeskim[tptype] + lnk.time[ts];
                            dist[tptype] = dist[tptype] + lnk.dist;
                            toll[tptype] = toll[tptype] + lnk.toll[ts];
                            timeFF[tptype] = timeFF[tptype] + lnk.fftime[ts];
                            ts = ts + Math.floor(timeskim[tptype] / 15);
                            if (ts > par.timesteps) {
                                ts = ts - par.timesteps;
                            }
                            aNd = bNd;
                            j = j - 1;
                        }
                    }
                }
                //calculate probability
                if (dist[0] > dist[1] * par.distmaxfactor) {
                    probability = 0;
                    //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' probability=0 distTf=' + dist[1] + ' distTl=' + dist[0]);
                } else if (dist[1] > dist[0] * par.distmaxfactor) {
                    probability = 1;
                    //logger.debug(`[${process.pid}]` + ' iter ' + iter + ' probability=1 distTf=' + dist[1] + ' distTl=' + dist[0]);
                } else {                                        
                    var scale = Math.pow(par.choicemodel.scalestdlen / dist[0], par.choicemodel.scalealpha);
                    var relia = par.choicemodel.timecoeff * par.choicemodel.reliacoeffratio * par.choicemodel.reliacoefftime *
                        ((timeFF[1] - timeskim[1]) * Math.pow(dist[1], (-1 * par.choicemodel.reliacoeffdist))
                            - (timeFF[0] - timeskim[0]) * Math.pow(dist[0], (-1 * par.choicemodel.reliacoeffdist)));
                    var utility = -1 * par.choicemodel.tollconst[currTp - 1] - scale * (par.choicemodel.timecoeff * (timeskim[0] - timeskim[1])
                        + par.choicemodel.tollcoeff * (toll[0] - toll[1]) + relia);
                    probability = 1 / (1 + Math.exp(utility));
                    //log file                    
                    //if (par.log.dpnode.indexOf(NodeNewtoOld.get(bNode.id)) != -1) {                             //hasn't been checked in this iteration  
                    //    choiceStream.write(iter + ',' + NodeNewtoOld.get(pkt.zint) + ',' + pkt.zj + ',' + pkt.tsint + ',' + currTp + ',' + NodeNewtoOld.get(bNode.id)
                    //        + ',' + NodeNewtoOld.get(mgNode.id) + ',' + Math.round(dist[0] * 100) / 100 + ',' + Math.round(dist[1] * 100) / 100 + ',' + Math.round(timeskim[0] * 100) / 100
                    //        + ',' + Math.round(timeskim[1] * 100) / 100 + ',' + timeFF[0] + ',' + timeFF[1] + ',' + toll[0] + ',' + toll[1] + ',' + Math.round(utility*100)/100
                    //        + ',' + Math.round(probability * 10000) / 10000 + os.EOL);
                    //}
                    //logger.debug('probability calculation: tollconst=' + par.choicemodel.tollconst[tp - 1] + ',scalesdlen=' + par.choicemodel.scalestdlen
                    //    + ',scalealpha=' + par.choicemodel.scalealpha + ',timecoeff=' + par.choicemodel.timecoeff
                    //    + ',tollcoeff=' + par.choicemodel.tollcoeff + ',reliacoeffratio=' + par.choicemodel.reliacoeffratio
                    //    + ',reliacoefftime=' + par.choicemodel.reliacoefftime + ',reliacoeffdist=' + par.choicemodel.reliacoeffdist);

                    //console.log('iter ' + iter + ' ' + currTp + ':' + NodeNewtoOld.get(pkt.zi) + '-' + pkt.zj + ':' + pkt.mode + ':' + pkt.pathType
                    //    + ' dp id=' + NodeNewtoOld.get(bNode.id) + ' distTl=' + Math.round(dist[0] * 100) / 100 + ',distTf=' + Math.round(dist[1] * 100) / 100 + ',timeTl=' + Math.round(timeskim[0] * 100) / 100
                    //    + ',timeTf=' + Math.round(timeskim[1] * 100) / 100 + ',timeFFTl=' + Math.round(timeFF[0]*100)/100 + ',timeFFTf=' + timeFF[1] + ',Toll=' + Math.round(toll[0] * 100) / 100
                    //    + ',utility=' + Math.round(utility * 100) / 100 + ',probability=' + Math.round(probability * 10000) / 10000);
                }

                //calculate vol
                var splitVol = [0, 0];
                if (probability == 0) {
                    splitVol[1] = pkt.vol;
                } else if (probability == 1) {
                    splitVol[0] = pkt.vol;
                } else {
                    splitVol[0] = pkt.vol * probability;
                    splitVol[1] = pkt.vol * (1 - probability);
                }  
                if (splitVol[buildPathType] >= par.minpacketsize) {
                    if (splitVol[currPathType] < par.minpacketsize) {
                        splitVol[buildPathType] = pkt.vol;
                        if (splitVol[currPathType] > 0) {
                            var splitCnt = par.maxsplitcount + 1;
                        } else {
                            var splitCnt = splitCnt + 1;
                        }
                        var packet = {
                            iter: iter, ts: currTp, zi: bNode.id, zj: pkt.zj, vol: splitVol[buildPathType], path: bldPath, pathType: ptype[buildPathType],
                            mode: pkt.mode, zint: pkt.zint, tsint: pkt.tsint, totTime: timeskim[buildPathType], splitCnt: splitCnt
                        };
                        mvTasks.push(packet);
                        break;
                    } else { 
                        var splitCnt = splitCnt + 1;
                        var packet = {
                            iter: iter, ts: currTp, zi: bNode.id, zj: pkt.zj, vol: splitVol[buildPathType], path: bldPath, pathType: ptype[buildPathType],
                            mode: pkt.mode, zint: pkt.zint, tsint: pkt.tsint, totTime: timeskim[buildPathType], splitCnt: splitCnt
                        };
                        mvTasks.push(packet);
                        pkt.vol = splitVol[currPathType];
                    }                                       
                } else { 
                    if (splitVol[currPathType] > 0) {
                        pkt.splitCnt = par.maxsplitcount + 1;
                    } else {
                        pkt.splitCnt = pkt.splitCnt + 1;
                    }
                }                                
                //logger.debug(`[${process.pid}]` + ' iter' + iter + ' nanosec=' + process.hrtime(tm1)[1] / 1000000 + ' skim end');
            }                
            aNode = path.pop();
        } //end loop        
        callback(); 
    }
}

redisJob.subscribe("job");
//log redis error
redisClient.on("error", function (err) {
    console.log(`[${process.pid}] redis error ` + err);
});

//********START******** 
async.series([
    function (callback) {
        logStream.write('read run list file...');
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
            startTimeStep = par.starttimestep;
            endTimeStep = par.timesteps;
            rdnd(function (err, result) {
                logStream.write(`read node ` + par.zonenum + ' zones');
                callback();
            });
        },
        function (callback) {
            rdlnk(function (err, result) {
                logStream.write(`read link ` + result + ' links');
                callback();
            });
        }
    ],
    function () {
        logStream.write(`worker node is ready`);
    });
}

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
                var spZone = 0;
                var zoneNode = null;
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
                                    console.log('iter' + iter + ' thread ' + result + ' znpath=' + stats.zonepath + ' zonepkt=' + stats.zonepacket + 
                                        ' dppath=' + stats.dppath  + ' dppkt=' + stats.dppacket);
                                    if (result == par.numprocesses) {
                                        console.log('iter' + iter + ' --- sp and mv done ---');
                                        redisClient.publish('job', 'linkvolredis');
                                        redisClient.publish('job_status', 'linkvolredis');
                                    }
                                });
                            } else {
                                var tsk = result.split(','); //iter-tp-zone-mode
                                iter = tsk[0];
                                timeStep = parseInt(tsk[1]);
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
                                //read tasks
                                redisClient.select(5);
                                redisClient.get(spZone + ':' + timeStep + ':' + mode, function (err, result) {
                                    if (result != null) {
                                        var tsks = result.split(',');   //from spZone to all destination zones 'zj:vol'
                                        tsks.forEach(function (tsk) {
                                            var arrtsk = tsk.split(':');    //tsk = 'zj:vol'
                                            var packet = {
                                                iter: iter, ts: timeStep, zi: parseInt(spZone), zj: parseInt(arrtsk[0]), vol: parseFloat(arrtsk[1]),
                                                path: [], pathType: 'ct', mode: mode, zint: parseInt(spZone), tsint: timeStep, totTime: 0, splitCnt: 0
                                            };
                                            mvTasks.push(packet);  
                                        });                                            
                                    }
                                    callback();
                                });
                            },
                            function (callback) {
                                //**build sp for a single centroid by mode, timestep
                                zoneNode = allNodes.get(parseInt(spZone));
                                sp(zoneNode, par.zonenum, parseInt(timeStep), mode, pathType, iter, 0, 0, 1, [], function (err, result) {
                                    //logger.info(`[${process.pid}]` + ' iter' + iter + ' sp zone=' + spZone);
                                    callback();
                                })
                            },
                            function (callback) {
                                //move all tasks
                                var pkt = mvTasks.pop();
                                while (pkt != null) {                                                                                   
                                    mv(pkt, function (err, result) {
                                        //logger.info(`[${process.pid}]` + ' iter' + iter + ' moved ' + NodeNewtoOld.get(pkt.zi) + ' to ' + NodeNewtoOld.get(pkt.zj));                                                                                               
                                    });
                                    pkt = mvTasks.pop(); 
                                    //logger.info(`[${process.pid}]` + ' iter' + iter + ' task pop');
                                }
                                callback();
                            }],
                            function () { 
                                zoneNode.path = [];
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
                                            strVol = '-1';
                                        } else {
                                            strVol = strVol + ',-1';
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
                        console.log('iter' + iter + ' --- write vol to redis done --- ');
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
                                console.log('iter' + iter + ' --- linkupdate done ---');
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
                                stepSize = 30 * Math.pow(iter, 3) / ((iter + 1) * (2 * iter + 1) * (3 * Math.pow(iter, 2) + 3 * iter - 1));
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
                                        if (arrV_cur[i] == '-1') {
                                            var msaVol = parseFloat(arrV_pre[i]);
                                        } else {
                                            var msaVol = parseFloat(arrV_pre[i]) * (1 - stepSize) + parseFloat(arrV_cur[i]) * stepSize;
                                        }
                                        if (strVol == '') {
                                            strVol = msaVol.toString();
                                        } else {
                                            strVol = strVol + ',' + msaVol.toString();
                                        }
                                        volMode.push(msaVol); 
                                        volModeDiff.push(Math.abs(msaVol - parseFloat(arrV_pre[i])));
                                    }
                                    redisClient.set(key1, strVol, function (err, result) {
                                        callback();
                                    });
                                });
                            });
                        } else {
                            var key1 = link + ":" + iter;
                            redisClient.get(key1, function (err, result) {
                                //logger.info(`[${process.pid}]` + ' iter' + iter + ' msa vol key=' + key1 + ' ' + result);
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
                                        var cgTime = lnk.fftime[i + 1] * (1 + lnk.alpha * Math.pow(Math.min(vol[i] * 4 / lnk.cap, par.maxvc), lnk.beta));
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
                                    var diff = Math.round((vht[i] - vhtPre[i])*1000)/1000;
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
                                    var toll = par.tollpolicy.mintoll + (par.tollpolicy.maxtoll - par.tollpolicy.mintoll) * Math.pow(vc, par.tollpolicy.exp);
                                    toll = Math.round(Math.min(toll, par.tollpolicy.maxtoll)*100)/100;
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
                                        tollStream.write(iter + ',' + (i + 1) + ',' + lnk.aNd.id + ',' + lnk.bNd.id + ',' + Math.round(vol[i]*100)/100 + ',' +
                                            lnk.cap + ',' + Math.round(vc*100)/100 + ',' + toll + ',' + msaToll + ',' + par.tollpolicy.exp + ',' + par.tollpolicy.mintoll + ',' +
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
        choiceStream.end();
        process.exit(0); //End server
    }
});