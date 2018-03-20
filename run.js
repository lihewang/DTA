
//---Parameters---
var bucketName = 'eltod-1';       //match Google Cloud storage bucket name

var clusterName = "eltod";
var zone = "us-central1-a";
var numNodes = 3;
var numWorkerPods = 5;
var machineType = 'g1-small'; 
//var machineType = 'n1-standard-1';
var deleteCluster = true;              //delete cluster after model run finished
//-----------------

//clear console
process.stdout.write('\033c');
console.log('----------------------------------------------');
console.log('|    Google Container Engine Control v1.0    |');
console.log('|    (c)2018                                 |');
console.log('----------------------------------------------');

const { exec } = require('child_process');
var prjId = '';
exec("gcloud config list --format=json", (err, stdout) => {
    var configList = JSON.parse(stdout);
    console.log('PROJECT_ID = ' + configList.core.project);
    console.log('BUCKET_NAME = ' + bucketName);
    console.log('USER_ACCOUNT = ' + configList.core.account);
    prjId = configList.core.project;
    init();
});

var init = function () {
    //set pods number in runlist
    var runlistFile = 'runlist.json';
    var fs = require('fs');
    var par = JSON.parse(fs.readFileSync(runlistFile));
    par.runs.forEach(function (scen) {
        par[scen].overwrite.numprocesses = numWorkerPods;
    });
    fs.writeFileSync(runlistFile, JSON.stringify(par, null, "\t"));
    console.log('number of worker pods = ' + numWorkerPods);

    //copy runlist file
    process.stdout.write('copy runlist to cloud storage ... ');
    exec('gsutil cp ' + runlistFile + ' gs://' + bucketName + '/', (err, stdout, stderr) => {
        if (err) {
            console.log(err);
        } else {
            console.log(stdout);
            //clean storage finish file
            exec('gsutil rm gs://' + bucketName + '/output/runfinished', (err, stdout, stderr) => {
                if (err) {
                    createCluster(); //no output from previous run
                } else {
                    console.log('prepare storage done' + stdout);
                    createCluster();
                    //copyOutput();
                }
            });
        }
    });
}
var createCluster = function () {
    var symbols = ['-', '\\', '|', '/'];;
    var ticks = 0;
    objInterval = setInterval(function () {
        ticks = ticks + 1;
        process.stdout.clearLine();        
        process.stdout.cursorTo(0);
        process.stdout.write('creating container cluster ... ' + symbols[ticks % 4] + ' (' + Math.round(1 * ticks / 60 * 10) / 10 + ' min)');
    }, 1000);

    //create cluster
    exec('gcloud container clusters create ' + clusterName + ' --zone=' + zone + ' --num-nodes=' + numNodes + ' --machine-type=' + machineType + ' --scopes=storage-rw'
        , (err, stdout, stderr) => {
            if (err) {
                console.log('create cluster ' + err);
                return;
            }
            clearInterval(objInterval);
            console.log(' done');
            console.log(stdout);

            //get credentials
            exec('gcloud container clusters get-credentials ' + clusterName + ' --zone ' + zone + ' --project ' + prjId, (err, stdout, stderr) => {
                if (err) {
                    console.log('get cluster credentials err ' + err);
                    return;
                }
                console.log('get cluster credentials done' + stdout);

                createPods();
            });
        });
}

var createPods = function () {
    //create redis
    exec('kubectl create -f redis.yaml', (err, stdout, stderr) => {
        if (err) {
            console.log('create redis ' + err);
            return;
        }
        console.log('create redis done - ' + stdout);

        //create worker
        exec('kubectl create -f worker.yaml', (err, stdout, stderr) => {
            if (err) {
                console.log('create worker ' + err);
                return;
            }
            console.log('create worker done - ' + stdout);
            //set worker replicas
            exec('kubectl scale --replicas=' + numWorkerPods + ' deployment/worker', (err, stdout, stderr) => {
                if (err) {
                    console.log('set worker replicas ' + err);
                    return;
                }
                console.log('set worker replicas done - ' + stdout);

                //create main and run model  
                exec('kubectl create -f main.yaml', (err, stdout, stderr) => {
                    if (err) {
                        console.log('create main ' + err);
                        return;
                    }
                    console.log('create main done - ' + stdout);
                    copyOutput();
                });
            });
        });
    });
}

var copyOutput = function () {
    var symbols = ['-', '\\', '|', '/'];
    var ticks = 0;
    objItl = setInterval(function () {
        ticks = ticks + 1;
        process.stdout.clearLine();
        process.stdout.cursorTo(0);
        process.stdout.write('running model ... ' + symbols[ticks % 4] + ' (' + Math.round(1 * ticks / 60 * 10) / 10 + ' min)');       
    }, 1000);  
    objRun = setInterval(function () {
        exec('gsutil ls gs://' + bucketName + '/output/runfinished', (err, stdout, stderr) => {
            if (err) {
                return;
            } else {
                clearInterval(objItl);
                clearInterval(objRun);
                console.log(' done');
                delCluster();
            }
        });
    }, 10000);
}

var delCluster = function () {
    exec('gsutil cp -r gs://' + bucketName + '/output/* ./output/', (err, stdout, stderr) => {
        if (err) {  
            console.log('copy output ' + err);
        } else {
            if (deleteCluster) {
                var symbols = ['-', '\\', '|', '/'];
                var ticks = 0;
                objI = setInterval(function () {
                    ticks = ticks + 1;
                    process.stdout.clearLine();
                    process.stdout.cursorTo(0);
                    process.stdout.write('deleting cluster ... ' + symbols[ticks % 4] + ' (' + Math.round(1 * ticks / 60 * 10) / 10 + ' min)');
                }, 1000);

                exec('gcloud container clusters delete ' + clusterName + ' --zone=' + zone + ' --quiet', (err, stdout, stderr) => {
                    if (err) {  
                        console.log('WARNING: cluser clearn up ' + err);
                        console.log('YOU ARE STILL BEING CHARGED FOR BY GOOGLE! GO TO GOOGLE CLOUD CONSOLE TO DELETE THE CLUSTER!');
                    } else {
                        clearInterval(objI);
                        console.log(' done');
                        console.log('end of model run ' + stdout);
                    }
                });
            } else {                
                console.log('WARNING: YOU ARE STILL BEING CHARGED FOR BY GOOGLE! GO TO GOOGLE CLOUD CONSOLE TO DELETE THE CLUSTER!');
                console.log('end of model run');
            }
        }
    });    
}