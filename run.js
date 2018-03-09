//var google = require('googleapis');
//var container = google.container('v1');
//var K8s = require('k8s');

var prjId = "dta-beta";
var zone = "us-central1-a";
var clusterName = "eltod";
var numNodes = 3;
var machineType = 'n1-standard-1';
var bucketName = 'eltod-beta'
var deleteCluster = true;

//clear console
process.stdout.write('\033c');
console.log('----------------------------------------------');
console.log('|    Google Container Engine Control v1.0    |');
console.log('|    (c)2018                                 |');
console.log('----------------------------------------------');

const { exec } = require('child_process');
//clean output storage
exec('gsutil rm -r gs://' + bucketName + '/output', (err, stdout, stderr) => {
    if (err) {
        createCluster(); //no output from previous run
    } else {
        console.log('clean storage done ' + stdout);
        createCluster();
        //copyOutput();
    }    
});

var createCluster = function () {
    var symbols = ['-', '\\', '|', '/'];;
    var ticks = 0;
    objInterval = setInterval(function () {
        ticks = ticks + 1;
        process.stdout.clearLine();
        process.stdout.cursorTo(0);
        process.stdout.write('creating container cluster ... ' + symbols[ticks % 4] + ' (' + Math.round(5 * ticks / 60 * 10) / 10 + ' min)');
    }, 5000);

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
}

var copyOutput = function () {
    var symbols = ['-', '\\', '|', '/'];
    var ticks = 0;
    objItl = setInterval(function () {
        ticks = ticks + 1;
        process.stdout.clearLine();
        process.stdout.cursorTo(0);
        process.stdout.write('running model ... ' + symbols[ticks % 4] + ' (' + Math.round(10 * ticks / 60 * 10) / 10 + ' min)');
        exec('gsutil ls gs://' + bucketName + '/output/runfinished', (err, stdout, stderr) => {
            if (err) {  
                return;
            } else {
                clearInterval(objItl);
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
                console.log('deleting cluster ...');
                exec('gcloud container clusters delete ' + clusterName + ' --zone=' + zone + ' --quiet', (err, stdout, stderr) => {
                    if (err) {  
                        console.log('WARNING: cluser clearn up ' + err);
                        console.log('YOU ARE STILL BEING CHARGED FOR BY GOOGLE! GO TO GOOGLE CLOUD CONSOLE TO DELETE THE CLUSTER!');
                    } else {
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