var async = require('async');
var google = require('googleapis');
var container = google.container('v1');
var K8s = require('k8s');

var prjId = "dta-beta";
var zone = "us-central1-a";
var clusterName = "eltod";
var kubectlBin = 'C:\\Users\\lihe.wang\\AppData\\Local\\Google\\Cloud SDK\\google-cloud-sdk\\bin\\kubectl.exe';
var key = require('./dta-beta-c31385fcfaac.json'); //service account key
var endPnt = '35.226.114.170';
//clear console
process.stdout.write('\033c');

var jwtClient = new google.auth.JWT(
    key.client_email,
    null,
    key.private_key,
    ['https://www.googleapis.com/auth/devstorage.read_write',
     'https://www.googleapis.com/auth/cloud-platform'], // an array of auth scopes
    null
);

jwtClient.authorize(function (err) {
    if (err) {
        console.log('authorize err ' + err);
    } else {
        console.log('access to google cloud authorized');
        //createCluster();
        //deployRedis();
        deployWorker();
    }
});

var createCluster = function () {
    //create cluster
    var request = {
        projectId: prjId,
        zone: zone,
        resource: {
            cluster: {
                name: clusterName,
                initialNodeCount: "3",
                nodeConfig: {
                    "machineType": "f1-micro"
                }
            }
        },
        auth: jwtClient
    };

    container.projects.zones.clusters.create(request, function (err, result) {
        if (err) {
            console.log('create cluster err ' + err);
        } else {
            var request = {
                projectId: prjId,
                zone: zone,
                clusterId: clusterName,
                auth: jwtClient
            };
            var dots = '';
            //wait until the cluster is running
            objInterval = setInterval(function () {                
                container.projects.zones.clusters.get(request, function (err, res) {
                    if (err) {
                        console.log('get cluster info err ' + err);
                    } else {
                        if (res.data.status == 'RUNNING') {
                            endPnt = res.data.endpoint;
                            console.log('');
                            console.log('cluster is running at ' + endPnt);
                            clearInterval(objInterval);
                            deployRedis();
                        } else {
                            dots = dots + '.';
                            process.stdout.clearLine();
                            process.stdout.cursorTo(0); 
                            process.stdout.write('creating cluster ' + dots);
                        }
                    }
                });
            }, 10000);                
        }
    });
}    

var deployRedis = function () {
    console.log('deploy redis');
    var kubectl = K8s.kubectl({
        endpoint: 'https://' + endPnt,
        binary: kubectlBin
    })
    kubectl.command('create -f redis.yaml', function (err, result) {
        if (err) {
            console.log('create redis ' + err);
        }
        console.log('create redis ' + result);
        kubectl.command('expose redis --port 6379', function (err, result) {
            if (err) {
                console.log('expose redis ' + err);
            }
            console.log('expose redis - ' + result);
            deployWorker();
        });
    });    
}

var deployWorker = function () {
    console.log('deploy workers');
    var kubectl = K8s.kubectl({
        endpoint: 'https://' + endPnt,
        binary: kubectlBin
    })
    //wait until redis is running
    var dots = '';
    objInterval2 = setInterval(function () {
        kubectl.command('get service -o jsonpath="{.items[?(@.metadata.name==\'redis\')].metadata.name}"', function (err, result) {
            if (err) {
                console.log('get service ' + err);
            } else {                
                if (result == '"redis"') {
                    clearInterval(objInterval2);
                    console.log('redis deployed');
                    kubectl.command('create -f worker.yaml', function (err, result) {
                        if (err) {
                            console.log('create workers ' + err);
                        }
                        console.log('create workers - ' + result);
                    });
                } else {
                    dots = dots + '.';
                    process.stdout.clearLine();
                    process.stdout.cursorTo(0);
                    process.stdout.write('creating redis ' + dots);
                }
            }
        });
    }, 5000);                
    ////workers
    
}
//var deployWorker = setInterval(function () {
//    console.log('creating redis ');
//    //process.exit();
//}, 1000);
  /*
  //delete cluster
  var request = {
    projectId: "dta-cloud",
    zone: 'us-east1-b',
    clusterID: "model-cluster",
    auth: authClient
  };
  container.projects.zones.clusters.delete(request, function(err, result) {
    if (err) {
      console.log(err);
    } else {
      console.log(result);
    }
  });*/
//module.exports.auth = auth;
//module.exports.createCluster = createCluster;