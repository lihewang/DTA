var async = require('async');
var google = require('googleapis');
var container = google.container('v1');
var K8s = require('k8s');
//var stringify = require('json-stringify');

var prjId = "dta-01";
var zone = "us-central1-a";
var clusterName = "eltod";
var key = require('./dta-01-7b4aaca7da3d.json'); //service account key

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
        createCluster(); 
        //deployRedis();
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
            }
        },
        auth: jwtClient
    };

    container.projects.zones.clusters.create(request, function (err, result) {
        if (err) {
            console.log('create cluster err ' + err);
        } else {
            console.log('creat cluster');
            var request = {
                projectId: prjId,
                zone: zone,
                clusterId: clusterName,
                fields: "status",
                auth: jwtClient
            };
            //wait until the cluster is running
            objInterval = setInterval(function () {
                container.projects.zones.clusters.get(request, function (err, res) {
                    if (err) {
                        console.log('get cluster info err ' + err);
                    } else {
                        if (res.data.status == 'RUNNING') {
                            console.log('cluster is running');
                            clearInterval(objInterval);
                            deployRedis();
                        } else {
                            console.log('waiting for cluster to be ready ...');
                        }
                    }
                });
            }, 10000);                
        }
    });
}    

var deployRedis = function () {
    console.log('creating redis ');
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