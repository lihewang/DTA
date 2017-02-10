
var google = require('googleapis');
var container = google.container('v1');
var clusterName = "model-cluster";

//function auth(callback){
    google.auth.getApplicationDefault(function(err, authClient) {  
    if (err) {       
        console.log('Authentication failed because of ', err);
        callback(err,null);
    }
    if (authClient.createScopedRequired && authClient.createScopedRequired()) {
         console.log('auth');
        var scopes = ['https://www.googleapis.com/auth/cloud-platform'];
        authClient = authClient.createScoped(scopes);
        console.log(authClient);
        //callback(null,authClient);
    }
  
//create cluster
  var request = {
    projectId: "dta-cloud",
    zone: "us-east1-b",
    resource: {
        cluster: {
        name: clusterName,
        initialNodeCount: "2",
        }
    },
    auth: authClient
  };

  container.projects.zones.clusters.create(request, function(err, result) {
    if (err) {
      console.log(err);
    } else {
      console.log(result);
    }
  });

});

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


