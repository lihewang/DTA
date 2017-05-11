//Get redis docker image from docker official repository (19.82MB)
docker pull redis:alpine

//Run redis container on local host (test purpose)
docker run -d -p 6379:6379 redis:alpine

//Tag docker image before pushing it to Google Container Registory
docker tag redis:alpine gcr.io/bright-primacy-140715/redis:alpine

//Push docker image to GC Registory
gcloud docker -- push gcr.io/bright-primacy-140715/redis:alpine

//Create GC cluster*
sudo gcloud container clusters create dta-cluster --zone us-central1-a --num-nodes=3

//Get auth
gcloud auth application-default login

//init gcloud
sudo gcloud init

//Get root access
sudo -i

//Deploy kubernetes dashboard 
sudo kubectl create -f https://rawgit.com/kubernetes/dashboard/master/src/deploy/kubernetes-dashboard.yaml
sudo kubectl proxy
http://localhost:8001/ui

//Deploy container to cluster
kubectl run redis-server5 --image=gcr.io/bright-primacy-140715/redis:redis-alpine --port=6379


//Create deployment
kubectl create -f ./redis.yaml

//Delete deployment
kubectl delete -f ./redis.yaml

//ssh to the pod
kubectl exec -it <pod_name> -- sh

//Delete cluster
gcloud container clusters delete dta-cluster --zone=us-central1-a
