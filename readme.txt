//Get redis docker image from docker official repository (19.82MB)
docker pull redis:alpine

//Run redis container on local host (test purpose)
docker run -d -p 6379:6379 redis:alpine

//Tag docker image before pushing it to Google Container Registory
docker tag redis:alpine gcr.io/bright-primacy-140715/redis:alpine

//Build docker image
sudo docker build -f ./worker_Docker/Dockerfile . -t gcr.io/bright-primacy-140715/worker:1.0

//Push docker image to GC Registory
gcloud docker -- push gcr.io/bright-primacy-140715/redis:alpine
sudo gcloud docker -- push gcr.io/bright-primacy-140715/main:1.0
sudo gcloud docker -- push gcr.io/bright-primacy-140715/worker:1.0

//Create GC cluster*
sudo gcloud container clusters create dta-cluster --zone us-central1-a --num-nodes=3 --machine-type=f1-micro --scopes https://www.googleapis.com/auth/cloud_debugger

//Get auth
sudo gcloud auth application-default login

//init gcloud
sudo gcloud init

//Get root access
sudo -i

//Deploy kubernetes dashboard 
sudo kubectl proxy
http://localhost:8001/ui

//Deploy container to cluster
kubectl run redis-server5 --image=gcr.io/bright-primacy-140715/redis:redis-alpine --port=6379


//Create deployment
sudo kubectl create -f ./redis.yaml
sudo kubectl create -f ./worker.yaml

//Delete deployment
kubectl delete -f ./redis.yaml
sudo kubectl delete -f ./worker.yaml

//ssh to the pod
kubectl exec -it <pod_name> -- sh

//Delete cluster
gcloud container clusters delete dta-cluster --zone=us-central1-a
