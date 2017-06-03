//Get redis docker image from docker official repository (19.82MB)
docker pull redis:alpine

//Run redis container on local host (test purpose)
docker run -d -p 6379:6379 redis:alpine

//Tag docker image before pushing it to Google Container Registory
docker tag redis:alpine gcr.io/bright-primacy-140715/redis:alpine
sudo docker tag launcher.gcr.io/google/redis3:latest gcr.io/bright-primacy-140715/redis:3

//Build worker docker image
sudo docker build -f ./worker_Docker/Dockerfile . -t gcr.io/bright-primacy-140715/worker:latest
sudo docker build -f ./main_Docker/Dockerfile . -t gcr.io/bright-primacy-140715/main:latest

//Remove all images
docker rmi $(docker images -a -q)

//Push docker image to GC Registory
gcloud docker -- push gcr.io/bright-primacy-140715/redis:alpine
sudo gcloud docker -- push gcr.io/bright-primacy-140715/worker:latest
sudo gcloud docker -- push gcr.io/bright-primacy-140715/main:latest

//Create GC cluster*
sudo gcloud container clusters create dta-cluster --zone us-central1-a --num-nodes=3 --machine-type=f1-micro --scopes https://www.googleapis.com/auth/cloud_debugger

//Get auth
sudo gcloud auth application-default login

//init gcloud
sudo gcloud init

//Get root access
sudo -i

//Get kube-dns
sudo kubectl get svc kube-dns --namespace=kube-system

//Deploy kubernetes dashboard 
sudo kubectl proxy
http://localhost:8001/ui

//Deploy container to cluster
kubectl run redis-server5 --image=gcr.io/bright-primacy-140715/redis:redis-alpine --port=6379


//Create deployment
sudo kubectl create -f ./redis.yaml
sudo kubectl create -f ./worker.yaml
sudo kubectl create -f ./main.yaml

//Delete deployment
sudo kubectl delete -f ./redis.yaml
sudo kubectl delete -f ./worker.yaml
sudo kubectl delete -f ./main.yaml

//Create service
kubectl expose deployment/worker

//Expose port
sudo kubectl expose deployment worker --type=LoadBalancer --port 8080
sudo kubectl expose deployment redis --type=LoadBalancer --port 6379

//ssh to the pod
sudo kubectl exec -it worker-370362994-bvq93 -- sh

//Delete cluster
sudo gcloud container clusters delete dta-cluster --zone=us-central1-a

//image location
gcr.io/bright-primacy-140715/worker:1.0
