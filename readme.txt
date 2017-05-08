//Get redis docker image from docker official repository (19.82MB)
docker pull redis:alpine

//Run redis container on local host (test purpose)
docker run -d -p 6379:6379 redis:alpine

//Tag docker image before pushing it to Google Container Registory
docker tag redis:alpine gcr.io/bright-primacy-140715/redis:alpine

//Push docker image to GC Registory
gcloud docker -- push gcr.io/bright-primacy-140715/redis:alpine

//Create GC cluster
gcloud container clusters create dta-cluster --zone us-central1-a --num-nodes=3

//Deploy container to cluster
kubectl run redis-server5 --image=gcr.io/bright-primacy-140715/redis:redis-alpine --port=6379

//Delete cluster
gcloud container clusters delete dta-cluster --zone=us-central1-a

