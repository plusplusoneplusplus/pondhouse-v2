# Point your shell to minikube's docker-daemon
eval $(minikube docker-env)

# copy build directory to here
mkdir -p build
cp ../../build/pond_server ./build/pond_server
cp ../../build/libsqlparser.so ./build/libsqlparser.so

# Build the image
docker build --no-cache -t pond-server:0.0.1 .

# Save the image to minikube
minikube image load pond-server:0.0.1

kubectl apply -f ./k8s/deployment.yaml
kubectl apply -f ./k8s/service.yaml