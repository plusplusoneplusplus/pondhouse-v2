#!/bin/bash

# Read version from version.txt
version=$(cat $(dirname "$0")/version.txt)
export VERSION=$version

echo "Deploying pond server version ${VERSION}"

# Set docker environment to minikube's docker
eval $(minikube docker-env)

# copy build directory to here
mkdir -p gRPC/build
cp ../../build/pond_server ./gRPC/build/pond_server
cp ../../build/libsqlparser.so ./gRPC/build/libsqlparser.so

# Copy proto file for web application
mkdir -p web/proto
cp ../proto/pond_service.proto ./web/proto/

cd gRPC
# Build the server image
docker build -t pond-server:${VERSION} .
cd ..

# Build the envoy image
cd envoy
docker build -t pond-envoy:${VERSION} .
cd ..

# Build the web application
cd web
docker build -t pond-web:${VERSION} .
cd ..

# Apply k8s configurations with version substitution
envsubst < $(dirname "$0")/k8s/deployment.yaml | kubectl apply -f -
envsubst < $(dirname "$0")/k8s/web-deployment.yaml | kubectl apply -f -
kubectl apply -f $(dirname "$0")/k8s/service.yaml