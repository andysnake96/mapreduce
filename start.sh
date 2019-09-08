#!/usr/bin/env bash
#EC2 SETUP  ENV FOR MAP REUDCE EXE
#USAGE <> FOR WORKER  <MASTER> FOR MASTER

#Download needed packages for code get and build
sudo yum install -y golang git
#get code
git clone https://andysnake96@bitbucket.org/andysnake96/mapreduceextended.git
cd mapreduceextended
#TODO MOVE TO BRANCH TEST
git checkout origin/faultTollerant_localVersion
#get aws go packages
go get -u github.com/aws/aws-sdk-go/aws
go get -u github.com/aws/aws-sdk-go/service/s3
#Build master / worker with right env
cd worker
go build
cd ../master
go build
cd ..
#MASTER START WITH SETTED ANY 1 ARGUMENT, else started worker
if [ -z "$1" ]; then
    ./master/master
else
    ./worker/worker
fi
