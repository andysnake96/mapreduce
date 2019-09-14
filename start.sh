#!/usr/bin/env bash
#EC2 SETUP  ENV FOR MAP REUDCE EXECUTION
#needed package downloaded; internal project dependencies handled by makefile

sudo yum install -y golang git htop
#get code
git clone https://andysnake96@bitbucket.org/andysnake96/mapreduceextended.git
cd mapreduceextended

if [[ $1 == "master" ]]; then
    echo "starting master...."
    make master
    ./master/master
else
    echo "starting worker...."
    make worker
    ./worker/worker
fi
