#!/usr/bin/env bash
#UPLOAD SCRIPT TO S3 IF $1==upload
if [[ $1 == "upload" ]];then
    aws s3 cp start.sh s3://mapreducechunks
    exit 0
fi

#EC2 SETUP  ENV FOR MAP REUDCE EXECUTION
#needed package downloaded; internal project dependencies handled by makefile
sudo yum install -y golang git htop
#get code
git clone https://andysnake96@bitbucket.org/andysnake96/mapreduceextended.git
cd mapreduceextended
myIp="$(dig +short myip.opendns.com @resolver1.opendns.com)"
if [[ $1 == "master" ]]; then
    echo "starting master...."
    make master
    sudo chmod +x master/master
    ./master/master
else
    echo "starting worker...."
    make worker
    sudo chmod +x worker/worker
    ./worker/worker | tee log_$myIp.log
fi
