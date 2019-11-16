#!/usr/bin/env bash
#UPLOAD SCRIPT TO S3 IF $1==upload
if [[ $1 == "upload" ]];then
    aws s3 cp start_master.sh s3://mapreducechunks
    exit 0
fi
sudo yum install -y golang  htop
GET_CODE_WITH_S3=1
RESTART_PORT=4444 #message on this port on worker node cause restarting of worker
#not sync restart simulation
RAND_SLEEP_ON_RESTART=1
MAXSLEEPTIME=5
MAXSLEEPTIMESUBSEC=9696069
cd /home/ec2-user
if [[ $GET_CODE_WITH_S3 == 1 ]];then
    aws s3 cp  s3://mapreducechunks/tmpDump.zip tmpDump.zip
    unzip tmpDump.zip -d go
    cd go
else #get code by git repo
    sudo yum install -y git
    git clone https://andysnake96@bitbucket.org/andysnake96/mapreduceextended.git
    cd mapreduceextended
fi
myIp="$(dig +short myip.opendns.com @resolver1.opendns.com)"

mkdir ../tmp
export GOPATH=$(realpath .)
export GOCACHE=$(realpath ../tmp)

make master
sudo chmod +x master/master
#./master/master
