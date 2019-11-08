#!/usr/bin/env bash
#UPLOAD SCRIPT TO S3 IF $1==upload
if [[ $1 == "upload" ]];then
    aws s3 cp start.sh s3://mapreducechunks
    exit 0
fi
sudo yum install -y golang  htop
GET_CODE_WITH_S3=1
RESTART_PORT=4444 #message on this port on worker node cause restarting of worker
#not sync restart simulation
RAND_SLEEP_ON_RESTART=1
MAXSLEEPTIME=5
MAXSLEEPTIMESUBSEC=9696069
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

workerEndlessRetryDgb(){
    trap "" HUP				    #may allow restart on some fail crushing whole script?
    for (( ; ; ))                           #handle restart request
    do
        echo "RESTARTING WORKER...." | nc -l ${RESTART_PORT} -w 0
	if [[ $RAND_SLEEP_ON_RESTART == 1 ]];then
    		#RANDOM SLEEP TO AVOID PARTIALLY SYNC START
		sleepSec="$(shuf -i 0-$MAXSLEEPTIME -n 1)"
		sleepSec="$(shuf -i 0-$MAXSLEEPTIMESUBSEC -n 1)"
		sleep $MAXSLEEPTIME.$MAXSLEEPTIMESUBSEC
	fi
	sudo killall worker
        #PUSH GENERATED LOG TO S3
	echo -e "\n\n\n\n" >>log_$myIp.log 
        netstat -la >> log_$myIp.log 
        aws s3 cp log_$myIp.log  s3://mapreducechunks
        ./worker/worker > log_$myIp.log 2>&1 &
    done
}
mkdir ../tmp
export GOPATH=$(realpath .)
export GOCACHE=$(realpath ../tmp)
if [[ $1 == "master" ]]; then
    echo "starting master...."
    make master
    sudo chmod +x master/master
    ./master/master
else
    make worker
    sudo chmod +x worker/worker
    echo "starting worker...."
    workerEndlessRetryDgb &			#separated endless loop
    ./worker/worker > log_$myIp.log 2>&1
fi
