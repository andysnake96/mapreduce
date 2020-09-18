#!/usr/bin/env bash
PK_PATH="/home/andysnake/aws/SSNAKE96.pem"
PID_REG_CMD="echo $$ >> newPids.list"
SSH_CMD="ssh -o \"StrictHostKeyChecking no \" -i $PK_PATH ec2-user@"
INIT_WAIT_TIME=300
RESTART_PORT=4444
INSTANCES_HN_FILENAME="instances.list"
BOTO3_WRAP_PYTHON_FILENAME="EC2instances.py"
spawn_terminal_sh_to_ec2(){
	if [ -z "$1" ] 
	then
          echo $SSH_CMD
          echo $SSH_CMD | $( cat pastToXClipboard.sh )
	  return
	fi
    xfce4-terminal --hold -e "$SSH_CMD$1" -T $1
}

spawn_terminals_ssh_to_ec2_instances(){
    echo "waiting for initialization of instances"
    sleep $1
    for hostname in $(cat ${INSTANCES_HN_FILENAME} );do
        xfce4-terminal --hold -e "$SSH_CMD$hostname" -T ${hostname}
    done

}
if [[ $# < 1 ]];then
	echo  "usage spawn | spawn_ssh_to | relay | master | terminate_instances | terminate | get_hostnames | running| restart | clean-logs | num worker to start"
	exit 1
fi
nice -n 20 ./spinner.sh &
spinner_pid="$!"
trap 'kill $spinner_pid && exit 0' INT 
echo $!
if [[ $1 == "spawn" ]]; then
    spawn_terminals_ssh_to_ec2_instances $2
elif [[ $1 == "spawn_ssh_to" ]]; then
    spawn_terminal_sh_to_ec2 $2
elif [[ $1 == "relay" ]]; then
    relayHostName="$(python3 $BOTO3_WRAP_PYTHON_FILENAME relay)"
    echo "RELAY_ACTIVATION_SSH_CMD_COMPLD"
    echo "ssh -fNTR 6000:localhost:6000 ec2-user@$relayHostName -i $PK_PATH"
    echo "ssh -fNTR 6666:localhost:6666 ec2-user@$relayHostName -i $PK_PATH"
    echo "ssh -fNTR 5555:localhost:5555 ec2-user@$relayHostName -i $PK_PATH"
    sleep 20
    ssh -o "StrictHostKeyChecking no " -fNTR 6000:localhost:6000 ec2-user@$relayHostName -i $PK_PATH
    ssh -o "StrictHostKeyChecking no " -fNTR 6666:localhost:6666 ec2-user@$relayHostName -i $PK_PATH
    ssh -o "StrictHostKeyChecking no " -fNTR 5555:localhost:5555 ec2-user@$relayHostName -i $PK_PATH
    export RELAY_HN=$relayHostName
elif [[ $1 == "master" ]]; then
    python3 $BOTO3_WRAP_PYTHON_FILENAME "master" 
    echo $SSH_CMD | xclip -selection c #extra to copy the template of ssh connection
elif [[ $1 == "terminate_instances" ]]; then
    python3 $BOTO3_WRAP_PYTHON_FILENAME "terminate_instances" $(cat ${INSTANCES_HN_FILENAME} | xargs -d "\n")
    rm ${INSTANCES_HN_FILENAME}
elif [[ $1 == "running" ]]; then
    aws ec2 describe-instances | grep "running" | wc -l
elif [[ $1 == "terminate" ]]; then
    python3 $BOTO3_WRAP_PYTHON_FILENAME "terminate"
    rm ${INSTANCES_HN_FILENAME}
elif [[ $1 == "get_hostnames" ]]; then
    aws ec2 describe-instances | grep -Eo "ec2.*com" | uniq > ${INSTANCES_HN_FILENAME}
elif [[ $1 == "terminate_shells" ]]; then
    sudo killall xfce4-terminal
elif [[ $1 == "restart" ]]; then
     aws s3 rm s3://mapreducechunks/ --recursive --exclude "*" --include "log*"
     for hostname in $(cat ${INSTANCES_HN_FILENAME} );do
        #nc ${hostname} ${RESTART_PORT} -q 0 < $INSTANCES_HN_FILENAME
        nc ${hostname} ${RESTART_PORT} -q 0  < /dev/null
	echo $?
    done
elif [[ $1 == "clean-logs" ]]; then
    aws s3 rm s3://mapreducechunks/ --recursive --exclude "*" --include "log*"
else
	#python3 $BOTO3_WRAP_PYTHON_FILENAME $1 > ${INSTANCES_HN_FILENAME}
	python3 $BOTO3_WRAP_PYTHON_FILENAME $1 
    	aws ec2 describe-instances | grep -Eo "ec2.*com" | uniq > ${INSTANCES_HN_FILENAME}
	grep -n $RELAY_HN $INSTANCES_HN_FILENAME
fi

kill $spinner_pid
#docker on ec2 quick install
#sudo yum -y install docker 
#sudo service docker start
#sudo chmod 666 /var/run/docker.sock
#sudo usermod -a -G docker ec2-user
aws s3 cp --recursive --exclude "*" --include "log*" s3://mapreducechunks/ .
