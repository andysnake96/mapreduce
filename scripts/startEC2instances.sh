#!/usr/bin/env bash
PK_PATH="/home/andysnake/aws/SSNAKE96.pem"
SSH_CMD="ssh -o \"StrictHostKeyChecking no \" -i $PK_PATH ec2-user@"
INIT_WAIT_TIME=300
INSTANCES_HN_FILENAME="instances.list"
spawn_terminals_ssh_to_ec2_instances(){
    echo "waiting for initialization of instances"
    sleep $1
    for hostname in $(cat ${INSTANCES_HN_FILENAME} );do
        echo "${SSH_CMD}${hostname}"
        xfce4-terminal --hold -e "$SSH_CMD$hostname" -T ${hostname}
    done

}

echo "usage: num instance to start"

if [[ $1 == "spawn" ]]; then
    spawn_terminals_ssh_to_ec2_instances $2
else
python3 startEc2Instances.py $1 > ${INSTANCES_HN_FILENAME}

fi

