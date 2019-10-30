#!/bin/bash
if [[ $1 == "upload" ]];then
    aws s3 cp startSSHRemotePortForwarding.sh s3://mapreducechunks
    exit 0
fi

sudo su
sudo echo -e "\nGatewayPorts yes\nAllowTcpForwarding all" >> /etc/ssh/sshd_config
sudo /etc/init.d/sshd restart


echo -n "$(dig +short myip.opendns.com @resolver1.opendns.com):6000" > MASTER_ADDRESS
aws s3 cp MASTER_ADDRESS s3://mapreducechunks
