#!/usr/bin/env bash
#developped by andysnake96
#script to init netcat relay to implement a port forwarding service for a double Natted EndUser using a remote server
#connection relay (implemented simply by GNU netcat) will be installed on natted EndHost and on server
#must be provided newline separated list of Ports to expose and list of different Relay Ports 
#see Readme.md
killall nc
my_pubblic_ip="$(dig +short myip.opendns.com @resolver1.opendns.com)"
echo "I'm at " $my_pubblic_ip
if [ $# != 5 ]
then
	echo "usage: 0|1 (client,server) , 0|1 (OS Based on debian redHat), ToExposePortsFilePath, [RelayPortsFilePath], remote server IP "
	exit 1
fi

listen_netcat_debian_based="nc -l -k -p"
listen_netcat_redHat_based="nc -l -k"
listen_netcat=$listen_netcat_redHat_based
#netcat commmand set for different OSs
if [ $2 == 0 ]
then
	listen_netcat=$listen_netcat_debian_based
fi

ports_toexpose_num="$(wc -l $3 | awk '{ print $1 }')"
if [ $ports_toexpose_num !=  $(wc -l $4 | awk '{ print $1 }') ] || [ ports_toexpose_num == 0 ]
then
	echo "invalid relay port file passed, different num of lines "
	exit 1
fi
for l in $(seq 1 $ports_toexpose_num);do
	portToExpose="$(awk -v line=$l 'NR==line' $3 )"
	relayPort="$(awk -v line=$l 'NR==line' $4 )"
	echo $portToExpose $relayPort $l $5
	if [ $1 == 1 ]  #PORT FORWARD SERVER
	then 
		$listen_netcat $portToExpose | $listen_netcat $relayPort &
	else
		nc $5 $relayPort | nc localhost $portToExpose 		 &
	fi
done
