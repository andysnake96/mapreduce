#!/usr/bin/env bash
#developped by andysnake96
#test fault tollerant of application via several re iteration with random fault enabled
#Hp instances already up so only restart needed --> master instance on ec2 doen't need special permission to re init worker nodes on ec2
NUM_TESTS=100
lastIterationFailed=false

for i in $(seq 1 $NUM_TESTS);do
    cd ..
    ./master/master > master.log &
    master_pid="$!"
    cd scripts
    ./startEC2instances.sh restart
    #FAIL LOGGING
    if [[ $lastIterationFailed == true ]]; then
        mkdir fail$i
        mv masterFail.log fail$i
        ./getLogs.sh
        mv log_* fail$i
        lastIterationFailed=false
    fi
    wait $master_pid        #block until master failed, seeing return value fail logging will be done at next iteration (needed worker upload theirs logs)
    if [[ $? != 0 ]]; then
        lastIterationFailed=true
        cp master.log masterFail.log
        echo " FAILED AT RUN $i "
    fi
done