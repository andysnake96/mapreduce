#!/usr/bin/env python3
import boto3
import time
import sys

#scripts to -connect to ec2 basing on user level aws config in ~
#           - start ec2 instances
#           - block until all instances are RUNNING (for ec2) and then print to stdout
#           - integrable with terminal spawn script

ec2Client=boto3.client("ec2")
INSTANCE_CHECK_POLLING_TIME=3
EC2_RUNNING_CODE=16
def startInstancePortRelay():
    #start port forward relay
    launchTemplate= LaunchTemplate={
        'LaunchTemplateName': 'port_forward_relay_server',
        'Version': '6'
    }
    response=ec2Client.run_instances(MaxCount=1,MinCount=1,LaunchTemplate=launchTemplate)
    for instance in response["Instances"]:	#single line
        return instance["InstanceId"]				#requested just 1 instance

def startMaster(userDataScriptPath):
    #start ec2 instance with user data loaded from argument path
    userDataFile=open(userDataScriptPath)
    userData=userDataFile.read()
    userDataFile.close()
    launchTemplate= LaunchTemplate={
        'LaunchTemplateName': 'EC2-INIT-S3DOWN-SCRIPT',
        #'Version': '1'
        'Version': '3'
    }
    #response=ec2Client.run_instances(MaxCount=1,MinCount=1,LaunchTemplate=launchTemplate,UserData=userDataScriptPath)
    response=ec2Client.run_instances(MaxCount=1,MinCount=1,LaunchTemplate=launchTemplate,UserData=userData)
    instancesIds=list()
    for instance in response["Instances"]:
        instancesIds.append(instance["InstanceId"])
    print("STARTED MASTER INSTANCE")
    return instancesIds


#INSTANCE_TYPE="t3.nano"
INSTANCE_TYPE=""
def startInstances(num):
    #start num ec2 instances
    launchTemplate= LaunchTemplate={
        'LaunchTemplateName': 'EC2-INIT-S3DOWN-SCRIPT',
        #'Version': '1'
        'Version': '3'
    }
    if INSTANCE_TYPE!="":
        response=ec2Client.run_instances(MaxCount=num,MinCount=num,InstanceType=INSTANCE_TYPE,LaunchTemplate=launchTemplate)
    else:
        response=ec2Client.run_instances(MaxCount=num,MinCount=num,LaunchTemplate=launchTemplate)
    instancesIds=list()
    for instance in response["Instances"]:
        instancesIds.append(instance["InstanceId"])
    print("STARTED ",num," INSTANCES")
    return instancesIds

def waitForReadyInstance(instanceId):
    #wait for instanceId ready among started ec2 istances and return his public dns name
    while 96>0:
        resp=    ec2Client.describe_instances(InstanceIds=[instanceId])    
        instances=    resp["Reservations"][0]["Instances"]
        for instance in instances:
            if instance["InstanceId"] == instanceId and instance["State"]["Code"]==EC2_RUNNING_CODE:
                return instance["PublicDnsName"]
        time.sleep(INSTANCE_CHECK_POLLING_TIME)


def waitForReadyInstances(instancesId):
    #wait for instanceId ready among started ec2 istances and return his public dns name
    nonReadyInstances=instancesId
    readyInstancesHostNames=list()
    while len(readyInstancesHostNames)<len(instancesId):
        resp=    ec2Client.describe_instances(InstanceIds=instancesId)    
        instances=    resp["Reservations"][0]["Instances"]
        for instance in instances:
            if instance["InstanceId"] in nonReadyInstances and instance["State"]["Code"]==EC2_RUNNING_CODE:
                readyInstancesHostNames.append(instance["PublicDnsName"])
                nonReadyInstances.pop(nonReadyInstances.index(instance["InstanceId"]))
                #print("NEW READY INSTANCE, ",instance["PublicDnsName"])
        time.sleep(INSTANCE_CHECK_POLLING_TIME)
    return readyInstancesHostNames

def killInstances(hostnames):
    ec2 = boto3.resource('ec2')
    instances=list(ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]).all())
    for i in instances:
        for hn in hostnames:
            if i.public_dns_name==hn :
                i.terminate()

def killRunningInstances():
    ec2 = boto3.resource('ec2')
    killStat=ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]).terminate()
    print("terminated\t",len(killStat[0]['TerminatingInstances']))


MASTER_SCRIPT_PATH="start_master.sh"
if __name__=="__main__":
    if len(sys.argv)<2:
    	print("usage terminate_instances | terminate | master | relay | num_worker_to_start")
    	exit(1)
    instanceNum=0
    if sys.argv[1]=="terminate_instances":
        killInstances(sys.argv[2:])
        exit(0)
    elif sys.argv[1]=="terminate":
        killRunningInstances()
        exit(0)
    elif sys.argv[1]=="master":
        ids=startMaster(MASTER_SCRIPT_PATH)
        _id=ids[0]
        hn=waitForReadyInstance(_id)
        print(hn)
        exit(0)
    elif sys.argv[1]=="relay":
        instance=startInstancePortRelay()
        hostName=waitForReadyInstance(instance)
        print(hostName)
        exit(0)
    else:
        instanceNum=int(sys.argv[1])
    instances=startInstances(instanceNum)
    hostNames=waitForReadyInstances(instances)
    for hostName in hostNames:
        print(hostName)
