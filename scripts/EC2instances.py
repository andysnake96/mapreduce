#!/usr/bin/env python3
import boto3
import time
import sys

#scripts to -connect to ec2 basing on user level aws config in ~
#           - start ec2 instances
#           - block until all instances are RUNNING (for ec2) and then print to stdout
#           - integrable with terminal spawn script

ec2Client=boto3.client("ec2")
INSTANCE_CHECK_POLLING_TIME=10
EC2_RUNNING_CODE=16
def startInstancePortRelay():
    #start port forward relay
    launchTemplate= LaunchTemplate={
        'LaunchTemplateName': 'port_forward_relay_server',
        'Version': '6'
    }
    response=ec2Client.run_instances(MaxCount=1,MinCount=1,LaunchTemplate=launchTemplate)
    instancesIds=list()
    for instance in response["Instances"]:
        instancesIds.append(instance["InstanceId"])
    return instancesIds

def startInstances(num):
    #start num ec2 instances
    launchTemplate= LaunchTemplate={
        'LaunchTemplateName': 'EC2-INIT-S3DOWN-SCRIPT',
        'Version': '1'
    }
    response=ec2Client.run_instances(MaxCount=num,MinCount=num,LaunchTemplate=launchTemplate)
    instancesIds=list()
    for instance in response["Instances"]:
        instancesIds.append(instance["InstanceId"])
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
    ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]).terminate()
    print("terminated")



if __name__=="__main__":
    instanceNum=1
    if len(sys.argv)>1:
        if sys.argv[1]=="terminate_instances":
            killInstances(sys.argv[2:])
            exit()
        if sys.argv[1]=="terminate":
            killRunningInstances()
            exit()
        if sys.argv[1]=="relay":
            instances=startInstancePortRelay()
            hostNames=waitForReadyInstances(instances)
            print(hostNames[0])
            exit()
        instanceNum=int(sys.argv[1])
    instances=startInstances(instanceNum)
    hostNames=waitForReadyInstances(instances)
    for hostName in hostNames:
        print(hostName)
