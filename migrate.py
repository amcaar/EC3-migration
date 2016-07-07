#! /usr/bin/env python

'''
Created on 08/06/2016
Migration script for EC3 clusters
2014 - GRyCAP - Universitat Politecnica de Valencia
@author: Amanda Calatrava
'''

import time
import threading
import subprocess
import logging
import os.path
import argparse
import json
import sys

#import boto3


# Iniciates the migration logging
def init():
    logging.basicConfig(filename="/home/ubuntu/migrate.log", 
                        level=logging.DEBUG, 
                        format='%(asctime)s: %(levelname)-8s %(message)s',
                        datefmt='%m-%d-%Y %H:%M:%S',
                        filemode='w')
                        
    logging.info('************ Starting migration daemon **************')

def parse_scontrol(out):
    if out.find("=") < 0: return []
    r = []
    for line in out.split("\n"):
        line = line.strip()
        if not line: continue
        d = {}; r.append(d); s = False
        for k in [ j for i in line.split("=") for j in i.rsplit(" ", 1) ]:
            if s: d[f] = k
            else: f = k
            s = not s
    return r

# Obtains the list of jobs that are in execution in SLURM
def get_job_info():
    exit = parse_scontrol(run_command("sshpass -p 'yoY0&yo' ssh -o StrictHostKeyChecking=no ubuntu@" + args.ip[0] + "scontrol -o show jobs -a"))
    job_list = {}
    if exit:
        for key in exit:
            if key["JobState"] == "RUNNING": #or key["JobState"] == "NODE_FAIL":
                job_list [str(key["JobId"])] = str(key["BatchHost"])
    return job_list
    
# Checks if there are some checkpoint files for the job
def check_ckpt_file(job_id):
    if os.path.exists("/home/ubuntu/" + str(job_id)): 
    #if os.path.exists("/var/slurm/checkpoint/" + str(job_id)): 
        return True 
    else: 
        return False
        
# Obtains the command used to launch the job, in order to relaunch it again
def obtain_sbatch_command(job_id):
    jobs = parse_scontrol(run_command("sshpass -p 'yoY0&yo' ssh -o StrictHostKeyChecking=no ubuntu@" + args.ip[0] + "scontrol -o show jobs -a"))
    command = ""
    if jobs:
        for key in jobs:
            if key["JobId"] == str(job_id):
                command = str(key["Command"])
    logging.info("Command for job " + job_id + " is: " + command)
    return command

    
# Launch migration daemon
def launch_daemon():

    # Initialize the parser
    parser = argparse.ArgumentParser(description='Tool for migrating the a virtual cluster deployed with EC3 together with its workload.')
    parser.add_argument('ip', metavar='ip-frontend', nargs=1, help='Indicate the IP of the frontend that needs to be migrated.')
    parser.add_argument('-n', '--cluster-name', dest='cluster_name', nargs=1, type=int, default=None, help='The cluster name of the EC3 cluster to clone.')
    parser.add_argument('-d', '--destination', nargs=1, default=None, dest='destination', help='The cloud infrastructure where the cluster will be cloned.')
    args = parser.parse_args()
    
    if args.cluster_name is None:
        parser.error("The cluster name is mandatory")

    if args.destination is None:
        parser.error("The cloud provider destination is mandatory")


    # First, we clone the infrastructure
    try:
        run_command("./ec3 clone " + args.cluster_name + " -d " + args.destination + " -a auth_clone.dat -u http://servproject.i3m.upv.es:8899")
        logging.info("Cluster " + args.cluster_name + " cloned succesfully")
    except:
        logging.error("Error cloning the cluster")
        sys.exit(-1)
        
    # Obtain the job list running in the cluster
    job_list = get_job_info()
    
    # Checkpoint all the jobs running in the cluster
    for key, value in job_list.iteritems():
        try:
            logging.info("Performing a checkpoint to the job "+ key)
            run_command("sshpass -p 'yoY0&yo' ssh -o StrictHostKeyChecking=no ubuntu@" + args.ip[0] + "scontrol checkpoint create " + key)
            logging.info("Checkpointing performed successfully.")
        except: 
            logging.error("Error checkpointing the job " + key)
    
    # Copy the checkpoint files to a bucket in Amazon S3
    for key, value in job_list.iteritems():
        try:
            logging.info("Uploading checkpoint files to S3")
            run_command("sshpass -p 'yoY0&yo' ssh -o StrictHostKeyChecking=no ubuntu@" + args.ip[0] + "aws s3 cp " + key + "/ s3://amcaar-cluster-migration/" + key + "/ --recursive")
            logging.info("Upload performed succesfully")
        except:
            logging.error("Error while uploading checkpoint files to S3")

    # Obtain the IP of the front-end of the cloned cluster
    exit = " "
    try:
        exit = run_command("./ec3 list --json")
        json_data = json.loads(exit)
    except:
        logging.error("could not obtain information about EC3 clusters (%s)" % (exit))
        return None
        
    clon_ip = ""
    if json_data:
        for cluster, details in json_data.items():
            for element in details:
                clustername = str(element['name'])
                if 'cloned' in clustername:
                    clon_ip = str(element['IP'])
                    break
    
    if clon_ip != "":
        logging.info("Success obtaining IP of the cloned cluster")
    else:
        logging.error ("Error obtaining the IP of the cloned cluster")
        sys.exit(-1)
    
        
    # Download checkpoint files from the bucket in Amazon S3 to the cloned cluster
    for key, value in job_list.iteritems():
        try:
            logging.info("Downloading checkpoint files to S3")
            run_command("sshpass -p 'yoY0&yo' ssh -o StrictHostKeyChecking=no ubuntu@" + clon_ip + "aws s3 cp s3://amcaar-cluster-migration/" + key + "/ /home/ubuntu/" + key + "/ --recursive")
            logging.info("Download performed succesfully")
        except:
            logging.error("Error while downloading checkpoint files to S3")
        
    # Restart the jobs in the cloned cluster
    for key, value in job_list.iteritems():
        try:
            logging.info("Restarting the job "+ key)
            run_command("sshpass -p 'yoY0&yo' ssh -o StrictHostKeyChecking=no ubuntu@" + clon_ip + "scontrol checkpoint restart " + key)
            logging.info("Restart performed successfully.")
        except: 
            logging.error("Error restarting the job " + key)
    
    
#############################################################
#    	Class and methods to execute bash commands          #
#############################################################
    
class CommandError(Exception):pass

class DownNodeError(Exception):pass

def run_command(command, shell=False):
    string = " "
    try:
        logging.debug("executing: %s" % command)
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    except:
        if type(command)==list: command = string.join(command)
        logging.error('Could not execute command "%s"' %command)
        raise
    
    (output, err) = p.communicate()
    if p.returncode != 0:
        if type(command)==list: command = string.join(command)
        logging.error(' Error in command "%s"' % command)
        logging.error(' Return code was: %s' % p.returncode)
        logging.error(' Error output was:\n%s' % err)
        if err == "scontrol_checkpoint error: Required node not available (down, drained or reserved)\n":
            raise DownNodeError()
        else:
            raise CommandError()
    else:
        return output
            
# main method
if __name__ == '__main__':
    init()
    launch_daemon()
