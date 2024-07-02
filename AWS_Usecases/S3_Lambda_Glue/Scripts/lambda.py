import json
import os
import boto3
import sys

def run_glue_job(Args):
    """
    starting job run using start_job_run()
    """
    glue = boto3.client("glue")
    Job_Name ="uc_job"
    glue.start_job_run(JobName=Job_Name, Arguments=Args)
    print("successfully started  Glue job run....!!! ",Job_Name)

def lambda_handler(event, context):
    print(event)

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    print(bucket , key)
    
    #Args pass bucket name,filename to glue and it will get through getResolvedOptions() with sys
    Args = {"--file": key, "--bkt": bucket}
    run_glue_job(Args=Args)



