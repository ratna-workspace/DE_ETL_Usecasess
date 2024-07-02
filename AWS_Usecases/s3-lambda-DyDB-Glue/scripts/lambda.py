import boto3
from boto3.dynamodb.conditions import Key

def query_config_table(filename):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('uc_config_table')
    
    response = table.query(
        KeyConditionExpression=Key('filename').eq(filename)
    )
    
    data = response.get('Items')
    
    item = data[0]
    return item
    
    
def run_glue_job(job_name,Args):
    glue = boto3.client("glue")
    glue.start_job_run(JobName=job_name, Arguments=Args)
    print("successfully started  Glue job run....!!! ",job_name)
    
    

def lambda_handler(event, context):
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    print(bucket , key)
    
    filename = key.split("/")[-1].split(".")[0]
    
    item = query_config_table(filename)
    
    glue_job = item["glue_job"]
    target_file_type = item["target_file_type"]
    target_path = item["target_path"]
    
    Args = {"--file": key, "--bkt": bucket,"--target_file_type": target_file_type, "--target_path": target_path}
    run_glue_job(glue_job,Args)