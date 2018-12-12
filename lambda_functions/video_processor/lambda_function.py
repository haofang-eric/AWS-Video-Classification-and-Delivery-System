# coding: utf-8
import boto3
import time

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
rekognition = boto3.client('rekognition')

# --------------- Main handler ------------------

def lambda_handler(event, context):
    # get video bucket and key
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # start video detection
    response = rekognition.start_label_detection(
    Video={
        'S3Object': {
            'Bucket': bucket,
            'Name': key,
        }
    },
    
    NotificationChannel={
        'SNSTopicArn': 'arn:aws:sns:us-<>',
        'RoleArn': 'arn:aws:iam:<>'
    },
    
    JobTag='tag1'
    )
    
    # get job_id for a specific video analysis task
    job_id = response['JobId']
    label = rekognition.get_label_detection(
        JobId = job_id,
        MaxResults=10,
        )
    if not job_id:
        return "job_id not detected"
        
    # store the video job_id and key name in dynamodb
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('<>')
    item = {'vid': job_id, 'category': 'Unknown', 'keyname': key}
    response = table.put_item(Item=item)
    print("Writting", response)
    return "LF1 finished,jobid %s" % job_id
    
    
