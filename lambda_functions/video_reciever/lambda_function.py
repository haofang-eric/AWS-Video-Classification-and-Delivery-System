import boto3
import json 
import time

def lambda_handler(event, context):
    
    modify = event['Records'][0]['body']    
    d = json.loads(modify)
    message = d['Message']
    d2 = json.loads(message)
    job_id = d2['JobId']
    
    rekognition = boto3.client('rekognition')
    result = rekognition.get_label_detection(
        JobId = job_id,
        MaxResults=10,
        )
        
    Labels = result['Labels']    
    label_set = set()    
    for ele in Labels:
        label_set.add(ele['Label']['Name'])
        if len(label_set) == 3:
            break
    label_result = " + ".join(list(label_set))
    
    process_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('<>')
    update_response = table.update_item(
    Key={
        'vid':job_id,
        'category' : 'Unknown'
    },
    UpdateExpression="set label = :r , record_time = :s",
    ExpressionAttributeValues={
        ':r': label_result,
        ':s': process_time
    },
    ReturnValues="UPDATED_NEW"
    )
    for i, tag in enumerate(list(label_set)):
        update_response = table.update_item(
        Key={
            'vid':job_id,
            'category' : 'Unknown'
        },
        UpdateExpression="set label_{cnt} = :r".format(cnt=str(i)),
        ExpressionAttributeValues={
            ':r': tag
        },
        ReturnValues="UPDATED_NEW"
        )
    
    print(update_response)
