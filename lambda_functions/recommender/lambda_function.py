import boto3
import json 
import time, datetime
from boto3.dynamodb.conditions import Key, Attr
from words import GREETINGS, TYPE

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('<>')
    
    now=datetime.datetime.now()
    delta=datetime.timedelta(days=3)
    time_bound = now-delta
    time_bound = time_bound.strftime("%Y-%m-%d %H:%M:%S")
    print("TIME BOUNDARY", time_bound)
    
    fe = (Attr('label_0').is_in(TYPE) |  Attr('label_1').is_in(TYPE) | Attr('label_2').is_in(TYPE))& Attr("record_time").gte(time_bound)
    pe = "keyname, record_time"
    response = table.scan(
        FilterExpression=fe,
        ProjectionExpression=pe
        )

    BUCKET_NAME = "ngnvideotagger"
    s3 = boto3.client('s3')
    
    sns_message_list = [GREETINGS.format(type=",".join(TYPE), time_b=time_bound)] 
    for item in response.get("Items"):
        url = "https://s3.console.aws.amazon.com/s3/object/<>/{key_name}?region=us-east-1&tab=overview".format(key_name=item.get("keyname"))
        response = s3.head_object(Bucket=BUCKET_NAME, Key=item.get("keyname"))
        if not response.get('Metadata'):
            continue
        sns_message_list.append(' '.join([url, response.get('Metadata').get('itime'), response.get('Metadata').get('iname')]))
    # Create an SNS client
    sns = boto3.client('sns')
    # Publish a simple message to the specified SNS topic
    response = sns.publish(
        TopicArn='arn:aws:sns:us-east-1:<>',    
        Message='\n'.join(sns_message_list),    
    )
    return "LF3 done"
