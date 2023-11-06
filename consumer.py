import time
import boto3
import json
import argparse
import logging
import os

S3 = boto3.client('s3')
DDB = boto3.client('dynamodb', region_name='us-east-1')
SQS = boto3.client('sqs', region_name='us-east-1')


def getArgs(): #-rb [reqeust bucket] -wb [web bucket] -dwt [dynamo db database]
    parser = argparse.ArgumentParser(description='Transfers data from request bucket to web bucket or dynamo db database')
    group = parser.add_mutually_exclusive_group(required=True)

    parser.add_argument('-rb', type=str, required=True, help='enter request bucket address') 
    parser.add_argument('-rq', type=str, help='renter request queue address')
    group.add_argument('-wb', type=str, help='enter web bucket address')
    group.add_argument('-dwt', type=str, help='enter dynamo db address')
    

    args = parser.parse_args()
    return args
    

def create_log_directory(log_file_path):
    log_directory = os.path.dirname(log_file_path)
    os.makedirs(log_directory, exist_ok=True)

def convertToFileFormat(owner, widgetId):
    owner = owner.lower().replace(" ", "-")
    return f"widgets/{owner}/{widgetId}"


def otherAttributes(json_object):
    widget_id = json_object['widgetId']
    owner = json_object.get('owner')
    label = json_object.get('label')
    description = json_object.get('description')

    item = {
        'id': {'S': widget_id}
    }
    if owner:
        item['owner'] = {'S': owner}
    if label:
        item['label'] = {'S': label}
    if description:
        item['description'] = {'S': description}

    other_attributes = json_object.get('otherAttributes', [])
    if other_attributes:
        for attribute in other_attributes:
            attribute_name = attribute.get('name')
            attribute_value = attribute.get('value')
            item[attribute_name] = {'S': attribute_value}

    return item


def otherAttributesUpdate(json_object):
    owner = json_object.get('owner')
    label = json_object.get('label')
    description = json_object.get('description')

    item = {}
    if owner is not None:
        if len(owner) == 0: 
            item['owner'] = {'Action': 'DELETE'}
        else:
            item['owner'] = {'Action': 'PUT', 'Value': {'S': owner}}
    if label is not None:
        if len(label) == 0:
            item['label'] = {'Action': 'DELETE'}
        else:
            item['label'] = {'Action': 'PUT', 'Value': {'S': label}}
    if description is not None:
        if len(description) == 0:
            item['description'] = {'Action': 'DELETE'}
        else:
            item['description'] = {'Action': 'PUT', 'Value': {'S': description}}

    other_attributes = json_object.get('otherAttributes', [])
    if other_attributes is not None:
        for attribute in other_attributes:
            attribute_name = attribute.get('name')
            attribute_value = attribute.get('value')
            if len(attribute_value) == 0:
                 item[attribute_name] = {'Action': 'DELETE'}
            else:
                item[attribute_name] = {'Action': 'PUT', 'Value': {'S': attribute_value}}

    return item


def check_item_exists(database, widgetId):
    try:
        response = DDB.get_item(
            TableName=database,
            Key={'id': {'S': widgetId}},
            ProjectionExpression='id'  # Check only for the existence of the 'id' attribute
        )
        return 'Item' in response
    except DDB.exceptions.ResourceNotFoundException:
        return False  # Table does not exist, so the item doesn't exist


def create(json_object, args):
    web = args.wb 
    database = args.dwt      

    del json_object["type"]
    del json_object["requestId"]
    widgetId = json_object["widgetId"]
    owner = json_object["owner"]

    fileString = convertToFileFormat(owner, widgetId)
    

    logging.info(f"Put or update in DynamoDB table {database} a widget with key: {widgetId}")

    if web != None:
        S3.put_object(
            Body=json.dumps(json_object),
            Bucket=web,
            Key=fileString
        )
    else:
        item = otherAttributes(json_object)
        #table = DDB.Table('widgets') #not sure
        # print to database
    
        response = DDB.put_item(
            TableName=database,
            Item=item
            #ConditionExpression='attribute_not_exists(product_name)'
        )
            

def update(json_object, args):
    web = args.wb
    database = args.dwt

    widgetId = json_object["widgetId"]
    owner = json_object["owner"]

    fileString = convertToFileFormat(owner, widgetId)

    logging.info(f"Update DynamoDB table {database} for widget with key: {widgetId}")

    if web != None:
        # Update the JSON content in the S3 bucket
        S3.put_object(
            Body=json.dumps(json_object),
            Bucket=web,
            Key=fileString
        )
    else:
        if check_item_exists(database, widgetId):
            item = otherAttributesUpdate(json_object)
            
            #print(item)
            # Update the item in the DynamoDB table
            response = DDB.update_item(
                TableName=database,
                Key={'id': {'S': widgetId}},
                AttributeUpdates=item
                # You may need to specify a ConditionExpression here to ensure the item exists before updating
            )
        else:
            logging.warning(f"Widget with ID {widgetId} does not exist. Update skipped.")

    
def delete(json_object, args):
    web = args.wb
    database = args.dwt

    widgetId = json_object["widgetId"]
    owner = json_object["owner"]

    fileString = convertToFileFormat(owner, widgetId)

    logging.info(f"Delete widget with key: {widgetId}")

    if web != None:
        # Delete the object from the S3 bucket
        S3.delete_object(
            Bucket=web,
            Key=fileString
        )
    else:
        widget_id = json_object['widgetId'] #remove

        # Delete the item from the DynamoDB table
        response = DDB.delete_item(
            TableName=database,
            Key={'id': {'S': widget_id}}
            # You may need to specify a ConditionExpression here to ensure the item exists before deleting
        )


def processData(args, json_data):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                        handlers=[
                            logging.FileHandler("logs/consumer.log"),
                            logging.StreamHandler()
                        ]
                        )
    
    json_object = json.loads(json_data)

    requestId = json_object["requestId"]
    widgetId = json_object["widgetId"]
    option = json_object["type"]

    logging.info(f"Process a {option} request for widget {widgetId} in request {requestId}")
    
    if option == "create":
        create(json_object, args)
    if option == "update":
        update(json_object, args)
    if option == "delete":
        delete(json_object, args)


if __name__ == '__main__':
    args = getArgs()
    requests = args.rb
    queue = args.rq
      
    wait = True
    waited = False
    create_log_directory("logs/")
            
    while wait:
        if queue:
            response = SQS.receive_message(
                QueueUrl=queue,
                MaxNumberOfMessages=10
            )
            if 'Messages' in response:
                for message in response['Messages']:
                    receipt_handle = message['ReceiptHandle']
                    processData(args, message['Body'])
                    SQS.delete_message(
                        QueueUrl=queue,
                        ReceiptHandle=receipt_handle
                    )
            else:
                if waited == False:
                    time.sleep(.1)
                    waited = True
                else:
                    wait = False 
        else:
            response = S3.list_objects_v2(Bucket=requests) 
            if len(response.get('Contents', [])) != 0:
                objects = sorted(response.get('Contents', []), key=lambda x: x['Key'])
                for obj in objects:
                    key = obj['Key']
                    response = S3.get_object(Bucket=requests, Key=key)
                    json_data = response['Body'].read().decode('utf-8')
                    processData(args, json_data)
                    S3.delete_object(Bucket=requests, Key=key)
                waited = False
            else:
                if waited == False:
                    time.sleep(.1)
                    waited = True
                else:
                    wait = False
        
