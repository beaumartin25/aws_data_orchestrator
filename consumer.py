import time
import boto3
import json
import argparse
import logging

S3 = boto3.client('s3')
DDB = boto3.client('dynamodb', region_name='us-east-1')
SQS = boto3.client('sqs', region_name='us-east-1')


def getArgs(): #-rb [reqeust bucket] -wb [web bucket] -dwt [dynamo db database]
    parser = argparse.ArgumentParser(description='Transfers data from request bucket to web bucket or dynamo db database')
    group = parser.add_mutually_exclusive_group(required=True)

    parser.add_argument('-rb', type=str, required=True, help='enter request bucket address') 
    group.add_argument('-wb', type=str, help='enter web bucket address')
    group.add_argument('-dwt', type=str, help='enterdynamo db address')

    args = parser.parse_args()
    return args
    

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
        
    

        # response = DDB.update_item(
        #     TableName=database,
        #     Key=key,
        #     UpdateExpression=update_expression,
        #     ExpressionAttributeValues=expression_attribute_values,
        #     ExpressionAttributeNames=expression_attribute_names
        # )
        item = otherAttributesUpdate(json_object)
        #print(item)
        # Update the item in the DynamoDB table
        response = DDB.update_item(
            TableName=database,
            Key={'id': {'S': widgetId}},
            AttributeUpdates=item
            # You may need to specify a ConditionExpression here to ensure the item exists before updating
        )

    
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



def processData(line, args):
    requests = args.rb 

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                        handlers=[
                            logging.FileHandler("logs/consumer.log"),
                            logging.StreamHandler()
                        ]
                        )

    key = line['Key']
    

    # Retrieve the JSON content from S3
    response = S3.get_object(Bucket=requests, Key=key)
    json_data = response['Body'].read().decode('utf-8')
    
    try:
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

        # Delete the object from the S3 bucket after processing
        S3.delete_object(Bucket=requests, Key=key)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON object {key}: {str(e)}")




if __name__ == '__main__':
    args = getArgs()
    requests = args.rb 
      
    wait = True
    waited = False
            
    while wait:
        
        response = S3.list_objects_v2(Bucket=requests) #how would I get just one request but still get it by lowest key first
        if len(response.get('Contents', [])) != 0:
            objects = sorted(response.get('Contents', []), key=lambda x: x['Key'])
            for obj in objects:
                processData(obj, args)
            waited = False
        else:
            if waited == False:
                time.sleep(.1)
                waited = True
            else:
                wait = False
        
