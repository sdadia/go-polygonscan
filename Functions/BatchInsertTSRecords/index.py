import json
import time
from base64 import b64encode, b64decode
from multiprocessing import Process, Pipe
import dynamo_helper
import logging
import boto3
import itertools
import datetime

logging.basicConfig(
    level=logging.INFO)

dynamo_put_client = boto3.client('dynamodb')
dynamodb_resource = boto3.resource("dynamodb")
serializer = boto3.dynamodb.types.TypeSerializer()

def convert_to_putItem_format(item):
    return {
        "PutRequest": {"Item": dynamo_helper.serialize_to_ddb(item, serializer)}
    }

def convert_to_dynamo_put_item_format(data):
    """
    Converts the aggregate values to DynamoDB's client PutItemRequest format for
    sending data in batches.
    """

    logging.info("Converting the data to dynamo Put Item Type")

    # convert to put item_type
    TS_values_as_put_item = list(
        map(convert_to_putItem_format, data)
    )
    logging.debug(
        "PutItem Formatted Data : {}".format((TS_values_as_put_item))
    )
    logging.info("Converting the data to dynamo Put Item Type...Done")

    return TS_values_as_put_item


def batch_write_tagged_data_to_DynamoDB(tagged_data, 
                TSDynamo_tablename="pvcam-test-TripCalculation-TimeseriesTable-Daily-2019-09-06"):
                    
    TS_values_as_put_item = list(
        map(convert_to_putItem_format, tagged_data)
    )
    print(TS_values_as_put_item)
    
    dynamo_putItems = convert_to_dynamo_put_item_format(aggregate_values)

    for record  in tagged_data:
        s=time.time()
        response = dynamo_put_client.batch_write_item(RequestItems={TSDynamo_tablename: record})
        e = time.time()
        print("Time taken for batch write : {}".format(e-s))
        
    

def extract_data_from_kinesis(event):
    logging.info("Extracting data from Kinesis")

    all_records = []
    for r in event["Records"]:
        data = b64decode(r["kinesis"]["data"]).decode("utf-8")
        data = json.loads(data)
        data = {**data, **data["gps"]}
        del data["gps"]
        print(data)
        all_records.append(data)

    logging.info("All records : {}".format((all_records)))

    logging.info("Extracting data from Kinesis...Done")

    return all_records

def chunks(l, n=25):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i : i + n]




def lambda_handler(event, context):
    data = extract_data_from_kinesis(event)
    dynamo_putItems = convert_to_dynamo_put_item_format(data)
    print(dynamo_putItems)
    
    for batch in chunks(dynamo_putItems, 25):
        response = dynamo_put_client.batch_write_item(
            RequestItems={"pvcam-test-TripCalculation-TimeseriesTable-Daily-2019-09-06": batch}
        )
        logging.info(
            "Response from updating metric in batch : {}".format(
                response["ResponseMetadata"]["HTTPStatusCode"]
            )
        )

    
    
    
    return 'Done adding records to daynamo using ODM'
