# regular imports
from boto3.dynamodb.conditions import Key
from pandas.io.json import json_normalize
import boto3
from base64 import b64decode
import logging
import json
import os
import pandas as pd

# custom library imports
import sys

sys.path.append("../")
try:
    from helper_functions import dynamo_helper
    from aggregation_lib import aggregation_lib

    logging.warning("Running Locally so module imports are different")
except Exception as e:
    logging.warning("Running on Lambda")
    import dynamo_helper
    import aggregation_lib

######################################################
##                                                  ##
##      Logging Information                         ##
##                                                  ##
######################################################
root = logging.getLogger()
if root.handlers:
    for handler2 in root.handlers:
        root.removeHandler(handler2)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

######################################################
##                                                  ##
##      Environment Variable Decryption/Decoding    ##
##                                                  ##
######################################################
env_vars = {}
envVarsList = ["MetricDynamoDBTableName"]
for var in envVarsList:
    if var in os.environ.keys():
        env_vars[var] = os.environ[var]
logging.info("Environment variables are : {}".format(env_vars))

######################################################
##                                                  ##
##      Database Connection Initialisation          ##
##                                                  ##
######################################################
dynamodb_resource = boto3.resource("dynamodb")
table = dynamodb_resource.Table(env_vars["MetricDynamoDBTableName"])
dynamo_client = boto3.client("dynamodb")
serializer = boto3.dynamodb.types.TypeSerializer()
deserializer = boto3.dynamodb.types.TypeDeserializer()


def extract_data_from_record(record, deserializer):
    """
    Extracts the SpanID, timestamp converts a DBStream object to a dictionary with the required key and metrics

    Returns
    -------
    data = {'Key1': ...,
            'Key2': ...,
            'Metric1' : ...,
            'Metric2' : ....,
            'Metric3' : ....,

            }
    """
    data = {
        **(
            dynamo_helper.deserializer_from_ddb(
                record["dynamodb"]["Keys"], deserializer
            )
        ),
        **(
            dynamo_helper.deserializer_from_ddb(
                record["dynamodb"]["NewImage"], deserializer
            )
        ),
    }
    return data


def get_metric_data_from_dynamo_batch(unique_span_timestamps):
    """
    Gets the metric data from dynamo for given spanId. Does a query on the spanID.

    Parameters
    ----------
    unique_span_timestamps: list of Dict
        The list of dict containing the spanID and granular timestamp for wheihc we are queryung the data
    """

    logging.info("Getting metrics for unique spanId and timestamps")

    # rename the span_id to spanid_metric_name
    # unique_span_timestamps = [x.update({x['spanId'] : x['spanId'] + "_speed"}) for x in unique_span_timestamps]
    # logging.info('renamed unique_span_timestamps')

    # serialize the data to send for querying
    serialized_unique_span_timestamps = [
        dynamo_helper.serialize_to_ddb(x, serializer)
        for x in unique_span_timestamps
    ]
    logging.info(
        "Need to get metrics for {} Unqiue span-timestamp".format(
            (len(serialized_unique_span_timestamps))
        )
    )

    # query data in batches of 100
    all_low_level_data = []
    for c in chunks(serialized_unique_span_timestamps, 100):

        response = dynamo_client.batch_get_item(
            RequestItems={env_vars["MetricDynamoDBTableName"]: {"Keys": c}}
        )
        logging.debug("Response from BatchgetData : {}".format(response))

        low_level_data = response["Responses"][
            env_vars["MetricDynamoDBTableName"]
        ]
        all_low_level_data.extend(low_level_data)

    logging.info(
        "Len of data received from BatchGetItem Dynamo : {}".format(
            (len(all_low_level_data))
        )
    )

    # Deserialize the data
    python_data = [
        dynamo_helper.deserializer_from_ddb(x, deserializer)
        for x in all_low_level_data
    ]
    logging.info(
        "Len of deserialized data from BatchGetItem Dynamo : {}".format(
            (len(python_data))
        )
    )

    logging.info("Getting metrics for unique spanId and timestamps...Done")

    return python_data


def convert_to_putItem_format(item):
    return {
        "PutRequest": {"Item": dynamo_helper.serialize_to_ddb(item, serializer)}
    }


def convert_to_dynamo_put_item_format(aggregate_values):
    """
    Converts the aggregate values to DynamoDB's client PutItemRequest format for
    sending data in batches.
    """

    logging.info("Converting the data to dynamo Put Item Type")

    # convert all types to string - as float is not convertible to DynamoDB Type
    aggregate_values_as_list = list(
        (aggregate_values.to_dict(orient="index")).values()
    )

    # convert to put item_type
    aggregated_values_as_put_item = list(
        map(convert_to_putItem_format, aggregate_values_as_list)
    )
    logging.debug(
        "PutItem Formatted Data : {}".format((aggregated_values_as_put_item))
    )
    logging.info("Converting the data to dynamo Put Item Type...Done")

    return aggregated_values_as_put_item


# Create a function called "chunks" with two arguments, l and n:
def chunks(l, n=25):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i : i + n]


def format_event_data(extracted_data):
    """
    This function converts to dataframe, floors the time to minute and converts
    other columns to float, and spanId, timestamp column to string back
    """

    logging.info("Converting event data to Dataframe")

    # convert to df
    event_df = pd.DataFrame(json_normalize(extracted_data))
    logging.info(
        "Event Dataframe is: \n{}".format(event_df[["timestamp", "spanId"]])
    )
    event_df["timestamp"] = (pd.to_datetime(event_df["timestamp"])).astype(
        "datetime64[m]"
    )  # floor to minute
    logging.info(
        "Event Dataframe is: \n{}".format(event_df[["timestamp", "spanId"]])
    )

    for col in ["spanId", "timestamp"]:
        event_df
        event_df[col] = event_df[col].astype(str)

    # convert columns to float
    cols_to_float = list(event_df.columns)
    print(cols_to_float)
    cols_to_float.remove("spanId")
    cols_to_float.remove("timestamp")
    print(cols_to_float)
    for col in ["speed"]:
        event_df[col] = event_df[col].astype(float)

    # add a count column
    event_df["count"] = 1.0

    # change the event data frame to include the metric name
    event_df["spanId"] = event_df["spanId"] + "_speed"

    logging.info("Converting event data to Dataframe...Done")

    return event_df


def format_combined_event_data(extracted_data):
    """
    This function converts to dataframe, floors the time to minute and converts
    other columns to float, and spanId, timestamp column to string back
    """

    logging.info("Converting event data to Dataframe")

    # convert to df
    event_df = pd.DataFrame(extracted_data)

    # convert timecolumn to datetime and floor
    event_df["timestamp"] = pd.to_datetime(event_df["timestamp"])
    event_df["timestamp"] = event_df["timestamp"].astype(
        "datetime64[m]"
    )  # floor to minute

    for col in ["spanId", "timestamp"]:
        event_df[col] = event_df[col].astype(str)

    # convert columns to float
    cols_to_float = list(event_df.columns)
    cols_to_float.remove("spanId")
    cols_to_float.remove("timestamp")
    for col in ['speed']:
        event_df[col] = event_df[col].astype(float)

    logging.info("Converting event data to Dataframe...Done")

    return event_df


def find_unique_span_timestamp_pairs(event_df):
    """
    Find the unique spanId and granular timestamps from the DBstream even

    Returns
    -------
    data : list of Dict
        returned value is a list of dict
        [ {'spanId': '...' , 'timestamp' : '...'},
          {'spanId': '...' , 'timestamp' : '...'},
          {'spanId': '...' , 'timestamp' : '...'},
        ]
    """
    logging.info("Finding unique spanId-timestamp")

    unique_span_timestamps = event_df.drop_duplicates(
        subset=["spanId", "timestamp"]
    )[["spanId", "timestamp"]]

    unique_span_timestamps = list(
        unique_span_timestamps.to_dict(orient="index").values()
    )

    logging.info(
        "Len of unique span-timestamp pairs : {}".format(
            len(unique_span_timestamps)
        )
    )

    logging.info("Finding unique spanId-timestamp...Done")

    return unique_span_timestamps


def combine_event_dynamo_df(event_df, metrics_from_dynamo_df):
    """
    Stacks the event dataframe and metrics from DynamoDB. Additionally converts
    spanId and timestamp column to string. And other columns to float
    """
    logging.info("Combining 2 event and metric dataframe")

    all_metrics = pd.concat(
        [event_df, metrics_from_dynamo_df],
        ignore_index=True,
        sort=False,
        axis=0,
    )

    all_metrics = format_combined_event_data(all_metrics)

    logging.info("Combining 2 event and metric dataframe...Done")
    return all_metrics


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


def handler(event, context):
    logging.info(
        "Number of records in event : {}".format(len(event["Records"]))
    )

    #############################
    # Extract data from Kinesis #
    #############################
    extracted_data = extract_data_from_kinesis(event)
    logging.info("Len of Extracted records : {}".format((len(extracted_data))))

    # Convert to Dataframe and set correct types
    event_df = format_event_data(extracted_data)
    logging.debug("Formatted DF : \n{}".format(event_df))

    # find unique span-timestamp combos
    # returned value is a list of dict
    # [ {'spanId': '...' , 'timestamp' : '...'},
    #   {'spanId': '...' , 'timestamp' : '...'},
    #   {'spanId': '...' , 'timestamp' : '...'},
    # ]
    unique_span_timestamps = find_unique_span_timestamp_pairs(event_df)
    logging.info(
        "Len of Unqiue span-timestamp pairs : {}".format(
            (len(unique_span_timestamps))
        )
    )
    logging.debug(
        "Unqiue span-timestamp pairs : \n{}".format((unique_span_timestamps))
    )

    # get the metrics for these unique span-timestamp combos
    metrics_from_dynamo = get_metric_data_from_dynamo_batch(
        unique_span_timestamps
    )
    metrics_from_dynamo_df = pd.DataFrame(metrics_from_dynamo)
    logging.info(
        "Len of metrics received from dynamo : {}".format(
            len(metrics_from_dynamo)
        )
    )

    # combine the dataframe we got from DynamoDB and the dataframe from event
    combined_df = combine_event_dynamo_df(event_df, metrics_from_dynamo_df)
    logging.debug("Combined dataframe : \n{}".format((combined_df)))

    # Update the metrics
    aggregate_values = aggregation_lib.update_average_metrics(
        combined_df,
        group_by_columns=["spanId", "timestamp"],
        mean_columns=["speed"],
        count_column=["count"],
    )
    logging.debug("Updated metric values are : \n{}".format(aggregate_values))

    # convert to dynamo Put Item type - as we want to do batch write to dynamo
    dynamo_putItems = convert_to_dynamo_put_item_format(aggregate_values)

    # send to dynamo in batches-batch size is 25 (Max value by Dynamo)
    for batch in chunks(dynamo_putItems, 25):
        response = dynamo_client.batch_write_item(
            RequestItems={env_vars["MetricDynamoDBTableName"]: batch}
        )
        logging.info(
            "Response from updating metric in batch : {}".format(
                response["ResponseMetadata"]["HTTPStatusCode"]
            )
        )
    return "Done updating speed"
