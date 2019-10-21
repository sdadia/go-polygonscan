# regular imports
from base64 import b64decode
import ciso8601
from pprint import pformat
from pvapps_odm.Schema.models import AggregationModel
from pvapps_odm.ddbcon import Connection
from pvapps_odm.ddbcon import dynamo_dbcon
import boto3
import datetime
import json
import logging
import os
import pandas as pd
import aggregation_lib


# custom library imports
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
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger("speed_log")

######################################################
##                                                  ##
##      Environment Variable Decryption/Decoding    ##
##                                                  ##
######################################################
env_vars = {}
envVarsList = ["AggDynamoDBTableName"]
for var in envVarsList:
    if var in os.environ.keys():
        env_vars[var] = os.environ[var]
env_vars["AggDynamoDBTableName"] = str(AggregationModel.Meta.table_name)
logger.info("Environment variables are : {}".format(env_vars))
print("Environment variables are : {}".format(env_vars))

######################################################
##                                                  ##
##      Database Connection Initialisation          ##
##                                                  ##
######################################################
ddb = dynamo_dbcon(AggregationModel, conn=Connection())
ddb.connect()


def get_metric_data_from_dynamo_batch_from_ODM_table_read(
    unique_span_timestamps
):
    """
    Gets the metric data from dynamo for given spanId. Does a query on the spanID.

    Parameters
    ----------
    unique_span_timestamps: list of Dict
        The list of dict containing the spanID and granular timestamp for wheihc we are queryung the data
    """

    logger.info(
        "Getting metrics for unique spanId and timestamps using ODM read"
    )
    # convert to specific format for querying the dynamoDB table
    all_data_for_query = []
    for x in unique_span_timestamps:
        print(x)

        modified_ts = ciso8601.parse_datetime(x[1]).replace(
            tzinfo=datetime.timezone.utc, microsecond=0
        )
        all_data_for_query.append((x[0] + "_speed", modified_ts))

    logger.info(
        "Converted timestamp to UTC format : \n{}".format(
            pformat(all_data_for_query)
        )
    )

    unique_span_timestamps_for_get = all_data_for_query

    # extract the items in the list

    logger.info(
        "Data converted as model for querying: \n{}".format(
            pformat(unique_span_timestamps_for_get)
        )
    )

    response = ddb.batch_get(unique_span_timestamps_for_get)
    logger.info("Response from dynamo : \n{}".format(pformat(response)))

    aggregate_values_from_dynamo = [x.attribute_values for x in response]
    logger.info(
        "Response as dictionary from Dynamo : \n{}".format(
            pformat(aggregate_values_from_dynamo)
        )
    )

    for x in aggregate_values_from_dynamo:
        x["speed"] = x.pop("value")

    logger.info(
        "Response renamed from Dynamo : \n{}".format(
            pformat(aggregate_values_from_dynamo)
        )
    )

    return aggregate_values_from_dynamo


# DATETIME_FORMAT = "%Y-%m%d %H:%M:%S"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
DATETIME_FORMAT2 = "%Y-%m-%d %H:%M:%S.%f"
DATETIME_FORMAT3 = "%Y-%m-%d %H:%M:%S"


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

    logger.info("Converting event data to Dataframe")

    # convert to df
    for x in extracted_data:
        # x["timestamp"] = datetime.datetime.strftime(
        # datetime.datetime.strptime(x["timestamp"], DATETIME_FORMAT)
        # .replace(tzinfo=datetime.timezone.utc, microsecond=0)
        # .replace(second=0),
        # DATETIME_FORMAT2,
        # )
        x["timestamp"] = str(
            ciso8601.parse_datetime(x["timestamp"]).replace(
                second=0, microsecond=0
            )
        )

    logging.debug(
        "Data after converting to timestamp : \n{}".format(
            pformat(extracted_data)
        )
    )
    event_df = pd.DataFrame(extracted_data)

    logging.debug("Event Dataframe is: \n{}".format(event_df))
    event_df["timestamp"] = pd.to_datetime(event_df["timestamp"])
    # floor to minute
    logging.debug(
        "Timestamp,SpanId,speed in event dataframe is: \n{}".format(
            event_df[["timestamp", "spanId", "speed"]]
        )
    )

    for col in ["spanId", "timestamp"]:
        event_df
        event_df[col] = event_df[col].astype(str)

    # convert columns to float
    event_df["speed"] = event_df["speed"].astype(float)

    # add a count column
    event_df["count"] = 1.0

    # change the event data frame to include the metric name
    event_df["spanId"] = event_df["spanId"] + "_speed"

    logger.info(
        "Formatted event data frame is : \n{}".format(
            event_df[["timestamp", "spanId", "speed", "count"]]
        )
    )
    event_df.rename(columns={"spanId": "spanId_metricname"}, inplace=True)

    logger.info("Converting event data to Dataframe...Done")

    return event_df[["timestamp", "spanId_metricname", "speed", "count"]]


def format_combined_event_data(extracted_data):
    """
    This function converts to dataframe, floors the time to minute and converts
    other columns to float, and spanId, timestamp column to string back
    """

    logger.info("Converting event data to Dataframe")

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
    for col in ["speed"]:
        event_df[col] = event_df[col].astype(float)

    logger.info("Converting event data to Dataframe...Done")

    return event_df


def combine_event_dynamo_df(event_df, metrics_from_dynamo_df):
    """
    Stacks the event dataframe and metrics from DynamoDB. Additionally converts
    spanId and timestamp column to string. And other columns to float
    """
    logger.info("Combining 2 event and metric dataframe")

    all_metrics = pd.concat(
        [event_df, metrics_from_dynamo_df],
        ignore_index=True,
        sort=False,
        axis=0,
    )
    all_metrics["timestamp"] = all_metrics["timestamp"].astype(str)

    # all_metrics = format_combined_event_data(all_metrics)

    logger.info("Combining 2 event and metric dataframe...Done")
    return all_metrics


def extract_data_from_kinesis(event):
    logger.info("Extracting data from Kinesis")

    all_records = []
    for r in event["Records"]:
        data = b64decode(r["kinesis"]["data"]).decode("utf-8")
        data = json.loads(data)
        data = {**data, **data["gps"]}
        del data["gps"]
        # print(data)
        all_records.append(data)

    logging.debug("All records : {}".format((all_records)))

    logger.info("Extracting data from Kinesis...Done")

    return all_records


def find_unique_span_timestamp(extracted_data):
    """
    Extracted unique span and timestamps

    Returns
    -------
    List of tuples [('1234', '2019-05-22 10:45:07.154000'),
                    ('1234', 2019-05-22 10:47:07.154000)]
    """
    # floor the timestamp to the minute
    all_data = []
    for x in extracted_data:
        all_data.append((x["spanId"], x["timestamp"]))

    logging.debug(
        "Extracted spanId and timestamps are : \n{}".format(pformat(all_data))
    )

    # find the unique span-ts pairs
    unique_span_timestamps = list(set(all_data))
    logger.info(
        "Extracted unqiue spanId and timestamps are : \n{}".format(
            pformat(unique_span_timestamps)
        )
    )
    return unique_span_timestamps


def update_data_in_dynamo_using_ODM(aggregate_values_as_list):
    """
    Converts the aggregate values to DynamoDB's client PutItemRequest format for
    sending data in batches.
    """

    logger.info("Updating aggregate tables using ODM")

    # convert all types to string - as float is not convertible to DynamoDB Type
    # print(aggregate_values_as_list)
    data_as_ODM_model = []
    for x in aggregate_values_as_list:
        # print(x)
        d2 = {
            "spanId_metricname": x["spanId_metricname"],
            "timestamp": ciso8601.parse_datetime(x["timestamp"]).replace(
                second=0, microsecond=0
            ),  # convert to datetime for storing in dynamo
            # "timestamp": datetime.datetime.strptime(
            # x["timestamp"], DATETIME_FORMAT3
            # ).replace(
            # second=0, microsecond=0
            # ),  # convert to datetime for storing in dynamo
            "value": x["speed"],
            "count": x["count"],
        }

        data_as_ODM_model.append(AggregationModel(**d2))

    logging.debug("As aggregate model : {}".format(data_as_ODM_model))

    ddb.session.add_items(data_as_ODM_model)
    ddb.session.commit_items()

    logger.info("Updating aggregate tables using ODM...Done")

    return data_as_ODM_model


def handler(event, context):
    logger.info("Number of records in event : {}".format(len(event["Records"])))

    #############################
    # Extract data from Kinesis #
    #############################
    extracted_data = extract_data_from_kinesis(event)
    logger.info("Len of Extracted records : {}".format((len(extracted_data))))

    ##############################
    # Find unqiue span timestamp #
    ##############################
    unique_span_timestamps = find_unique_span_timestamp(extracted_data)

    ###########################################
    # Get the metrics for these span-ts pairs #
    ###########################################
    metrics_from_dynamo = get_metric_data_from_dynamo_batch_from_ODM_table_read(
        unique_span_timestamps
    )
    logger.info(
        "Len of metrics received from dynamo : {}".format(
            len(metrics_from_dynamo)
        )
    )
    metrics_from_dynamo_df = pd.DataFrame(metrics_from_dynamo)
    logger.info(
        "Metrics from Dynamo converted to dataframe : \n{}".format(
            (metrics_from_dynamo_df)
        )
    )

    ###################################################
    # Combine the metric and event dataframe into one #
    ###################################################
    # convert the event_df
    event_df = format_event_data(extracted_data)
    logging.debug("Event DF : \n{}".format(event_df))
    logging.debug("Metric DF : \n{}".format(metrics_from_dynamo_df))

    # combine the dataframe we got from DynamoDB and the dataframe from event
    combined_df = combine_event_dynamo_df(event_df, metrics_from_dynamo_df)
    logger.info("Combined dataframe : \n{}".format((combined_df)))

    ######################
    # Update the metrics #
    ######################
    aggregate_values = aggregation_lib.update_average_metrics(
        combined_df,
        group_by_columns=["spanId_metricname", "timestamp"],
        mean_columns=["speed"],
        count_column=["count"],
    )
    logger.info("Updated metric values are : \n{}".format(aggregate_values))

    #######################################
    # Update these new values in dynamoDB #
    #######################################
    aggregate_values_as_list = list(
        (aggregate_values.to_dict(orient="index")).values()
    )
    update_data_in_dynamo_using_ODM(aggregate_values_as_list)

    return "Done updating speed"
