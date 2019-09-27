# Module Imports
from boto3.dynamodb.conditions import Key
from jsonschema import ValidationError, validate
from operator import itemgetter
from pprint import pformat, pprint
import amazondax
import boto3
import botocore
import datetime
import dynamo_helper
import json
import logging
import numpy as np
import os
import pandas as pd
import time


root = logging.getLogger()
if root.handlers:
    for handler2 in root.handlers:
        root.removeHandler(handler2)
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S.%s",
)

######################################################
##                                                  ##
##      Environment Variable Decryption/Decoding    ##
##                                                  ##
######################################################
env_vars = {}
envVarsList = ["SpanTable", "AggregateTable", "DAXUrl"]

for var in envVarsList:
    if var in os.environ.keys():
        env_vars[var] = os.environ[var]
logging.info(env_vars)

######################################################
##                                                  ##
##      Database Connection Initialisation          ##
##                                                  ##
######################################################
dynamodb_resource = boto3.resource("dynamodb")
serializer = boto3.dynamodb.types.TypeSerializer()
deserializer = boto3.dynamodb.types.TypeDeserializer()
metric_table = dynamodb_resource.Table(env_vars["AggregateTable"])

# enable dax
if "DAXUrl" in env_vars:

    session = botocore.session.get_session()
    dax = amazondax.AmazonDaxClient(
        session,
        region_name="us-east-1",
        endpoints=[
            env_vars['DAXUrl']
        ],
    )
    dynamo_dax_client = dax
else:
    dynamo_dax_client = boto3.client("dynamodb")

# dynamo_dax_client = boto3.client("dynamodb")


######################################################
##                                                  ##
##      Default incoming_event_schema of incoming request          ##
##                                                  ##
######################################################
incoming_event_schema = {
    "type": "object",
    "properties": {
        "deviceId": {"type": "string"},
        "start_datetime": {"type": "string"},
        "end_datetime": {"type": "string"},
        "trip_time_diff": {"type": "number", "default": 13},
    },
    "required": ["deviceId", "start_datetime", "end_datetime"],
}

TRIP_TIME_DIFF = 10
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def get_trips_pandas(
    sorted_list_of_dicts,
    user_start_time,
    user_end_time,
    time_diff_between_spans,
):
    """
    Finds the relevant trips from the sorted list of spans. This function internally sorts the data
    in ascending order by start time.

    Parameters
    ----------
    list_of_spans_dict : list of dicts
    The list of dicts containing ATLEAST the following fields :

    [{'spanId': ..,
    'start_time': str,
    'end_time' : str} ,
    {}, {} ...]

    time_diff_between_spans : float (minutes) (default : 15)
    The difference between 2 spans in minutes to indicate a new trip. The default value is 15
    minutes.


    Returns
    -------
    trips : dictionary
    The dictionary containing the start, end time and the span ids for that trip.
    The trip_num will be a integer
    The start, end time will be a string in YYYY-MM-DD
    The span ids will be a list of strings of the ids

    {
    "trip_num" : {"start_time" : ..,
    "end_time" : ...,
    "spanIds" = [....]},

    "trip_num" : {"start_time" : ..,
    "end_time" : ...,
    "spanIds" = [....]},
    }
    """
    logging.info("Finding trips")
    logging.info(
        "Selected Time Difference between spans for trip seperation: {} minutes".format(
            time_diff_between_spans
        )
    )

    # convert to data frame, set type as date time and sort in ascending order by timestamp
    if not isinstance(sorted_list_of_dicts, list):
        sorted_list_of_dicts = [sorted_list_of_dicts]

    df = pd.DataFrame(sorted_list_of_dicts)
    df = df[df.start_time != df.end_time]

    df[["start_time_", "end_time_"]] = df[["start_time", "end_time"]].apply(
        pd.to_datetime
    )
    # print(df)

    # keep the data only between user specified start and end_time
    df = df[
        (df["start_time_"] > user_start_time)
        & (df["end_time_"] < user_end_time)
    ]

    df.sort_values(by="start_time_", ascending=True, inplace=True)
    print(df)

    # shift time up
    df["start_time_next_trip"] = df["start_time_"].shift(-1)
    print(df)

    # differente between start time of next trip and end time of current trip
    df["diff"] = (df["start_time_next_trip"] - df["end_time_"]).shift(1)
    print(df)

    # convert the differentce in secpnds
    df["diff_in_seconds"] = df["diff"].fillna(
        pd.Timedelta(seconds=0)
    ) / np.timedelta64(1, "s")
    print(df)

    # 15 minute rule - and binarize the breaks
    df["new_trip_start_indicator"] = np.where(
        df["diff_in_seconds"] >= time_diff_between_spans * 60, 1, 0
    )
    print(df)

    # create the trip_number for each span
    trip_number = 1
    trip_id_indicator = []
    for idx, indicator in enumerate(list(df["new_trip_start_indicator"])):
        if indicator == 1:
            trip_number += 1
        trip_id_indicator.append(trip_number)
    df["trip_id_indicator"] = trip_id_indicator
    print(df)

    df["start_time_"] = df["start_time_"].astype(str)
    df["end_time_"] = df["end_time_"].astype(str)
    logging.debug("Calculated Data frame is : \n{}".format(df))

    # extract the min and max time for each trip
    final_result_set = (
        (
            df.groupby("trip_id_indicator").agg(
                {"start_time_": min, "end_time_": max, "spanId": list}
            )
        )
        .rename(columns={"end_time_": "end_time", "start_time_": "start_time"})
        .to_dict(orient="index")
    )
    pprint(final_result_set)

    logging.info("Finding trips...Done")
    return final_result_set


def get_span_data_from_dynamo_dax(deviceId):
    logging.info(
        "Getting span data from dynamoDAX for deviceId : {}".format(deviceId)
    )

    response = dynamo_dax_client.get_item(
        TableName=str(env_vars["SpanTable"]),
        Key={"deviceId": {"S": str(deviceId)}},
    )

    logging.info("response from dax : {}".format(response))

    logging.info(
        "Getting span data from dynamoDAX for deviceId : {}...Done".format(
            deviceId
        )
    )

    if response == {}:
        hasattr(response, "Item")
        logging.warn("No Span Data exist for deviceId : {}".format(deviceId))
        return []
    else:
        deserialized_data = dynamo_helper.deserializer_from_ddb(
            response["Item"], deserializer
        )
        return json.loads(deserialized_data["spans"])


def preprocess_list_of_spans(list_of_spans_dict):
    """
    Sorts the list of span which we get from dyamo accoding to the start_time
    in ascending order. And converts the time to python date time object.

    Parameters
    ----------
    list_of_spans_dict : list of dicts
        The list of dicts containing ATLEAST the following fields :

        [{'spanId': ..,
         'start_time': str,
         'end_time' : str} ,
          {}, {} ...]
    """
    data = list_of_spans_dict
    logging.info("Formatting and sorting")
    for d in list_of_spans_dict:
        print(d)
        list_of_spans_dict[d]["start_time"] = datetime.datetime.strptime(
            list_of_spans_dict[d]["start_time"], DATETIME_FORMAT
        ).timestamp()
        list_of_spans_dict[d]["end_time"] = datetime.datetime.strptime(
            list_of_spans_dict[d]["end_time"], DATETIME_FORMAT
        ).timestamp()

    # sort according to the end time in descending order
    data = sorted(data, key=itemgetter("start_time"), reverse=True)

    logging.info("Formatting and sorting...Done")
    return data


def get_speed_data_from_dynamo(spanIds):
    """
    Gets all the spans for a vehicle id
    """
    all_data = []
    for sp in spanIds:
        logging.info("Getting metric Data from DynaomoDB for spanId : {}".format(sp))
        response = metric_table.query(
            KeyConditionExpression=Key("spanId").eq(str(sp + "_speed"))
        )

        logging.info(
            "Len of response from DynamoDB is : {}".format(
                len(response["Items"])
            )
        )
        logging.info("Getting Data from DynaomoDB...Done")
        all_data.extend(response["Items"])

    if len(all_data) == 0:
        logging.warn(
            "No speed metrics found for given span Ids : {}".format(spanIds)
        )

    return all_data


def aggregate_speed_for_trip(spanIds):

    speed_data = get_speed_data_from_dynamo(spanIds)

    if len(speed_data) == 0:
        return {"avg_speed": None}
    else:
        df = pd.DataFrame(speed_data)
        df["speed_mul_count"] = df["speed"] * df["count"]
        avg_speed = df["speed_mul_count"].sum() / df["count"].sum()
        avg_speed = float(avg_speed) / 10.0
        return {"avg_speed": float(avg_speed)}


def handler(event, context):
    print(event)
    logging.info("Given API query : \n{}".format(pformat(event)))

    logging.info("Parsing event")
    try:
        validate(event, incoming_event_schema)
    except ValidationError as e:
        logging.error("Incoming event does not match schema")
        logging.error(e)
        raise

    # if trip merging time is not specified, then set to default value
    if "trip_time_diff" not in event:
        logging.warn(
            "User did not specify, using default timing for trip time diff : {}".format(
                TRIP_TIME_DIFF
            )
        )
        event["trip_time_diff"] = TRIP_TIME_DIFF

    # query device from dynamo and get all the spans
    span_data = get_span_data_from_dynamo_dax(event["deviceId"])
    logging.info("returned span data : {}".format(span_data))

    # # if no spans are returned, then the trips does not exist
    if len(span_data) == 0:
        trips = {"trips": []}
    else:  # else find the trips
        # calculate trips
        print("Using pandas")
        trips = get_trips_pandas(
            span_data,
            event["start_datetime"],
            event["end_datetime"],
            event["trip_time_diff"],
        )

        for trip_id in trips.keys():
            spanss = trips[trip_id]["spanId"]

            # find average speed
            trips[trip_id]["metrics"] = aggregate_speed_for_trip(spanss)

    trips = {"trips": list(trips.values())}
    print(trips)

    return json.dumps(trips)
