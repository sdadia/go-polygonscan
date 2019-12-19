# Module Imports
from boto3.dynamodb.conditions import Key
from jsonschema import ValidationError, validate
from operator import itemgetter
from pprint import pformat, pprint
from typing import List
import amazondax
import boto3
import botocore
import ciso8601
import datetime
import json
import logging
import numpy as np
import os
import pandas as pd
import pytz
from pynamodb.exceptions import TableDoesNotExist

from pvapps_odm.Schema.models import (
    SpanModel,
    AggregationModel,
    StationaryIdlingModel,
)
from pvapps_odm.ddbcon import dynamo_dbcon
from pvapps_odm.ddbcon import Connection

from typing import Dict, List


root = logging.getLogger()
if root.handlers:
    for handler2 in root.handlers:
        root.removeHandler(handler2)
logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S.%s",
)
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

######################################################
##                                                  ##
##      Environment Variable Decryption/Decoding    ##
##                                                  ##
######################################################
env_vars = {}
envVarsList = ["SpanDynamoDBTableName"]

for var in envVarsList:
    if var in os.environ.keys():
        env_vars[var] = os.environ[var]

print(SpanModel.Meta.table_name)
logging.info("Environment variables are : {}".format(env_vars))

######################################################
##                                                  ##
##      Database Connection Initialisation          ##
##                                                  ##
######################################################
ddb_stationary_idling = dynamo_dbcon(AggregationModel, conn=Connection())
ddb_stationary_idling.connect()

# enable dax
if "DAXUrl" in env_vars:
    logging.warning("Using DAX")
    session = botocore.session.get_session()
    dax = amazondax.AmazonDaxClient(
        session, region_name="us-east-1", endpoints=[env_vars["DAXUrl"]]
    )
    dynamo_dax_client = dax
else:
    logging.warning("Not using DAX")
    dynamo_dax_client = boto3.client("dynamodb")


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
    sorted_list_of_dicts: List[Dict],
    user_start_time: str,
    user_end_time: str,
    time_diff_between_spans: int,
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
    assert isinstance(user_start_time, str)
    assert isinstance(user_end_time, str)
    user_end_time = ciso8601.parse_datetime(user_end_time)
    user_start_time = ciso8601.parse_datetime(user_start_time)

    # convert to data frame, set type as date time and sort in ascending order by timestamp
    if not isinstance(sorted_list_of_dicts, list):
        sorted_list_of_dicts = [sorted_list_of_dicts]
    pprint(sorted_list_of_dicts)

    # keep the data only between user specified start and end_time
    span_id_to_delete = []
    for idx, span in enumerate(sorted_list_of_dicts):
        if not isinstance(span["start_time"], datetime.datetime):
            span_start_time = ciso8601.parse_datetime(span["start_time"])
        else:
            span_start_time = span["start_time"]

        if not isinstance(span["end_time"], datetime.datetime):
            span_end_time = ciso8601.parse_datetime(span["end_time"])
        else:
            span_end_time = span["end_time"]

        query_start_time = user_start_time
        query_end_time = user_end_time

        # case 1: query bigger
        # span      x-------------x
        # query y--------------------y
        # keep this span
        if (query_start_time < span_start_time) and (query_end_time) > (
            span_end_time
        ):
            continue
        # case 2: left overlap
        # span       x----------------x
        # query   y----------y
        # keep this span
        elif (query_start_time < span_start_time) and (
            query_end_time > span_start_time
        ):
            continue
        # case 3: right overlap
        # span       x----------------x
        # query                 y----------y
        # keep this span
        elif (query_start_time < span_end_time) and (
            query_end_time > span_end_time
        ):
            continue
        # case 4: query inside
        # span       x----------------x
        # query           y----y
        # keep this span
        elif (query_start_time > span_start_time) and (
            query_end_time < span_end_time
        ):
            continue
        else:
            span_id_to_delete.append(idx)

    # delete spans at those index
    span_id_to_delete.sort(reverse=True)
    for t in span_id_to_delete:
        del sorted_list_of_dicts[t]
    # pprint(sorted_list_of_dicts)

    df = pd.DataFrame(sorted_list_of_dicts)
    df = df[df.start_time != df.end_time]

    df[["start_time_", "end_time_"]] = df[["start_time", "end_time"]].apply(
        pd.to_datetime
    )

    # if latitude, longitude does not exist for a span, keep it as nan
    for col in ["start_lat", "start_lng", "end_lat", "end_lng"]:
        if col not in df.columns:
            df[col] = None

    # print(df)

    df.sort_values(by="start_time_", ascending=True, inplace=True)
    # print(df)

    # shift time up
    df["start_time_next_trip"] = df["start_time_"].shift(-1)
    # print(df)

    # differente between start time of next trip and end time of current trip
    df["diff"] = (df["start_time_next_trip"] - df["end_time_"]).shift(1)
    # print(df)

    # convert the differentce in secpnds
    df["diff_in_seconds"] = df["diff"].fillna(
        pd.Timedelta(seconds=0)
    ) / np.timedelta64(1, "s")
    # print(df)

    # 15 minute rule - and binarize the breaks
    df["new_trip_start_indicator"] = np.where(
        df["diff_in_seconds"] >= time_diff_between_spans * 60, 1, 0
    )
    # print(df)

    # create the trip_number for each span
    trip_number = 1
    trip_id_indicator = []
    for idx, indicator in enumerate(list(df["new_trip_start_indicator"])):
        if indicator == 1:
            trip_number += 1
        trip_id_indicator.append(trip_number)
    df["trip_id_indicator"] = trip_id_indicator
    # print(df)

    df["start_time_"] = df["start_time_"].astype(str)
    df["end_time_"] = df["end_time_"].astype(str)
    logging.info("Calculated Data frame is : \n{}".format(df))
    df.replace({pd.np.nan: ""}, inplace=True)  # replace nan with ""

    # extract the min and max time for each trip
    final_result_set = (
        (
            df.groupby("trip_id_indicator").agg(
                {
                    "start_time_": min,
                    "end_time_": max,
                    "spanId": list,
                    "start_lat": list,
                    "end_lat": list,
                    "start_lng": list,
                    "end_lng": list,
                }
            )
        )
        .rename(columns={"end_time_": "end_time", "start_time_": "start_time"})
        .to_dict(orient="index")
    )

    for t in final_result_set:
        for x in ["start_lng", "start_lat"]:
            final_result_set[t][x] = final_result_set[t][x][0]
        for x in ["end_lng", "end_lat"]:
            final_result_set[t][x] = final_result_set[t][x][-1]
    # pprint(final_result_set)

    logging.info("Finding trips...Done")
    return final_result_set


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
        # print(d)
        # list_of_spans_dict[d]["start_time"] = datetime.datetime.strptime(
        # list_of_spans_dict[d]["start_time"], DATETIME_FORMAT
        # ).timestamp()
        # list_of_spans_dict[d]["end_time"] = datetime.datetime.strptime(
        # list_of_spans_dict[d]["end_time"], DATETIME_FORMAT
        # ).timestamp()

        list_of_spans_dict[d]["start_time"] = ciso8601.parse_datetime(
            list_of_spans_dict[d]["start_time"]
        ).timestamp()
        list_of_spans_dict[d]["end_time"] = ciso8601(
            list_of_spans_dict[d]["end_time"]
        ).timestamp()

    # sort according to the end time in descending order
    data = sorted(data, key=itemgetter("start_time"), reverse=True)

    logging.info("Formatting and sorting...Done")
    return data


def get_stationary_idling_state_transitions(deviceId):
    try:
        response = ddb_stationary_idling.get_object(deviceId, None)
        return (
            json.loads(response.idling_state_transition),
            json.loads(response.stationary_state_transition),
        )
    except Exception as e:
        logger.error(e)
        logger.warning(
            "Data for stationary and idling time transition does not exist for deviceId : {}. Will return default Time and Current State which are empty".format(
                deviceId
            )
        )
        stationary_state_transition_default = {"time": [], "curr": []}
        idling_state_transition_default = {"time": [], "curr": []}
        return (
            idling_state_transition_default,
            stationary_state_transition_default,
        )


def find_actual_time_from_state_transitons(state_transition_dictionary):
    # print(state_transition_dictionary)
    C = state_transition_dictionary["curr"]
    T = state_transition_dictionary["time"]
    started = False
    time_1 = None
    time_2 = None
    total_time = 0.0
    index = 0

    if C[-1] == 1:
        logger.warning(
            "Post Correction needed as last value for state transition is 1"
        )
        post_correction_needed = True
    else:
        post_correction_needed = False

    for ts, status in zip(T, C):
        if (status == 1) and (not started):
            time_1 = ts
            started = True
        elif (status == 0) and (started):
            time_2 = ts
            started = False

            total_time += time_2 - time_1
            time_1 = time_2 = None

    return total_time, post_correction_needed


def string_time_to_unix_epoch(data):
    if isinstance(data, list):
        ans = []
        for date in data:
            ans.append(
                ciso8601.parse_datetime(date)
                .replace(tzinfo=pytz.UTC)
                .timestamp()
            )

        return ans
    else:
        return (
            ciso8601.parse_datetime(data).replace(tzinfo=pytz.UTC).timestamp()
        )


from bisect import bisect_left, bisect_right


def find_ge(a, x):
    "Find leftmost item greater than or equal to x"
    i = bisect_left(a, x)
    if i != len(a):
        return i
    raise ValueError


def find_le(a, x):
    "Find rightmost value less than or equal to x"
    i = bisect_right(a, x)
    if i:
        return i - 1
    raise ValueError


def keep_relevant_data_for_stationary_idling_btw_start_end_time(
    start_time: float, end_time: float, data
):
    assert end_time > start_time, logger.error(
        "Start time cannot be less than end time"
    )

    logger.debug("Function parameters are : \n{}".format(pformat(locals())))
    start_time = string_time_to_unix_epoch(start_time)
    end_time = string_time_to_unix_epoch(end_time)

    logger.warning(
        "Start time after conversion is : {} \t {}".format(start_time, end_time)
    )

    try:
        start_index = find_ge(data["time"], start_time)
    except ValueError:
        logger.warning(
            "Start time is greater than any time for the state transitions"
        )
        start_index = -1

    try:
        end_index = find_le(data["time"], end_time)
    except ValueError:
        logger.warning(
            "end time is less than any time for the state transitions"
        )
        end_index = 0

    logger.debug(
        "starting index : {}\t ending index : {}".format(start_index, end_index)
    )

    data["time"] = data["time"][start_index:end_index]
    data["curr"] = data["curr"][start_index:end_index]

    return data


def aggregate_stationary_idling_time(
    deviceId: str, trip_start_time: str, trip_end_time: str
) -> Dict:
    """
    Function finds the stationary and idling time for a trip

    Parameters
    ----------
    deviceId : str
        The deviceId for which we need to find the stationary and idle time
    trip_start_time: str
        The time of the start of the trip in YYYY-MM-DD HH:MM:SS.Used to limit the data for a specific trip.
    trip_end_time: str
        The time of the end of the trip in YYYY-MM-DD HH:MM:SS. Used to limit the data for a specific trip.

    Returns
    -------
    Dict: {}

    """
    logger.debug("Function parameters are : \n{}".format(pformat(locals())))

    (
        idling_state_transition,
        stationary_state_transition,
    ) = get_stationary_idling_state_transitions(deviceId)

    if stationary_state_transition["time"]:
        stationary_state_transition = keep_relevant_data_for_stationary_idling_btw_start_end_time(
            trip_start_time, trip_end_time, stationary_state_transition
        )

        if stationary_state_transition["time"]:

            # print(stationary_state_transition)
            # find the total stationary time
            (
                total_stationary_time,
                post_correction_needed,
            ) = find_actual_time_from_state_transitons(
                stationary_state_transition
            )
            if post_correction_needed:
                total_stationary_time += (
                    string_time_to_unix_epoch(trip_end_time)
                    - stationary_state_transition["time"][-1]
                )
        else:
            logger.warning(
                "stationary time data not found in given time range returning -1"
            )
            total_stationary_time = -1

    else:
        logger.warning("Stationary Time Data not found returning -1")
        total_stationary_time = -1

    if idling_state_transition["time"]:
        # keep only the transitions for the times between a given trip
        idling_state_transition = keep_relevant_data_for_stationary_idling_btw_start_end_time(
            trip_start_time, trip_end_time, idling_state_transition
        )
        if idling_state_transition["time"]:

            # find total idling time
            (
                total_idling_time,
                post_correction_needed,
            ) = find_actual_time_from_state_transitons(idling_state_transition)

            if post_correction_needed:
                total_idling_time += (
                    string_time_to_unix_epoch(trip_end_time)
                    - idling_state_transition["time"][-1]
                )

        else:
            logger.warning(
                "idling time data not found in given time range returning -1"
            )
            total_idling_time = -1
    else:
        logger.warning("Idling Time Data not found returning -1")
        total_idling_time = -1

    return {
        "stationary_time": round(total_stationary_time, 2),
        "idling_time": round(total_idling_time, 2),
    }


def get_spans_for_device_from_partiuclar_table(
    date_device_dictionary_list: List,
):
    """
    Expected input : [{'date': '24112019', 'deviceId': '1'},
     {'date': '25112019', 'deviceId': '1'},
     {'date': '25112019', 'deviceId': '2'}]

    Expected output :
    {'24112019': {'1': SpanModel(deviceId='1', spans='abc')},
     '25112019': {'1': SpanModel(deviceId='1', spans='pqr'),
                  '2': SpanModel(deviceId='2', spans='[]'),
                  '3': SpanModel(deviceId='3', spans='[]')}}
    """
    data = {}
    for val in date_device_dictionary_list:
        date = val["date"]
        deviceId = val["deviceId"]
        if date not in data.keys():
            data[date] = []
        if deviceId not in data[date]:
            data[date].append(deviceId)

    data2 = {}
    for date in data:

        SpanModel.Meta.table_name = env_vars["SpanDynamoDBTableName"] + date
        SpanModel._connection = (
            None  # after changing model's table name - set it to none
        )
        ddb = dynamo_dbcon(SpanModel, conn=Connection())
        ddb.connect()

        ans = data[date]
        # try to get the data, but if the table does not exist, then return empty span
        try:
            ODM_ans = ddb.batch_get(data[date])
        except TableDoesNotExist as e:
            logger.error("Error is : {}".format(e))
            logger.error("Returning empty spans as we got an error")
            ODM_ans = [SpanModel(**{"deviceId": deviceId, "spans": []})]

        if len(data[date]) != len(ODM_ans):
            missing_deviceids = list(
                set(data[date]) - set([x.deviceId for x in ODM_ans])
            )

            for devid in missing_deviceids:
                ODM_ans.append(SpanModel(**{"deviceId": devid, "spans": []}))

        for d in ODM_ans:
            if date not in data2:
                data2[date] = {}
            data2[date][d.deviceId] = d

    data3 = {}
    for date in data2:
        for deviceId, model in data2[date].items():
            if date not in data3:
                data3[date] = {}

            data3[date][deviceId] = model.attribute_values
            try:
                data3[date][deviceId]["spans"] = format_spans(
                    json.loads(data3[date][deviceId]["spans"])
                )
            except TypeError as e:
                pass

    return data3


def format_spans(
    span_list, to_format_as_time=["start_time", "end_time"], as_datetime=True
):
    logger.info("Formatting span data")

    if as_datetime:
        for x in span_list:
            for t in to_format_as_time:
                x[t] = ciso8601.parse_datetime(x[t])
                # x[t] = datetime.datetime.strptime(x[t], DATETIME_FORMAT)
    else:
        for x in span_list:
            for t in to_format_as_time:
                x[t] = str(x[t])

    logger.info("Formatting span data...Done")
    return span_list


def handler(event, context):
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
        logging.warning(
            "User did not specify, using default timing for trip time diff : {}".format(
                TRIP_TIME_DIFF
            )
        )
        event["trip_time_diff"] = TRIP_TIME_DIFF

    # query device from dynamo and get all the spans

    # create the device-date dictionary from start and end time of the query
    # [{'date': '24112019', 'deviceId': '1'},
    # {'date': '25112019', 'deviceId': '1'},
    date_device_dictionary_list = []
    start_day = ciso8601.parse_datetime(event["start_datetime"])
    date = start_day
    end_day = ciso8601.parse_datetime(event["end_datetime"])
    while date <= end_day:
        date_device_dictionary_list.append(
            {
                "deviceId": str(event["deviceId"]),
                "date": date.strftime("%d%m%Y"),
            }
        )
        date = date + datetime.timedelta(days=1)

    # get the spans from different tables between these start and end time
    date_device_ODM_object = get_spans_for_device_from_partiuclar_table(
        date_device_dictionary_list
    )
    # extract the spans between these 2 times
    span_data = []
    for date in date_device_ODM_object:
        spans = date_device_ODM_object[date][event["deviceId"]]["spans"]
        if len(spans):
            span_data.extend(spans)
    logging.info("returned span data : {}".format(pformat(span_data)))
    # print(span_data)
    # sys.exit(1)
    # # if no spans are returned, then the trips does not exist
    if len(span_data) == 0:
        trips = {}
    else:  # else find the trips
        # calculate trips
        print("Using pandas")
        trips = get_trips_pandas(
            span_data,
            event["start_datetime"],
            event["end_datetime"],
            event["trip_time_diff"],
        )

        if trips == {}:
            logging.warning("No Trips found")

        # get other metrics - speed - stationary and idling time
        for trip_id in trips.keys():
            spanss = trips[trip_id]["spanId"]

            if "metrics" not in trips[trip_id]:
                trips[trip_id]["metrics"] = {}

            # # find average speed - dan will find the speed
            # trips[trip_id]["metrics"] = aggregate_speed_for_trip(spanss)

            # find the stationary and idling time
            stationry_idling_time_dict = aggregate_stationary_idling_time(
                event["deviceId"],
                trips[trip_id]["start_time"],
                trips[trip_id]["end_time"],
            )
            trips[trip_id]["metrics"].update(stationry_idling_time_dict)

    trips = {"trips": list(trips.values())}

    logger.info("Calculated trips are : {}".format(trips))
    return trips
