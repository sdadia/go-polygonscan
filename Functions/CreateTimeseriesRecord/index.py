from base64 import b64decode
from pprint import pformat
from pvapps_odm.Schema.models import SpanModel
from pvapps_odm.ddbcon import Connection, dynamo_dbcon
import amazondax
import boto3
import botocore.session
import ciso8601
import datetime
# import dynamo_helper
import itertools
import json
import logging
import os
import uuid
import sys
from pprint import pprint

# sess = dynamo_session(SpanModel)


root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(filename)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

######################################################
##                                                  ##
##      Environment Variable Decryption/Decoding    ##
##                                                  ##
######################################################
env_vars = {}
envVarsList = ["SpanDynamoDBTableName", "DAXUrl", "OutputKinesisStreamName"]
for var in envVarsList:
    if var in os.environ.keys():
        env_vars[var] = os.environ[var]
logger.info("Environment variables are : {}".format(env_vars))
######################################################
##                                                  ##
##      Database Connection Initialisation          ##
##                                                  ##
######################################################
boto3.resource("dynamodb", region_name="us-east-1")
deserializer = boto3.dynamodb.types.TypeDeserializer()
serializer = boto3.dynamodb.types.TypeSerializer()


SpanModel.Meta.table_name = env_vars[
    "SpanDynamoDBTableName"
] + datetime.datetime.utcnow().strftime("%d%m%Y")
logging.info(
    "Span Table name after using ENV var : {}".format(SpanModel.Meta.table_name)
)
ddb = dynamo_dbcon(SpanModel, conn=Connection())
ddb.connect()


if "DAXUrl" in env_vars:
    logger.warning("Using Dynamo with DAX")
    session = botocore.session.get_session()
    dax = amazondax.AmazonDaxClient(
        session, region_name="us-east-1", endpoints=[env_vars["DAXUrl"]]
    )
    dynamo_client = dax
else:
    logger.warning("Using Dynamo without DAX")
    dynamo_client = boto3.client("dynamodb")

kinesis_client = boto3.client("kinesis")


DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
# DATETIME_FOMRAT = "%Y-%m-%dT%H:%M:%SZ"

################################
# ODM imports
################################


def generate_uuid():
    return str(uuid.uuid4())


def _grouper(iterable, n=25):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def get_spans_for_devices_from_DAX_batch_usingODM(device_ids):
    """
    Gets the spans for list of deviceIds from DynamoDB DAX
    """
    logger.info("Getting Spans data for specific device from DAX using ODM")
    only_device_ids = [x["deviceId"] for x in device_ids]
    print(only_device_ids)
    response = ddb.batch_get(only_device_ids)
    logger.debug("Response from request on DAX using ODM : {}".format(response))

    # extract the attributes from the object
    response = [x.attribute_values for x in response]
    logger.info(
        "Response attributes from request on DAX using ODM : {}".format(
            response
        )
    )

    for x in response:
        x["spans"] = json.loads(x["spans"])
        x["spans"] = format_spans(x["spans"])
        x["spans"] = sort_data_by_date(x["spans"], "end_time")

    logger.info("Response spans from after formatting: {}".format(response))

    logger.info(
        "Getting Spans data for specific device from DAX using ODM...Done"
    )
    return response


# def get_spans_for_devices_from_DAX_batch(device_ids):
    # """
    # Gets the spans for list of deviceIds from DynamoDB DAX
    # """
    # logger.info("Getting spans for deviceIds from DAX")

    # all_low_level_data = []

    # serialized_device_ids = [
        # dynamo_helper.serialize_to_ddb(x, serializer) for x in device_ids
    # ]

    # chunks = _grouper(serialized_device_ids, 50)

    # for c in chunks:
        # response = dynamo_client.batch_get_item(
            # RequestItems={env_vars["SpanDynamoDBTableName"]: {"Keys": c}}
        # )
        # low_level_data = response["Responses"][
            # env_vars["SpanDynamoDBTableName"]
        # ]

        # logger.info(
            # "low level response from batch get spans : {}".format(
                # low_level_data
            # )
        # )
        # all_low_level_data.extend(low_level_data)

    # all_spans_device_info = [
        # dynamo_helper.deserializer_from_ddb(x, deserializer)
        # for x in all_low_level_data
    # ]

    # for x in all_spans_device_info:
        # x["spans"] = json.loads(x["spans"])
        # x["spans"] = format_spans(x["spans"])
        # x["spans"] = sort_data_by_date(x["spans"], "end_time")

    # logger.debug("ALL span data : {}".format(pformat(all_spans_device_info)))
    # logger.info("Getting spans for deviceIds from DAX...Done")

    # return all_spans_device_info


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


def format_data_pts_in_rec(
    span_list, to_format_as_time=["timeStamp"], as_datetime=True
):
    logger.info("Formatting record data")

    if as_datetime:
        for x in span_list:
            # print(x)
            for t in to_format_as_time:
                # x[t] = datetime.datetime.strptime(x[t], DATETIME_FORMAT)
                x[t] = ciso8601.parse_datetime(x[t])
    else:
        for x in span_list:
            for t in to_format_as_time:
                x[t] = str(x[t])

    logger.info("Formatting record data...Done")
    return span_list


def sort_data_by_date(data_list, attribute_to_sort, reverse=False):
    """
    Sorts array of dicts by attribute provided, given that the attribute is formatted as datetime

    TODO: Format as epoch
    """

    data_sorted = sorted(
        data_list, key=lambda i: i[attribute_to_sort], reverse=reverse
    )

    return data_sorted


def remove_invalid_trip_data(telematics_data):
    """
    This function removes the datapoints with invalid GPS coordinates.
    The invalid GPS coordinates are the one whose status key is zero.

    Parameters
    ----------
    telematics_data : list of dictionary
        The telematics data in pvams format
        [
            {
                "acc": "0",
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                "gps": {
                    "GPSTime": "2019-05-22T10:45:14Z",
                    "alt": "41.10",
                    "course": "326.74",
                    "geoid": "55.00",
                    "lat": "5319.8246N",
                    "lng": "622.34160W",
                    "speed": "0.98",
                    "status": "1",
                },
                "io": "00000000",
                "timeStamp": "2019-05-22T10:45:14.154Z",
            },
            {
                "acc": "0",
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                "gps": {
                    "GPSTime": "2019-05-22T10:45:14Z",
                    "alt": "41.10",
                    "course": "326.74",
                    "geoid": "55.00",
                    "lat": "0.0",
                    "lng": "0.0",
                    "speed": "0.0",
                    "status": "0",
                },
                "io": "00000000",
                "timeStamp": "2019-05-22T10:45:14.154Z",
            },
        ]

    Returns
    -------
    valid_data : list of dictionary
        The same dataset without the invalid points
    """
    logger.info("Removing invalid trip data")

    trip = []

    for index, element in enumerate(telematics_data):
        # ignore acc Off and parking > 180 sec
        # if element["parkTime"] > 180 and element["acc"] == 0:
        # continue
        # ignore points with incorrect latitude and longitude
        # print(element)
        if element["gps"]["status"] == "0":
            continue
        # if element["gps"]["lat"] == "0.0" or element["gps"]["lng"] == "":
        # continue
        else:
            trip.append(element)

    logger.info("Number of data points which are valid : {}".format(len(trip)))
    logger.info("Removing invalid trip data...Done")

    return trip


def find_spans(start_time, end_time, all_spans):

    str_start_time = start_time[0]
    dt_start_time = start_time[1]
    str_end_time = end_time[0]
    dt_end_time = end_time[1]

    logger.info("{}".format((dt_start_time, type(dt_start_time))))
    logger.info("{}".format((dt_end_time, type(dt_end_time))))

    # 10 minutes
    acceptable_time_delta = 10 * 60
    for idx, span in enumerate(all_spans):
        # logger.info('Checking Span: {}'.format(span['spanId']))
        # logger.info(span['start_time'])
        # logger.info(span['end_time'])
        # print("span finding logic - {}".format(span))
        span_start = span["start_time"]
        span_end = span["end_time"]

        if dt_start_time >= span_start:
            # Case 1: Lean Right

            if dt_start_time > span_end:
                # Case 1.1: Complete Right

                if (
                    dt_end_time - span_end
                ).total_seconds() > acceptable_time_delta:
                    # Case 1.1.1 - Too far right:
                    # Input:             ----
                    # Spans: ----

                    # logger.info('Case 1.1.1: MET')
                    pass

                else:
                    # Case 1.1.2 - In range right:
                    # Input:     ----
                    # Spans: ----

                    # Update End Time
                    # logger.info('Case 1.1.2: MET')
                    return (idx, span["spanId"], [("end_time", str_end_time)])

            else:
                # Case 1.2: Overlap right

                if dt_end_time > span_end:
                    # Case 1.2.1 - Overlap right:
                    # Input:   ----
                    # Spans: ----

                    # Update End Time
                    # logger.info('Case 1.2.1: MET')
                    return (idx, span["spanId"], [("end_time", str_end_time)])

                else:
                    # Case 1.2.2 - In between:
                    # Input:   ----
                    # Spans: --------

                    # logger.info('Case 1.2.2: MET')
                    return (idx, span["spanId"], [])

        else:
            # Case 2: Lean Left

            if dt_end_time < span_start:
                # Case 2.1: Complete Left

                if (
                    span_start - dt_start_time
                ).total_seconds() < acceptable_time_delta:
                    # Case 2.1.1 - In range left:
                    # Input: ----
                    # Spans:     ----

                    # Update Start Time
                    # logger.info('Case 2.1.1: MET')
                    return (
                        idx,
                        span["spanId"],
                        [("start_time", str_start_time)],
                    )

                else:
                    # Case  2.1.2 - Too far left:
                    # Input: ----
                    # Spans:             ----

                    # logger.info('Case 2.1.2: MET')
                    pass

            else:
                # Case 2.2: Overlap Left

                if dt_end_time < span_end:
                    # Case 2.2.1 - Overlap left:
                    # Input: ----
                    # Spans:   ----

                    # Update Start Time
                    # logger.info('Case 2.2.1: MET')
                    # print(idx, span["spanId"], [("start_time", str_start_time))
                    return (
                        idx,
                        span["spanId"],
                        [("start_time", str_start_time)],
                    )

                else:
                    # Case 2.2.2 - Covering:
                    # Input: --------
                    # Spans:   ----

                    # Update Start Time
                    # logger.info('Case 2.2.2: MET')
                    return (
                        idx,
                        span["spanId"],
                        [
                            ("start_time", str_start_time),
                            ("end_time", str_end_time),
                        ],
                    )

    return (None, None, [])


def create_span(start_time, end_time):
    logger.info("Creating new span")

    new_span_uuid = generate_uuid()

    new_span = {
        "start_time": start_time,
        # "start_time": str(start_time),
        "end_time": end_time,
        # "end_time": str(end_time),
        "spanId": new_span_uuid,
    }

    logger.info("Newly Created span : {}".format(new_span))
    logger.info("Creating new span...Done")
    # spans.append(new_span)

    return new_span


def update_span(spans, span_index, timestamps):
    # print("****")
    # print(spans, span_index, timestamps)
    # time
    format2 = "%Y-%m-%dT%H:%M:%SZ"

    if span_index is not None:
        # print(timestamps)
        for t in timestamps:
            spans[span_index][t[0]] = t[1]

    return spans


def update_span_gps(spans, span_index,  attrs_to_update, start_lat, start_lng, end_lat, end_lng):
    if span_index is not None:
        for time in attrs_to_update:
            # if start time is changed - update start gps too
            if time[0] == "start_time":
                logger.info("Updating the start lat/lng ({},{}) for spanid : {}".format(start_lat, start_lng, spans[span_index]['spanId']))
                spans[span_index]['start_lat'] = start_lat
                spans[span_index]['start_lng'] = start_lng
            # if end time is changed - update end gps too
            elif time[0] == "end_time":
                spans[span_index]['end_lat'] = end_lat
                spans[span_index]['end_lng'] = end_lng

    return spans


def process_spans(all_spans, array_start_time, array_end_time, start_lat, start_lng, end_lat, end_lng):
    logger.info("Process function start time : {}".format(array_start_time))
    logger.info("Process function end time : {}".format(array_end_time))
    # logger.info("all spans provided are : {}".format(all_spans))

    if len(all_spans) == 0:
        newly_created_span = create_span(array_start_time[0], array_end_time[0])
        # create the start and end gps
        if (start_lat is not None) and (end_lat is not None) and (start_lng is not None) and (end_lng is not None):
            newly_created_span['start_lat'] =  start_lat
            newly_created_span['start_lng'] = start_lng
            newly_created_span['end_lat'] =  end_lat
            newly_created_span['end_lng'] = end_lng
        else:
            logger.warn("Start lat, lng, end lat lng are none, will not update the GPS coordinates of the span")

        all_spans.append(newly_created_span)
        logger.warning(
            "Creating span for first time ever: {}".format(all_spans)
        )
        return all_spans, newly_created_span["spanId"], True

    else:

        # Parse Device Spans for Appropriate Spans to Update
        span_index, span_id, attrs_to_update = find_spans(
            array_start_time, array_end_time, all_spans
        )

        # print(span_index, span_id, attrs_to_update)

        if span_index is None:
            logger.warning("No span Index found, so creating a new span")
            newly_created_span = create_span(
                array_start_time[0], array_end_time[0]
            )
            if (start_lat is not None) and (end_lat is not None) and (start_lng is not None) and (end_lng is not None):
                # create the start and end gps
                newly_created_span['start_lat'] =  start_lat
                newly_created_span['start_lng'] = start_lng
                newly_created_span['end_lat'] =  end_lat
                newly_created_span['end_lng'] = end_lng
            else:
                logger.warn("Start lat, lng, end lat lng are none, will not update the GPS coordinates of the span")

            all_spans.append(newly_created_span)
            return all_spans, newly_created_span["spanId"], True

        elif (span_index is not None) and attrs_to_update == []:
            logger.info(
                "Span found. But data is already in the span. Not updating any time"
            )
            return all_spans, span_id, False

        elif (span_index is not None) and attrs_to_update != []:
            logger.info("Found a span. Updating {}".format(attrs_to_update))
            print(attrs_to_update)
            update_span(all_spans, span_index, attrs_to_update)
            if (start_lat is not None) and (end_lat is not None) and (start_lng is not None) and (end_lng is not None):
                update_span_gps(all_spans, span_index, attrs_to_update,  start_lat, start_lng, end_lat, end_lng)
            else:
                logger.warn("Start lat, lng, end lat lng are none, will not update the GPS coordinates of the span")

            return all_spans, span_id, True

        # # If a span has been found, an index will be returned
        # if span_index is not None and attrs_to_update != []:

        # logger.info("Found Span: {}".format(span_id))

        # for attr in attrs_to_update:
        # logger.info("Update {} to {}".format(attr[0], attr[1]))
        # all_spans = update_span(all_spans, span_index, attrs_to_update)

        # return all_spans, span_index, True

        # elif span_index is None and attrs_to_update == []:
        # # create a new span
        # newly_created_span = create_span(
        # array_start_time[0], array_end_time[0]
        # )
        # all_spans.append(newly_created_span)

        # return all_spans, newly_created_span, True
        # elif span_index is not None and attrs_to_update != []:
        # logger.warn("Span is existing, but no values to update ")

        # # infill
        # return all_spans, span_id, False


def get_all_records_in_event2(event):
    logger.info("Getting all the records from event")

    all_records = []  # holds all records
    for rec in event["Records"]:

        decoded_rec = json.loads(
            b64decode(rec["kinesis"]["data"]).decode("utf-8")
        )
        all_records.append(decoded_rec)

        logger.debug("Decoded record is : {}".format(decoded_rec))
        logger.info("Len of decoded record is : {}".format(len(decoded_rec)))

    logger.info("Getting all the records from event...Done")

    return all_records


def get_all_records_in_event(event):
    logger.info("Getting all the records from event")

    all_records = []  # holds all records
    for rec in event["Records"]:
        data = {}

        decoded_rec = json.loads(
            b64decode(rec["kinesis"]["data"]).decode("utf-8")
        )

        for d in decoded_rec["message"]["payload"]["context"]["tracking"]:
            d["deviceId"] = decoded_rec["message"]["from"]

        data = {
            "deviceId": decoded_rec["deviceId"],
            "data": decoded_rec["message"]["payload"]["context"]["tracking"],
        }
        all_records.append(data["data"])
        # all_records.append(decoded_rec)

        logger.debug("Decoded record is : \n{}".format(pformat(decoded_rec)))
        logger.info("Len of decoded record is : {}".format(len(decoded_rec)))

    logger.info("Getting all the records from event...Done")

    return all_records


def get_unique_device_ids_from_records(all_records):
    logger.info("Finding unique device Ids from all records")

    unique_deviceIds_list = list(
        set([rec[0]["deviceId"] for rec in all_records])
    )
    unique_deviceIds_list = [
        {"deviceId": str(x)} for x in unique_deviceIds_list
    ]
    logger.info("Unqiue device Ids : {}".format(unique_deviceIds_list))

    logger.info("Finding unique device Ids from all records...Done")

    return unique_deviceIds_list


def tag_data(rec, spanId):
    logger.info("Tagging {} points ".format(len(rec)))

    spanId = str(spanId)

    for r in rec:
        logger.info("Record to tag : {}".format(r))
        r["spanId"] = spanId

    logger.info("Tagging...Done")
    return r


# def convert_to_putItem_format(item):
    # return {
        # "PutRequest": {"Item": dynamo_helper.serialize_to_ddb(item, serializer)}
    # }


def update_modified_device_spans_in_dynamo(device_spans_dict):
    logger.info("updating modified device spans in dynamo")

    # modified_devices_span_dict = [
    # x for x in device_spans_dict if "modified" in x
    # ]
    modified_devices_span_dict = device_spans_dict

    for m_dev in modified_devices_span_dict:
        # convert time to sstring
        for sp in m_dev["spans"]:
            sp["start_time"] = str(sp["start_time"])
            sp["end_time"] = str(sp["end_time"])

        m_dev["spans"] = json.dumps(m_dev["spans"])
        # del m_dev["modified"]
    # pprint(modified_devices_span_dict)
    logger.info(
        "Converting to json modified device spans : {}".format(
            modified_devices_span_dict
        )
    )

    serialized_modified_devices_span_dict = []
    for m in modified_devices_span_dict:
        serialized_modified_devices_span_dict.append(
            convert_to_putItem_format(m)
        )

    logger.info(
        "Serialized modified device spans : {}".format(
            serialized_modified_devices_span_dict
        )
    )

    chunks = _grouper(serialized_modified_devices_span_dict, 50)

    for c in chunks:
        response = dynamo_client.batch_write_item(
            RequestItems={env_vars["SpanDynamoDBTableName"]: c}
        )

        logger.info(
            "Response from updating spans to daynamo ; {}".format(response)
        )

    logger.info("updating modified device spans in dynamo...Done")


def _split_span_across_2_days(span):
    assert span["start_time"].date() != span["end_time"].date(), logger.error(
        "Given Span {} is not across 2 days".format(span)
    )
    end_time_day_1 = span["start_time"].replace(
        hour=23, minute=59, second=59, microsecond=0
    )
    start_time_day_2 = span["end_time"].replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    spanId_day_2 = generate_uuid()

    day_1_span = {
        "spanId": span["spanId"],
        "start_time": span["start_time"],
        "end_time": end_time_day_1,
    }
    day_2_span = {
        "spanId": spanId_day_2,
        "start_time": start_time_day_2,
        "end_time": span["end_time"],
        "parent": day_1_span["spanId"],
    }
    # else:

    return day_1_span, day_2_span


def _map_device_spans_to_date(spans):
    if not isinstance(spans, list):
        spans = [spans]
    data = {}
    for d in spans:
        date_ = str(d["start_time"].date())
        if data.get(date_):
            data[date_].append(d)
        else:
            data[date_] = [d]

    return data


def update_modified_device_spans_in_dynamo_using_ODM(date_device_ODM_object):
    for date in date_device_ODM_object:
        objects_to_write = []
        SpanModel.Meta.table_name = env_vars["SpanDynamoDBTableName"] + date
        SpanModel._connection = (
            None
        )  # after changing model's table name - set it to none
        ddb = dynamo_dbcon(SpanModel, conn=Connection())
        ddb.connect()

        for device in date_device_ODM_object[date]:
            for sp in date_device_ODM_object[date][device]["spans"]:
                sp["start_time"] = str(sp["start_time"])
                sp["end_time"] = str(sp["end_time"])
            date_device_ODM_object[date][device]["spans"] = json.dumps(
                date_device_ODM_object[date][device]["spans"]
            )

            objects_to_write.append(
                SpanModel(**date_device_ODM_object[date][device])
            )

        ddb.session.add_items(objects_to_write)
        try:
            ddb.session.commit_items()
        except Exception as e:

            logger.error("Got and Error : {}".format(e))
        # pprint(objects_to_write)
        # print("\n\n")
    return "successfully written"


def update_modified_device_spans_in_dynamo_using_ODM2(device_spans_dict):
    logger.info("updating modified device spans in dynamo using ODM")
    logger.debug("Spans Dict is : \n{}".format(device_spans_dict))

    modified_devices_span_dict = [
        d for d in device_spans_dict if d.get("modified")
    ]
    logger.debug(
        "Devices whose spans are modified are : \n{}".format(
            pformat(modified_devices_span_dict)
        )
    )

    # split the spans which are across 2 days
    for idx, modified_device in enumerate(modified_devices_span_dict):
        spans_ = modified_device["spans"]
        index_to_remove = []
        cross_days_spans = []
        for index, sp in enumerate(spans_):

            # span splitting across days
            if sp["start_time"].date() != sp["end_time"].date():
                logger.warning(
                    "Span is across days for : \n{}".format(pformat(sp))
                )
                logger.warning("Going to split the span across the days")
                day1_span, day2_span = _split_span_across_2_days(sp)
                index_to_remove.append(index)
                cross_days_spans.extend([day1_span, day2_span])

        index_to_remove.sort(reverse=True)
        for i in index_to_remove:
            del modified_device["spans"][i]
        modified_device["spans"].extend(cross_days_spans)

    # get the spans for each day for eachd evice
    # {
    # date : device : [spans ...],
    # device: [spans],
    # date : device : [spans ...],
    # device: [spans],
    # }

    data_by_date = {}
    for idx, modified_device in enumerate(modified_devices_span_dict):
        spans_ = modified_device["spans"]
        device_id = modified_device["deviceId"]
        ans = _map_device_spans_to_date(spans_)

        for date_, spans in ans.items():
            if not data_by_date.get(date_):  # if date does not exist, create it
                logger.warning("Date {} does not exist".format(date_))
                data_by_date[date_] = {}
            if not data_by_date[date_].get(
                device_id
            ):  # if device does not exist create it
                data_by_date[date_][device_id] = []
                logger.warning(
                    "device {} does not exist for {} date".format(
                        device_id, date_
                    )
                )

            data_by_date[date_][device_id].extend(spans)

    # send the data to the right table for each device based on the date attribute
    for index, (date, data) in enumerate(data_by_date.items()):
        data_as_odm_model = []

        SpanModel.Meta.table_name = env_vars["SpanDynamoDBTableName"] + "".join(
            date.split("-")[::-1]
        )
        SpanModel._connection = (
            None
        )  # after changing model's table name - set it to none

        ddb = dynamo_dbcon(SpanModel, conn=Connection())
        ddb.connect()

        for device_id, spans in data.items():
            for sp in spans:
                sp["start_time"] = str(
                    sp["start_time"]
                )  # format the data as string
                sp["end_time"] = str(sp["end_time"])
            # print(spans)
            spans = json.dumps(spans)
            data_as_odm_model.append(
                SpanModel(**{"deviceId": device_id, "spans": spans})
            )
        ddb.session.add_items(data_as_odm_model)
        ddb.session.commit_items()

    logger.info("updating modified device spans in dynamo using ODM...Done")
    return data_by_date


def send_tagged_data_to_kinesis(tagged_data):
    logger.debug("Sending tagged data to kinesis")

    # convert to kinesis record format!
    tagged_data_as_kinesis_record_format = []
    for t in tagged_data:
        tagged_data_as_kinesis_record_format.append(
            {"Data": json.dumps(t), "PartitionKey": str(t["spanId"])}
        )
        logger.debug(
            "Tagged data as kinesis format : {}".format(
                tagged_data_as_kinesis_record_format
            )
        )

    # send the tagged data
    chunks = _grouper(tagged_data_as_kinesis_record_format, 25)
    for c in chunks:
        response = kinesis_client.put_records(
            # Records=c, StreamName="dan-span-output"
            Records=c,
            StreamName=env_vars["OutputKinesisStreamName"],
        )
        logger.info(
            "Response from Outputing to Kinesis Stream : {}".format(
                pformat(response["ResponseMetadata"]["HTTPStatusCode"])
            )
        )

    logger.info("Sending tagged data to kinesis...Done")


def _split_records_across_days(all_valid_records):
    record_index_across_days = []
    record_break_index = []

    for idx, rec in enumerate(all_valid_records):
        start_time = ciso8601.parse_datetime(rec[0]["timeStamp"])
        end_time = ciso8601.parse_datetime(rec[-1]["timeStamp"])

        if start_time.date() != end_time.date():
            logger.warning(
                "Got data between 2 dates for record number ; {}".format(idx)
            )
            logger.warning("Record across dates is : {}".format(rec))
            index_to_break_at = None
            for idx2, r in enumerate(rec):
                if (
                    ciso8601.parse_datetime(r["timeStamp"]).date()
                    != start_time.date()
                ):
                    record_index_across_days.append(idx)
                    record_break_index.append(idx2)
                    break
    # return record_index_across_days, record_break_index

    records_across_days = []
    for index, break_in_index in zip(
        record_index_across_days, record_break_index
    ):
        day_1_record = all_valid_records[index][0:break_in_index]
        day_2_record = all_valid_records[index][break_in_index:]

        records_across_days.append(day_1_record)
        records_across_days.append(day_2_record)

    record_index_across_days.reverse()
    for index in record_index_across_days:
        del all_valid_records[index]

    all_valid_records.extend(records_across_days)

    return all_valid_records


def find_date_device_combos_from_records(all_valid_records):
    """
    Return a list of the date and device belonging to those dates

    # date is in ddMMYYYY for mat

    [{'date': '24112019', 'deviceId': '1'},
     {'date': '25112019', 'deviceId': '1'},
     {'date': '25112019', 'deviceId': '2'}]

    """
    data = {}
    for rec in all_valid_records:
        date = (
            ciso8601.parse_datetime(rec[0]["timeStamp"])
            .date()
            .strftime("%d%m%Y")
        )
        deviceId = rec[0]["deviceId"]

        if date not in data:
            data[date] = []
        if (
            deviceId not in data[date]
        ):  # only add if the device has not been previously added
            data[date].append(deviceId)

    data2 = []
    for k, v in data.items():
        for val in v:
            data2.append({"date": k, "deviceId": val})

    return data2


def get_data_for_device_from_particular_table_using_OMD(
    date_device_dictionary_list
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
            None
        )  # after changing model's table name - set it to none
        ddb = dynamo_dbcon(SpanModel, conn=Connection())
        ddb.connect()

        ans = data[date]
        ODM_ans = ddb.batch_get(data[date])

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
                data3[date][deviceId]["spans"] = sort_data_by_date(data3[date][deviceId]["spans"], "end_time")
            except TypeError as e:
                pass

    # pprint(data3)

    return data3


def handler(event, context):
    logger.info("Starting handler")
    logger.info("Event is : {}".format(event))

    ##############################
    # Get all records from event #
    ##############################
    all_records = get_all_records_in_event(event)
    # print(all_records)

    ######################################
    # keep only valid values in a record #
    ######################################
    all_valid_records = []
    for rec in all_records:
        valid_rec = remove_invalid_trip_data(rec)
        if valid_rec == []:
            continue
        else:
            all_valid_records.append(valid_rec)
    logger.info(
        "Len of Valid records before splitting across days are : {}".format(
            len(all_valid_records)
        )
    )

    # split the records across days, if it has some
    all_valid_records = _split_records_across_days(all_valid_records)
    logger.info(
        "Len of Valid records after splitting across days are : {}".format(
            len(all_valid_records)
        )
    )

    if len(all_valid_records) == 0:
        return "No records are valid"

    #################################
    # get all the unique device IDs #
    #################################
    # unique_deviceIds = get_unique_device_ids_from_records(all_valid_records)
    date_device_combos = find_date_device_combos_from_records(all_valid_records)

    ########################################
    # get spans for these unique deviceIds #
    ########################################
    # device_spans_dict = get_spans_for_devices_from_DAX_batch_usingODM(
    # unique_deviceIds
    # )
    date_device_ODM_object = get_data_for_device_from_particular_table_using_OMD(
        date_device_combos
    )
    logger.info(
        "Date-Device Spans Dict after getting data from ODM :  {}".format(
            pformat(date_device_ODM_object)
        )
    )
    # print(date_device_combos)

    ################
    # Tagged data ##
    ################
    tagged_data = []

    # For each record, find the span
    for rec_index, rec in enumerate(all_valid_records):
        # print(rec_index)
        # pprint(rec[0])
        logger.info("Processing record number : {}".format(rec_index))

        rec = format_data_pts_in_rec(rec)
        logger.debug("Formatted valid record is : {}".format(rec))

        current_rec_device_id = rec[0]["deviceId"]
        day = rec[0]["timeStamp"].date().strftime("%d%m%Y")  # as string
        logger.info("Day for record is : {}".format(day))
        current_rec_device_id_spans = date_device_ODM_object[day][
            current_rec_device_id
        ]["spans"]

        logger.info(
            "Current deviceId : {} and spans are : {}".format(
                current_rec_device_id, current_rec_device_id_spans
            )
        )

        #################
        # Find the span #
        #################
        # extract valid records only
        valid_records_for_gps = [x for x in rec if x['gps']['status'] != "invalid"]
        if len(valid_records_for_gps) > 0:
            start_lat = valid_records_for_gps[0]["gps"]['lat']
            start_lng = valid_records_for_gps[0]["gps"]['lng']
            end_lat = valid_records_for_gps[-1]["gps"]['lat']
            end_lng = valid_records_for_gps[-1]["gps"]['lng']
        else:
            start_lat = None
            start_lng = None
            end_lat = None
            end_lng = None


        array_end_time = rec[-1]["timeStamp"]
        array_start_time = rec[0]["timeStamp"]
        dt_array_end_time = rec[-1]["timeStamp"]
        dt_array_start_time = rec[0]["timeStamp"]

        logger.info(
            "Times are : {}\t{}\t{}\t{}".format(
                array_end_time,
                array_start_time,
                dt_array_end_time,
                dt_array_start_time,
            )
        )

        all_spans, spanId_for_tagging, modified = process_spans(
            current_rec_device_id_spans,
            (array_start_time, dt_array_start_time),
            (array_end_time, dt_array_end_time),
            start_lat,
            start_lng,
            end_lat,
            end_lng
        )

        ############################################################
        # update the of spans for specific device for speficic day #
        ############################################################
        date_device_ODM_object[day][current_rec_device_id]["spans"] = all_spans
        logger.debug(
            "Date Device ODM object after finding spans : {}".format(
                date_device_ODM_object
            )
        )

        ##############################################
        # Tag all valid points in the current record #
        ##############################################
        for r in rec:
            r["spanId"] = spanId_for_tagging
            r["timestamp"] = str(r["timeStamp"])
            del r["timeStamp"]
            tagged_data.append(r)

        logger.info(
            "Len All tagged data after tagging: {}".format(len(tagged_data))
        )
        logger.debug("All tagged data after tagging: {}".format(tagged_data))

    ###################################################
    # batch update the devices whose spans we updated #
    ###################################################
    # update_modified_device_spans_in_dynamo_using_ODM2(device_spans_dict)
    update_modified_device_spans_in_dynamo_using_ODM(date_device_ODM_object)

    #########################################
    # Writing tagged data to kinesis stream #
    #########################################
    send_tagged_data_to_kinesis(tagged_data)

    logger.info("\nAt the end : \n{}".format(pformat(date_device_ODM_object)))

    return date_device_ODM_object
