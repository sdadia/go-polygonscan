from base64 import b64encode, b64decode
from boto3.dynamodb.conditions import Key, Attr, AttributeNotExists
from pprint import pprint, pformat
import dynamo_helper
import amazondax
import base64
import boto3
import botocore.session
import datetime
import functools
import itertools
import json
import logging
import os
import sys
import time
import uuid
from pvapps_odm.Schema.models import SpanModel
from pvapps_odm.session import dynamo_session
from pvapps_odm.ddbcon import dynamo_dbcon
from pvapps_odm.ddbcon import Connection
import ciso8601

sess = dynamo_session(SpanModel)


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
env_vars["SpanDynamoDBTableName"] = SpanModel.Meta.table_name
logger.info("Environment variables are : {}".format(env_vars))
######################################################
##                                                  ##
##      Database Connection Initialisation          ##
##                                                  ##
######################################################
boto3.resource("dynamodb", region_name="us-east-1")
deserializer = boto3.dynamodb.types.TypeDeserializer()
serializer = boto3.dynamodb.types.TypeSerializer()


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


def get_spans_for_devices_from_DAX_batch(device_ids):
    """
    Gets the spans for list of deviceIds from DynamoDB DAX
    """
    logger.info("Getting spans for deviceIds from DAX")

    all_low_level_data = []

    serialized_device_ids = [
        dynamo_helper.serialize_to_ddb(x, serializer) for x in device_ids
    ]

    chunks = _grouper(serialized_device_ids, 50)

    for c in chunks:
        response = dynamo_client.batch_get_item(
            RequestItems={env_vars["SpanDynamoDBTableName"]: {"Keys": c}}
        )
        low_level_data = response["Responses"][
            env_vars["SpanDynamoDBTableName"]
        ]

        logger.info(
            "low level response from batch get spans : {}".format(
                low_level_data
            )
        )
        all_low_level_data.extend(low_level_data)

    all_spans_device_info = [
        dynamo_helper.deserializer_from_ddb(x, deserializer)
        for x in all_low_level_data
    ]

    for x in all_spans_device_info:
        x["spans"] = json.loads(x["spans"])
        x["spans"] = format_spans(x["spans"])
        x["spans"] = sort_data_by_date(x["spans"], "end_time")

    logger.debug("ALL span data : {}".format(pformat(all_spans_device_info)))
    logger.info("Getting spans for deviceIds from DAX...Done")

    return all_spans_device_info


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


def process_spans(all_spans, array_start_time, array_end_time):
    logger.info("Process function start time : {}".format(array_start_time))
    logger.info("Process function end time : {}".format(array_end_time))
    # logger.info("all spans provided are : {}".format(all_spans))

    if len(all_spans) == 0:
        newly_created_span = create_span(array_start_time[0], array_end_time[0])
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
            all_spans.append(newly_created_span)
            return all_spans, newly_created_span["spanId"], True

        elif (span_index is not None) and attrs_to_update == []:
            logger.info(
                "Span found. But data is already in the span. Not updating any time"
            )
            return all_spans, span_id, False

        elif (span_index is not None) and attrs_to_update != []:
            logger.info("Found a span. Updating {}".format(attrs_to_update))
            update_span(all_spans, span_index, attrs_to_update)
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

        try:
            decoded_rec = json.loads(
                b64decode(rec["kinesis"]["data"]).decode("utf-8")
            )
            print(decoded_rec)
            for d in decoded_rec["message"]["payload"]["context"]["tracking"]:
                d["deviceId"] = decoded_rec["message"]["from"]

            data = {
                "deviceId": decoded_rec["deviceId"],
                "data": decoded_rec["message"]["payload"]["context"]["tracking"],
            }
            all_records.append(data["data"])
            # all_records.append(decoded_rec)

            logger.debug("Decoded Record is : \n{}".format(pformat(decoded_rec)))
            logger.info("Len of decoded record is : {}".format(len(decoded_rec)))
        except Exception as e:
            logger.error('Unable to process message: {}'.format(e))

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


def convert_to_putItem_format(item):
    return {
        "PutRequest": {"Item": dynamo_helper.serialize_to_ddb(item, serializer)}
    }


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


def update_modified_device_spans_in_dynamo_using_ODM(device_spans_dict):
    logger.info("updating modified device spans in dynamo using ODM")

    # modified_devices_span_dict = [
    # x for x in device_spans_dict if "modified" in x
    # ]
    modified_devices_span_dict = device_spans_dict

    for m_dev in modified_devices_span_dict:
        # convert time to sstring
        for sp in m_dev["spans"]:
            sp["start_time"] = (sp["start_time"]).strftime(DATETIME_FORMAT)
            # sp["start_time"] = (sp["start_time"]).isoformat()

            # sp["end_time"] = (sp["end_time"]).isoformat()
            sp["end_time"] = (sp["end_time"]).strftime(DATETIME_FORMAT)
            # sp["start_time"] = datetime.datetime.strfmt(sp["start_time"], DATETIME_FORMAT)
            # sp["end_time"] = str(sp["end_time"])

        m_dev["spans"] = json.dumps(m_dev["spans"])

    # pprint(modified_devices_span_dict)
    logger.debug(
        "Converting to json modified device spans : {}".format(
            pformat(modified_devices_span_dict)
        )
    )

    spans_dict_as_OMD_spanmodel = []
    for x in modified_devices_span_dict:
        data_for_ODM = {"deviceId": x["deviceId"], "spans": x["spans"]}
        spans_dict_as_OMD_spanmodel.append(SpanModel(**data_for_ODM))
    logging.info(
        "Spans to update as ODM Models : \n{}".format(
            pformat(spans_dict_as_OMD_spanmodel)
        )
    )

    ddb.session.add_items(spans_dict_as_OMD_spanmodel)
    ddb.session.commit_items()

    # sess.add_items(spans_dict_as_OMD_spanmodel)
    # sess.commit_items()

    logger.info("updating modified device spans in dynamo using ODM...Done")


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

    logger.info("Len of Valid records are : {}".format(len(all_valid_records)))
    logger.debug("Valid records are : {}".format(all_valid_records))

    if len(all_valid_records) == 0:
        return "No records are valid"

    #################################
    # get all the unique device IDs #
    #################################
    unique_deviceIds = get_unique_device_ids_from_records(all_valid_records)

    ########################################
    # get spans for these unique deviceIds #
    ########################################
    device_spans_dict = get_spans_for_devices_from_DAX_batch_usingODM(
        unique_deviceIds
    )
    # print(device_spans_dict)

    ################
    # Tagged data ##
    ################
    tagged_data = []

    # For each record, find the span
    for rec_index, rec in enumerate(all_valid_records):
        logger.info("Processing record number : {}".format(rec_index))

        rec = format_data_pts_in_rec(rec)
        logger.debug("Current valid record is : {}".format(rec))

        current_rec_device_id = rec[0]["deviceId"]
        try:
            current_rec_device_id_spans = []
            for x in device_spans_dict:
                if x["deviceId"] == current_rec_device_id:
                    current_rec_device_id_spans = x["spans"]

        except Exception as e:
            logger.error("Error is : {}".format(e))
            current_rec_device_id_spans = []

        logger.info(
            "Current deviceId : {} and spans are : \n{}".format(
                current_rec_device_id, current_rec_device_id_spans
            )
        )

        #################
        # Find the span #
        #################
        array_end_time = rec[-1]["timeStamp"]
        array_start_time = rec[0]["timeStamp"]
        dt_array_end_time = rec[-1]["timeStamp"]
        dt_array_start_time = rec[0]["timeStamp"]

        logger.info(
            "Faulty times are : {}\t{}\t{}\t{}".format(
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
        )
        logger.info(
            "Things found after processing spans : {}\t{}\t{}".format(
                all_spans, spanId_for_tagging, modified
            )
        )
        logger.info("All spans after finding everything : {}".format(all_spans))

        ###################################
        # update the global list of spans #
        ###################################
        if modified:
            if device_spans_dict != []:
                for i in device_spans_dict:
                    if i["deviceId"] == current_rec_device_id_spans:
                        logger.info(
                            "Found the span - updating their all span and adding modified tag"
                        )
                        i["spans"] = all_spans
                        i["modified"] = 1
            else:
                device_spans_dict.append(
                    {
                        "deviceId": current_rec_device_id,
                        "spans": all_spans,
                        "modified": 1,
                    }
                )

        logger.info("modified Updated span dict : {}".format(device_spans_dict))

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

    # update_modified_device_spans_in_dynamo(device_spans_dict)
    update_modified_device_spans_in_dynamo_using_ODM(device_spans_dict)

    #########################################
    # Writing tagged data to kinesis stream #
    #########################################
    send_tagged_data_to_kinesis(tagged_data)

    logger.info("Starting...Done")

    return "Succesful"
