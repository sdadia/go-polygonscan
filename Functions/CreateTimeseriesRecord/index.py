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

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

######################################################
##                                                  ##
##      Environment Variable Decryption/Decoding    ##
##                                                  ##
######################################################
env_vars = {}
envVarsList = ["SpanDynamoDBTableName", "DAXUrl"]
for var in envVarsList:
    if var in os.environ.keys():
        env_vars[var] = os.environ[var]

######################################################
##                                                  ##
##      Database Connection Initialisation          ##
##                                                  ##
######################################################
boto3.resource("dynamodb", region_name="us-east-1")
deserializer = boto3.dynamodb.types.TypeDeserializer()
serializer = boto3.dynamodb.types.TypeSerializer()

if "DAXUrl" in env_vars:
    logging.warn("Using Dynamo with DAX")
    session = botocore.session.get_session()
    dax = amazondax.AmazonDaxClient(
        session,
        region_name="us-east-1",
        endpoints=[
            "dan-test-cluster.sdpavt.clustercfg.dax.use1.cache.amazonaws.com:8111"
        ],
    )
    dynamo_client = dax
else:
    logging.warn("Using Dynamo without DAX")
    dynamo_client = boto3.client("dynamodb")

kinesis_client = boto3.client("kinesis")


DATETIME_FOMRAT = "%Y-%m-%d %H:%M:%S"


def generate_uuid():
    return str(uuid.uuid4())


def _grouper(iterable, n=25):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def get_spans_for_devices_from_DAX_batch(device_ids):
    """
    Gets the spans for list of deviceIds from DynamoDB DAX
    """
    logging.info("Getting spans for deviceIds from DAX")

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

        logging.info(
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

    logging.debug("ALL span data : {}".format(pformat(all_spans_device_info)))
    logging.info("Getting spans for deviceIds from DAX...Done")

    return all_spans_device_info


def format_spans(
    span_list, to_format_as_time=["start_time", "end_time"], as_datetime=True
):
    logging.info("Formatting span data")

    if as_datetime:
        for x in span_list:
            for t in to_format_as_time:
                x[t] = datetime.datetime.strptime(x[t], DATETIME_FOMRAT)
    else:
        for x in span_list:
            for t in to_format_as_time:
                x[t] = str(x[t])

    logging.info("Formatting span data...Done")
    return span_list


def format_data_pts_in_rec(
    span_list, to_format_as_time=["timeStamp"], as_datetime=True
):
    logging.info("Formatting record data")

    if as_datetime:
        for x in span_list:
            for t in to_format_as_time:
                x[t] = datetime.datetime.strptime(x[t], DATETIME_FOMRAT)
    else:
        for x in span_list:
            for t in to_format_as_time:
                x[t] = str(x[t])

    logging.info("Formatting record data...Done")
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
    logging.info("Finding relevant trip non trip data")

    trip = []

    for index, element in enumerate(telematics_data):
        # ignore acc Off and parking > 180 sec
        if element["parkTime"] > 180 and element["acc"] == 0:
            continue
        # ignore points with incorrect latitude and longitude
        elif (
            element["gps"]["latitude"] is None
            or element["gps"]["longitude"] is None
        ):
            continue
        else:
            trip.append(element)

    logging.info("Number of data points which are valid : {}".format(len(trip)))
    logging.info("Finding relevant trip non trip data...Done")

    return trip


def find_spans(start_time, end_time, all_spans):

    str_start_time = start_time[0]
    dt_start_time = start_time[1]
    str_end_time = end_time[0]
    dt_end_time = end_time[1]

    logging.info("{}".format((dt_start_time, type(dt_start_time))))
    logging.info("{}".format((dt_end_time, type(dt_end_time))))

    # 10 minutes
    acceptable_time_delta = 10 * 60
    for idx, span in enumerate(all_spans):
        # logging.info('Checking Span: {}'.format(span['spanId']))
        # logging.info(span['start_time'])
        # logging.info(span['end_time'])
        print("span finding logic - {}".format(span))
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

                    # logging.info('Case 1.1.1: MET')
                    pass

                else:
                    # Case 1.1.2 - In range right:
                    # Input:     ----
                    # Spans: ----

                    # Update End Time
                    # logging.info('Case 1.1.2: MET')
                    return (idx, span["spanId"], [("end_time", str_end_time)])

            else:
                # Case 1.2: Overlap right

                if dt_end_time > span_end:
                    # Case 1.2.1 - Overlap right:
                    # Input:   ----
                    # Spans: ----

                    # Update End Time
                    # logging.info('Case 1.2.1: MET')
                    return (idx, span["spanId"], [("end_time", str_end_time)])

                else:
                    # Case 1.2.2 - In between:
                    # Input:   ----
                    # Spans: --------

                    # logging.info('Case 1.2.2: MET')
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
                    # logging.info('Case 2.1.1: MET')
                    return (
                        idx,
                        span["spanId"],
                        [("start_time", str_start_time)],
                    )

                else:
                    # Case  2.1.2 - Too far left:
                    # Input: ----
                    # Spans:             ----

                    # logging.info('Case 2.1.2: MET')
                    pass

            else:
                # Case 2.2: Overlap Left

                if dt_end_time < span_end:
                    # Case 2.2.1 - Overlap left:
                    # Input: ----
                    # Spans:   ----

                    # Update Start Time
                    # logging.info('Case 2.2.1: MET')
                    return (idx, span["spanId"], ("start_time", str_start_time))

                else:
                    # Case 2.2.2 - Covering:
                    # Input: --------
                    # Spans:   ----

                    # Update Start Time
                    # logging.info('Case 2.2.2: MET')
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
    logging.info("Creating new span")

    new_span_uuid = generate_uuid()

    new_span = {
        "start_time": start_time,
        # "start_time": str(start_time),
        "end_time": end_time,
        # "end_time": str(end_time),
        "spanId": new_span_uuid,
    }

    logging.info("Newly Created span : {}".format(new_span))
    logging.info("Creating new span...Done")
    # spans.append(new_span)

    return new_span


def update_span(spans, span_index, timestamps):

    format2 = "%Y-%m-%dT%H:%M:%SZ"

    if span_index is not None:

        for t in timestamps:
            spans[span_index][t[0]] = t[1]

    return spans


def process_spans(all_spans, array_start_time, array_end_time):
    logging.info("Process function start time : {}".format(array_start_time))
    logging.info("Process function end time : {}".format(array_end_time))

    if len(all_spans) == 0:
        newly_created_span = create_span(array_start_time[0], array_end_time[0])
        all_spans.append(newly_created_span)
        logging.warn("Creating span for first time ever: {}".format(all_spans))
        return all_spans, newly_created_span["spanId"], True

    else:
        # Parse Device Spans for Appropriate Spans to Update
        span_index, span_id, attrs_to_update = find_spans(
            array_start_time, array_end_time, all_spans
        )

        print(span_index, span_id, attrs_to_update)

        if span_index is None:
            logging.warn("No span Index found, so creating a new span")
            newly_created_span = create_span(
                array_start_time[0], array_end_time[0]
            )
            all_spans.append(newly_created_span)
            return all_spans, newly_created_span["spanId"], True

        elif (span_index is not None) and attrs_to_update == []:
            logging.info(
                "Span found. But data is already in the span. Not updating any time"
            )
            return all_spans, span_id, False

        elif (span_index is not None) and attrs_to_update != []:
            logging.info("Found a span. Updating {}".format(attrs_to_update))
            update_span(all_spans, span_index, attrs_to_update)
            return all_spans, span_id, True

        # # If a span has been found, an index will be returned
        # if span_index is not None and attrs_to_update != []:

        # logging.info("Found Span: {}".format(span_id))

        # for attr in attrs_to_update:
        # logging.info("Update {} to {}".format(attr[0], attr[1]))
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
        # logging.warn("Span is existing, but no values to update ")

        # # infill
        # return all_spans, span_id, False


def get_all_records_in_event(event):

    logging.info("Getting all the records from event")

    all_records = []  # holds all records
    for rec in event["Records"]:

        decoded_rec = json.loads(
            b64decode(rec["kinesis"]["data"]).decode("utf-8")
        )
        all_records.append(decoded_rec)

        logging.debug("Decoded record is : {}".format(decoded_rec))
        logging.info("Len of decoded record is : {}".format(len(decoded_rec)))

    logging.info("Getting all the records from event...Done")

    return all_records


def get_unique_device_ids_from_records(all_records):

    logging.info("Finding unique device Ids from all records")

    unique_deviceIds_list = list(
        set([rec[0]["deviceId"] for rec in all_records])
    )
    unique_deviceIds_list = [
        {"deviceId": str(x)} for x in unique_deviceIds_list
    ]
    logging.info("Unqiue device Ids : {}".format(unique_deviceIds_list))

    logging.info("Finding unique device Ids from all records...Done")

    return unique_deviceIds_list


def tag_data(rec, spanId):
    logging.info("Tagging {} points ".format(len(rec)))

    spanId = str(spanId)

    for r in rec:
        logging.info("Record to tag : {}".format(r))
        r["spanId"] = spanId

    logging.info("Tagging...Done")
    return r


def convert_to_putItem_format(item):
    return {
        "PutRequest": {"Item": dynamo_helper.serialize_to_ddb(item, serializer)}
    }


def update_modified_device_spans_in_dynamo(device_spans_dict):
    logging.info("updating modified device spans in dynamo")

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

    logging.info(
        "Converting to json modified device spans : {}".format(
            modified_devices_span_dict
        )
    )

    serialized_modified_devices_span_dict = []
    for m in modified_devices_span_dict:
        serialized_modified_devices_span_dict.append(
            convert_to_putItem_format(m)
        )

    logging.info(
        "Serialized modified device spans : {}".format(
            serialized_modified_devices_span_dict
        )
    )

    chunks = _grouper(serialized_modified_devices_span_dict, 50)

    for c in chunks:
        response = dynamo_client.batch_write_item(
            RequestItems={env_vars["SpanDynamoDBTableName"]: c}
        )

        logging.info(
            "Response from updating spans to daynamo ; {}".format(response)
        )

    logging.info("updating modified device spans in dynamo...Done")


def send_tagged_data_to_kinesis(tagged_data):
    logging.info("Sending tagged data to kinesis")

    # convert to kinesis record format!
    tagged_data_as_kinesis_record_format = []
    for t in tagged_data:
        tagged_data_as_kinesis_record_format.append(
            {"Data": json.dumps(t), "PartitionKey": str(t["spanId"])}
        )
        logging.info(
            "Tagged data as kinesis format : {}".format(
                tagged_data_as_kinesis_record_format
            )
        )

    # send the tagged data
    chunks = _grouper(tagged_data_as_kinesis_record_format, 25)
    for c in chunks:
        response = kinesis_client.put_records(
            Records=c, StreamName=os.getenv('TaggedDataStream','NA')
        )
        logging.info(
            "Response from Outputing to Kinesis Stream : {}".format(response)
        )

    logging.info("Sending tagged data to kinesis...Done")


def handler(event, context):
    logging.info("Starting handler")

    logging.debug("Event is : {}".format(event))

    ##############################
    # Get all records from event #
    ##############################
    all_records = get_all_records_in_event(event)

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

    logging.info("Len of Valid records are : {}".format(len(all_valid_records)))
    logging.info("Valid records are : {}".format(all_valid_records))

    if len(all_valid_records) == 0:
        return "No records are valid"

    #################################
    # get all the unique device IDs #
    #################################
    unique_deviceIds = get_unique_device_ids_from_records(all_valid_records)

    ########################################
    # get spans for these unique deviceIds #
    ########################################
    device_spans_dict = get_spans_for_devices_from_DAX_batch(unique_deviceIds)

    ################
    # Tagged data ##
    ################
    tagged_data = []

    # For each record, find the span
    for rec_index, rec in enumerate(all_valid_records):
        logging.info("Processing record number : {}".format(rec_index))

        rec = format_data_pts_in_rec(rec)
        logging.info("Current valid record is : {}".format(rec))

        current_rec_device_id = rec[0]["deviceId"]
        try:
            current_rec_device_id_spans = []
            for x in device_spans_dict:
                if x["deviceId"] == current_rec_device_id:
                    current_rec_device_id_spans = x["spans"]

        except Exception as e:
            logging.error("Error is : {}".format(e))
            current_rec_device_id_spans = []

        logging.info(
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

        logging.info(
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
        logging.info(
            "Things found after processing spans : {}\t{}\t{}".format(
                all_spans, spanId_for_tagging, modified
            )
        )
        logging.info(
            "All spans after finding everything : {}".format(all_spans)
        )

        ###################################
        # update the global list of spans #
        ###################################
        if modified:
            if device_spans_dict != []:
                for i in device_spans_dict:
                    if i["deviceId"] == current_rec_device_id_spans:
                        logging.info(
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

        logging.info(
            "modified Updated span dict : {}".format(device_spans_dict)
        )

        ##############################################
        # Tag all valid points in the current record #
        ##############################################
        for r in rec:
            r["spanId"] = spanId_for_tagging
            r["timestamp"] = str(r["timeStamp"])
            del r["timeStamp"]
            tagged_data.append(r)

        logging.info(
            "Len All tagged data after tagging: {}".format(len(tagged_data))
        )
        logging.info("All tagged data after tagging: {}".format(tagged_data))

    ###################################################
    # batch update the devices whose spans we updated #
    ###################################################

    update_modified_device_spans_in_dynamo(device_spans_dict)

    #########################################
    # Writing tagged data to kinesis stream #
    #########################################
    send_tagged_data_to_kinesis(tagged_data)

    logging.info("Starting...Done")

    return "Succesful"
