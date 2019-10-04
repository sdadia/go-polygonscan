import logging
import datetime
import os
import sys
import unittest
import json
from pprint import pprint, pformat

logging.getLogger("speed_log").setLevel(logging.INFO)

log = logging.getLogger("test_speed_logger")
log.setLevel(logging.INFO)


# this contains the code for trip calculation
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

sys.path.append("/home/sahil/Documents/aggregation/aggregation_lib/")
try:
    log.warning("Running Locally so module imports are different")
    import aggregation_lib

except Exception as e:
    log.warning("Running on Lambda")
    import aggregation_lib

from Functions.SpeedAggregations.index import (
    handler,
    extract_data_from_kinesis,
    find_unique_span_timestamp,
    format_event_data,
    update_data_in_dynamo_using_ODM,
    DATETIME_FORMAT3,
)
from Functions import SpeedAggregations

pprint(logging.log)


class Test_speed_aggregation(unittest.TestCase):
    @unittest.SkipTest
    def test_handler(self):
        with open("./sample_speed_aggregation_input_event.json") as f:
            event = json.load(f)
        logging.debug("Event is :\n{}".format(pformat(event)))
        handler(event, None)

    def test_extract_data_from_kinesis(self):
        logging.info("Testing extract data from kinesis")
        with open("./sample_speed_aggregation_input_event.json") as f:
            event = json.load(f)
        log.debug("Event is :\n{}".format(pformat(event)))
        ans = extract_data_from_kinesis(event)

        expected_response = [
            {
                "GPSTime": "2019-05-22T10:45:05Z",
                "acc": "0",
                "alt": "41.00",
                "course": "63.60",
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                "geoid": "55.00",
                "io": "00000000",
                "lat": "5319.8250N",
                "lng": "622.34220W",
                "spanId": "123",
                "speed": "0.28",
                "status": "1",
                "timestamp": "2019-05-22 10:45:05.154000",
            },
            {
                "GPSTime": "2019-05-22T10:45:06Z",
                "acc": "0",
                "alt": "41.00",
                "course": "148.87",
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                "geoid": "55.00",
                "io": "00000000",
                "lat": "5319.8250N",
                "lng": "622.34210W",
                "spanId": "123",
                "speed": "0.83",
                "status": "1",
                "timestamp": "2019-05-22 10:45:06.154000",
            },
            {
                "GPSTime": "2019-05-22T10:45:07Z",
                "acc": "0",
                "alt": "41.00",
                "course": "137.71",
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                "geoid": "55.00",
                "io": "00000000",
                "lat": "5319.8249N",
                "lng": "622.34200W",
                "spanId": "123",
                "speed": "0.52",
                "status": "1",
                "timestamp": "2019-05-22 10:45:07.154000",
            },
        ]

        for e1, e2 in zip(ans, expected_response):
            self.assertCountEqual(e1, e2)

        logging.info("Testing extract data from kinesis...Done")

    def test_find_unique_span_timestamp(self):
        input_data = [
            {"spanId": "123", "timestamp": "2019-05-22 10:45:07.154000"},
            {"spanId": "123", "timestamp": "2019-05-22 10:45:07.154000"},
            {"spanId": "122", "timestamp": "2019-05-22 10:45:07.154000"},
        ]
        ans = find_unique_span_timestamp(input_data)

        expected_response = [
            ("123", "2019-05-22 10:45:07.154000"),
            ("122", "2019-05-22 10:45:07.154000"),
        ]

        for e1, e2 in zip(sorted(ans), sorted(expected_response)):
            self.assertEqual(e1, e2)

    def test_format_event_data(self):
        input_data = [
            {
                "spanId": "123",
                "timestamp": "2019-05-22 10:46:07.154000",
                "speed": 120,
            },
            {
                "spanId": "123",
                "timestamp": "2019-05-22 10:45:07.154000",
                "speed": 160,
            },
            {
                "spanId": "122",
                "timestamp": "2019-05-22 10:45:07.154000",
                "speed": 280,
            },
        ]

        expected_output = [
            {
                "timestamp": "2019-05-22 10:46:07.154000+00:00",
                "spanId_metricname": "123_speed",
                "speed": 120.0,
                "count": 1.0,
            },
            {
                "timestamp": "2019-05-22 10:45:07.154000+00:00",
                "spanId_metricname": "123_speed",
                "speed": 160.0,
                "count": 1.0,
            },
            {
                "timestamp": "2019-05-22 10:45:07.154000+00:00",
                "spanId_metricname": "122_speed",
                "speed": 280.0,
                "count": 1.0,
            },
        ]
        event_df = format_event_data(input_data)
        # print(event_df)

        for e1, e2 in zip(
            expected_output, list(event_df.to_dict(orient="index").values())
        ):
            # print(e1, e2)
            self.assertCountEqual(e1, e2)

    def test_update_data_in_dynamo_using_ODM(self):
        data = [
            {
                "timestamp": "2019-05-22 10:44:07",
                "spanId_metricname": "123_speed",
                "speed": 120.0,
                "count": 1.0,
            },
            {
                "timestamp": "2019-05-22 10:45:07",
                "spanId_metricname": "123_speed",
                "speed": 160.0,
                "count": 2.0,
            },
            {
                "timestamp": "2019-05-22 10:46:07",
                "spanId_metricname": "122_speed",
                "speed": 280.0,
                "count": 3.0,
            },
        ]

        expected_output = [
            {
                "timestamp": "2019-05-22 10:44:00",
                "spanId_metricname": "123_speed",
                "value": 120.0,
                "count": 1.0,
            },
            {
                "timestamp": "2019-05-22 10:45:00",
                "spanId_metricname": "123_speed",
                "value": 160.0,
                "count": 2.0,
            },
            {
                "timestamp": "2019-05-22 10:46:00",
                "spanId_metricname": "122_speed",
                "value": 280.0,
                "count": 3.0,
            },
        ]
        ans = update_data_in_dynamo_using_ODM(data)
        ans = [x.attribute_values for x in ans]
        for e1, e2 in zip(expected_output, ans):
            e2["timestamp"] = str(e2["timestamp"])
            self.assertEqual(e1, e2)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test_speed_aggregation))
    return suite
