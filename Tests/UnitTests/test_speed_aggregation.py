import logging
import os
import sys
import unittest
import json
from pprint import pprint, pformat

logging.getLogger('speed_log').setLevel(logging.INFO)

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
)
from Functions import SpeedAggregations

pprint(logging.log)


class Test_speed_aggregation(unittest.TestCase):

    def test_handler(self):
        with open("./sample_speed_aggregation_input_event2.json") as f:
            event = json.load(f)
        logging.debug("Event is :\n{}".format(pformat(event)))
        handler(event, None)

    def test_extract_data_from_kinesis(self):
        with open("./sample_speed_aggregation_input_event.json") as f:
            event = json.load(f)
        log.debug("Event is :\n{}".format(pformat(event)))
        ans = extract_data_from_kinesis(event)

        expected_response = [
            {
                "acc": 1,
                "course": "251.5",
                "deviceId": "12296",
                "gpstime": "2019-09-04 00:24:11",
                "io": 268602755,
                "latitude": "51.611922",
                "longitude": "-0.044040",
                "parkTime": 1442,
                "spanId": "be092656-fc46-417a-9074-47b5e6f57d77",
                "speed": 0,
                "status": "valid",
                "timestamp": "2019-09-04 00:24:11",
            },
            {
                "acc": 1,
                "course": "251.5",
                "deviceId": "12296",
                "gpstime": "2019-09-04 00:24:21",
                "io": 268602755,
                "latitude": "51.611995",
                "longitude": "-0.043943",
                "parkTime": 0,
                "spanId": "be092656-fc46-417a-9074-47b5e6f57d77",
                "speed": 40,
                "status": "valid",
                "timestamp": "2019-09-04 00:24:21",
            },
        ]

        for e1, e2 in zip(ans, expected_response):
            self.assertCountEqual(e1, e2)

    def test_find_unique_span_timestamp(self):
        input_data = [
            {"spanId": "123", "timestamp": "2019-09-04 00:24:11"},
            {"spanId": "123", "timestamp": "2019-09-04 00:24:21"},
            {"spanId": "122", "timestamp": "2019-09-04 00:25:41"},
        ]
        ans = find_unique_span_timestamp(input_data)

        expected_response = [
            ("123", "2019-09-04 00:24:00"),
            ("122", "2019-09-04 00:25:00"),
        ]

        for e1, e2 in zip(sorted(ans), sorted(expected_response)):
            self.assertEqual(e1, e2)

    def test_format_event_data(self):
        input_data = [
            {"spanId": "123", "timestamp": "2019-09-04 00:25:21", "speed": 120},
            {"spanId": "123", "timestamp": "2019-09-04 00:25:31", "speed": 160},
            {"spanId": "122", "timestamp": "2019-09-04 00:25:41", "speed": 280},
        ]

        expected_output = [
            {
                "timestamp": "2019-09-04 00:25:00+00:00",
                "spanId_metricname": "123_speed",
                "speed": 120.0,
                "count": 1.0,
            },
            {
                "timestamp": "2019-09-04 00:25:00+00:00",
                "spanId_metricname": "123_speed",
                "speed": 160.0,
                "count": 1.0,
            },
            {
                "timestamp": "2019-09-04 00:25:00+00:00",
                "spanId_metricname": "122_speed",
                "speed": 280.0,
                "count": 1.0,
            },
        ]
        event_df = format_event_data(input_data)

        for e1, e2 in zip(
            expected_output, list(event_df.to_dict(orient="index").values())
        ):
            # print(e1, e2)
            self.assertCountEqual(e1, e2)


if __name__ == "__main__":
    unittest.main()
