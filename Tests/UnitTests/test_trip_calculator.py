import mock
import logging
import os
import sys
import unittest
from pprint import pprint, pformat
import json

# this contains the code for trip calculation
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

logging.getLogger("m2").setLevel(logging.CRITICAL)


# mock environment variables
class Test_trip_calculator_pandas(unittest.TestCase):

    data = [
        {
            "spanId": "-1",
            "start_time": "2019-11-26 06:00:00",
            "end_time": "2019-11-26 07:12:00",
        },
        {
            "spanId": "1",
            "start_time": "2019-11-26 08:14:00",
            "end_time": "2019-11-26 08:20:00",
        },
        {
            "spanId": "3",
            "start_time": "2019-11-26 08:48:00",
            "end_time": "2019-11-26 08:58:00",
        },
        {
            "spanId": "0",
            "start_time": "2019-11-26 08:00:00",
            "end_time": "2019-11-26 08:12:00",
        },
        {
            "spanId": "4",
            "start_time": "2019-11-26 09:04:00",
            "end_time": "2019-11-26 09:20:00",
        },
        {
            "spanId": "5",
            "start_time": "2019-11-26 10:00:00",
            "end_time": "2019-11-26 10:20:00",
        },
        {
            "spanId": "6",
            "start_time": "2019-11-26 11:00:00",
            "end_time": "2019-11-26 11:20:00",
        },
        {
            "spanId": "7",
            "start_time": "2019-11-26 13:00:00",
            "end_time": "2019-11-26 14:20:00",
        },
        {
            "spanId": "9",
            "start_time": "2019-11-26 20:00:00",
            "end_time": "2019-11-26 22:20:00",
        },
        {
            "spanId": "8",
            "start_time": "2019-11-26 16:00:00",
            "end_time": "2019-11-26 16:20:00",
        },
        {
            "spanId": "10",
            "start_time": "2019-11-26 22:22:00",
            "end_time": "2019-11-26 23:20:00",
        },
        {
            "spanId": "2",
            "start_time": "2019-11-26 08:43:00",
            "end_time": "2019-11-26 08:46:00",
        },
    ]

    def test_default_settings(self):
        """
        Check if we get the expected output in the trip dictionary
        """
        from Functions.CalculateTrips.index import get_trips_pandas

        expected_output = {
            1: {
                "end_time": "2019-11-26 07:12:00",
                "spanId": ["-1"],
                "start_time": "2019-11-26 06:00:00",
            },
            2: {
                "end_time": "2019-11-26 08:20:00",
                "spanId": ["0", "1"],
                "start_time": "2019-11-26 08:00:00",
            },
            3: {
                "end_time": "2019-11-26 09:20:00",
                "spanId": ["2", "3", "4"],
                "start_time": "2019-11-26 08:43:00",
            },
            4: {
                "end_time": "2019-11-26 10:20:00",
                "spanId": ["5"],
                "start_time": "2019-11-26 10:00:00",
            },
            5: {
                "end_time": "2019-11-26 11:20:00",
                "spanId": ["6"],
                "start_time": "2019-11-26 11:00:00",
            },
            6: {
                "end_time": "2019-11-26 14:20:00",
                "spanId": ["7"],
                "start_time": "2019-11-26 13:00:00",
            },
            7: {
                "end_time": "2019-11-26 16:20:00",
                "spanId": ["8"],
                "start_time": "2019-11-26 16:00:00",
            },
            8: {
                "end_time": "2019-11-26 23:20:00",
                "spanId": ["9", "10"],
                "start_time": "2019-11-26 20:00:00",
            },
        }

        ans = get_trips_pandas(
            self.data,
            user_start_time="2019-11-26 00:00:00",
            user_end_time="2019-11-26 23:59:59",
            time_diff_between_spans=15,
        )

        self.assertDictEqual(ans, expected_output)

    def test_different_time_difference_between_spans(self):
        from Functions.CalculateTrips.index import get_trips_pandas

        logging.info("Testing Different Trip Seperation Time")

        time_diff = 30  # minutes
        ans = get_trips_pandas(
            self.data,
            user_start_time="2019-11-26 00:00:00",
            user_end_time="2019-11-26 23:59:59",
            time_diff_between_spans=time_diff,
        )

        expected_output = {
            1: {
                "end_time": "2019-11-26 07:12:00",
                "spanId": ["-1"],
                "start_time": "2019-11-26 06:00:00",
            },
            2: {
                "end_time": "2019-11-26 09:20:00",
                "spanId": ["0", "1", "2", "3", "4"],
                "start_time": "2019-11-26 08:00:00",
            },
            3: {
                "end_time": "2019-11-26 10:20:00",
                "spanId": ["5"],
                "start_time": "2019-11-26 10:00:00",
            },
            4: {
                "end_time": "2019-11-26 11:20:00",
                "spanId": ["6"],
                "start_time": "2019-11-26 11:00:00",
            },
            5: {
                "end_time": "2019-11-26 14:20:00",
                "spanId": ["7"],
                "start_time": "2019-11-26 13:00:00",
            },
            6: {
                "end_time": "2019-11-26 16:20:00",
                "spanId": ["8"],
                "start_time": "2019-11-26 16:00:00",
            },
            7: {
                "end_time": "2019-11-26 23:20:00",
                "spanId": ["9", "10"],
                "start_time": "2019-11-26 20:00:00",
            },
        }

        self.assertLessEqual(len(ans), len(self.data))
        self.assertDictEqual(ans, expected_output)

    def test_handler(self):

        from Functions.CalculateTrips.index import handler

        with open("./sample_trip_calculation_input.json") as f:
            event = json.load(f)

        trips = handler(event, None)
        pprint(trips)


if __name__ == "__main__":
    unittest.main()
