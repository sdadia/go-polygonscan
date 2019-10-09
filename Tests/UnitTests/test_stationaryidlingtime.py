import json
import ciso8601
import random
import datetime
import logging
import os
import sys
import unittest
from pprint import pformat, pprint


sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
from Functions.StationaryIdlingTimeAggregations.index import (
    verify_valid_state_values,
    update_state_transitions,
    find_time_location,
)


logging.getLogger("Functions.StationaryIdlingTimeAggregations.index").setLevel(
    os.environ.get("LOG_LEVEL", logging.INFO)
)
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

import os

def convert_date_to_timestamp_unix(data):
    return [(ciso8601.parse_datetime(x[0]).timestamp(), x[1]) for x in data] 


class TestStationaryIdlingTimeAggregations(unittest.TestCase):
    data_1 = [
        ("2019-06-26 12:10:36", 0),
        ("2019-06-26 12:11:36", 1),
        ("2019-06-26 12:12:36", 1),
        ("2019-06-26 12:13:36", 1),
        ("2019-06-26 12:14:36", 0),
        ("2019-06-26 12:15:36", 1),
        ("2019-06-26 12:16:36", 1),
        ("2019-06-26 12:17:36", 0),
        ("2019-06-26 12:18:36", 0),
        ("2019-06-26 12:19:36", 0),
        ("2019-06-26 12:20:36", 1),
        ("2019-06-26 12:21:36", 1),
        ("2019-06-26 12:22:36", 1),
    ]
    data_1 = [(ciso8601.parse_datetime(x[0]).timestamp(), x[1]) for x in data_1]

    expected_ans_data_1 = {
        "prev": [-1, 0, 1, 0, 1, 0],
        "time": [
            "2019-06-26 12:10:36",
            "2019-06-26 12:11:36",
            "2019-06-26 12:14:36",
            "2019-06-26 12:15:36",
            "2019-06-26 12:17:36",
            "2019-06-26 12:20:36",
        ],
        "curr": [0, 1, 0, 1, 0, 1],
    }
    expected_ans_data_1["time"] = [
        ciso8601.parse_datetime(x).timestamp()
        for x in expected_ans_data_1["time"]
    ]

    def test_verify_valid_state_values_fail(self):
        input_state_values = [1, 2, 3, 4]
        valid_state_values = [-1, 1, 0]

        self.assertRaises(
            ValueError,
            verify_valid_state_values,
            input_state_values,
            valid_state_values,
        )

    def test_verify_valid_state_values_pass(self):
        input_state_values = [1, 1]
        valid_state_values = [-1, 1, 0]

        self.assertEqual(
            1, verify_valid_state_values(input_state_values, valid_state_values)
        )

    def test_update_state_transitions_unequal_state_transition_dictionary_len(
        self
    ):
        self.assertRaises(
            AssertionError,
            update_state_transitions,
            self.data_1,
            state_transition_dictionary={"prev": [], "time": [], "curr": [1]},
        )

    def test_update_state_transitions(self):
        value = self.data_1[0:2]
        value.reverse()
        print(value)
        new_state_transition = update_state_transitions(
              value,
            state_transition_dictionary={"prev": [], "time": [], "curr": []},
        )
        print(new_state_transition)

        self.assertEqual(new_state_transition['prev'], self.expected_ans_data_1['prev'][:2])

    def test_find_time_location(self):
        # test case 1 - time is in between
        T = self.expected_ans_data_1["time"]
        new_time = ciso8601.parse_datetime("2019-06-26 12:13:36").timestamp()
        index_1, loc = find_time_location(new_time, T)
        self.assertEqual(index_1, 1)
        self.assertEqual(loc, "after")


        # test case 2 - time before everything
        T = self.expected_ans_data_1["time"]
        new_time = ciso8601.parse_datetime("2019-06-26 12:09:36").timestamp()
        index_1, loc = find_time_location(new_time, T)
        self.assertEqual(index_1, 0)
        self.assertEqual(loc, "start")


        # test case 2 - time after everything
        T = self.expected_ans_data_1["time"]
        new_time = ciso8601.parse_datetime("2019-06-26 12:39:36").timestamp()
        index_1, loc = find_time_location(new_time, T)
        self.assertEqual(index_1, -1)
        self.assertEqual(loc, "end")


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestStationaryIdlingTimeAggregations))
    return suite
