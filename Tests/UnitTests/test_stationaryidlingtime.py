import ciso8601
import random

random.seed(1)
import logging
import os
import sys
import unittest


# Function import to test
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


def convert_date_to_timestamp_unix(data):
    ans = []

    for date, status in data:
        print(date, status)
        ans.append(
            # (datetime.datetime.strptime(date, DATE_FORMAT).timestamp(), status)
            (string_time_to_unix_epoch(date) , status)
        )

    return ans


def string_time_to_unix_epoch(data):
    if isinstance(data, list):
        ans = []
        for date in data:
            # ans.append(datetime.datetime.strptime(date, DATE_FORMAT).timestamp())
            ans.append(ciso8601.parse_datetime(date).timestamp())

        return ans
    else:
        return ciso8601.parse_datetime(data).timestamp()


class TestStationaryIdlingTimeAggregations(unittest.TestCase):
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


class TestTransitions(unittest.TestCase):
    data_1 = [
        ("2019-06-26T12:10:36Z", 0),
        ("2019-06-26T12:11:36Z", 1),
        ("2019-06-26T12:12:36Z", 1),
        ("2019-06-26T12:13:36Z", 1),
        ("2019-06-26T12:14:36Z", 0),
        ("2019-06-26T12:15:36Z", 1),
        ("2019-06-26T12:16:36Z", 1),
        ("2019-06-26T12:17:36Z", 0),
        ("2019-06-26T12:18:36Z", 0),
        ("2019-06-26T12:19:36Z", 0),
        ("2019-06-26T12:20:36Z", 1),
        ("2019-06-26T12:21:36Z", 1),
        ("2019-06-26T12:22:36Z", 1),
    ]
    data_1 = convert_date_to_timestamp_unix(data_1)
    print(data_1)

    expected_ans_data_1 = {
        "prev": [-1, 0, 1, 0, 1, 0],
        "time": [
            "2019-06-26T12:10:36Z",
            "2019-06-26T12:11:36Z",
            "2019-06-26T12:14:36Z",
            "2019-06-26T12:15:36Z",
            "2019-06-26T12:17:36Z",
            "2019-06-26T12:20:36Z",
        ],
        "curr": [0, 1, 0, 1, 0, 1],
    }
    expected_ans_data_1["time"] = string_time_to_unix_epoch(
        expected_ans_data_1["time"]
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

    def test_update_state_transitions_data_in_reverse(self):
        # some data
        value = self.data_1[0:2]
        value.reverse()
        new_state_transition = update_state_transitions(
            value,
            state_transition_dictionary={"prev": [], "time": [], "curr": []},
        )
        self.assertEqual(
            new_state_transition["prev"], self.expected_ans_data_1["prev"][:2]
        )
        self.assertEqual(
            new_state_transition["time"], self.expected_ans_data_1["time"][:2]
        )
        self.assertEqual(
            new_state_transition["curr"], self.expected_ans_data_1["curr"][:2]
        )

        # all data
        value = self.data_1.copy()
        value.reverse()
        new_state_transition = update_state_transitions(
            value, new_state_transition
        )
        self.assertEqual(
            new_state_transition["prev"], self.expected_ans_data_1["prev"]
        )
        self.assertEqual(
            new_state_transition["time"], self.expected_ans_data_1["time"]
        )
        self.assertEqual(
            new_state_transition["curr"], self.expected_ans_data_1["curr"]
        )

    def test_update_state_transitions_data_in_forward(self):
        value = self.data_1[0:2]
        new_state_transition = update_state_transitions(
            value,
            state_transition_dictionary={"prev": [], "time": [], "curr": []},
        )
        # some data
        value = self.data_1.copy()
        new_state_transition = update_state_transitions(
            value,
            # state_transition_dictionary={"prev": [], "time": [], "curr": []},
            new_state_transition,
        )
        self.assertEqual(
            new_state_transition["prev"], self.expected_ans_data_1["prev"]
        )
        self.assertEqual(
            new_state_transition["time"], self.expected_ans_data_1["time"]
        )
        self.assertEqual(
            new_state_transition["curr"], self.expected_ans_data_1["curr"]
        )

    def test_update_state_transitions_data_shuffle(self):
        value = self.data_1[0:2]
        new_state_transition = update_state_transitions(
            value,
            state_transition_dictionary={"prev": [], "time": [], "curr": []},
        )

        value = self.data_1.copy()
        random.shuffle(value)
        new_state_transition = update_state_transitions(
            value,
            state_transition_dictionary={"prev": [], "time": [], "curr": []},
        )
        self.assertEqual(
            new_state_transition["prev"], self.expected_ans_data_1["prev"]
        )
        self.assertEqual(
            new_state_transition["time"], self.expected_ans_data_1["time"]
        )
        self.assertEqual(
            new_state_transition["curr"], self.expected_ans_data_1["curr"]
        )

    def test_find_time_location(self):
        # test case 1 - time is in between
        T = self.expected_ans_data_1["time"]
        # new_time = ciso8601.parse_datetime("2019-06-26T12:13:06Z").timestamp()
        new_time = string_time_to_unix_epoch("2019-06-26T12:13:06Z")
        index_1, loc = find_time_location(new_time, T)
        self.assertEqual(index_1, 1)
        self.assertEqual(loc, "between")

        # test case 2 - time before everything
        T = self.expected_ans_data_1["time"]
        # new_time = ciso8601.parse_datetime("2019-06-26T12:09:36Z").timestamp()
        new_time = string_time_to_unix_epoch("2019-06-26T12:09:36Z")
        index_1, loc = find_time_location(new_time, T)
        self.assertEqual(index_1, 0)
        self.assertEqual(loc, "start")

        # test case 2 - time after everything
        T = self.expected_ans_data_1["time"]
        # new_time = ciso8601.parse_datetime("2019-06-26T12:39:36Z").timestamp()
        new_time = string_time_to_unix_epoch("2019-06-26T12:39:36Z")
        index_1, loc = find_time_location(new_time, T)
        self.assertEqual(index_1, -1)
        self.assertEqual(loc, "end")

        # test case 4 - time reprated
        T = self.expected_ans_data_1["time"]
        new_time = T[2]
        index_1, loc = find_time_location(new_time, T)
        self.assertEqual(index_1, None)
        self.assertEqual(loc, "repeat")


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestStationaryIdlingTimeAggregations))
    suite.addTest(unittest.makeSuite(TestTransitions))
    return suite
