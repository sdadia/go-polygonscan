from pprint import pformat
import ciso8601
import datetime
import json
import logging
import mock
import pandas as pd

pd.set_option("display.max_rows", 500)
import pytz
import random
import sys
import time
import unittest

random.seed(1)
import os

os.environ["localhost"] = "True"
from pvapps_odm.Schema.models import StationaryIdlingModel

# Function import to test
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
from Functions.StationaryIdlingTimeAggregations.index import (
    verify_valid_state_values,
    update_state_transitions,
    find_time_location,
    convert_PTC_to_df,
    find_actual_time_from_state_transitons,
    update_state_transitions_using_TC,
    extract_data_from_kinesis,
    get_stationary_idling_state_transitions,
    handler,
    write_stationary_idling_state_transitions_to_dynamo,
)


logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))


def convert_date_to_timestamp_unix(data):
    ans = []

    for date, status in data:
        ans.append((string_time_to_unix_epoch(date), status))

    return ans


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


def convert_unix_epoch_to_ts(data):
    if isinstance(data, list):
        ans = []
        for date in data:
            # ans.append(datetime.datetime.strptime(date, DATE_FORMAT).timestamp())
            ans.append(datetime.datetime.utcfromtimestamp(date))

        return ans
    else:
        return datetime.datetime.utcfromtimestamp(data)


class TestStateTransitionFunction(unittest.TestCase):
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

    # class TestTransitions(unittest.TestCase):
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
    # print(data_1)

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

    expected_value_for_time_data_1 = 300  # seconds

    # data without post correction
    data_2 = [
        ("2019-06-26T12:10:36Z", 0),
        ("2019-06-26T12:11:36Z", 1),
        ("2019-06-26T12:12:36Z", 1),
        ("2019-06-26T12:13:36Z", 1),
        ("2019-06-26T12:14:36Z", 0),
        ("2019-06-26T12:15:36Z", 1),
        ("2019-06-26T12:16:36Z", 1),
        ("2019-06-26T12:17:36Z", 0),
        ("2019-06-26T12:18:36Z", 0),
    ]
    data_2 = convert_date_to_timestamp_unix(data_2)
    # print(data_1)

    expected_ans_data_2 = {
        "prev": [-1, 0, 1, 0, 1],
        "time": [
            "2019-06-26T12:10:36Z",
            "2019-06-26T12:11:36Z",
            "2019-06-26T12:14:36Z",
            "2019-06-26T12:15:36Z",
            "2019-06-26T12:17:36Z",
        ],
        "curr": [0, 1, 0, 1, 0],
    }
    expected_ans_data_2["time"] = string_time_to_unix_epoch(
        expected_ans_data_2["time"]
    )

    expected_value_for_time_data_2 = 300  # seconds

    # data

    def test_update_state_transitions_unequal_dictionary_len_raise_Exception(
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

    def test_find_actual_time_from_state_transitons(self):
        ans, post_correction_needed = find_actual_time_from_state_transitons(
            self.expected_ans_data_1
        )
        self.assertEqual(ans, self.expected_value_for_time_data_1)
        self.assertEqual(post_correction_needed, True)

        # try without post correction
        ans, post_correction_needed = find_actual_time_from_state_transitons(
            self.expected_ans_data_2
        )
        self.assertEqual(ans, self.expected_value_for_time_data_2)
        self.assertEqual(post_correction_needed, False)

    def test_update_state_transitions_using_TC_raises_exception(self):
        self.assertRaises(
            AssertionError,
            update_state_transitions_using_TC,
            self.data_1,
            state_transition_dictionary={"time": [], "curr": [1]},
        )

    def test_update_state_transitions_using_TC_forward(self):
        value = self.data_1[0:2]
        new_state_transition = update_state_transitions_using_TC(
            value, state_transition_dictionary={"time": [], "curr": []}
        )

        value = self.data_1.copy()
        random.shuffle(value)
        new_state_transition = update_state_transitions_using_TC(
            value, new_state_transition
        )
        self.assertEqual(
            new_state_transition["time"], self.expected_ans_data_1["time"]
        )
        self.assertEqual(
            new_state_transition["curr"], self.expected_ans_data_1["curr"]
        )


class TestStationaryIdlingTimeAggregations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        StationaryIdlingModel.create_table()
        time.sleep(1)

    # @classmethod
    # def tearDownClass(cls):
    # time.sleep(5)
    # StationaryIdlingModel.delete_table()

    # with open("sample_stationary_idling_time_input.json") as f:
    with open("sample_stationary_idling_time_input.json") as f:
        event = json.load(f)
    trip_end_time = event[-1]["timeStamp"]

    stationary_data = []
    idling_data = []
    for d in event:
        if d["gps"]["speed"] == 0:
            d["stationary"] = 1
            stationary_data.append((d["timeStamp"], 1))

            if d["acc"] == 1:
                idling_data.append((d["timeStamp"], 1))
            else:
                idling_data.append((d["timeStamp"], 0))
        elif d["gps"]["speed"] > 0:
            d["stationary"] = 0
            stationary_data.append((d["timeStamp"], 0))
            idling_data.append((d["timeStamp"], 0))

    # logger.debug("Event is : {}".format(pformat(event)))
    expected_idling_time_value = 115
    expected_stationary_time_value = 240

    def test_icar_dataset_stationary_time(self):
        data = convert_date_to_timestamp_unix(self.stationary_data)
        random.shuffle(data)

        # ans = update_state_transitions(
        # data, {"prev": [], "time": [], "curr": []}
        # )
        ans = update_state_transitions_using_TC(data, {"time": [], "curr": []})
        total_time, post_correction_needed = find_actual_time_from_state_transitons(
            ans
        )

        if post_correction_needed:
            total_time += (
                string_time_to_unix_epoch(self.trip_end_time) - ans["time"][-1]
            )
        self.assertEqual(total_time, self.expected_stationary_time_value)

    def test_icar_dataset_idling_time(self):
        data = convert_date_to_timestamp_unix(self.idling_data)
        random.shuffle(data)

        # ans = update_state_transitions(
        # data, {"prev": [], "time": [], "curr": []}
        # )
        ans = update_state_transitions_using_TC(data, {"time": [], "curr": []})
        total_time, post_correction_needed = find_actual_time_from_state_transitons(
            ans
        )
        # logger.info("Idling time before pre-correction : {}".format())

        if post_correction_needed:
            total_time += (
                string_time_to_unix_epoch(self.trip_end_time) - ans["time"][-1]
            )
        self.assertEqual(total_time, self.expected_idling_time_value)

    def test_write_stationary_idling_state_transitions_to_dynamo(self):
        deviceId = "123"
        expected_idling_state_transition = {"time": [123, 124], "curr": [1, 0]}
        expected_stationary_state_transition = {
            "time": [223, 224],
            "curr": [0, 1],
        }

        write_stationary_idling_state_transitions_to_dynamo(
            deviceId,
            expected_idling_state_transition,
            expected_stationary_state_transition,
        )

        idling_state, stationary_state = get_stationary_idling_state_transitions(
            deviceId
        )

        for k in list(expected_stationary_state_transition.keys()):
            self.assertTrue(
                expected_stationary_state_transition[k], stationary_state[k]
            )
            self.assertTrue(
                expected_idling_state_transition[k], idling_state[k]
            )

    # @unittest.SkipTest
    @mock.patch(
        "Functions.StationaryIdlingTimeAggregations.index.get_stationary_idling_state_transitions"
    )
    def test_handler_mock(self, mock_get_data_from_dynamo):
        with open("./sample_speed_aggregation_input_event.json") as f:
            event = json.load(f)

        mock_get_data_from_dynamo.return_value = (
            {"prev": [], "time": [], "curr": []},
            {"prev": [], "time": [], "curr": []},
        )
        result = handler(event, None)
        # print(result)

    def test_extract_data_from_kinesis(self):
        logger.info("Testing extract data from kinesis")
        with open("./sample_speed_aggregation_input_event.json") as f:
            event = json.load(f)
        logger.debug("My Event is :\n{}".format(pformat(event)))
        all_records = extract_data_from_kinesis(event)

        expected_all_records = {
            "9b59fd3e-17e0-11e9-ab14-d663bd873": {
                "idling_data": [
                    (1558521905.154, 0),
                    (1558521906.154, 0),
                    (1558521907.154, 0),
                ],
                "stationary_data": [
                    (1558521905.154, 0),
                    (1558521906.154, 0),
                    (1558521907.154, 0),
                ],
            }
        }

        self.assertEqual(
            list(expected_all_records.keys()), list(all_records.keys())
        )
        for key in expected_all_records:
            self.assertEqual(
                all_records[key]["idling_data"],
                expected_all_records[key]["idling_data"],
            )
            self.assertEqual(
                all_records[key]["stationary_data"],
                expected_all_records[key]["stationary_data"],
            )

        logger.info("Testing extract data from kinesis...Done")


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestStationaryIdlingTimeAggregations))
    suite.addTest(unittest.makeSuite(TestStateTransitionFunction))
    return suite
