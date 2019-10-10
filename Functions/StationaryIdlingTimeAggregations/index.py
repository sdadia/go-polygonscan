import logging
from collections import deque
import pandas as pd
from typing import List, Dict, Tuple
from pprint import pformat
import datetime

pd.set_option("float_format", "{:.2f}".format)

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def convert_unix_epoch_to_ts(data):
    if isinstance(data, list):
        ans = []
        for date in data:
            # ans.append(datetime.datetime.strptime(date, DATE_FORMAT).timestamp())
            ans.append(datetime.datetime.utcfromtimestamp(date))

        return ans
    else:
        return datetime.datetime.utcfromtimestamp(data)


def verify_valid_state_values(
    state_value_list: List, valid_state_values: List = [0, 1, -1]
):
    invalid_state_values = list(set(state_value_list) - set(valid_state_values))
    logger.debug("Invalid state values are : {}".format(invalid_state_values))

    # if invalid state values are present, then raise exception
    if len(invalid_state_values) > 0:
        logger.error(
            "State list contain the following invalid state values : {}".format(
                invalid_state_values
            )
        )
        raise ValueError("State list contains invalid values")
    else:
        return 1


def convert_PTC_to_df(P, T, C):
    df = pd.DataFrame.from_dict({"P": P, "T": T, "C": C})
    df["T"] = pd.to_datetime(df["T"], unit="s")
    return df


def find_time_location(new_time: int, T: List):
    # logger.debug("Function arguments are : \n{}".format(pformat(locals())))

    if new_time > T[-1]:
        logger.warning("Given time is greater than all timestamps")
        return (-1, "end")
    elif new_time == T[-1]:
        logger.warning("Repeated timestamp : {}".format(new_time))
        return (None, "repeat")
    elif new_time < T[0]:
        logger.warning("Given time is less than all timestamps")
        return (0, "start")
    elif new_time == T[0]:
        logger.warning("Repeated timestamp : {}".format(new_time))
        return (None, "repeat")
    else:
        for index in range(0, len(T) - 1):
            if new_time == T[index]:
                logger.warning("Repeated timestamp : {}".format(new_time))
                return (None, "repeat")
            elif T[index] < new_time < T[index + 1]:
                logger.info(
                    "Found new time {} is between {} and {}".format(
                        new_time, T[index], T[index + 1]
                    )
                )
                return (index, "between")


def clean_up(T, C):
    """
    Specified the index to remove
    """
    index_to_remove = []
    for index in range(len(T) - 1):
        if (C[index] == C[index + 1]) and ((T[index + 1] - T[index]) <= 60):
            index_to_remove.append(index + 1)

    logger.warning(
        "Going to remove following index from PTC : {}".format(index_to_remove)
    )
    return sorted(index_to_remove, reverse=True)


def update_state_transitions(
    data: List[Tuple], state_transition_dictionary: Dict
):
    """
    This function updates state transition. The state transition list must look like this.

    NOTE : The timestamps must be of python datetime type

    Parameters
    ----------
    data : list of tuples
        The data is the states we want to find the transitions for. The data must look like this
        [ ("12:20", 1), ("12:23", 1), ("12:24", 0) .....]. Where the first part must be in UNIX TIME.
        Second part must be the state value. Valid state values are 0, 1, "-"
    state_transition_dictionary : dictionary with the following format
        {
         'prev' : [-, F, F, 0, F, 0],
         'curr' : [-, F, F, 0, F, 0],
         'Time' : ["12:30", "12:45", "12:37", "12:30", "12:45", "12:37"]
        }

        Prev_state   Time   Curr_state
        -           10:12   1 (F)
        F           10:14   0
    """

    logger.debug("Function arguments are : \n{}".format(pformat(locals())))

    verify_valid_state_values(state_transition_dictionary["prev"])
    verify_valid_state_values(state_transition_dictionary["curr"])

    # check of the lenght of the state transition list, if all are not equal, raise exception
    assert (
        len(state_transition_dictionary["curr"])
        == len(state_transition_dictionary["prev"])
        == len(state_transition_dictionary["time"])
    ), logger.error(
        "Length of the elements in the state transition dictionary are not equal"
    )

    P = deque(state_transition_dictionary["prev"])
    T = deque(state_transition_dictionary["time"])
    C = deque(state_transition_dictionary["curr"])

    for d in data:
        logger.debug(
            "Current data point is : {}".format(
                (pd.to_datetime(d[0], unit="s"), d[1])
            )
        )
        new_point_ts = d[0]
        new_point_state = d[1]

        # first time entry - always add the point
        if len(P) == 0:
            logger.warning("Creating first time entry to PTC")
            P.append(-1)
            T.append(new_point_ts)
            C.append(new_point_state)

            logger.debug(
                "PTC after update : \n{}".format(convert_PTC_to_df(P, T, C))
            )
            continue

        # if not first time entry find the location of the point
        index, loc = find_time_location(new_point_ts, T)
        # print(index, loc)

        # start means before every point
        # P   T       C
        # -1  10:00   F        <- new point inserted
        # F   10:20   0
        if loc == "start":
            # print("updating start")
            P.insert(0, -1)
            T.insert(0, new_point_ts)
            C.insert(0, new_point_state)

            # set the index+1 P equal to index C
            P[index + 1] = C[index]

        # P   T       C
        # -1  10:00   F
        # F   10:20   0        <- new point inserted
        elif loc == "end":
            # print("updating end")
            P.append(-1)
            T.append(new_point_ts)
            C.append(new_point_state)

            # set the index P equal to index-1 C
            P[-1] = C[-2]

        # P   T       C
        # -1  10:18   F
        # F   10:20   0        <- new point inserted
        # 0   10:22   0
        elif loc == "between":
            # print("updating between")
            P.insert(index + 1, -1)
            T.insert(index + 1, new_point_ts)
            C.insert(index + 1, new_point_state)
            # print([pd.to_datetime(x, unit='s') for x in (T[index], T[index+1], T[index+2] )])

            # set the P value of record we just added to that of the above
            P[index + 1] = C[index]

            # Set the P value of the record after the record  we just added to be equal to
            # the C value of the record we just added
            P[index + 2] = C[index + 1]

        # P   T       C
        # -1  10:00   F
        # F   10:20   0        <- same point repeated then just forget about it!
        elif loc == "repeat":
            continue

        logger.debug(
            "PTC after update : \n{}".format(convert_PTC_to_df(P, T, C))
        )

    # remove the following index
    index_to_remove = clean_up(T, C)
    for idx in index_to_remove:
        del P[idx]
        del T[idx]
        del C[idx]
    logger.debug("PTC after cleanup : \n{}".format(convert_PTC_to_df(P, T, C)))

    return {"prev": list(P), "time": list(T), "curr": list(C)}


def find_actual_time_from_state_transitons(state_transition_dictionary):
    C = state_transition_dictionary["curr"]
    T = state_transition_dictionary["time"]
    started = False
    time_1 = None
    time_2 = None
    total_time = 0.0
    for ts, status in zip(T, C):

        # print(convert_unix_epoch_to_ts(ts), status)
        if (status == 1) and (not started):
            time_1 = ts
            started = True
        elif (status == 0) and (started):
            time_2 = ts
            started = False

            total_time += time_2 - time_1
            time_1 = time_2 = None

        # print(ts, status, (time_1), (time_2), total_time)
    return total_time
