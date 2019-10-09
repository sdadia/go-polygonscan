import logging
import pandas as pd
from typing import List, Dict, Tuple
from pprint import pprint, pformat
import inspect
import functools
pd.set_option('display.width', 1000)
pd.set_option('float_format', '{:.2f}'.format)
pd.set_option('display.max_columns', 500)

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


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
    return pd.DataFrame.from_dict({"P": P, "T": T, "C": C})


def find_time_location(new_time: int, T:List):
    logger.debug("Function arguments are : \n{}".format(pformat(locals())))

    # if new time is greater than everything
    if new_time > T[-1]:
        logger.warning("Given time is greater than all timestamps")
        return (-1, "end")
    elif new_time < T[0]:
        logger.warning("Given time is less than all timestamps")
        return (0, "start")
    else:
        for index in range(len(T) - 1):
            if T[index] < new_time < T[index + 1]:
                logger.info(
                    "Found new time {} is between {} and {}".format(
                        new_time, T[index], T[index + 1]
                    )
                )
                return (index, "after")


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
        [ ("12:20", 1), ("12:23", 1), ("12:24", 0) .....]. Where the first part must be time.
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

    P = state_transition_dictionary["prev"]
    T = state_transition_dictionary["time"]
    C = state_transition_dictionary["curr"]

    for d in data:
        logger.debug("Current data point is : {}".format(d))
        new_point_ts = d[0]
        new_point_state = d[1]

        # first time entry
        if len(P) == 0:
            P.append(-1)
            T.append(new_point_ts)
            C.append(new_point_state)
            continue

        index, loc = find_time_location(new_point_ts, T)

        if loc == 'start':
            P.insert(0, -1)
            T.insert(0, new_point_ts)
            C.insert(0, new_point_state)
            
            # set the index+1 P equal to index C
            P[index+1] = C[index]


                

        logging.info(
            "PTC after update : \n{}".format(convert_PTC_to_df(P, T, C))
        )

    return {'prev': P, 'time': T, 'curr':C}
