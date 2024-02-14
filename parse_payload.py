import numpy as np
import pandas as pd
from datetime import datetime
from dateutil import parser as datetime_parser

from typing import List, Dict, Tuple, Union, Any

from HeaderData import SignalHeader, TimeInfo


def parse_payload(
        payload: List[Dict[str, List[List[Union[int, float]]]]],
        signals_header: Dict[str, List[SignalHeader]]
) -> Dict[str, pd.DataFrame]:
    data: Dict[str, List[Dict[str, Any]]] = dict()
    for msg in payload:
        for ky, val in msg.items():
            datapoints: List[Dict[str, Any]] = []

            if ky in signals_header:
                head = signals_header[ky]

                # loop through all entrys in this message
                for row in val:
                    assert len(row) == len(head)

                    datapoint = {info.name: info.dtype(el) for el, info in zip(row, head)}
                    datapoints.append(datapoint)
            elif ky in ["HFCallEvent", "HFBlockEvent", "HFTimestamp"]:
                # parse timestamp if exists
                if "Time" in val:
                    val["Time"] = datetime_parser.parse(val["Time"])
                datapoints.append(val)
            else:
                raise Exception(f"Unrecognized data key {ky} in payload.")

            # append signal
            if ky not in data:
                data[ky] = []
            data[ky] += datapoints

    # make DataFrame
    for ky, val in data.items():
        data[ky] = pd.DataFrame(val)
    return data


def to_unix_time(t: datetime):
    return int(t.timestamp() * 1e9)


def construct_time(data: Dict[str, pd.DataFrame], initial_time: TimeInfo = None):
    """
    sync time for HFData: CYCLE, HFCallEvent: HFProbeCounter, HFBlockEvent: HFProbeCounter, HFTimestamp: HFProbeCounter

    :param data:
    :return:
    """
    if "HFTimestamp" in data:
        mapping = {
            "HFCallEvent": "HFProbeCounter",
            "HFBlockEvent": "HFProbeCounter",
            "HFData": "CYCLE",
        }

        xp = data["HFTimestamp"]["HFProbeCounter"]
        yp = data["HFTimestamp"]["Time"].apply(lambda x: to_unix_time(x))

        if initial_time is not None:
            xp0 = initial_time.start_counter
            yp0 = initial_time.start_time

            xp = [xp0] + xp.to_list()
            yp = [to_unix_time(yp0)] + yp.to_list()

        for ky, val in mapping.items():
            if ky in data:
                x = data[ky][val]
                # (linear) interpolation

                y = np.interp(x, xp, yp)

                data[ky]["Time"] = pd.to_datetime(pd.Series(y, name="Time"))
    return data


if __name__ == "__main__":
    # TODO: sync data source
    pass



