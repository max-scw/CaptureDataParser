import numpy as np
import pandas as pd
from datetime import datetime
from dateutil import parser as datetime_parser
from dateutil.tz import tz

from typing import List, Dict, Tuple, Union, Any

from CaptureDataParser.HeaderData import SignalHeaderHF, SignalHeaderLF, TimeInfo
from CaptureDataParser.utils import cast_dtype, rename_signal, interplin


def parse_payload(
        payload: List[Dict[str, List[List[Union[int, float]]]]],
        signals_header: Dict[str, List[SignalHeaderHF | SignalHeaderLF]],
        rename_hfdata: bool = False,
        components: List[str] = None
) -> Dict[str, pd.DataFrame]:
    data: Dict[str, List[Dict[str, Any]]] = dict()
    for msg in payload:
        for ky, val in msg.items():
            datapoints: List[Dict[str, Any]] = []

            if (components is not None) and (ky not in components):
                # shortcut
                continue

            if ky in signals_header:
                head = signals_header[ky]

                # loop through all entrys in this message
                for row in val:
                    # assert len(row) == len(head)

                    if ky == "LFData":
                        assert all(el in row for el in ["address", "value", "value_type", "timestamp"])
                        # get correct header
                        for hd in head:
                            if hd.address == row["address"]:
                                break
                        try:
                            # casting may fail if json-message is brocken
                            value = cast_dtype(row["value_type"])(row["value"])

                            # standard message contents
                            time = row["timestamp"]
                            datapoint = {
                                hd.address: value,
                                "Time": datetime_parser.parse(time) if isinstance(time, str) else time,
                            }
                        except:
                            datapoint = None

                        # add hf probe counter if available
                        if (datapoint is not None) and ("HFProbeCounter" in row):
                            datapoint["HFProbeCounter"] = row["HFProbeCounter"]
                    elif ky == "HFData" and rename_hfdata:
                        datapoint = {rename_signal(hd): hd.dtype(el) for el, hd in zip(row, head)}
                    else:  # "HFData"
                        datapoint = {hd.name: hd.dtype(el) for el, hd in zip(row, head)}

                    if datapoint is not None:
                        datapoints.append(datapoint)
            elif ky in ["HFCallEvent", "HFBlockEvent", "HFTimestamp"]:
                # parse timestamp if exists
                if ("Time" in val) and isinstance(val["Time"], str):
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
    :param initial_time:
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
        # keep info on time zone localization
        tzinfo = data["HFTimestamp"]["Time"][0].tzinfo

        # TODO: add plausibility check with initial_time.hf_cycle_time

        if initial_time is not None:
            xp0 = initial_time.start_counter
            yp0 = initial_time.start_time

            xp = xp.to_list()
            yp = yp.to_list()

            if xp0 != xp[0]:
                xp = [xp0] + xp
                yp = [to_unix_time(yp0)] + yp

        for ky, val in mapping.items():
            if ky in data:
                x = data[ky][val]
                # (linear) interpolation
                y = interplin(x, xp, yp)

                # convert to datetime object
                time = pd.to_datetime(pd.Series(y, name="Time"), utc=tzinfo == tz.tzutc())
                if tzinfo != tz.tzutc():
                    # localize to non-UTC time zone
                    time.apply(lambda x: x.tz_localize(tzinfo))

                data[ky]["Time"] = time
    return data
