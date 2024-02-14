from dateutil import parser as datetime_parser
import numpy as np
import json

from typing import List, Dict

from HeaderData import (
    HeaderData,
    Version,
    Machine,
    Job,
    TimeInfo,
    SignalHeader
)


def cast_dtype(dtype: str) -> type:
    """
    replace string-based data type by its actual numpy type
    :param dtype: string specifying the data type
    :return: data type
    """
    if dtype.upper() == "INTEGER":
        return np.int32
    elif dtype.upper() == "FLOAT":
        return np.float32
    elif dtype.upper() == "DOUBLE":
        return np.double
    elif dtype.upper() == "STRING":
        return str
    else:
        raise Exception(f"Unrecognized data type {dtype}.")


def parse_signals(signals: List[Dict[str, str]]) -> List[SignalHeader]:
    info = []
    for el in signals:
        info.append(SignalHeader(
            name=el["Name"],
            dtype=cast_dtype(el["Type"]),
            axis=el["Axis"],
            address=el["Address"]
        ))
    return info


def parse_header(header: dict) -> HeaderData:

    # version
    version = Version(
            output_file_format=header["Version"]["outputFileFormatVersion"],
            recorder=header["Version"]["RecorderVersion"]
        )

    # machine
    machine = Machine(
            name=header["MachineInfo"]["MachineName"],
            cf_card_id=header["MachineInfo"]["CFCard"]
        )

    # job
    assert len(header["JobDescription"]) == 2  # FIXME: for debugging. Only TriggersOn and TriggersOff
    # job: ['"TriggersOn":{"activeTool":"6"}', '"TriggersOff":{"ncCode":"M6"}']
    job_description = dict()
    for el in header["JobDescription"]:
        ky, val = el.split(":", 1)
        job_description[ky.strip('"').strip("'")] = json.loads(val)

    job = Job(
            trigger_on=job_description["TriggersOn"],
            trigger_off=job_description["TriggersOff"],
        )

    # time
    time = TimeInfo(
            start_time=datetime_parser.parse(header["Initial"]["Time"]),
            hf_cycle_time=int(header["CycleTimeMs"]) if "CycleTimeMs" in header else -1,
            start_counter=int(header["Initial"]["HFProbeCounter"]) if "HFProbeCounter" in header["Initial"] else -1,
        )

    # signals
    signals = dict()
    if "SignalListHFData" in header:
        signals["HFData"] = parse_signals(header["SignalListHFData"])
    if "SignalListLFData" in header:
        signals["LFData"] = parse_signals(header["SignalListLFData"])
    if "SignalListExternalData" in header:
        signals["ExternalData"] = parse_signals(header["SignalListExternalData"])

    return HeaderData(
        version=version,
        machine=machine,
        job=job,
        time=time,
        signals=signals
    )

