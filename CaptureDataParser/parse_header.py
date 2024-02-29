from dateutil import parser as datetime_parser
import json

from typing import List, Dict, Literal
import warnings

from CaptureDataParser.HeaderData import (
    HeaderData,
    Version,
    Machine,
    Job,
    TimeInfo,
    SignalHeaderHF,
    SignalHeaderLF
)
from CaptureDataParser.utils import cast_dtype


def parse_signals(signals: List[Dict[str, str]], mode: Literal["hf", "lf"]) -> List[SignalHeaderHF] | List[SignalHeaderLF]:
    # {'Name': 'CYCLE', 'Type': 'INTEGER', 'Axis': 'Cycle', 'Address': 'CYCLE'}
    info = []
    for el in signals:
        if mode == "hf":
            sig = SignalHeaderHF(
                name=el["Name"],
                dtype=cast_dtype(el["Type"]),
                axis=el["Axis"],
                address=el["Address"]
            )
        elif mode == "lf":
            sig = SignalHeaderLF(
                id=int(el["id"]),
                device=el["device"],
                address=el["path"],
                name=el["label"] if el["label"] else None,
                sampling_period_ms=int(el["samplingPeriod"])
            )
        else:
            raise Exception(f"Unknown mode {mode}. Only 'hf' and 'lf' are supported.")

        info.append(sig)
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
        signals["HFData"] = parse_signals(header["SignalListHFData"], "hf")
    if "SignalListLFData" in header:
        signals["LFData"] = parse_signals(header["SignalListLFData"], "lf")
    if "SignalListExternalData" in header:
        if len(header["SignalListExternalData"]) > 0:
            warnings.warn("SignalListExternalData not yet tested.")
        signals["ExternalData"] = parse_signals(header["SignalListExternalData"], "hf")

    return HeaderData(
        version=version,
        machine=machine,
        job=job,
        time=time,
        signals=signals
    )

