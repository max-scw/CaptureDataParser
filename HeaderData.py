from pydantic import BaseModel
from datetime import datetime

from typing import Optional, List, Dict


"""
This file contains the Pydantic data models used to model the header of a message of Capture4Analysis data file. 
Capture4Analysis is a data recorder for Siemens SINUMERIK Edge and a Siemens trademark.
"""


class Version(BaseModel):
    output_file_format: str
    recorder: str
    # 'outputFileFormatVersion': '1.0', 'RecorderVersion': '3.1.0-36'


class Machine(BaseModel):
    name: str
    cf_card_id: str


class TimeInfo(BaseModel):
    # cycle time (ms) => HF data
    hf_cycle_time: Optional[int] = -1
    # initial: start time, cycle counter
    start_time: datetime
    start_counter: Optional[int] = -1


class Job(BaseModel):
    # job: ['"TriggersOn":{"activeTool":"6"}', '"TriggersOff":{"ncCode":"M6"}']
    trigger_on: Dict[str, str]
    trigger_off: Dict[str, str]


class SignalHeaderHF(BaseModel):
    # {'Name': 'CYCLE', 'Type': 'INTEGER', 'Axis': 'Cycle', 'Address': 'CYCLE'}
    name: str
    dtype: type
    axis: str
    address: str
    # id: int
    # sampling_period


class SignalHeaderLF(BaseModel):
    # {'id': '0', 'device': '/Channel/State/actToolIdent', 'path': '/Channel/State/actToolIdent', 'label': '', 'samplingPeriod': 500}
    id: int
    device: str
    address: str  # path
    name: Optional[str] = None  # label
    sampling_period_ms: int
    dtype: Optional[type] = None


class HeaderData(BaseModel):
    """
    Main data model. Used to represent the header of a recording by the software Capture4Analysis.
    """
    # version: file-format, record
    version: Version

    # machine info: name, cf-card-number
    machine: Machine

    # time: start time, cycle counter, HFData cycle time
    time: TimeInfo

    # signals
    # SignalListHFData, SignalListLFData, SignalListExternalData
    signals: Dict[str, List[SignalHeaderHF | SignalHeaderLF]]

    # job
    job: Job

    # grouped signals
    # grouped: Optional[Dict[str, Dict[str, List[SignalHeader]]]] = dict()