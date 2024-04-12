from pathlib import Path
import json
import pandas as pd

from CaptureDataParser.parse_header import parse_header
from CaptureDataParser.parse_payload import parse_payload
from CaptureDataParser.CapturePayload import CapturePayload

from typing import Union, List


def read_json(file: Union[Path, str]) -> dict:
    if isinstance(file, str):
        file = Path(file)

    if not file.is_file():
        raise FileNotFoundError

    # read file
    with open(file, "r", encoding="utf-8") as fid:
        data = json.load(fid)

    return data


def read_capture_recording(file: Union[Path, str]) -> (dict, dict, dict):
    data = read_json(file)

    header = data["Header"]  # description + context
    payload = data["Payload"]  # signals
    footer = data["Footer"]  # stack messages together
    return header, payload, footer


def parse(
        files: Union[Union[Path, str], List[Union[Path, str]]],
        rename_hfdata: bool = False
):
    if isinstance(files, (str, Path)):
        files = [files]

    # read files
    content = dict()
    for fl in files:
        content[fl.name] = read_capture_recording(fl)

    # find start file: loop through footers
    next_filename = None
    for filename, (_, _, footer) in content.items():
        if footer["FilePathChain"]["Previous"] is None:
            next_filename = filename
            break

    if next_filename is None:
        raise FileNotFoundError("No start file of recording found.")

    # walk through contents
    signals = dict()
    head0 = None
    i = 1
    while True:
        header, payload, footer = content[next_filename]

        # parse single file
        head = parse_header(header)
        raw = parse_payload(payload, head.signals, rename_hfdata=rename_hfdata)

        # keep initial time information
        if len(signals) == 0:
            head0 = head

        # concatenate signals
        for ky, vl in raw.items():
            if ky in signals:
                signals[ky] += (vl, )
            else:
                signals[ky] = (vl, )

        # update next filename
        next_filename = footer["FilePathChain"]["Next"]
        if next_filename is None:
            break
        i += 1

    # concatenate all fields
    for ky, vl in signals.items():
        signals[ky] = pd.concat(vl, axis=0)

    return CapturePayload(raw, head0.time)



