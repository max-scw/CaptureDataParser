from pathlib import Path
import json

from CaptureDataParser.parse_header import parse_header
from CaptureDataParser.parse_payload import parse_payload
from CaptureDataParser.CapturePayload import CapturePayload

from typing import Union


def parse(file: Union[Path, str], rename_hfdata: bool = False):
    if isinstance(file, str):
        file = Path(file)

    if isinstance(file, Path):
        with open(file, "r", encoding="utf-8") as fid:
            data = json.load(fid)

    header = data["Header"]  # description + context
    payload = data["Payload"]  # signals
    footer = data["Footer"]  # stack messages together

    head = parse_header(header)
    raw = parse_payload(payload, head.signals, rename_hfdata=rename_hfdata)

    return CapturePayload(raw, head.time)
