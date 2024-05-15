import logging

from utils import (
    default_argument_parser,
    parse_arguments,
    get_files,
    get_signal
)

from typing import Dict, Any


def get_data_characteristics(signals, directory, file_extension, start_index, in_seconds: bool = False) -> Dict[str, Any]:
    # loop over files to determine the min, max, length values
    characteristics = {
        "min_value": 9e9,
        "max_value": -9e9,
        "max_length": 0
    }
    for key in signals:
        for fl, df, _ in get_files(directory, file_extension, start_index):
            sig = get_signal(df, key, in_seconds=in_seconds)

            if sig.max() > characteristics["max_value"]:
                characteristics["max_value"] = sig.max()
            if sig.min() < characteristics["min_value"]:
                characteristics["min_value"] = sig.min()
            if len(sig) > characteristics["max_length"]:
                characteristics["max_length"] = len(sig)
    logging.info(
        f"Charachteristics: {characteristics}\n"
        f"for signals: {signals}"
    )
    return characteristics


if __name__ == "__main__":
    parser = default_argument_parser()
    parser.add_argument("--signal", type=str, nargs="+", help="Signal key", required=True)

    opt = parse_arguments(parser)

    # process data
    get_data_characteristics(
        opt.signals,
        source=opt.source,
        file_extension=opt.file_extension,
        start_index=opt.start_index
    )

