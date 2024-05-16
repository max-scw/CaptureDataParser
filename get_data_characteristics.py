from pathlib import Path
import logging

from utils import (
    default_argument_parser,
    parse_arguments,
    get_files,
    get_signal
)

from typing import Dict, Any, Union, List


def get_data_characteristics(
        signals,
        data_directory: Union[str, Path],
        file_extension: str = None,
        path_to_metadata: Union[str, Path] = None,
        filter_key: Union[Any, List[Any]] = None,
        start_index: int = 0,
        in_seconds: bool = False
) -> Dict[str, Any]:
    # loop over files to determine the min, max, length values
    characteristics = {
        "min_value": 9e9,
        "max_value": -9e9,
        "max_length": 0
    }
    for key in signals:
        for fl, df, _ in get_files(
                data_directory=data_directory,
                file_extension=file_extension,
                path_to_metadata=path_to_metadata,
                start_index=start_index,
                filter_key=filter_key
        ):
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
    # meta data
    parser.add_argument("--path-to-metadata", type=str, default=None,
                        help="Metadata file or file pattern (usually called 'info.csv')")
    parser.add_argument("--filter-key", type=str, nargs="+", default=None,  # "/Channel/State/actTNumber"
                        help="Column in meta data file (e.g. '/Channel/State/actTNumber').")

    opt = parse_arguments(parser)

    # process data
    get_data_characteristics(
        opt.signals,
        data_directory=opt.source,
        file_extension=opt.file_extension,
        path_to_metadata=opt.path_to_metadata,
        filter_key=opt.filter_key,
        start_index=opt.start_index
    )

