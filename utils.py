from pathlib import Path
import pandas as pd
import json
from tqdm import tqdm

from argparse import ArgumentParser
from setproctitle import setproctitle

from typing import Union, Dict


def save_dict_of_dataframes(filename: Union[str, Path], dictionary: Dict[str, pd.DataFrame]) -> bool:
    """Converts a dictionary of pandas.DataFrames to a dictionary of dictionaries and saves them in a JSON file."""
    # convert all dataframes to dictionaries and dump into a JSON file
    dictionary_ = {ky: vl.to_dict() for ky, vl in dictionary.items()}
    with open(filename, "w") as fid:
        json.dump(dictionary_, fid)
    return True


def read_dict_of_dataframes(filename: Union[str, Path]) -> Dict[str, pd.DataFrame]:
    with open(filename, "r") as fid:
        dictionary_ = json.load(fid)
    return {ky: pd.DataFrame(vl) for ky, vl in dictionary_.items()}


def get_files(directory: Union[str, Path], file_extension: str = None) -> (Path, pd.DataFrame):

    pattern = f"*.{file_extension.strip('.')}" if file_extension is not None else "*.*"
    # get files
    files = list(Path(directory).glob(pattern))
    # loop over files
    for fl in tqdm(files):
        # read file
        df = pd.read_csv(fl)
        # convert timestamp
        df["Time"] = pd.to_datetime(df['Time'], format="ISO8601")
        yield fl, df


def default_argument_parser():
    parser = ArgumentParser()
    parser.add_argument("--source", type=str, help="Directory where zipped recordings are stored")
    parser.add_argument("--destination", type=str, default="",
                        help="Directory where extracted recordings should be placed to")
    parser.add_argument("--file-extension", type=str, default="", help="File type")

    parser.add_argument('--process-title', type=str, default=None, help="Names the process")
    return parser


def parse_arguments(parser) -> bool:
    opt = parser.parse_args()

    if opt.process_title:
        setproctitle(opt.process_title)
        return True
    else:
        return False
