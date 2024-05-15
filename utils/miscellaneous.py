from pathlib import Path
import pandas as pd
import numpy as np
import json
from tqdm import tqdm

from typing import Union, Dict, Tuple, List, Any


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


def get_list_of_files(
        path: Union[str, Path],
        file_extension: str = None,
        # filter
        filter_key: str = None
):
    if (file_extension is not None) and (file_extension != ""):
        path = Path(path) / (f"**/*" + f".{file_extension.strip('.')}")

    # get files
    if filter_key:  # assume meta data file
        # read metadata
        try:
            info = read_info_files(path)
        except FileNotFoundError as ex:
            raise FileNotFoundError(f"Metadata file(s) not found on {path.as_posix()}: {ex}")

        # filter data
        filter_key_value_uq = info[filter_key].unique()
        filter_key_value_uq = filter_key_value_uq[np.invert(np.isnan(filter_key_value_uq))]

        files_per_key = dict()
        for ky in filter_key_value_uq:
            lg = info[filter_key] == ky
            files_per_key[ky] = info["filename"][lg]
    else:
        files_per_key = {None: list(Path().glob(path))}

    for ky, files in files_per_key.items():
        yield files, ky


def get_files(
        path: Union[str, Path],
        file_extension: str = None,
        # filter
        filter_key: str = None,
        start_index: int = 0,

) -> Tuple[Path, pd.DataFrame, Any]:

    for files, key_filter in get_list_of_files(path, file_extension, filter_key):
        # loop over files
        for i in tqdm(range(start_index, len(files))):
            file = files[i]
            # read file
            df = pd.read_csv(file)
            # convert timestamp
            if "Time" in df:
                df["Time"] = pd.to_datetime(df["Time"], format="ISO8601")
            df.name = file.as_posix()
            yield file, df, key_filter


def read_info_files(path: str = "info*.csv") -> pd.DataFrame:
    """read meta data file"""
    files = []
    for fl in Path().glob(path):
        df = pd.read_csv(fl)
        files.append(df)

    df = pd.concat(files)
    # sort by recording date
    df.sort_values(by="date", inplace=True, ignore_index=True)
    return df
