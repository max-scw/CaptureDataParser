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
        directory: Union[str, Path],
        # filter
        path_to_metadata: Union[str, Path] = None,
        filter_keys: Union[Any, List[Any]] = None
):

    # get files
    if path_to_metadata is not None:
        # read metadata
        try:
            info = read_info_files(path_to_metadata)
        except FileNotFoundError as ex:
            raise FileNotFoundError(f"Metadata file(s) not found on {path_to_metadata.as_posix()}: {ex}")

        if filter_keys is None:
            files_per_key = info["filename"]
        else:
            # filter data
            files_per_key = info.groupby(filter_keys)["filename"]
            files_per_key = [(ky, fls.apply(lambda x: directory / x)) for ky, fls in files_per_key]
    else:
        files_per_key = {None: list(Path().glob(directory))}

    for ky, files in files_per_key.items():
        yield files, ky


def get_files(
        directory: Union[str, Path],
        # filter
        path_to_metadata: Union[str, Path] = None,
        filter_key: Union[Any, List[Any]] = None,
        start_index: int = 0,

) -> Tuple[Path, pd.DataFrame, Any]:

    for files, key_filter in get_list_of_files(directory, path_to_metadata, filter_key):
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


def read_info_files(path: Union[str, Path] = "info*.csv") -> pd.DataFrame:
    """read meta data file"""
    files = []
    for fl in Path().glob(path.as_posix() if isinstance(path, Path) else path):
        df = pd.read_csv(fl)
        files.append(df)

    df = pd.concat(files)
    # drop duplicates if some of the files contain redundant data
    df = df.drop_duplicates()
    # sort by recording date
    df.sort_values(by="date", inplace=True, ignore_index=True)
    return df
