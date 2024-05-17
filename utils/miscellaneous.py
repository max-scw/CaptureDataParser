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
        data_directory: Union[str, Path],
        file_extension: str = None,
        # filter
        path_to_metadata: Union[str, Path] = None,
        filter_keys: Union[Any, List[Any]] = None
) -> Tuple[List[Path], Union[Any, List[Any]]]:
    # ensure pathlib object
    data_directory = Path(data_directory)
    # create extension pattern
    pattern = "*." + file_extension.strip(".") if file_extension else "*"

    def reconstruct_path(x: str):
        p = data_directory / x
        return p.with_suffix(file_extension).resolve() if file_extension else p

    # get files
    if path_to_metadata is not None:
        # read metadata
        try:
            info = read_info_files(path_to_metadata)
        except FileNotFoundError as ex:
            raise FileNotFoundError(f"Metadata file(s) not found on {path_to_metadata.as_posix()}: {ex}")

        if filter_keys is None:
            files_per_key = {None: info["filename"].apply(reconstruct_path).tolist()}
        else:
            # filter data
            files_per_key = info.groupby(filter_keys)["filename"]
            files_per_key = {ky: fls.apply(reconstruct_path).tolist() for ky, fls in files_per_key}
    else:

        files_per_key = {None: list(Path(data_directory).glob(pattern))}

    for ky, files in files_per_key.items():
        yield files, ky


def get_files(
        files: Union[List[Union[str, Path]], Tuple[List[Union[str, Path]], Any]],
        start_index: int = 0,
) -> Tuple[Path, pd.DataFrame, Any]:

    if isinstance(files, tuple) and isinstance(files[0], list) and isinstance(files[0][0], (str, Path)):
        files = files[0]

    # loop over files
    for i in tqdm(range(start_index, len(files))):
        file = files[i]
        # read file
        df = pd.read_csv(file)
        # convert timestamp
        if "Time" in df:
            df["Time"] = pd.to_datetime(df["Time"], format="ISO8601")
        df.name = file.as_posix()
        yield file, df


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


