from pathlib import Path
import pandas as pd
import json
from tqdm import tqdm

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


def get_files(directory: Union[str, Path], file_extension: str = None, start_index: int = 0) -> (Path, pd.DataFrame):

    pattern = f"**/*" + (f".{file_extension.strip('.')}" if (file_extension is not None) and (file_extension != "") else "")
    # get files
    files = list(Path(directory).glob(pattern))
    # loop over files
    for i in tqdm(range(start_index, len(files))):
        file = files[i]
        # read file
        df = pd.read_csv(file)
        # convert timestamp
        if "Time" in df:
            df["Time"] = pd.to_datetime(df["Time"], format="ISO8601")
        yield file, df
