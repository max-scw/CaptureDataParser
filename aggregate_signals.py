import argparse
from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm
from setproctitle import setproctitle
import json

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=str, help="Directory where zipped recordings are stored")
    parser.add_argument("--destination", type=str, default="",
                        help="Directory where extracted recordings should be placed to")
    parser.add_argument("--file-extension", type=str, default="", help="File type")
    parser.add_argument("--window-size", type=float, default=1,
                        help="Window size. See --in-seconds to make it a duration")
    parser.add_argument("--in-seconds", action="store_true",
                        help="Window size. See --in-seconds to make it a duration")
    parser.add_argument("--method", type=str, default="rms",
                        help="Aggregation method. Can be 'rms', 'sum', 'mean', 'absSum', or 'absMean'.")

    parser.add_argument('--process-title', type=str, default=None, help="Names the process")

    opt = parser.parse_args()

    if opt.process_title:
        setproctitle(opt.process_title)

    # get files
    files = list(Path(opt.source).glob(f"*.{opt.file_extension.strip('.')}"))

    method = opt.method.lower()

    data = dict()
    for fl in tqdm(files):
        # read file
        df = pd.read_csv(fl)
        # convert timestamp
        df["Time"] = pd.to_datetime(df['Time'], format="ISO8601")

        # aggregate signal
        dft = df.drop("Time", axis=1)
        if opt.in_seconds:
            dt = np.nanmedian(df["Time"].diff())  # nanosecond

            # rows per second
            wz = int(round(opt.window_size * 10 ** 9 / int(dt)))
        else:
            wz = int(opt.window_size)

        # arguments for rolling window function
        kwargs = {
            "window": wz,
            "step": wz,
            "center": True
        }

        # methods
        if (len(method) > 3) and (method[:3] == "abs"):
            dft = dft.abs()
            method = method[3:]

        if method in ("rms", "rmse"):
            df_aggregated = dft.pow(2).rolling(**kwargs).apply(lambda x: np.sqrt(x.mean()))
        elif method == "sum":
            df_aggregated = dft.rolling(**kwargs).mean()
        elif method == "mean":
            df_aggregated = dft.rolling(**kwargs).mean()
        else:
            raise ValueError(f"Unknown method: {method}")

        data[fl.stem] = df_aggregated.drop(0)  # drop first row because it is always NaN


    export_filename = f"aggregation_{wz}{'s' if opt.in_seconds else ''}_{method}.json"
    export_file = Path(opt.destination) / export_filename
    save_dict_of_dataframes(export_file, data)

    # data2 = read_dict_of_dataframes(export_file)