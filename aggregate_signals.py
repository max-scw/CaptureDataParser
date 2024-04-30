import argparse
from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm
from setproctitle import setproctitle

import warnings

from utils import save_dict_of_dataframes, get_files, default_argument_parser, parse_arguments


if __name__ == "__main__":
    parser = default_argument_parser()
    parser.add_argument("--window-size", type=float, default=1,
                        help="Window size. See --in-seconds to make it a duration")
    parser.add_argument("--in-seconds", action="store_true",
                        help="Window size. See --in-seconds to make it a duration")
    parser.add_argument("--method", type=str, default="rms",
                        help="Aggregation method. Can be 'rms', 'sum', 'mean', 'absSum', or 'absMean'.")


    opt = parse_arguments(parser)

    # process input
    method = opt.method.lower()

    data = dict()
    for fl, df in get_files(opt.source, opt.file_extension):

        # aggregate signal
        dft = df.drop("Time", axis=1)
        if opt.in_seconds:
            dt = np.nanmedian(df["Time"].diff())  # nanosecond
            if dt < 1:
                warnings.warn(f"Sample time is less than 1 nanosecond. Implausible. Skipping file {fl.name}.")
                continue

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