from pathlib import Path
import pandas as pd
import numpy as np

import warnings

from utils import get_files, save_dict_of_dataframes, default_argument_parser, parse_arguments

from typing import Union


def aggregate_signal(
        signals: pd.DataFrame,
        method: str,
        window_size: Union[int, float],
        in_seconds: bool = False
) -> Union[pd.DataFrame, None]:
    method = method.lower()

    # aggregate signal
    dft = signals.drop("Time", axis=1)
    if in_seconds:
        dt = np.nanmedian(signals["Time"].diff())  # nanosecond
        if dt < 1:
            warnings.warn(f"Sample time is less than 1 nanosecond. Implausible. Skipping file {fl.name}.")
            return None

        # rows per second
        wz = int(round(window_size * 10 ** 9 / int(dt)))
    else:
        wz = int(window_size)

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

    return df_aggregated.drop(0)  # drop first row because it is always NaN


if __name__ == "__main__":
    parser = default_argument_parser()
    parser.add_argument("--signal", type=str,  nargs='+', help="Signal key", required=True)
    # add arguments to aggregate signals
    parser.add_argument("--window-size", type=float, default=-1,
                        help="Window size. See --in-seconds to make it a duration")
    parser.add_argument("--in-seconds", action="store_true",
                        help="Window size. See --in-seconds to make it a duration")
    parser.add_argument("--method", type=str, default="rms",
                        help="Aggregation method. Can be 'rms', 'sum', 'mean', 'absSum', or 'absMean'.")
    opt = parse_arguments(parser)

    # process user inputs
    keys = opt.signal
    # TODO: allow patterns

    export_path = Path(opt.destination)
    # create directory if it does nox exist
    if not export_path.exists():
        export_path.mkdir(parents=True, exist_ok=True)

    # process data
    for key in keys:
        data = dict()
        for fl, df in get_files(opt.source, opt.file_extension):
            try:
                sig = df[key]
            except KeyError as ex:
                raise Exception(f"Signal '{key}' not found in {fl.as_posix()}. "
                                f"Available signals are {', '.join(df.keys())}")
            except Exception as ex:
                raise Exception(f"Failed to process {fl.as_posix()} with exception: {ex}")

            if opt.window_size > 0:
                sig = aggregate_signal(
                    sig,
                    method=opt.method,
                    window_size=opt.window_size,
                    in_seconds=opt.in_seconds
                )
            data[fl.stem] = sig

        # save data
        if len(data) > 0:
            filename = export_path / f"{key.replace('|', '_')}_{len(data)}.json"
            save_dict_of_dataframes(filename, data)
        else:
            warnings.warn(f"No data found for key '{key}'")
