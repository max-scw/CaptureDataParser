from pathlib import Path
import pandas as pd
import numpy as np

import warnings

from utils import get_files, save_dict_of_dataframes, default_argument_parser, parse_arguments

from typing import Union


def get_time_period(time: pd.Series, in_seconds: bool = False) -> int | float:
    dt_ns = int(np.nanmedian(time[:10000].diff()))  # in nanoseconds
    return (dt_ns / 10 ** 9) if in_seconds else dt_ns


def aggregate_signal(
        signals: pd.DataFrame,
        method: str,
        window_size: Union[int, float],
        in_seconds: bool = False,
        time: pd.Series = None,
) -> Union[pd.DataFrame, None]:
    method = method.lower()

    # aggregate signal
    if in_seconds:
        assert len(signals) == len(time), "Length of signals and time series do not match"
        dt = get_time_period(time)  # nanosecond
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
        signals = signals.abs()
        method = method[3:]

    if method in ("rms", "rmse"):
        df_aggregated = signals.pow(2).rolling(**kwargs).apply(lambda x: np.sqrt(x.mean()))
    elif method == "sum":
        df_aggregated = signals.rolling(**kwargs).mean()
    elif method == "mean":
        df_aggregated = signals.rolling(**kwargs).mean()
    else:
        raise ValueError(f"Unknown method: {method}")

    return df_aggregated.drop(0)  # drop first row because it is always NaN


if __name__ == "__main__":
    parser = default_argument_parser()
    parser.add_argument("--signal", type=str,  nargs='+', help="Signal key", required=True)
    # add arguments to aggregate signals
    parser.add_argument("--window-size", type=float, default=-1,
                        help="Window size. See --in-seconds to make it a duration")
    parser.add_argument("--limit", type=float, default=-1,
                        help="Limit the signal length. May be an index or a duration if --in-seconds is used.")
    parser.add_argument("--in-seconds", action="store_true",
                        help="Window size and limit will be interpreted as a time value.")
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
        for fl, df in get_files(opt.source, opt.file_extension, opt.start_index):
            try:
                sig = df[key]
                # time
                if opt.in_seconds:
                    if "Time" in df:
                        time = df["Time"]
                    else:
                        warnings.warn(f"No time found in {fl.as_posix()}. Skipping this file.")
                        continue
                else:
                    time = None
            except KeyError as ex:
                raise Exception(f"Signal '{key}' not found in {fl.as_posix()}. "
                                f"Available signals are {', '.join(df.keys())}")
            except Exception as ex:
                raise Exception(f"Failed to process {fl.as_posix()} with exception: {ex}")

            # aggregate signal
            if opt.window_size > 0:
                try:
                    sig = aggregate_signal(
                        sig,
                        method=opt.method,
                        window_size=opt.window_size,
                        in_seconds=opt.in_seconds
                    )
                except Exception as ex:
                    raise Exception(f"Failed to aggregate {key} in {fl.as_posix()} with exception: {ex}")
            # limit signal length
            if opt.limit > 0:
                try:
                    fct = (1 / get_time_period(time, True)) if opt.in_seconds else 1
                    end = int(round(opt.limit * fct))
                    # slice signal
                    sig = sig[:end]
                except Exception as ex:
                    raise Exception(f"Failed to limit {key} in {fl.as_posix()} with exception: {ex}")
            data[fl.stem] = sig

        # save data
        if len(data) > 0:
            filename = f"{key.replace('|', '_')}_{len(data)}"
            if opt.window_size > 0:
                filename += f"_{opt.method}{opt.window_size}" + "s" if opt.in_seconds else ""
            if opt.limit > 0:
                filename += f"_{opt.limit}" + "s" if opt.in_seconds else ""
            save_dict_of_dataframes(export_path / filename, data)
        else:
            warnings.warn(f"No data found for key '{key}'")
