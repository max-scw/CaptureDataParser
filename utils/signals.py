import pandas as pd
import numpy as np
import warnings

from typing import Union, List, Tuple


def get_signal(
        df: pd.DataFrame,
        key: str,
        window_size: Union[int, float] = 0,
        method: str = "rmse",
        in_seconds: bool = False,
        limit: Union[int, float] = 0
) -> Union[pd.Series, None]:
    try:
        sig = df[key]
    except KeyError as ex:
        raise Exception(f"Signal '{key}' not found in {df.name}. "
                        f"Available signals are {', '.join(df.keys())}")

    time = df["Time"] if "Time" in df else None

    if in_seconds and (time is None):
        warnings.warn(f"No time found in {df.name}.")
        return None

    if window_size > 0:
        sig = aggregate_signal(
            sig,
            method,
            window_size,
            in_seconds,
            time=time,
        )

    # limit signal length
    if limit > 0:
        try:
            fct = (1 / get_time_period(time, True)) if in_seconds else 1
            end = int(round(limit * fct))
            # slice signal
            sig = sig[:end]
        except Exception as ex:
            raise Exception(f"Failed to limit {key} in {df.name} with exception: {ex}")

    return sig


def get_time_period(time: pd.Series, in_seconds: bool = False) -> Union[int, float]:
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
            warnings.warn(f"Sample time is less than 1 nanosecond. Implausible.")
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
    elif method == "median":
        df_aggregated = signals.rolling(**kwargs).median()
    elif method == "max":
        df_aggregated = signals.rolling(**kwargs).max()
    elif method == "min":
        df_aggregated = signals.rolling(**kwargs).min()
    else:
        raise ValueError(f"Unknown method: {method}")

    return df_aggregated.drop(0)  # drop first row because it is always NaN
