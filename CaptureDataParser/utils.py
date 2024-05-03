import re
import hashlib
import numpy as np
import pandas as pd
import re

from CaptureDataParser.HeaderData import SignalHeaderHF

from typing import List, Union


def cast_dtype(dtype: str) -> type:
    """
    replace string-based data type by its actual numpy type
    :param dtype: string specifying the data type
    :return: data type
    """
    if dtype.lower() == "integer":
        return int
    elif dtype.lower() == "float":
        return np.float32
    elif dtype.lower() == "double":
        return np.double
    elif dtype.lower() == "string":
        return str
    elif dtype.lower() == "uint":
        return int
    else:
        raise Exception(f"Unrecognized data type {dtype}.")


re_signal_name_head = re.compile("[\w\-\.:]+(?=\|(\d|[a-cA-Cx-zX-ZsS]))", re.ASCII)
re_signal_axis = re.compile("([a-cx-z]|sp)\d+", re.IGNORECASE | re.ASCII)


def get_signal_name_head(name: str) -> str:
    """
    extracts main part (aka. "head") of a signal name, i.e. everything before ...|<number> which indicates the axis
    :param name:
    :return: name head or full name
    """
    m = re_signal_name_head.match(name)
    if m is None:
        return name
    else:
        return m.group()


def rename_signal(head: SignalHeaderHF) -> str:
    """
    renames the enumerated axes by their corresponding name (i.e. <name head>|<axis number> => <name head>|<axis name>
    :param head:
    :return: signal name
    """
    # check axis is an expected machine tool axis (X, Y, Z, A, B, C, SP
    m = re_signal_axis.match(head.axis)
    if m is not None:
        axis = head.axis
        name = get_signal_name_head(head.name)
        return f"{name}|{axis}"
    else:
        return head.name


def hash_list(elements: list) -> str:
    """
    creates a unique hash string for a list.

    The function transforms all elements to a string and hashes them individually. Eventually it concatenates all the
    hash strings and hashes this string again to obtain a single hash string.

    Note: calculating a hash for str(elements) yields different results for identical lists of elements!

    :param elements: list of elements to hash
    :return: unique hash string
    """
    hash_fnc = hashlib.new('sha256')
    tmp = []
    for el in elements:
        # transform to (binary) string and hash every element
        hash_fnc.update(str(el).encode())
        tmp.append(hash_fnc.hexdigest())
    # hash list of hashes
    hash_fnc.update("".join(tmp).encode())
    return hash_fnc.hexdigest()


def find_changed_rows(df: pd.DataFrame) -> list:
    """
    finds rows that differ from the next row in a dataframe
    :param df: table of which the rows should be checked
    :return: list of indices where the content differs from the next row in the table
    """
    # fill nans
    df = df.bfill()
    # consider only numeric columns
    lg_col = [pd.api.types.is_numeric_dtype(el) for el in df.dtypes]
    # differences between consecutive rows
    diff_num = df.loc[:, lg_col].diff()
    # only not nan columns
    lg_nan = diff_num.notna().any()
    lg_diff = diff_num.loc[:, lg_nan] != 0

    # not numeric columns
    lg_col_not = [not el for el in lg_col]
    # compare "manually", ignoring the index
    diff_str_ = df.iloc[1:, lg_col_not].astype(str).to_numpy() != df.iloc[:-1, lg_col_not].astype(str).to_numpy()
    # construct DataFrame
    diff_str = pd.DataFrame(diff_str_, columns=df.columns[lg_col_not], index=df.index[1:])

    # concatenate
    differences = pd.concat((lg_diff, diff_str), axis=1)

    # Find indices of rows where at least one True occurs (first row is NaN due to diff-operation)
    lg = differences[1:].any(axis=1)
    return list(lg[lg].index)


def check_key_pattern(columns: list, key_pattern: str):
    re_ky = re.compile(key_pattern)
    for col in columns:
        m = re_ky.match(col)
        if m:
            return col
    return None


def interplin(
        x: Union[np.ndarray, List[Union[int, float]]],
        xp: Union[np.ndarray, List[Union[int, float]]],
        yp: Union[np.ndarray, List[Union[int, float]]]
) -> List[Union[int, float]]:
    # Sort xp and yp together based on the values in xp
    xp, yp = zip(*sorted(zip(xp, yp)))

    # Convert xp and yp to numpy arrays for efficient computations
    xp = np.array(xp)
    yp = np.array(yp)

    interpolated_values = []

    for i in range(len(x)):
        # Find the index of the closest point in xp that is less than or equal to x[i]
        idx = np.searchsorted(xp, x[i], side='right') - 1

        if idx == -1:
            # Extrapolate if x[i] is smaller than the smallest value in xp
            slope = (yp[1] - yp[0]) / (xp[1] - xp[0])
            interpolated_value = yp[0] + slope * (x[i] - xp[0])
            # Check if interpolated value is zero
            if interpolated_value == 0:
                # Saturate to the nearest non-zero value
                idx_nonzero = np.argmax(yp != 0)
                interpolated_value = yp[idx_nonzero]
        elif idx == len(xp) - 1:
            # Extrapolate if x[i] is larger than the largest value in xp
            slope = (yp[-1] - yp[-2]) / (xp[-1] - xp[-2])
            interpolated_value = yp[-1] + slope * (x[i] - xp[-1])
            # Check if interpolated value is zero
            if interpolated_value == 0:
                # Saturate to the nearest non-zero value
                idx_nonzero = np.argmax(yp != 0)
                interpolated_value = yp[idx_nonzero]
        else:
            # Interpolate linearly between the two closest points
            x_left, x_right = xp[idx], xp[idx + 1]
            y_left, y_right = yp[idx], yp[idx + 1]
            slope = (y_right - y_left) / (x_right - x_left)
            interpolated_value = y_left + slope * (x[i] - x_left)

        interpolated_values.append(interpolated_value)

    return interpolated_values


