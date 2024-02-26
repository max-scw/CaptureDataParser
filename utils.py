import re
import hashlib
import numpy as np
import pandas as pd


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


re_signal_name_head = re.compile("[\w\-\.:]+(?=\|\d)", re.ASCII)


def get_signal_name_head(name: str) -> str:
    m = re_signal_name_head.match(name)
    if m is None:
        return name
    else:
        return m.group()


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


def find_changed_rows(df: pd.DataFrame, ignore_first_rows: int = 0) -> list:
    # fill nans
    df = df.bfill()
    # consider only numeric columns
    lg_col = [pd.api.types.is_numeric_dtype(el) for el in df.dtypes]
    # differences between consecutive rows
    differences = df.loc[:, lg_col].diff() != 0
    # # first row where differences occur
    # differences[2:].any(axis=1).idxmax()
    # Find indices of rows where at least one True occurs
    lg = differences[ignore_first_rows:].any(axis=1)
    return list(lg[lg].index)
