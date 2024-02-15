import re
import hashlib
import numpy as np


def cast_dtype(dtype: str) -> type:
    """
    replace string-based data type by its actual numpy type
    :param dtype: string specifying the data type
    :return: data type
    """
    if dtype.upper() == "INTEGER":
        return np.int32
    elif dtype.upper() == "FLOAT":
        return np.float32
    elif dtype.upper() == "DOUBLE":
        return np.double
    elif dtype.upper() == "STRING":
        return str
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
