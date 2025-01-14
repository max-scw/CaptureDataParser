import pandas as pd
import numpy as np
from pathlib import Path

from sklearn.model_selection import train_test_split
from random import shuffle

from typing import List, Dict, Any, Literal

from utils import read_info_files
from identify_tool_changes import cleanse_data


def find_data_to_last_in_use(data: pd.DataFrame, files_last_in_use: pd.DataFrame) -> pd.Index:
    """collects all indices that belong to a tool change"""
    # list object to apply list comparison
    filenames_last_in_use = files_last_in_use["filename"].tolist()
    # get indices of slices
    lg = data["filename"].apply(lambda x: x in filenames_last_in_use)
    idxs = lg[lg].index

    assert data.loc[idxs, "filename"].apply(lambda x: x in filenames_last_in_use).all()
    # assert data.loc[idxs, "filename"].tolist() == filenames_last_in_use #  must fail, because the list is not sorted by date

    return idxs



def slice_table(
        data: pd.DataFrame,
        files_last_in_use: pd.DataFrame
) -> pd.DataFrame:
    """generator to provide slices of data w.r.t. individual tools"""
    idxs = find_data_to_last_in_use(data, files_last_in_use)
    # get sorted list
    filenames_last_in_use = data.loc[idxs, "filename"].tolist()

    idx_start = df.index[0]
    for i, idx in enumerate(idxs):
        data_slice = data.loc[idx_start:idx]
        assert filenames_last_in_use[i] == data_slice["filename"].iloc[-1]
        if i > 0:
            assert filenames_last_in_use[i -1] != data_slice["filename"].iloc[0]

        yield data_slice
        # update start index of the slice
        idx_start = idx + 1


def naive_split(
        data_: list,
        split_fraction_des: Dict[Literal["Trn", "Val", "Tst"], float],
        stratify: Dict[Any, int] = None
) -> Dict[str, List[int]]:
    """splits the data randomly into sets for training, validation, and testing."""

    assert sum(split_fraction_des.values()) == 1, f"<split_fraction_des> must sum to 1."

    # split data stepwise
    train_size = split_fraction_des["Trn"] + split_fraction_des["Val"]
    stratify_ = [stratify[img] for img, _ in data_] if stratify else None
    x_trn_val, x_tst = train_test_split(data_, train_size=train_size, shuffle=True, stratify=stratify_)

    train_size = split_fraction_des["Trn"] / (split_fraction_des["Trn"] + split_fraction_des["Val"])
    stratify_ = [stratify[img] for img, _ in x_trn_val] if stratify else None
    x_trn, x_val = train_test_split(x_trn_val, train_size=train_size, shuffle=True, stratify=stratify_)
    return {"Trn": x_trn, "Val": x_val, "Tst": x_tst}


if __name__ == "__main__":
    # --- USER INPUT
    metadata_file: str = "./exported_recordings/info.csv"  # Path to info.csv file
    tool_last_in_use_file: str = "tools_last_in_use.csv"  # Path to tools_last_in_use.csv

    # split fractions
    trn: float = 0.5  # Split fraction of training data
    val: float = 0.2  # Split fraction of validation data
    tst: float = 0.3  # Split fraction of testing data

    n_random_splits: int = 5000  # How many random splits should be considered to find a minimum

    # ---
    assert trn + val + tst == 1, f"Split fractions (trn, val, tst) must sum to 1."
    assert Path(metadata_file).exists(), f"No file found at {metadata_file}."
    assert Path(tool_last_in_use_file).exists(), f"No file found at {tool_last_in_use_file}."

    split_fraction = {"Trn": trn, "Val": val, "Tst": tst}

    # path to meta information file
    df = read_info_files(metadata_file)
    th = 1000

    last_in_use = pd.read_csv(tool_last_in_use_file)

    # ignore very short records
    df = cleanse_data(df)

    keys_tool_id = [
        # '/Channel/State/actToolIdent',
        '/Channel/State/actTNumber',
    ]
    keys_tool_geometry = [
        # '/Channel/State/actToolLength1',
        '/Channel/State/actToolLength2',
        '/Channel/State/actToolRadius'
    ]

    # identify operations with more than 1.000 executions
    counts = df["G code hash"].value_counts()
    lg = counts > th
    hashs = list(lg[lg].index)

    n_ops_large = counts[lg].sum()
    print(
        f"{len(hashs)}/{len(lg)} operations have more than {th} occurrences. "
        f"That are {n_ops_large}/{len(df)} operations ({n_ops_large / len(df) *100:.2g}% of the data)."
    )

    # split according to tool type + G Code hash
    g_hash_counts = df["G code hash"].value_counts()

    g_hash_counts_lim = g_hash_counts[g_hash_counts > 100]
    weight = {"G code hash": g_hash_counts_lim / sum(g_hash_counts_lim)}

    tool_id = df["/Channel/State/actTNumber"].value_counts()
    weight["tool_id"] = pd.Series(np.full(len(tool_id), fill_value=2 / len(tool_id)), index=tool_id.index)

    weight["sum"] = pd.Series(0.2, ["sum"])

    info = pd.concat((g_hash_counts_lim, tool_id, pd.Series(len(df), ["sum"])))

    weights = pd.concat(weight.values())
    # normalize weights
    weights /= weights.sum()

    # determine desired values
    counts_des = pd.DataFrame({ky: info * vl for ky, vl in split_fraction.items()})

    data_slices_counts = dict()
    filenames = []
    for i, data in enumerate(slice_table(df, last_in_use)):
        filenames.append(data["filename"])

        g_code_hash_count = data["G code hash"].value_counts()
        lg = [el in weights for el in g_code_hash_count.index]
        data_slices_counts[i] = pd.concat(
            (
                # G code hash
                g_code_hash_count[lg],
                # tool ids
                data["/Channel/State/actTNumber"].value_counts(),
                # sum
                pd.Series(len(data), ["sum"])
             )
        )

    indices = list(range(len(data_slices_counts)))

    indices_split_min, best_count_keys = None, None
    cost_min = np.inf
    for i in range(n_random_splits):
        # shuffle indices
        shuffle(indices)
        # random split
        indices_split = naive_split(indices, split_fraction)

        count_keys = dict()
        for key, val in indices_split.items():
            # sum the counts
            counts_key = pd.Series(np.zeros(len(weights)), index=weights.index)
            for idx in val:
                ghc = data_slices_counts[idx]
                counts_key[ghc.index] += ghc
            count_keys[key] = counts_key
        count_keys = pd.DataFrame(count_keys)

        cost = np.sqrt(np.mean(((counts_des - count_keys) ** 2).mul(weights, axis=0)))

        if cost < cost_min:
            cost_min = cost
            indices_split_min = indices_split
            best_count_keys = count_keys
            print(cost_min)

    # display best split
    print(f"best split: {counts_des - count_keys}")

    # write the best data split to files
    for key, val in indices_split_min.items():
        names = pd.concat([filenames[el] for el in val]).sort_index()
        names.to_csv(f"{key}.txt", index=False)
