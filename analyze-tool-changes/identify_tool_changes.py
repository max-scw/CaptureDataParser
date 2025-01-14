from pathlib import Path
import pandas as pd
from itertools import chain
import re

from CaptureDataParser.utils import find_changed_rows
from utils import read_info_files


def cleanse_data(data_table: pd.DataFrame) -> pd.DataFrame:

    # ignore very short records
    n_min_rows = 2000
    table_clean = data_table[data_table["n_rows"] > n_min_rows]

    # ignore files with no tool info

    return table_clean


if __name__ == "__main__":
    # --- USER INPUT
    dir_metadata_files = ""
    pattern_metadata_file="info*.csv"

    # ---
    df = read_info_files(Path(dir_metadata_files) / pattern_metadata_file)


    keys_tool_id = [
        # '/Channel/State/actToolIdent',
        '/Channel/State/actTNumber',
    ]
    keys_tool_geometry = [
        '/Channel/State/actToolLength1',
        '/Channel/State/actToolLength2',
        '/Channel/State/actToolRadius'
    ]
    # --- merge columns if necessary
    # pattern to identify indexing in sinumerik variables
    re_index = re.compile(r"\[u\d(,\s?\d)?\]")
    # loop through all columns
    columns_to_drop = []
    for ky in df:
        # if column name contains an index
        m = re_index.search(ky)
        if m:
            # extract head of column name, i.e. without index
            ky_head = ky[:m.start()]
            if ky_head in df:
                # replace NaNs in column with head name with the values of the indexed column
                df[ky_head] = df[ky_head].fillna(df[ky])
                columns_to_drop.append(ky)
    # drop columns with names that have an index if there were merged
    df.drop(columns_to_drop, inplace=True, axis=1)
    df.drop_duplicates([
        "filename",
        "n_rows",
        "n_cols",
        "date",
        "G code hash",
        "/Channel/State/actTNumber",
        "/Channel/State/actToolLength2",
        "/Channel/State/actToolRadius"
    ], inplace=True)

    # --- cleanse data
    df_clean = cleanse_data(df).sort_values(by="date")

    # --- identify worn tools, i.e. signals where the tool was last used
    idx_new_tool = dict()
    idx_worn_tool = dict()
    for ky, tab in df_clean.groupby(keys_tool_id):
        tab_idx = find_changed_rows(tab[keys_tool_geometry])
        idx_new_tool[ky] = tab_idx

        # get previous index
        indices = list(tab.index)
        tab_idx_prev = [indices[indices.index(el) - 1] for el in tab_idx]
        idx_worn_tool[ky] = tab_idx_prev

    idx_worn_tools = list(chain.from_iterable(idx_worn_tool.values()))
    worn_tools = df.loc[idx_worn_tools, ["filename"]]

    worn_tools.to_csv("tool_last_in_use.csv", header=True, index=False)

    df.to_csv("info_new.csv", header=True, index=False)

    print(f"{len(worn_tools)} distinct tools in {len(df)} recordings.")
