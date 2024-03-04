import os
from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm

from typing import List

from CaptureDataParser import CapturePayload, parse
from CaptureDataParser.utils import find_changed_rows


import argparse
from setproctitle import setproctitle


def get_tool_info(payload: CapturePayload, keys: List[str] = None) -> (pd.DataFrame, int):
    index = 'HFProbeCounter'

    keys_geometry = [
        '/Channel/State/actToolLength1',
        '/Channel/State/actToolLength2',
        '/Channel/State/actToolRadius'
    ]
    if keys is None:
        keys = keys_geometry + [
            '/Channel/State/actToolIdent',
            '/Channel/State/actTNumber'
        ]
    tool = payload.get_item("LFData", keys + [index], not_na=True).ffill().bfill().drop_duplicates()

    keys_tool = [el for el in tool.columns if el != index]
    # ignore rows where both lengths and the radius is 0

    tool[keys_geometry] = tool[keys_geometry].replace(to_replace=0, value=np.nan).ffill()

    # find first row where tool keys change
    idxs = find_changed_rows(tool[keys_tool])
    if len(idxs) > 1:
        idx_ = np.unique(idxs)[0]
    else:
        idx_ = tool.index[-1]

    # get tool info (from last row)
    return tool.loc[idx_, keys_tool], tool.loc[idx_, index]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=str, help="Directory where zipped recordings are stored")
    parser.add_argument("--destination", type=str, help="Directory where extracted recordings should be placed to")
    parser.add_argument("--process-title", type=str, default=None, help="Names the process")
    parser.add_argument("--start-index", type=int, default=0, help="ith file to start from")
    parser.add_argument("--only-info", action="store_true", help="Do not export files, just collect information")
    parser.add_argument("--no-overwrote", action="store_true", help="Do not not overwrite existing files")

    opt = parser.parse_args()

    if opt.process_title:
        setproctitle(opt.process_title)

    folder_export = Path(opt.destination)
    folder_source = Path(opt.source)

    keys_toolinfo = [
        '/Channel/State/actToolIdent',
        '/Channel/State/actTNumber',
        '/Channel/State/actToolLength1',
        '/Channel/State/actToolLength2',
        '/Channel/State/actToolRadius'
    ]

    info = []
    k = 0
    for i, fl in enumerate(tqdm(list(folder_source.glob("**/*.json")))):
        # skip first files
        if i < opt.start_index:
            continue

        # construct export file name
        filename_export = folder_export / fl.with_suffix(".csv").name
        # skip if file exists and should not be overwritten
        if filename_export.exists() and opt.no_overwrite:
            continue

        data = parse(fl, rename_hfdata=True)

        id = data.hash_g_code()
        # extract tool information and limit signals to this exact tool
        tool_info, lim = get_tool_info(data, keys_toolinfo)

        n_rows, n_cols = data.get_item("HFData", limit_to=lim).shape
        info.append({
            "filename": fl.name,
            "n_rows": n_rows, "n_cols": n_cols,
            "date": data["HFData", "Time"][0],
            **tool_info,
            "G code hash": id,
        })

        # export HFData
        if not opt.only_info:
            columns_to_exclude = ["CYCLE", "HFProbeCounter"] #+ ["Time"]
            columns = [el for el in data["HFData"].columns if el not in columns_to_exclude]

            data.get_item("HFData", columns, not_na=True, limit_to=lim).to_csv(
                filename_export,
                header=True,
                index=False
            )

        k += 1

    df = pd.DataFrame(info)
    # sort by recording date
    df.sort_values(by="date", inplace=True, ignore_index=True)

    info_file = folder_export / "info.csv"
    while True:
        i = 1
        if info_file.exists():
            info_file = info_file.with_stem(f"info_{i}")
        else:
            break
        i += 1
    df.to_csv(info_file, header=True, index=False)

    find_changed_rows(df[keys_toolinfo])
    print(f"Exported {k} files + {info_file} to {opt.destination}.")
