import warnings
from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm
import logging

from typing import List

from CaptureDataParser import CapturePayload, parse
from CaptureDataParser.utils import find_changed_rows, check_key_pattern


from utils import default_argument_parser, parse_arguments


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

    # update keys:
    keys_geometry = [check_key_pattern(tool.columns, ky) for ky in keys_geometry]

    tool[keys_geometry] = tool[keys_geometry].replace(to_replace=0, value=np.nan).ffill()

    # find first row where tool keys change
    idxs = find_changed_rows(tool[keys_tool])
    if len(idxs) > 1:
        idx_ = np.unique(idxs)[0]
    else:
        idx_ = tool.index[-1]  # TODO: check indexing

    # get tool info (from last row)
    return tool.loc[idx_, keys_tool], tool.loc[idx_, index]


if __name__ == "__main__":
    parser = default_argument_parser()
    parser.add_argument("--only-info", action="store_true", help="Do not export files, just collect information")
    parser.add_argument("--no-overwrite", action="store_true", help="Do not not overwrite existing files")
    parser.add_argument("--compression", type=str, default=None,
                        help="Compresses exported file "
                             "('bz2', 'gzip', 'tar', 'xz', 'zip', 'zstd').")

    opt = parse_arguments(parser)

    folder_export = Path(opt.destination)
    folder_source = Path(opt.source)

    keys_toolinfo = [
        '/Channel/State/actToolIdent',
        '/Channel/State/actTNumber',
        '/Channel/State/actToolLength1',
        '/Channel/State/actToolLength2',
        '/Channel/State/actToolRadius'
    ]
    # this is a pattern. May continue with an index such as: [u1,1]

    # find files to parse
    files = []
    suffix_export_file = f".{opt.compression}" if opt.compression is not None else ".csv"
    # walk through folders
    for i, el in enumerate(folder_source.iterdir()):
        # skip first folders
        if i < opt.start_index:
            continue

        if el.is_dir():
            # construct export file name
            filename_export = folder_export / el.with_suffix(suffix_export_file).name
            # skip if file exists and should not be overwritten
            if (not filename_export.exists()) or (not opt.no_overwrite):
                files.append(list(el.glob("**/*.json")))

    info = []
    k = 0
    for i, fl in enumerate(tqdm(files)):
        foldername = fl[0].parent.name
        # parse file
        try:
            data = parse(fl, rename_hfdata=True)
        except Exception as ex:
            raise Exception(f"Failed to parse {foldername} with exception") from ex

        try:
            # create unique hash from G code
            id = data.hash_g_code()
        except Exception as ex:
            warnings.warn(
                f"Failed to hash G-code of {foldername} with exception: {ex}"
                "\nSkipping this file."
            )
            continue

        try:
            # extract tool information and limit signals to this exact tool
            tool_info, lim = get_tool_info(data, keys_toolinfo)
        except Exception as ex:
            raise Exception(f"Failed to get tool info {foldername} with the exception: {ex}")

        n_rows, n_cols = data.get_item("HFData", limit_to=lim).shape
        info.append({
            "filename": foldername,
            "n_rows": n_rows, "n_cols": n_cols,
            "date": data["HFData", "Time"][0],
            **tool_info,
            "G code hash": id,
        })

        # export HFData
        if not opt.only_info:
            columns_to_exclude = ["CYCLE", "HFProbeCounter"]
            columns = [el for el in data["HFData"].columns if el not in columns_to_exclude]

            # construct export file name
            filename_export = (folder_export / foldername).with_suffix(suffix_export_file)
            # export to CSV
            data.get_item("HFData", columns, not_na=True, limit_to=lim).to_csv(
                filename_export,
                header=True,
                index=False,
                compression=opt.compression
            )

        k += 1

    # create DataFrame
    df = pd.DataFrame(info)

    # sort by recording date
    if len(df) > 1:
        df.sort_values(by="date", inplace=True, ignore_index=True)

    info_file = folder_export / "info.csv"
    i = 0
    while True:
        if info_file.exists():
            info_file = info_file.with_stem(f"info_{i}")
        else:
            break
        i += 1
    df.to_csv(info_file, header=True, index=False)

    logging.info(f"Exported {k} files + {info_file.name} to {opt.destination}.")
