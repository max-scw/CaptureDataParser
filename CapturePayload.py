import hashlib

import pandas as pd

from typing import Dict, List, Tuple, Union

from HeaderData import HeaderData, SignalHeaderHF, SignalHeaderLF
from CaptureHeader import CaptureHeader
from parse_payload import construct_time
from utils import get_signal_name_head, hash_list


class CapturePayload:
    def __init__(self, data, header: HeaderData | CaptureHeader) -> None:
        self.data = construct_time(data, header.time)
        self.head = header

    def __repr__(self) -> str:
        repr_data = {ky: vl.shape for ky, vl in self.data.items()}
        return f"CapturePayload(data={repr_data}, header={self.head})"

    def __getitem__(
            self,
            item: Union[str, Tuple[str, Union[str, int]]]
    ):
        if not isinstance(item, (tuple, list)):
            item = (item, )

        group = item[0]
        key = item[1] if len(item) > 1 else None
        as_timeseries = item[2] if len(item) > 2 else False
        rename_signals = item[3] if len(item) > 3 else False

        # query data
        df = self.data[group][key] if key else self.data[group]

        if as_timeseries and "Time" in self.data[group]:
            df.set_index("Time", inplace=True)

        if rename_signals:
            columns_new = []
            for col in df.columns:
                # get_signal_name_head(col)
                sig_head = self.head.get_signal_header(group, col)
                if sig_head is not None:
                    col_new = sig_head.name
                    if isinstance(sig_head, SignalHeaderHF) and sig_head.axis.lower() not in ["cycle"]:
                        col_new += f"|{sig_head.axis}"
                        # TODO: what are the axis for NC-variables?
                else:
                    col_new = col
                columns_new.append(col_new)
            # rename columns
            df.columns = columns_new
        return df

    def groupby(
            self,
            group: str,
            key: str,
            as_timeseries: bool = False,
            rename_signals: bool = True
    ) -> pd.DataFrame:
        # query data
        df = self.data[group][self.head.groupby(group, key, "name")]

        if as_timeseries and "Time" in self.data[group]:
            df.set_index(self.data[group]["Time"], inplace=True)

        if rename_signals:
            columns_new = [f"{key}|{el}" for el in self.head.groupby(group, key, "axis")]
            df.columns = columns_new
        return df

    def hash_g_code(self) -> str:
        return hash_list(self["HFBlockEvent", "GCode"])
