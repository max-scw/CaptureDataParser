import pandas as pd

from typing import Dict, List, Tuple, Union

from HeaderData import HeaderData, SignalHeader
from parse_payload import construct_time


class CapturePayload:
    def __init__(self, data, header: HeaderData) -> None:
        self.data = construct_time(data, header.time)
        self.head = header

    def __repr__(self) -> str:
        repr_data = {ky: vl.shape for ky, vl in self.data.items()}
        return f"CapturePayload(data={repr_data}, header={self.head})"

    def groupby(
            self,
            signal: str,
            key: str,
            as_timeseries: bool = False,
            rename_signals: bool = True
    ) -> pd.DataFrame:
        if as_timeseries and "Time" in self.data[signal]:
            pass
        else:
            as_timeseries = False

        # query data
        columns = self.head.groupby(signal, key, "name") + ["Time"] if as_timeseries else []
        df = self.data[signal][columns]

        if as_timeseries:
            df.set_index("Time", inplace=True)

        if rename_signals:
            columns_new = [f"{key}|{el}" for el in self.head.groupby(signal, key, "axis")]
            df.columns = columns_new
        return df
