import pandas as pd

from typing import Dict, List, Tuple, Union

from HeaderData import HeaderData, SignalHeaderHF, SignalHeaderLF, TimeInfo
from parse_payload import construct_time
from utils import get_signal_name_head, hash_list

# workaround to construct the type
dict_keys = type({}.keys())


class CapturePayload:
    def __init__(self, data, timeinfo: TimeInfo = None) -> None:
        self.data = construct_time(data, timeinfo) if timeinfo else data
        # organize signal names into groups
        self._grouped_signals = self._group_signals(self.data)

    def __repr__(self) -> str:
        repr_data = {ky: vl.shape for ky, vl in self.data.items()}
        return f"CapturePayload(data={repr_data})"

    def __getitem__(
            self,
            item: Union[str, Tuple[str, Union[str, int]]]
    ):
        if not isinstance(item, (tuple, list)):
            item = (item, )

        group = item[0]
        key = item[1] if len(item) > 1 else None
        as_timeseries = item[2] if len(item) > 2 else False

        return self.get_item(
            group=group,
            key=key,
            as_timeseries=as_timeseries
        )

    def get_item(
            self,
            group: str,
            key: str = None,
            as_timeseries: bool = False,
            not_na: bool = False,
            limit_to_hfdata: bool = True,
            limit_to_time: pd.Timestamp = None
    ) -> pd.DataFrame | pd.Series:
        # query data
        df = self.data[group][key] if key else self.data[group]

        if limit_to_hfdata and ("HFTimestamp" in self.data) and ("HFProbeCounter" in self.data[group]):
            # get last / maximum probe counter
            hfprobe_counter_max = self.data["HFTimestamp"]["HFProbeCounter"].iloc[-1]
            # crop data
            lg = self.data[group]["HFProbeCounter"] <= hfprobe_counter_max
            df = df[lg]

        if limit_to_time and ("Time" in self.data[group]):
            lg = self.data[group]["Time"] <= limit_to_time
            df = df[lg[df.index]]

        if as_timeseries and ("Time" in self.data[group]):
            df.set_index(self.data[group]["Time"][df.index], inplace=True)

        return df.dropna(axis="index", how="all") if not_na else df

    def keys(self) -> dict_keys:
        return self.data.keys()

    def groups(self) -> dict_keys:
        return self.keys()

    @staticmethod
    def _group_signals(data: Dict[str, pd.DataFrame]):
        # group signals
        grouped: Dict[str, Dict[str, List[str]]] = dict()
        for ky, df in data.items():
            grouped[ky] = group_signal_names(df.columns)
        return grouped

    def groupby(
            self,
            group: str,
            key: str,
            as_timeseries: bool = False,
    ) -> pd.DataFrame:
        # query data
        keys = self._grouped_signals[group][key]
        df = self.data[group][keys]

        if as_timeseries and "Time" in self.data[group]:
            df.set_index(self.data[group]["Time"], inplace=True)

        return df

    def hash_g_code(self) -> str:
        group = "HFBlockEvent"
        key = "GCode"
        if group not in self.data:
            raise Exception(f"Group {group} not in data. No G-Code hashing available")
        if key not in self.data[group]:
            raise Exception(f"Signal {key} not in data['{group}']. No G-Code hashing available")
        return hash_list(self[group, key])


def group_signal_names(signals: List[SignalHeaderHF] | List[SignalHeaderLF] | List[str]):
    group: Dict[str, List[SignalHeaderHF] | List[SignalHeaderLF] | List[str]] = dict()
    for sig in signals:
        if isinstance(sig, str):
            string = sig
        elif isinstance(sig, (SignalHeaderHF, SignalHeaderLF)):
            string = sig.address
        else:
            raise Exception(f"Input type {type(sig)} was not expected.")

        name = get_signal_name_head(string)

        if name not in group:
            group[name] = []
        group[name] += [sig]

    return group
