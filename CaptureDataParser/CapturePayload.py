import datetime
import numpy as np
import pandas as pd


from typing import Dict, List, Tuple, Union, Literal

from CaptureDataParser.HeaderData import SignalHeaderHF, SignalHeaderLF, TimeInfo
from CaptureDataParser.parse_payload import construct_time
from CaptureDataParser.utils import get_signal_name_head, hash_list, check_key_pattern

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
        index_as = item[2] if len(item) > 2 else None

        return self.get_item(
            group=group,
            key=key,
            index_as=index_as
        )

    def _check_key(self, group, key):
        if key not in self.data[group]:
            # assume regex pattern
            new_ky = check_key_pattern(self.data[group].columns, key)
            if new_ky:
                key = new_ky
            else:
                raise KeyError(f"Key {key} not in self.data[{group}].")
        return key

    def get_item(
            self,
            group: str,
            key: str | List[str] = None,
            index_as: Literal["timeseries", "HFProbeCounter", "counter", None] = None,
            not_na: bool = False,
            limit_to: Literal["hfdata"] | int | datetime.datetime = None,
    ) -> pd.DataFrame | pd.Series:
        # query data
        if key:
            if isinstance(key, (list, tuple)):
                # assume regex pattern
                keys = []
                for ky in key:
                    keys.append(self._check_key(group, ky))
            else:
                keys = self._check_key(group, key)

            df = self.data[group][keys]
        else:
            df = self.data[group]

        # limit rows
        if (
                isinstance(limit_to, str) and
                (limit_to.lower() == "hfdata") and
                ("HFTimestamp" in self.data) and ("HFProbeCounter" in self.data[group])
        ):
            # get last / maximum probe counter
            hfprobe_counter_max = self.data["HFTimestamp"]["HFProbeCounter"].iloc[-1]
            # crop data
            lg = self.data[group]["HFProbeCounter"] <= hfprobe_counter_max
            df = df[lg]
        elif (
                isinstance(limit_to, (int, np.int32, np.int64)) and
                (("CYCLE" in self.data[group]) or ("HFProbeCounter" in self.data[group]))
        ):
            ky = "CYCLE" if "CYCLE" in self.data[group] else "HFProbeCounter"
            lg = self.data[group][ky] < limit_to
            df = df[lg[df.index]]
        elif isinstance(limit_to, datetime.datetime) and ("Time" in self.data[group]):
            lg = self.data[group]["Time"] < limit_to
            df = df[lg[df.index]]

        # set different index
        if isinstance(index_as, str):
            if (index_as.lower() == "timeseries") and ("Time" in self.data[group]):
                df.set_index(self.data[group]["Time"][df.index], inplace=True)
            elif (index_as.lower() in ("hfprobecounter", "counter")) and ("HFProbeCounter" in self.data[group]):
                df.set_index(self.data[group]["HFProbeCounter"][df.index], inplace=True)

        return df.dropna(axis="index", how="all") if not_na else df

    def keys(self) -> dict_keys:
        return self.data.keys()

    def groups(self) -> dict_keys:
        return self.keys()

    def group_signals(self, key: str = None) -> Union[Dict[str, List[str]], Dict[str, Dict[str, List[str]]]]:
        if key in self._grouped_signals:
            return self._grouped_signals[key]
        else:
            return self._grouped_signals


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
            **kwargs
    ) -> pd.DataFrame:
        # query data
        keys = self._grouped_signals[group][key]
        df = self.get_item(group, keys, **kwargs)
        return df

    def hash_g_code(self) -> str:
        group = "HFBlockEvent"
        key = "GCode"
        if group not in self.data:
            raise Exception(f"Group {group} not in data. No G-Code hashing available.")
        if key not in self.data[group]:
            raise Exception(f"Signal {key} not in data['{group}']. No G-Code hashing available.")
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
