from typing import Dict, List, Tuple, Union
from HeaderData import HeaderData, SignalHeader

from utils import get_signal_name_head


class CaptureHeader:
    def __init__(self, header: HeaderData) -> None:
        self.__header = header

        # inherit data model attributes
        for ky, val in dict(self.__header).items():
            setattr(self, ky, val)

        self.grouped_signals = self._group_signals()

    def __repr__(self) -> str:
        text = []
        for ky, vl in dict(self.__header).items():
            if ky == "signals":
                msg = {ky: [el.name for el in vl] for ky, vl in self.__header.signals.items()}
            else:
                msg = dict(vl)

            text.append(f"{ky}: {msg}")
        return f"CaptureHeader({', '.join(text)})"

    def _group_signals(self):
        # group signals
        grouped: Dict[str, Dict[str, List[SignalHeader]]] = dict()
        for ky, vl in self.signals.items():
            grouped[ky] = group_signal_names(vl)
        return grouped

    def groupby(self, signal: str, key: str, attribute: str = None) -> Union[Dict[str, List[SignalHeader]], List[SignalHeader], List[str]]:
        if signal in self.grouped_signals:
            if key in self.grouped_signals[signal]:
                if attribute is not None:
                    if attribute in SignalHeader.__fields__:
                        return [el.__getattribute__(attribute) for el in self.grouped_signals[signal][key]]
                    else:
                        raise Exception(f"Unknown attribute {attribute} for key {key}.")
                else:
                    return self.grouped_signals[signal][key]
            else:
                raise Exception(f"Unknown key for signal {signal}. Signal has only these keys: {self.grouped_signals[signal].keys()}.")
        else:
            raise Exception(f"Unknown signal {signal}. Signals can be grouped by {self.grouped_signals.keys()}")

    def get_signal_header(self, group: str, name: str) -> SignalHeader | None:
        for sig in self.signals[group]:
            if sig.name == name:
                return sig
        return None

def group_signal_names(signals: List[SignalHeader]):
    group: Dict[str, List[SignalHeader]] = dict()
    for sig in signals:
        string = sig.address
        name = get_signal_name_head(string)

        if name not in group:
            group[name] = []
        group[name] += [sig]

    return group