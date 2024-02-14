import re

from typing import Dict, List, Tuple, Union
from HeaderData import HeaderData, SignalHeader


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


def group_signal_names(signals: List[SignalHeader]):
    re_group = re.compile("[\w\-\.:]+(?=\|\d)", re.ASCII)

    group: Dict[str, List[SignalHeader]] = dict()
    for sig in signals:
        string = sig.address
        m = re_group.match(string)
        if m:
            name = m.group()
        else:
            name = string

        if name not in group:
            group[name] = []
        group[name] += [sig]

    return group