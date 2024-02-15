from typing import Dict, List, Tuple, Union
from HeaderData import HeaderData, SignalHeaderHF, SignalHeaderLF

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
        grouped: Dict[str, Dict[str, List[SignalHeaderHF] | List[SignalHeaderLF]]] = dict()
        for ky, vl in self.signals.items():
            grouped[ky] = group_signal_names(vl)
        return grouped

    def groupby(
            self,
            group: str,
            key: str,
            attribute: str = None
    ) -> Union[Dict[str, List[SignalHeaderHF] | List[SignalHeaderLF]], List[SignalHeaderHF], List[SignalHeaderLF], List[str]]:
        if group in self.grouped_signals:
            if key in self.grouped_signals[group]:
                if attribute is not None:
                    if attribute in self.signals[group][0].__fields__:
                        return [el.__getattribute__(attribute) for el in self.grouped_signals[group][key]]
                    else:
                        raise Exception(f"Unknown attribute {attribute} for key {key}.")
                else:
                    return self.grouped_signals[group][key]
            else:
                raise Exception(f"Unknown key for signal {group}. Signal has only these keys: {self.grouped_signals[group].keys()}.")
        else:
            raise Exception(f"Unknown signal {group}. Signals can be grouped by {self.grouped_signals.keys()}")

    def get_signal_header(self, group: str, name: str) -> SignalHeaderHF | SignalHeaderLF | None:
        for sig in self.signals[group]:
            if sig.name == name:
                return sig
        return None


def group_signal_names(signals: List[SignalHeaderHF] | List[SignalHeaderLF]):
    group: Dict[str, List[SignalHeaderHF] | List[SignalHeaderLF]] = dict()
    for sig in signals:
        string = sig.address
        name = get_signal_name_head(string)

        if name not in group:
            group[name] = []
        group[name] += [sig]

    return group