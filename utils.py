import re

re_signal_name_head = re.compile("[\w\-\.:]+(?=\|\d)", re.ASCII)


def get_signal_name_head(name: str) -> str:
    m = re_signal_name_head.match(name)
    if m is None:
        return name
    else:
        return m.group()
