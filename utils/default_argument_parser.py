import logging
import sys

import argparse
from setproctitle import setproctitle

from utils import cast_logging_level


def default_argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=str, help="Directory where zipped recordings are stored", required=False)
    parser.add_argument("--destination", type=str, default="",
                        help="Directory where extracted recordings should be placed to")
    parser.add_argument("--file-extension", type=str, default="", help="File type")
    parser.add_argument("--start-index", type=int, default=0, help="ith file to start from")

    parser.add_argument('--process-title', type=str, default=None, help="Names the process")
    parser.add_argument("--logging-level", type=str, default="DEBUG", help="Logging level")
    return parser


def parse_arguments(parser: argparse.ArgumentParser) -> argparse.Namespace:
    opt = parser.parse_args()

    if "process_title" in opt and opt.process_title:
        setproctitle(opt.process_title)

    # setup logging
    if "logging_level" in opt:
        logging.basicConfig(
            level=cast_logging_level(opt.logging_level, logging.INFO),
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                # logging.FileHandler(Path(get_env_variable("LOGFILE", "log")).with_suffix(".log")),
                logging.StreamHandler(sys.stdout)
            ],
        )

    return opt

