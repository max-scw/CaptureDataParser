from pathlib import Path
from zipfile import ZipFile
from tqdm import tqdm

import argparse
from setproctitle import setproctitle


def extract_files(folder_src: Path, folder_dst: Path = None) -> Path:
    if folder_dst is None:
        folder_dst = folder_src.parent / (folder_src.stem + "_extracted")

    files = list(folder_src.glob("*.zip"))
    for src in tqdm(files):
        dst = folder_dst / src.stem
        try:
            with ZipFile(src, "r") as fid:
                fid.extractall(dst)
        except Exception as ex:
            raise Exception(f"Failed to unzip {src.as_posix()} with exception: {ex}")
    return folder_dst


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=str, help='Directory where zipped recordings are stored')
    parser.add_argument("--destination", type=str, help='Directory where extracted recordings should be placed to')
    parser.add_argument('--process-title', type=str, default=None, help='Names the process')

    opt = parser.parse_args()

    if opt.process_title:
        setproctitle(opt.process_title)

    extract_files(Path(opt.source), Path(opt.destination))
    print("all files extracted.")
