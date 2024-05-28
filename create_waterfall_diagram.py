import logging
from pathlib import Path

import numpy as np
from matplotlib import pyplot as plt
from PIL import Image

from get_data_characteristics import get_data_characteristics
from utils import (
    default_argument_parser,
    parse_arguments,
    get_list_of_files,
    get_files,
    get_signal
)


if __name__ == "__main__":
    parser = default_argument_parser()
    parser.add_argument("--signal", type=str,  nargs="+", help="Signal key")

    parser.add_argument("--min-value", type=float, default=None, help="Minimum value of signals.")
    parser.add_argument("--max-value", type=float, default=None, help="Maximum value of signals.")
    parser.add_argument("--recordings-to-highlight", type=str, default=None,
                        help="File that lists the recordings to highlight")
    # add arguments to aggregate signals
    parser.add_argument("--window-size", type=float, default=-1,
                        help="Window size. See --in-seconds to make it a duration")
    parser.add_argument("--limit", type=float, default=-1,
                        help="Limit the signal length. May be an index or a duration if --in-seconds is used.")
    parser.add_argument("--in-seconds", action="store_true",
                        help="Window size and limit will be interpreted as a time value.")
    parser.add_argument("--method", type=str, default="rms",
                        help="Aggregation method. Can be 'rms', 'sum', 'mean', 'absSum', or 'absMean'.")
    # meta data
    parser.add_argument("--path-to-metadata", type=str, default=None,
                        help="Metadata file or file pattern (usually called 'info.csv')")
    parser.add_argument("--filter-key", type=str, nargs="+", default=None,  # "/Channel/State/actTNumber"
                        help="Column in meta data file (e.g. '/Channel/State/actTNumber').")
    parser.add_argument("--min-n-recordings", type=int, default=0,
                        help="Number of examples that need to exist to draw a diagram.")

    opt = parse_arguments(parser)

    # process data
    if (opt.max_value is None) or (opt.min_value is None) or (opt.max_length is None):
        logging.info("determine characteristic data values")
        characteristics = get_data_characteristics(
            opt.signal,
            data_directory=opt.source,
            file_extension=opt.file_extension,
            path_to_metadata=opt.path_to_metadata,
            filter_key=opt.filter_key,
            start_index=opt.start_index,
            in_seconds=opt.in_seconds
        )
        if opt.min_value is None:
            opt.min_value = characteristics["min_value"]
        if opt.max_value is None:
            opt.max_value = characteristics["max_value"]
        if (opt.limit is None) or (opt.limit < 0):
            opt.limit = characteristics["max_length"]

        logging.debug(opt)

    # --- main functionality
    stems_to_highlight = []
    if opt.recordings_to_highlight is not None:
        file_highlights = Path(opt.recordings_to_highlight)
        if file_highlights.exists() and file_highlights.is_file():
            # read file
            with open(file_highlights, "r") as fid:
                lines = fid.readlines()
            stems_to_highlight = [el.strip() for el in lines]
        else:
            raise FileNotFoundError(f"File {file_highlights.as_posix()} with recordings to highlight not found.")

    # Get the color map by name:
    cm = plt.get_cmap("viridis")

    h_highlight = 20  # pixel
    # per signal
    for key_sig in opt.signal:
        # loop over files
        for files, ky_flt in get_list_of_files(
                data_directory=opt.source,
                file_extension=opt.file_extension,
                path_to_metadata=opt.path_to_metadata,
                filter_keys=opt.filter_key,
                n_min=opt.min_n_recordings
        ):
            lines = []
            highlight = []
            for fl, df in get_files(files=files, start_index=opt.start_index):
                sig = get_signal(
                    df,
                    key_sig,
                    window_size=opt.window_size,
                    method=opt.method,
                    in_seconds=opt.in_seconds,
                    limit=opt.limit
                )

                # normalize signal
                sig_nrm = (sig - opt.min_value) / (opt.max_value - opt.min_value)
                # pad signal

                # Apply the colormap like a function to any array:
                colored_image = cm(sig_nrm)

                # Obtain a 4-channel image (R,G,B,A) in float [0, 1]
                # But we want to convert to RGB in uint8 and save it:
                # (colored_image[:, :, :3] * 255).astype(np.uint8)
                column = (colored_image[:, :3] * 255).astype(np.uint8)

                lines.append(column)
                # add info whether this recording should be highlighted or not
                highlight.append(True if fl.stem in stems_to_highlight else False)

            # pad image
            fill_value = (cm([0])[:, :3] * 255).astype(np.uint8)
            # maximum length
            n = max(map(len, lines))
            # pad matrix
            lines_rgb = [np.vstack((el, fill_value.repeat(n - len(el), axis=0))) for el in lines]
            # stack columns to matrix
            mat = np.stack(lines_rgb, axis=1)

            # add highlights
            if highlight:
                # add row of zero followed by max value
                sz_0 = (5, len(lines_rgb), 3)
                sz_255 = (h_highlight, len(lines_rgb), 3)

                rows = [
                    np.zeros(sz_0).astype(np.uint8),
                    np.repeat(
                        np.repeat(
                            np.array(highlight, dtype=np.uint8).reshape((1, -1, 1)) * 255,
                            repeats=h_highlight,
                            axis=0
                        ),
                        repeats=3,
                        axis=2
                    )
                ]
                # add rows to matrix
                mat = np.vstack([mat] + rows)

            # stack columns and convert to image
            img = Image.fromarray(mat)
            # save image
            filename_parts = ["WFD"]
            if ky_flt:
                filename_parts += [f"{el:g}" if isinstance(el, float) else f"{el}" for el in ky_flt]
            filename_parts += [key_sig.replace('|', '-'), f"{len(lines_rgb)}"]
            filename = "_".join(filename_parts) + ".png"
            # save image
            img.save(filename)

            logging.info(f"Image saved to {filename}.")
    logging.info(f"done {__file__}.")
