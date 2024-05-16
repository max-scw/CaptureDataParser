import logging

import numpy as np
from matplotlib import pyplot as plt
from PIL import Image

from get_data_characteristics import get_data_characteristics
from utils import (
    default_argument_parser,
    parse_arguments,
    get_files,
    get_signal
)


if __name__ == "__main__":
    parser = default_argument_parser()
    parser.add_argument("--signal", type=str,  nargs="+", help="Signal key")

    parser.add_argument("--min-value", type=float, default=None, help="Minimum value of signals.")
    parser.add_argument("--max-value", type=float, default=None, help="Maximum value of signals.")

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
    opt = parse_arguments(parser)

    # process data
    if (opt.max_value is None) or (opt.min_value is None) or (opt.max_length is None):
        characteristics = get_data_characteristics(
            opt.signal,
            directory=opt.source,
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

    # Get the color map by name:
    cm = plt.get_cmap("viridis")

    # per signal
    mat = []
    for key_sig in opt.signal:
        # loop over files
        ky_flt = None
        for fl, df, ky_flt in get_files(
                directory=opt.source,
                path_to_metadata=opt.path_to_metadata,
                filter_key=opt.filter_key,
                start_index=opt.start_index
        ):
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

            mat.append(column)

        # pad image
        fill_value = (cm([0])[:, :3] * 255).astype(np.uint8)
        # maximum length
        n = max(map(len, mat))
        # pad matrix
        mat = [np.vstack((el, fill_value.repeat(n - len(el), axis=0))) for el in mat]

        # stack columns and convert to image
        img = Image.fromarray(np.stack(mat, axis=1))
        # save image
        filename_parts = ["WFD", key_sig.replace('|', '-')]
        if ky_flt:
            filename_parts += list(ky_flt)
        filename = "_".join(filename_parts) + ".png"
        # save image
        img.save(filename)

        logging.info(f"Image saved to {filename}.")
    logging.info(f"done {__file__}.")
