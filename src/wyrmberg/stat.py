from __future__ import annotations

import sys

from idis import h5_data_file, watcher


def stat(template, first, last):

    h5_data_files = []

    for j in range(first, last + 1):
        filename = template % j
        dsetname = "/data"

        frames = 1000
        offset = 1000 * (j - 1)
        h5_data_files.append(h5_data_file(filename, dsetname, frames, offset))

    watcher(h5_data_files)


if __name__ == "__main__":
    stat(sys.argv[1], 1, int(sys.argv[2]))
