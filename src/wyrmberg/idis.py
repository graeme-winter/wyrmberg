from __future__ import annotations

import array
import os
import sys
import time

import h5py

from . import now


class h5_data_file:
    def __init__(self, filename, dsetname, frames, offset):
        self.filename = filename
        self.dsetname = dsetname
        self.file = None
        self.dset = None
        self.finished = False
        self.chunk_sizes = array.array("L", (0 for j in range(frames)))
        self.frames = frames
        self.offset = offset


def watcher(h5_data_files):
    """Pass a list of h5_data_file structures, will print the data file name,
    image number and chunk size information as soon as the images become
    readable."""

    finished = False

    # FIXME add timeouts

    while not finished:
        finished = True

        # FIXME check timeout

        for h5 in h5_data_files:

            if h5.finished:
                continue

            finished = False

            if h5.file is None:
                if not os.path.exists(h5.filename):
                    continue
                h5.file = h5py.File(h5.filename, "r", swmr=True)
                h5.dset = h5.file[h5.dsetname]
            else:
                h5.dset.id.refresh()

            h5.finished = True

            for j in range(h5.frames):
                if h5.chunk_sizes[j]:
                    continue
                s = h5.dset.id.get_chunk_info_by_coord((j, 0, 0))
                if s.size == 0:
                    h5.finished = False
                else:
                    h5.chunk_sizes[j] = s.size
                    t0 = time.time()
                    _, chunk = h5.dset.id.read_direct_chunk((j, 0, 0))
                    t1 = time.time()
                    print(f"READ {h5.offset + j} {len(chunk)} {t1 - t0:.6f} {now}")

            if h5.finished:
                h5.dset = None
                h5.file.close()

        time.sleep(0.1)

    return


def vds_info(root, master, dataset):
    """Read the VDS, construct the list of h5_data_file objects based on this
    and then go watch them for when the files appear..."""

    plist = dataset.id.get_create_plist()

    assert plist.get_layout() == h5py.h5d.VIRTUAL

    h5_data_files = []

    virtual_count = plist.get_virtual_count()

    for j in range(virtual_count):
        filename = plist.get_virtual_filename(j)
        dsetname = plist.get_virtual_dsetname(j)

        # Do a shuffle in here to get to the real filename if this is an
        # internal reference to an external file

        if filename == ".":
            link = master.get(dsetname, getlink=True)
            filename = os.path.join(root, link.filename)
            dsetname = link.path

        vspace = plist.get_virtual_vspace(j)
        frames = vspace.get_regular_hyperslab()[3][0]
        offset = vspace.get_regular_hyperslab()[0][0]
        print(filename, dsetname, frames, offset)
        h5_data_files.append(h5_data_file(filename, dsetname, frames, offset))

    watcher(h5_data_files)


if __name__ == "__main__":
    with h5py.File(sys.argv[1], "r", swmr=True) as f:
        d = f["/entry/data/data"]
        vds_info(os.path.split(sys.argv[1])[0], f, d)
