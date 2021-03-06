from __future__ import annotations

import json
import sys
import time
import os
from typing import Dict, Union

import h5py
import numpy
import zmq
import hdf5plugin

# FIXME add logging inc. progress for an external process...

# FIXME turn this into a class

# FIXME add a configuration system to this using configparser ->
# the address of the source would go into a configuration, along with
# frames per block etc.

# FIXME I probably have enough information in the header packet to
# make a master / nxs file with virtual data sets etc. (optional)

MAX_FRAMES_PER_BLOCK = 1000
COMPRESSION = {"compression": 32008, "compression_opts": (0, 2)}

PREFIX = None

blocks = {}
frames_per_block = {}
datasets = {}
meta = None

meta_info = {}  # type: Dict[str, Dict[int, Union[float,int,str]]]


def save_chunk(series, frame, messages):
    """Save a chunk into one of the HDF5 files"""
    block = frame // MAX_FRAMES_PER_BLOCK

    # push the chunk metadata to the right places (at least, stash)

    part1 = json.loads(messages[0].decode())
    part2 = json.loads(messages[1].decode())
    part4 = json.loads(messages[3].decode())

    meta_info["hash"][frame] = part1["hash"]
    meta_info["encoding"][frame] = part2["encoding"]
    meta_info["size"][frame] = part2["size"]
    meta_info["datatype"][frame] = part2["type"]
    meta_info["real_time"][frame] = part4["real_time"]
    meta_info["start_time"][frame] = part4["start_time"]
    meta_info["stop_time"][frame] = part4["stop_time"]

    for name in "frame", "frame_written", "offset_written":
        meta_info[name][frame] = frame

    # no idea what this is for
    meta_info["frame_series"][frame] = 0

    # now save the actual data...

    if block not in blocks:

        dtype = getattr(numpy, part2["type"])
        NY, NX = tuple(part2["shape"])

        blocks[block] = h5py.File(f"{PREFIX}_{block+1:06d}.h5", "w", libver="latest")
        datasets[block] = blocks[block].create_dataset(
            "data",
            shape=(MAX_FRAMES_PER_BLOCK, NY, NX),
            chunks=(1, NY, NX),
            dtype=dtype,
            **COMPRESSION,
        )
        blocks[block].swmr_mode = True
        frames_per_block[block] = 0

    chunk = messages[2]

    # FIXME add a debug mode where we have an assertion here that
    # this chunk size is the same as the packet chunk size claims

    offset = (frame % MAX_FRAMES_PER_BLOCK, 0, 0)

    datasets[block].id.write_direct_chunk(offset, chunk, 0)
    blocks[block].flush()

    # should probably have a check in here too that we can flush and close the file
    # once all the images have been written

    frames_per_block[block] += 1

    if frames_per_block[block] == MAX_FRAMES_PER_BLOCK:
        blocks[block].close()
        del blocks[block]
        del datasets[block]


def process_headers(series, headers):
    """Process headers from 0mq stream: simplon API 1.8, pushes content
    of this and the image metadata HDF5"""

    global meta, meta_info

    assert not meta
    assert not meta_info

    for k in [
        "datatype",
        "encoding",
        "frame",
        "frame_series",
        "frame_written",
        "hash",
        "offset_written",
        "real_time",
        "size",
        "start_time",
        "stop_time",
    ]:
        meta_info[k] = {}

    meta = h5py.File(f"{PREFIX}_meta.h5", "w")

    meta.create_dataset("series", data=series)

    _dectris = meta.create_group("_dectris")

    # yes this is super evil...
    config = eval(headers[1].decode().replace("true", "True").replace("false", "False"))
    for k in sorted(config):
        _dectris.create_dataset(k, data=config[k])

    # while we are here, check the number of images we are expecting and push
    # somewhere so we can add a progress bar

    # unpack header data into meta.h5 - the first part does not contain
    # anything which is useful - N.B. no copy operations per se yet...
    # switching the sizes around is very annoying

    flatfield_xy = tuple(json.loads(headers[2].decode())["shape"])
    mask_xy = tuple(json.loads(headers[4].decode())["shape"])
    countrate_xy = tuple(json.loads(headers[6])["shape"])

    meta.create_dataset("config", data=headers[1].decode())
    meta.create_dataset(
        "flatfield",
        data=numpy.frombuffer(headers[3], dtype=numpy.float32).reshape(
            flatfield_xy[1], flatfield_xy[0]
        ),
    )
    meta.create_dataset(
        "mask",
        data=numpy.frombuffer(headers[3], dtype=numpy.uint32).reshape(
            mask_xy[1], mask_xy[0]
        ),
    )
    meta.create_dataset(
        "countrate",
        data=numpy.frombuffer(headers[7], dtype=numpy.float32).reshape(
            countrate_xy[0], countrate_xy[1]
        ),
    )


def process_image(series, image, frame):
    """Process image packet"""
    save_chunk(series, frame, image)


def capture(endpoint, prefix):
    """Spin up a stream receiver to grab the data"""
    print(f"Connecting to data source: {endpoint}")
    print(f"Capturing data to {prefix}")

    directory = os.path.dirname(prefix)
    if not os.path.exists(directory):
        os.makedirs(directory)

    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect(endpoint)

    global PREFIX, meta, meta_info
    PREFIX = prefix

    frames = 0

    while True:
        messages = socket.recv_multipart()

        m0 = json.loads(messages[0].decode())

        htype = m0["htype"]
        series = m0["series"]

        if htype.startswith("dheader"):
            t0 = time.time()
            process_headers(series, messages)
        elif htype.startswith("dimage"):
            process_image(series, messages, m0["frame"])
            frames += 1
        elif htype.startswith("dseries"):
            print(f"Acquisition time: {time.time() - t0:.2f}s for {frames} images")

            for block in list(blocks):
                blocks[block].close()
                del blocks[block]
                del datasets[block]

            # flush the metadata tables - there are probably more efficient ways
            # of doing this...

            for k in [
                "datatype",
                "encoding",
                "frame",
                "frame_series",
                "frame_written",
                "hash",
                "offset_written",
                "real_time",
                "size",
                "start_time",
                "stop_time",
            ]:
                table = meta_info[k]
                str_types = ["datatype", "encoding", "hash"]
                if k in str_types:
                    values = numpy.array(
                        [table[image].encode("utf8") for image in sorted(table)]
                    )
                else:
                    values = numpy.array([table[image] for image in sorted(table)])
                meta.create_dataset(k, data=values)

            meta.close()

            meta = None
            meta_info = {}

            return prefix


def main():
    if len(sys.argv) != 3:
        sys.exit(f"{sys.argv[0]} tcp://i03-eiger01.diamond.ac.uk:9999 /path/to/prefix")

    endpoint, prefix = sys.argv[1], sys.argv[2]

    capture(endpoint, prefix)


if __name__ == "__main__":
    main()
