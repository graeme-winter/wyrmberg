import json
import zmq
import time
import sys
import os
import h5py
import numpy
import hdf5plugin

FRAMES_PER_BLOCK = 1000
COMPRESSION = {"compression": 32008, "compression_opts": (0, 2)}

blocks = {}
datasets = {}
meta = None

def save_chunk(series, frame, messages):
    """Save a chunk into one of the HDF5 files"""
    block = frame // FRAMES_PER_BLOCK

    # FIXME define the proper shape, data type, etc. -> will need more
    # metadata right here;

    if not block in blocks:
        part2 = json.loads(messages[1].decode())

        dtype = getattr(numpy, part2["type"])
        NY, NX = tuple(part2["shape"])

        blocks[block] = h5py.File(f"{series}_{block+1:06d}.h5", "w")
        datasets[block] = blocks[block].create_dataset(
            "data",
            shape=(FRAMES_PER_BLOCK, NY, NX),
            chunks=(1, NY, NX),
            dtype=dtype,
            **COMPRESSION,
        )

    chunk = messages[2]

    offset = (frame % FRAMES_PER_BLOCK, 0, 0)

    datasets[block].id.write_direct_chunk(offset, chunk, 0)


def process_headers(series, headers):
    """Process headers from 0mq stream: simplon API 1.8, pushes content 
    of this and the image metadata HDF5"""

    global meta

    assert meta is None

    meta = h5py.File(f"{series}_meta.h5", "w")

    meta["series"] = meta.create_dataset("series", data=series)

    # unpack header data into meta.h5 - the first part does not contain
    # anything which is useful - N.B. no copy operations per se yet...

    config = headers[1].decode()
    flatfield_xy = tuple(json.loads(headers[2].decode())["shape"])
    flatfield = numpy.frombuffer(headers[3], dtype=numpy.float32).reshape(flatfield_xy[1], flatfield_xy[0])
    mask_xy = tuple(json.loads(headers[4].decode())["shape"])
    mask = numpy.frombuffer(headers[5], dtype=numpy.uint32).reshape(mask_xy[1], mask_xy[0])
    countrate_xy = tuple(json.loads(headers[6])["shape"])
    countrate = numpy.frombuffer(headers[7], dtype=numpy.float32).reshape(countrate_xy[1], countrate_xy[0])




def process_image(series, image, frame):
    """Process image packet"""
    save_chunk(series, frame, image)


def main():
    context = zmq.Context()

    if len(sys.argv) != 2:
        sys.exit(f"{sys.argv[0]} tcp://i03-eiger01.diamond.ac.uk:9999")

    print(f"Connecting to data source: {sys.argv[1]}")

    socket = context.socket(zmq.PULL)
    socket.connect(sys.argv[1])

    frames = 0

    while True:
        messages = socket.recv_multipart()

        m0 = json.loads(messages[0].decode())

        htype = m0["htype"]
        series = m0["series"]

        if htype.startswith("dheader"):
            t0 = time.time()
            os.mkdir(f"{series}")
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


main()
