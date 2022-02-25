import h5py
import zmq
import sys
import time
import os
import random

# we need a series ID for this but it does not really matter what it is
# so long as it is actually constant for a given run
SERIES_ID = random.randint(10000, 20000)


def wait_until(t):
    """Dumb way of doing ersatz-real-time calculations of
    simply doing no-ops until the right time has passed"""

    while time.time() < t:
        pass


# metadata which we want to capture from meta file & use in image
# packets...
meta_info = {}


def make_send_header(socket, meta):
    """Read data from meta file to recreate the Eiger header packet and
    send this down the pipe"""

    # pull the stuff I need from the meta file - this may involve
    # copying large (72 MB) data blocks but I am assuming that this
    # will not be a rate limiting step as it only happens on the
    # virtual detector arm.

    t0 = time.time()
    with h5py.File(meta, "r") as f:

        # pull out the not-in-header meta stuff -> bucket above
        for k in "hash", "start_time", "stop_time", "real_time":
            meta_info[k] = f[k][()]

        part1 = (
            '{"header_detail":"all","htype":"dheader-1.0","series":%d}' % SERIES_ID
        ).encode()
        part2 = f["config"][()]

        # flatfield
        flatfield = f["flatfield"][()]
        ny, nx = flatfield.shape
        part3 = (
            '{"htype":"dflatfield-1.0","shape":[%d,%d],"type":"float32"}' % (nx, ny)
        ).encode()
        part4 = flatfield.tobytes()

        # pixel mask
        mask = f["mask"][()]
        ny, nx = mask.shape
        part5 = (
            '{"htype":"dpixelmask-1.0","shape":[%d, %d],"type":"uint32"}' % (nx, ny)
        ).encode()
        part6 = mask.tobytes()

        # countrate correction table
        countrate = f["countrate"][()]
        shape = countrate.shape
        part7 = (
            '{"htype":"dcountrate_table-1.0","shape":[%d,%d],"type":"float32"}' % shape
        ).encode()
        part8 = countrate.tobytes()

    # now send all this down the pipe
    t1 = time.time()
    socket.send_multipart((part1, part2, part3, part4, part5, part6, part7, part8))
    t2 = time.time()

    print(f"Times to make and send header: {t1 - t0:.2f} / {t2 - t1:.2f}")


def chunk_generator(nxs):
    """Python generator to produce the HDF5 compressed chunks. Yields
    the bit depth, dimensions, length of chunk and the chunk itself."""

    with h5py.File(nxs, "r") as f:
        data = f["/entry/data"]

        for k in sorted(d for d in data if d.startswith("data_")):
            dataset = data[k]
            depth = 8 * int(round(dataset.nbytes / dataset.size))
            d_id = dataset.id

            chunks, ny, nx = dataset.shape

            for j in range(chunks):
                offset = (j, 0, 0)
                filter_mask, chunk = d_id.read_direct_chunk(offset)
                yield (depth, (nx, ny), len(chunk), chunk)


def make_send_data(socket, nxs):
    """Make and send the data packets, assuming all the data are visible
    from nxs[/entry/data/data_*]."""

    # to do part4 properly will involve pulling information out from the
    # meta file

    t0 = time.time()
    dt = 0
    FRAME = 0
    for depth, (ny, nx), size, chunk in chunk_generator(nxs):

        start, stop, real, md5 = (
            meta_info["start_time"][FRAME],
            meta_info["stop_time"][FRAME],
            meta_info["real_time"][FRAME],
            meta_info["hash"][FRAME],
        )

        if dt == 0:
            dt = 1.0 / int(1e9 / real)
        t = t0 + (FRAME + 1) * dt

        part1 = (
            '{"frame":%d,"hash":"%s","htype":"dimage-1.0","series":%d}'
            % (FRAME, md5, SERIES_ID)
        ).encode()
        part2 = (
            '{"encoding":"bs%d-lz4<","htype":"dimage_d-1.0","shape":[%d,%d],"size":%d,"type":"uint%d"}'
            % (depth, nx, ny, size, depth)
        ).encode()
        part3 = chunk
        part4 = (
            '{"htype":"dconfig-1.0","real_time":%f,"start_time":%f,"stop_time":%f (real, start, stop)}'
        ).encode()
        wait_until(t)
        socket.send_multipart((part1, part2, part3, part4))
        FRAME += 1

    t1 = time.time()
    print(f"Times to make and send {FRAME} images: {t1 - t0:.2f}")


def make_send_end(socket):
    part1 = ('{"htype":"dseries_end-1.0","series":%d}' % SERIES_ID).encode()
    socket.send_multipart((part1,))


def main():
    """Read data from an Eiger HDF5 file (assuming DLS structure) and publish
    data over zeroMQ as an ersatz SIMPLON 1.8 API data stream."""

    prefix = sys.argv[1]

    meta = f"{prefix}_meta.h5"
    nxs = f"{prefix}.nxs"

    # check that _something_ exists - will work harder at this further down
    assert os.path.exists(nxs)
    assert os.path.exists(meta)

    # push socket - spec states port 9999

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://*:9999")

    make_send_header(socket, meta)
    make_send_data(socket, nxs)
    make_send_end(socket)


main()
