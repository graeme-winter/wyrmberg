import json
import zmq
import time
import sys
import os


def process_headers(series, headers):
    """Process headers from 0mq stream: simplon API 1.8"""

    for j, h in enumerate(headers):
        with open(f"{series}{os.sep}headers.{j}", "wb") as f:
            f.write(h)


def process_image(series, image, frame_id):
    """Process image packet"""

    for j, h in enumerate(image):
        with open(f"{series}{os.sep}{frame_id:06d}.{j}", "wb") as f:
            f.write(h)


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


main()
