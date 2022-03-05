import zmq
import sys


def main():
    context = zmq.Context()

    if len(sys.argv) != 2:
        sys.exit(f"{sys.argv[0]} tcp://i03-eiger01.diamond.ac.uk:9999")

    print(f"Connecting to data source: {sys.argv[1]}")

    socket = context.socket(zmq.PULL)
    socket.connect(sys.argv[1])

    while True:
        messages = socket.recv_multipart()


main()
