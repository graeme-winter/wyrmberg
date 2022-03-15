import os
import time
import sys

from odin import capture

root = "/dls/i03/data/2022/cm31105-2/tmp/wyrmberg_test"
zero = "tcp://i03-eiger01.diamond.ac.uk:9999"

j = 0

while True:
    directory = os.path.join(root, time.strftime("%Y%m%d-%H%M%S"))
    prefix = time.strftime("run-%H%M%S")

    print(f"MAKE {directory}")
    os.mkdir(directory)

    print(f"CAPTURE {directory}/{prefix}")
    sys.stdout.flush()
    capture(zero, os.path.join(directory, prefix))
    sys.stdout.flush()
