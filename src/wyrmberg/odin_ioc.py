import epics
import sys
import time


from odin import capture

endpoint = sys.argv[1]

def start_odin(pvname=None, value=None, char_value=None, **kwargs):
    if pvname == "odin:directory_prefix" and char_value:
        capture(endpoint, char_value)

pv = epics.PV("odin:directory_prefix")
pv.add_callback(start_odin)

# spin... 
while 1:
    time.sleep(10)