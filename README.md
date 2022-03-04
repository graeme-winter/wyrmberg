Wyrmberg
--------

A set of short Python scripts to simulate the behaviour of the data flow elements of an Eiger detector system. Dectris detectors are named after Swiss mountains. Seems only natural to name the fake detector system after a fictional mountain, and since this is somewhat backward, in starting with the data, an upside down one feels appropriate.

Three components to this: `loki` which will read HDF5 data given a prefix and reconstruct the 0mq packet stream which would correspond to the original data collection, at a rate comparable to the original frame rate provided a reasonable computer and connection. Following the simplon API specifications this broadcasts on port 9999. `thor` reads this channel and dumps every chunk of every message to disk with no effort to modify or do anything clever. If running in `/dev/shm` this could be a reasonable mechanism to capture data for intermediate processing before sending on to HDF5. `odin` is a version of this which captures data to HDF5 files following the "Diamond" model e.g. 1000 images per block etc. `null` simply reads the packets and does no work, useful for establishing if the source or sink are rate limiting. 

At the moment the filenames are defined by the sequence ID -> this should really be fixed to use the actual filenames but there is not a standard way of transmitting this that I am aware of... 

Usage
-----

Computer 2: the "sink" 

```
dials.python ~/git/wyrmberg/odin.py tcp://10.144.152.21:9999 /path/to/prefix
```

or 

```
dials.python ~/git/wyrmberg/thor.py tcp://10.144.152.21:9999
```

or

```
dials.python ~/git/wyrmberg/null.py tcp://10.144.152.21:9999
```

_Then_ on computer 1: the "source"

```
dials.python ~/git/wyrmberg/loki.py TestInsulin/Insulin_3/Insulin_3_1
```

here it is assumed that there will be `Insulin_3_1.nxs` and `Insulin_3_1_meta.h5` etc. available. These will be sent over the channel to the receiver. By default the rate will be no faster than the original acquisition rate, however if you export `NO_LOKI_WAIT=1` (or any non-null value) the time taken to send will be copied to the screen, which is an indication of the frame rate, provided a decent network connection. 

TODO:
 - add bandwidth logging
 - tidy / handle filenames somehow
 

