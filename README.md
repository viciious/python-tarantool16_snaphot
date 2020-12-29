# python-tarantool16_snapshot

[![Build Status](https://travis-ci.org/viciious/python-tarantool16_snaphot.svg?branch=v13_snapshots)](https://travis-ci.org/viciious/python-tarantool16_snaphot)

## Description

[Tarantool](https://github.com/tarantool/tarantool) 1.6+ snapshot reader.

## Build and installation

```sh
git clone https://github.com/viciious/python-tarantool16_snaphot.git
cd python-tarantool16_snaphot
git submodule sync
git submodule update --init --recursive
make install PYTHON=python3
# or
make bdist_rpm PYTHON=python3
rpm -ivh dist/python3-tarantool17-snapshot-1.4-1.x86_64.rpm
```

## Usage

```python

import tarantool17_snapshot as tarantool_snapshot
import msgpack # >= 1.0.2

count = 0
header = {} # the header dict is optional

try:
  for packed_meta, packed_row in tarantool_snapshot.iter("/snaps/00000000010388786179.snap", header = header):
    meta = msgpack.unpackb(packed_meta, strict_map_key=False, use_list=False)
    row = msgpack.unpackb(packed_row, strict_map_key=False, use_list=False)
    count += 1

    instance = header['Instance'] if 'Instance' in header else header['Server']
    print("instance is %s" % instance)

    print("type is %s" % meta[0])

    if 3 in meta:
      print("lsn is %s" % meta[3])
    if 4 in meta:
      print("timestamp is %s" % meta[4])

    if 16 in row:
      print("space id is %s" % row[16])
    if 33 in row:
      print("tuple is {0}".format(row[33]))

  print count
except Exception as e:
  print(repr(e))

```
