# python-tarantool16_snapshot

[![Build Status](https://travis-ci.org/viciious/python-tarantool16_snaphot.svg?branch=v13_snapshots)](https://travis-ci.org/viciious/python-tarantool16_snaphot)

## Description

[Tarantool](https://github.com/tarantool/tarantool) 1.6+ snapshot reader.

## Usage

```python

import tarantool17_snapshot as tarantool_snapshot
import msgpack

count = 0
header = {} # the header dict is optional

try: 
  for packed_meta, packed_row in tarantool_snapshot.iter("/snaps/00000000010388786179.snap", header = header):
    meta = msgpack.unpackb(packed_meta)
    row = msgpack.unpackb(packed_row)
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
      print("tuple is %s" % row[33])

  print count
except Exception as e:
  print(repr(e))

```
