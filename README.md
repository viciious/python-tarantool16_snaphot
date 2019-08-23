## Description

[Tarantool](https://github.com/tarantool/tarantool) 1.6+ snapshot reader.

## Usage

```python

import tarantool17_snapshot as tarantool_snapshot
import msgpack

count = 0
for _, row_data in tarantool_snapshot.iter("/snaps/00000000010388786179.snap"):
  row = msgpack.unpackb(row_data)
  count += 1

print count

```
