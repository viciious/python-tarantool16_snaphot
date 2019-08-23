import tarantool17_snapshot
import msgpack
from unittest import TestCase
import fixtures

class TestSnapshot(TestCase):
    def test_read_v12(self):
        metas = []
        rows = []

        try:
            for meta_data, row_data in tarantool17_snapshot.iter("testdata/v12/00000000000000000000.ok.snap"):
                meta = msgpack.unpackb(meta_data)
                row = msgpack.unpackb(row_data)

                metas.append(meta)
                rows.append(row)
        except:
            self.fail("Reading v12 snapshot failed")

        self.assertEquals(fixtures.v12_metas, metas)
        self.assertEquals(fixtures.v12_rows, rows)

    def test_read_v13(self):
        metas = []
        rows = []

        try:
            for meta_data, row_data in tarantool17_snapshot.iter("testdata/v13/00000000000000000000.ok.snap"):
                meta = msgpack.unpackb(meta_data)
                row = msgpack.unpackb(row_data)

                metas.append(meta)
                rows.append(row)
        except:
            self.fail("Reading v13 snapshot failed")

        self.assertEquals(fixtures.v13_metas, metas)
        self.assertEquals(fixtures.v13_rows, rows)

