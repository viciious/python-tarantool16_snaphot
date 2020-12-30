import tarantool17_snapshot
import msgpack
from unittest import TestCase
try:
    import fixtures
except:
    import tests.fixtures as fixtures

class TestSnapshot(TestCase):
    def test_read_v12(self):
        metas = []
        rows = []

        try:
            for meta_data, row_data in tarantool17_snapshot.iter("testdata/v12/00000000000000000000.ok.snap"):
                meta = msgpack.unpackb(meta_data, strict_map_key=False, raw=False)
                row = msgpack.unpackb(row_data, strict_map_key=False, raw=False)

                metas.append(meta)
                rows.append(row)
        except:
            self.fail("Reading v12 snapshot failed")

        self.assertEqual(fixtures.v12_metas, metas)
        self.assertEqual(fixtures.v12_rows, rows)

    def test_read_v12_header(self):
        header = {}

        try:
            tarantool17_snapshot.iter("testdata/v12/00000000000000000000.ok.snap", header=header)
        except:
            self.fail("Reading v12 snapshot header failed")

        self.assertEqual(fixtures.v12_header, header)

    def test_read_v13(self):
        metas = []
        rows = []

        try:
            for meta_data, row_data in tarantool17_snapshot.iter("testdata/v13/00000000000000000000.ok.snap"):
                meta = msgpack.unpackb(meta_data, strict_map_key=False, raw=False)
                row = msgpack.unpackb(row_data, strict_map_key=False, raw=False)

                metas.append(meta)
                rows.append(row)
        except:
            self.fail("Reading v13 snapshot failed")

        self.assertEqual(fixtures.v13_metas, metas)
        self.assertEqual(fixtures.v13_rows, rows)

    def test_read_v13_header(self):
        header = {}

        try:
            tarantool17_snapshot.iter("testdata/v13/00000000000000010005.ok.snap", header=header)
        except:
            self.fail("Reading v13 snapshot header failed")

        self.assertEqual(fixtures.v13_header, header)

    def test_bigsnap_v13(self):
        count = 0
        try:
            for meta_data, row_data in tarantool17_snapshot.iter("testdata/v13/00000000000000010005.ok.snap"):
                msgpack.unpackb(meta_data, strict_map_key=False, raw=False)
                msgpack.unpackb(row_data, strict_map_key=False, raw=False)

                count = count + 1
        except:
            self.fail("Reading v13 snapshot failed")

        self.assertEqual(count, 10511)

    def test_corrupted_zstd_v13(self):
        with self.assertRaises(tarantool17_snapshot.SnapshotError) as ctx:
            for _, _ in tarantool17_snapshot.iter("testdata/v13/corr.block.snap"):
                pass
        self.assertEqual(str(ctx.exception), "Error reading 'testdata/v13/corr.block.snap': zstd error: Corrupted block detected")

    def test_no_eof_v13(self):
        with self.assertRaises(tarantool17_snapshot.SnapshotError) as ctx:
            for _, _ in tarantool17_snapshot.iter("testdata/v13/no.eof.snap"):
                pass
        self.assertEqual(str(ctx.exception), "Error reading 'testdata/v13/no.eof.snap': truncated stream")

    def test_bad_version_xlog(self):
        with self.assertRaises(tarantool17_snapshot.SnapshotError) as ctx:
            for _, _ in tarantool17_snapshot.iter("testdata/version.bad.xlog"):
                pass
        self.assertEqual(str(ctx.exception), "Error opening 'testdata/version.bad.xlog': unknown header version: 0.07\n")

    def test_bad_format(self):
        with self.assertRaises(tarantool17_snapshot.SnapshotError) as ctx:
            for _, _ in tarantool17_snapshot.iter("testdata/format.bad.xlog"):
                pass
        self.assertEqual(str(ctx.exception), "Error opening 'testdata/format.bad.xlog': unknown file header: expected SNAP or XLOG")

