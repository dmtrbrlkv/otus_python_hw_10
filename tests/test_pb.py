import os
import unittest
import struct

import pb
MAGIC = 0xFFFFFFFF
HEADER_FORMAT = "IHH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
DEVICE_APPS_TYPE = 1
TEST_FILE = "test.pb.gz"


class TestPB(unittest.TestCase):
    deviceapps = [
        {"device": {"type": "idfa", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7c"},
         "lat": 67.7835424444, "lon": -22.8044005471, "apps": [1, 2, 3, 4]},
        {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "lat": 42, "lon": -42, "apps": [1, 2]},
        {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "lat": 42, "lon": -42, "apps": []},
        {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "apps": [1]},
    ]

    def tearDown(self):
        pass
        # os.remove(TEST_FILE)

    def test_write(self):
        bytes_written = pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
        self.assertTrue(bytes_written > 0)

        n_msg = 0
        size_msg = 0
        with open(TEST_FILE, "rb") as f:
            while True:
                buf = f.read(HEADER_SIZE)
                if not buf:
                    break
                magic, t, lenght = struct.unpack(HEADER_FORMAT, buf)
                size_msg += HEADER_SIZE
                msg = f.read(lenght)
                self.assertEqual(magic, MAGIC)
                self.assertEqual(t, DEVICE_APPS_TYPE)
                n_msg += 1
                size_msg += lenght

        self.assertEqual(n_msg, len(self.deviceapps))
        self.assertEqual(size_msg, bytes_written)


    def test_read(self):        
        pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)

        for i, d in enumerate(pb.deviceapps_xread_list_pb(TEST_FILE)):
            self.assertEqual(d, self.deviceapps[i])

        for i, d in enumerate(pb.deviceapps_xread_pb(TEST_FILE)):
            self.assertEqual(d, self.deviceapps[i])