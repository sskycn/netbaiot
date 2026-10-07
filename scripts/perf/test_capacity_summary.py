"""Preserve measurement-integrity checks for archived capacity evidence."""
import pathlib
import tempfile
import unittest
import capacity_summary

class SummaryTests(unittest.TestCase):
    def test_network_data_without_timestamps_is_not_invented(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)/'network.csv'
            path.write_text(',bytes_in,bytes_out,\nserver.1,100,200,\n')
            self.assertIsNone(capacity_summary.network_rates(path, 1, 2))
            path.write_text('epoch,process,bytes_in,bytes_out\n10,server.1,100,200\n12,server.1,500,300\n')
            result = capacity_summary.network_rates(path, 10, 12)
            self.assertEqual(result['rx_bytes_s'], 200)
            self.assertEqual(result['tx_bytes_s'], 50)
            self.assertIsNone(capacity_summary.network_rates(path, 100, 102))


if __name__ == "__main__":
    unittest.main()
