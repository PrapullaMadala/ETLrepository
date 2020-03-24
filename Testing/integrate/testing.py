import unittest
from automation import main
import os.path
from pathlib import Path


class TestETL(unittest.TestCase):
    def setUp(self):
        self.path = "integrate/fixtures/testdata.json"

    def test_file_path(self):
        self.assertTrue(os.path.exists(self.path), msg=' Path not exists')
        re = main(self.path)
        self.assertEqual(re, self.path, msg='something wrong')
        with open(self.path) as f:
            self.assertIsNotNone(f, msg='File not opened')

        f.close()
        print('test passed')

    def test_open(self):
        file = ""
        with self.assertRaises(FileNotFoundError):
            f = open(file)

            self.assertIsNotNone(f, msg='file opened')
            print('test passed')

    def test_file(self):
        pass


if __name__ == '__main__':
    unittest.main()
