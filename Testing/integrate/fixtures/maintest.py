import unittest.mock
from unittest.mock import patch, mock_open
from automation import main_open
import automation
from io import StringIO


class Maintest(unittest.TestCase):
    def setUp(self):
        self.path = "testdata.json"

    @patch('automation.os.path')
    def test_open(self, mock_path):
        print(mock_path)
        json_file = automation.json.dumps({'resource': 'jsonfile'})
        print(json_file)
        print(type(json_file))
        with patch('builtins.open', new_callable=mock_open) as mo:
            print(mo)
            mock_path.isfile.return_value = True
            mo.return_value = StringIO(json_file)
            data = main_open(self.path)
            print(data)
            # print(file.read())
            self.assertEqual(data, {'resource': 'jsonfile'})
            print(mo.mock_calls)
            mo.assert_called_once_with(self.path)
            self.assertTrue(mo.called)
            print(mo.call_count)
            mo.side_effect = FileNotFoundError
            print(mo.side_effect)
            with self.assertRaises(FileNotFoundError):
                main_open(self.path)

            self.assertTrue(mo.called)
            print(mo.call_count)
            print(mo.mock_calls)
            print('ferror')

    @patch('automation.json')
    def test_all(self, mock_json):
        print(mock_json)
        data = {'resource': 'jsonfile'}
        mock_json.load.return_value = data
        res = main_open(self.path)
        print(res)
        print(mock_json.load.mock_calls)
        print(mock_json.load.assert_called_once())
        print(mock_json.load.call_count)
        self.assertEqual(res, {'resource': 'jsonfile'})
        mock_json.load.side_effect = AttributeError
        error = main_open(self.path)
        print(error)
        print(self.assertIsNone(error, 'error occured'))
        print('success')
