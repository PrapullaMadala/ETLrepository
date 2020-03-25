from extraction import Extract
import os.path
import json


print("name:", __name__)


class Automate:
    def __init__(self, datasource, dataset, data):
        Extract(datasource, dataset, data)
#        Transform(datasource, dataset)


def getsources(etl_sources):

    if etl_sources is not None:
        for data_source, data_set, in etl_sources['data_sources'].items():
            print(data_source)
            print(data_set)
            for data in data_set:
                print(data)
                Automate(data_source, data_set, data)
    else:
        print('No resources')


def main_open(json_file):

    if os.path.exists(json_file) is True:
        res = os.path.exists(json_file)
        print(res)
        print(os.path.getsize(json_file))
        print(os.path.isfile(json_file))
        print(json_file)
        print("Control in automation.py at ln:31")
        with open(json_file) as file:
            print("Control in automation.py at ln:33")
            print(file)
            print("Control in automation.py at ln:37")
            try:
                data = json.load(file)
                print(data)
                return data
            except AttributeError as e:
                print(f'Exception raised: {e}')
                return None
    else:
        print(os.path.isfile(json_file))
        print(json_file)
        print('file not found')
        return None


if __name__ == '__main__':
    file_path = 'configuration.json'
    sources = main_open(file_path)
    print(type(main_open))
    print(main_open.__dict__)
    getsources(sources)
    print('ETL automation completed')
