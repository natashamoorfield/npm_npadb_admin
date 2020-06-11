import os.path
import json
import csv
import math


class Table(object):
    def __init__(self, env):
        self.env = env
        self.csv = [
            ['GB-CYM', 'Wales', 1],
            ['GB-ENG', 'England', 1],
            ['GB-NIR', 'Northern Ireland', 0],
            ['GB-SCT', 'Scotland', 1],
            ['IM', 'Isle of Man', 1],
            ['GG', 'Guernsey', 1],
            ['JE', 'Jersey', 1]
        ]
        self.max_name_length = 0
        for nation in self.csv:
            self.max_name_length = max(self.max_name_length, len(nation[1]))

    def prepare_csv(self):
        with open(self.env.source_filename) as f:
            raw_data = json.load(f)
        for key, value in raw_data.items():
            if key == 'GB':
                continue
            if key == 'GS':
                name = 'South Georgia and the South Sandwich Islands'
            else:
                name = value['name']
                name = name.replace(' & ', ' and ')
                name = name.replace('St.', 'Saint')
            self.csv.append([key, name, 0])
            self.max_name_length = max(self.max_name_length, len(name))

    def print_csv(self):
        for nation in self.csv:
            print(nation)

    def export_csv(self):
        with open(self.env.dest_filename, "w", newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerows(self.csv)

    def table_stats(self):
        line_1 = f'Record count: {len(self.csv)}'
        line_2 = f'Longest name: {self.max_name_length} characters'
        line_3 = f'Required field length: {self.mersenne_field_length(self.max_name_length)}'
        return f'{line_1}\n{line_2}\n{line_3}'

    @staticmethod
    def mersenne_field_length(max_len):
        try:
            return (2 ** (math.floor(math.log(max_len, 2)) + 1)) - 1
        except ValueError:
            return 0


class MyEnvironment(object):
    def __init__(self):
        data_root = '/home/natasha/CloudStation/npadb/all-the-stations'
        source_filepath = 'source-data/iso-3166-2.json-master'
        dest_filepath = 'data/gazetteer'
        self.source_filename = os.path.join(data_root, source_filepath, 'iso-3166-2.json')
        self.dest_filename = os.path.join(data_root, dest_filepath, 'nations.csv')


class Application(object):
    def __init__(self):
        self.env = MyEnvironment()

    def run(self):
        print(self.env.source_filename)
        print(self.env.dest_filename)
        nations = Table(self.env)
        nations.prepare_csv()
        nations.print_csv()
        nations.export_csv()
        print(nations.table_stats())


if __name__ == "__main__":
    app = Application()
    app.run()
