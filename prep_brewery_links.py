import os.path
import csv


class Table(object):
    BREWERY_LINK_MAP = {
        '2': 1,
        '5': 2,
        '7': 3,
        '13': 4,
        '14': 5,
        '15': 4,
        'Y': 7,
        'C': 6
    }

    def __init__(self, env):
        self.env = env
        self.new_table = []

    def prepare_csv(self):
        with open(self.env.source_filename_one) as f:
            for row in csv.reader(f, delimiter='\t'):
                if row[14] != '':
                    try:
                        new_row = [row[0], row[14], Table.BREWERY_LINK_MAP[row[13]]]
                    except KeyError:
                        print(row[0], row[13])
                    else:
                        self.new_table.append(new_row)
        with open(self.env.source_filename_two) as f:
            for row in csv.reader(f, delimiter='\t'):
                if row[2] in ['C', 'Y']:
                    new_row = [row[0], row[1], Table.BREWERY_LINK_MAP[row[2]]]
                    self.new_table.append(new_row)

    def print_csv(self):
        for row in self.new_table:
            print(row)

    def export_csv(self):
        with open(self.env.dest_filename, "w", newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerows(self.new_table)


class MyEnvironment(object):
    def __init__(self):
        data_root = '/home/natasha/CloudStation/npadb/all-the-stations/data'
        self.source_filename_one = os.path.join(data_root, 'ale/breweries.csv')
        self.source_filename_two = os.path.join(data_root, 'export/brewery_links.csv')
        self.dest_filename = os.path.join(data_root, 'ale/brewery_links.csv')


class Application(object):
    def __init__(self):
        print("Prepare Brewery Links Table Data")
        self.env = MyEnvironment()
        print(self.env.source_filename_one)
        print(self.env.source_filename_two)
        print(self.env.dest_filename)

    def run(self):
        brewery_links = Table(self.env)
        brewery_links.prepare_csv()
        brewery_links.print_csv()
        brewery_links.export_csv()


if __name__ == "__main__":
    app = Application()
    app.run()
