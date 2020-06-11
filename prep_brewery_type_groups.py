import os.path
import csv


class Table(object):
    def __init__(self, env):
        self.env = env
        self.new_table = []

    def prepare_csv(self):
        new_data = {}
        with open(self.env.source_filename_one) as f:
            for row in csv.reader(f, delimiter='\t'):
                new_data[(int(row[0])) >> 8] = row[5]
            for key, value in new_data.items():
                self.new_table.append([key, value, ''])

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
        self.source_filename_one = os.path.join(data_root, 'ale/brewery_status_codes.csv')
        self.dest_filename = os.path.join(data_root, 'ale/brewing_status_groups.csv')


class Application(object):
    def __init__(self):
        print("Prepare Brewery Type Groups Data")
        self.env = MyEnvironment()
        print(self.env.source_filename_one)
        print(self.env.dest_filename)

    def run(self):
        brewery_links = Table(self.env)
        brewery_links.prepare_csv()
        brewery_links.print_csv()
        brewery_links.export_csv()


if __name__ == "__main__":
    app = Application()
    app.run()
