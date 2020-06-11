import csv
import uuid
import os


class Table(object):
    def __init__(self):
        data_root = "/home/natasha/CloudStation/npadb/all-the-stations/data"
        self.source = os.path.join(data_root, "export/natasha.gender_descriptors.csv")
        self.dest = os.path.join(data_root, "user/gender_descriptors.csv")
        self.new_data = self.processed_source_data()

    def processed_source_data(self):
        new_data = []
        with open(self.source) as f:
            for row in csv.reader(f, delimiter='\t'):
                new_data.append([uuid.uuid4(), row[1]])
        return new_data

    def print_new_data(self):
        for row in self.new_data:
            print(row[0], row[1])

    def export_to_csv(self):
        with open(self.dest, 'w') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerows(self.new_data)


class Application(object):
    def __init__(self):
        self.table = Table()
        self.table.print_new_data()
        self.table.export_to_csv()

    def run(self):
        pass


if __name__ == "__main__":
    app = Application()
    app.run()
