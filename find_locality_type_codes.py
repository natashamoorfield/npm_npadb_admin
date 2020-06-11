import csv


class LocalityTypesTable(object):

    def __init__(self):
        self.localities = []
        self.locality_types = set()
        with open('/home/natasha/CloudStation/npadb/all-the-stations/data/export/natasha.localities.csv') as f:
            for row in csv.reader(f, delimiter='\t'):
                self.localities.append(row)
                if row[3] == "GZ":
                    print(row)
                self.locality_types.add(row[3])

    def print_localities_data(self):
        for row in self.localities:
            print(row)

    def print_locality_type_set(self):
        for item in self.locality_types:
            print(item)

    def export_csv(self):
        locality_type_id = 1
        new_csv = []
        for item in self.locality_types:
            row = [locality_type_id, item, f'ShortDescriptor #{locality_type_id}',
                   f'Long Descriptor #{locality_type_id}']
            new_csv.append(row)
            locality_type_id += 1
        with open('/home/natasha/CloudStation/npadb/all-the-stations/data/gazetteer/locality_types.csv', 'w') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerows(new_csv)


class MyApplication(object):
    def __init__(self):
        self.ltt = LocalityTypesTable()

    def run(self):
        self.ltt.print_locality_type_set()
        self.ltt.export_csv()


if __name__ == "__main__":
    app = MyApplication()
    app.run()
