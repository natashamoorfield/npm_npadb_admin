import mysql.connector
import csv
import os


class MyEnvironment(object):
    def __init__(self):
        self.dbc = mysql.connector.connect(
            user='natasha',
            host='localhost',
            password='Pgz2XN7VJ9B0',
            database='all_the_stations'
        )
        data_root = '/home/natasha/CloudStation/npadb/all-the-stations/data'
        source_filepath = 'export'
        dest_filepath = 'gazetteer'
        self.source_filename = os.path.join(data_root, source_filepath, 'natasha.towns.csv')
        self.dest_filename = os.path.join(data_root, dest_filepath, 'towns.csv')

    def clean_up(self):
        self.dbc.close()


class Table(object):
    def __init__(self, env: MyEnvironment):
        q = "select two_letter_code, town_type_id  from all_the_stations.town_types"
        self.env = env
        c = self.env.dbc.cursor()
        c.execute(q)
        self.town_types = dict(c.fetchall())
        self.new_csv = []

    def prepare_csv(self):
        with open(self.env.source_filename, newline='') as f:
            for row in csv.reader(f, delimiter='\t'):
                new_row = [row[0], row[1], row[2], row[3], row[4], self.town_types[row[5]]]
                self.new_csv.append(new_row)

    def print_csv(self):
        for row in self.new_csv:
            print(row)

    def export_csv(self):
        with open(self.env.dest_filename, "w", newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerows(self.new_csv)


class MyApplication(object):
    def __init__(self):
        self.env = MyEnvironment()

    def run(self):
        print(self.env.source_filename)
        print(self.env.dest_filename)
        towns = Table(self.env)
        towns.prepare_csv()
        towns.print_csv()
        towns.export_csv()
        self.env.clean_up()


if __name__ == "__main__":
    app = MyApplication()
    app.run()
