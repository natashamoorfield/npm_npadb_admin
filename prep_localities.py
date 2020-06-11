import mysql.connector
import os.path
import csv


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
        self.source_filename = os.path.join(data_root, source_filepath, 'natasha.localities.csv')
        self.dest_filename = os.path.join(data_root, dest_filepath, 'localities.csv')

    def clean_up(self):
        self.dbc.close()


class MyApplication(object):
    def __init__(self):
        self.env = MyEnvironment()

    def run(self):
        print(self.env.source_filename)
        print(self.env.dest_filename)
        table = Table(self.env)
        table.export_csv()
        self.env.clean_up()


class Table(object):
    def __init__(self, env: MyEnvironment):
        self.env = env
        self.locality_types = self.fetch_locality_types()
        self.localities = self.prepare_csv()

    def fetch_locality_types(self):
        q = "select two_letter_code, locality_type_id from locality_types"
        c = self.env.dbc.cursor()
        c.execute(q)
        return dict(c.fetchall())

    def prepare_csv(self):
        new_data = []
        with open(self.env.source_filename) as f:
            for row in csv.reader(f, delimiter='\t'):
                new_row = [row[0], row[1], self.locality_types[row[3]], row[2], row[2], row[4], row[5], row[6], row[7],
                           '']
                new_data.append(new_row)
        return new_data

    def export_csv(self):
        with open(self.env.dest_filename, "w", newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerows(self.localities)


if __name__ == "__main__":
    app = MyApplication()
    app.run()
