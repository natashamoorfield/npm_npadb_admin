import mysql.connector
import csv


class Table(object):
    def __init__(self):
        self.source_filepath = '/home/natasha/CloudStation/npadb/all-the-stations/external-data/network/stations.csv'
        self.dest_filepath_main = '/home/natasha/CloudStation/npadb/all-the-stations/data/network/stations.csv'
        self.dest_filepath_names = '/home/natasha/CloudStation/npadb/all-the-stations/data/network/station_names.csv'
        self.new_table = []
        self.new_names = []
        self.counter = 1

    def process(self):
        with open(self.source_filepath, newline='') as f:
            for row in csv.reader(f, delimiter='\t'):
                try:
                    osx = int(row[4])
                    osy = int(row[5])
                except (ValueError, TypeError):
                    osx = 0
                    osy = 0
                new_row = [self.counter]
                new_name = [self.counter, self.counter]
                new_row.append(int(row[0]))
                new_row.append(6)
                new_row.append(row[1])
                new_row.append(int(row[3]))
                new_row.append(osx)
                new_row.append(osy)
                if osx == 0 or osy == 0:
                    new_row.append(900)  # Null grid reference
                elif int(row[0]) == 1:
                    new_row.append(750)  # gr_source = "Third Party"
                else:
                    new_row.append(800)  # gr_source = "Not Known"
                self.new_table.append(new_row)
                new_name.append(row[2])  # station_name
                new_name.append(1)  # name_type = "MAIN"
                self.new_names.append(new_name)
                self.counter += 1

    def print_new_table(self):
        for row, name in zip(self.new_table, self.new_names):
            print(row, name)

    def dump_new_tables(self):
        with open(self.dest_filepath_main, "w", newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerows(self.new_table)
        with open(self.dest_filepath_names, "w", newline='') as f2:
            writer = csv.writer(f2, delimiter='\t')
            writer.writerows(self.new_names)


class Application(object):
    def __init__(self):
        self.env = MyEnvironment()
        self.data_table = Table()
        self.data_table.process()
        # self.data_table.print_new_table()
        self.data_table.dump_new_tables()

    def run(self):
        self.env.clean_up()


class MyEnvironment(object):
    def __init__(self):
        self.dbc = mysql.connector.connect(
            user='natasha',
            host='localhost',
            password='Pgz2XN7VJ9B0',
            database='all_the_stations'
        )

    def clean_up(self):
        self.dbc.close()


if __name__ == "__main__":
    app = Application()
    app.run()
