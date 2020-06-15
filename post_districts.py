import mysql.connector
import csv
import os


class PostCodeToDistrictConverter(object):
    stockton_division = {
        'DL2': 2610,
        'TS2': 2610,
        'TS8': 611,
        'TS15': 611,
        'TS16': 2610,
        'TS17': 611,
        'TS18': 2610,
        'TS19': 2610,
        'TS20': 2610,
        'TS21': 2610,
        'TS22': 2610,
        'TS23': 2610
    }


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
        self.source_filename = os.path.join(data_root, source_filepath, 'gss_admin_areas.csv')

    def clean_up(self):
        self.dbc.close()


class Table(object):
    def __init__(self, env: MyEnvironment):
        self.env = env
        self.record_count = 0
        self.error_count = 0

    def process_csv(self):
        with open(self.env.source_filename, newline='') as f:
            c = self.env.dbc.cursor(prepared=True)
            for row in csv.reader(f, delimiter='\t'):
                self.record_count += 1
                c.execute(self.districts_search_query(), (row[0],))
                results = c.fetchall()
                n = c.rowcount
                if n == 1:
                    # print('Processing...')
                    continue
                if n == 0:
                    error_message = "District not found."
                else:
                    error_message = "Duplicate district names found."
                self.error_count += 1
                print(f'{self.record_count:>3} {row[0]:.<40} {row[1]} {error_message}')

            c.close()

    def districts_update_query(self):
        pass

    @staticmethod
    def districts_search_query():
        q = "select * from districts where "
        q += "index_name = %s "
        q += "and district_type_id <> 17"
        return q


class MyApplication(object):
    def __init__(self):
        self.env = MyEnvironment()

    def run(self):
        print(self.env.source_filename)
        gss_areas = Table(self.env)
        gss_areas.process_csv()
        print()
        print(f'Records Processed:  {gss_areas.record_count:>4}')
        print(f'Errors Encountered: {gss_areas.error_count:>4}')
        self.env.clean_up()


if __name__ == "__main__":
    app = MyApplication()
    app.run()
