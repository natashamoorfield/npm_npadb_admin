import os.path
import csv


class Table(object):
    def __init__(self, env):
        self.env = env
        self.new_table = []

    def prepare_csv(self):
        with open(self.env.source_filename) as f:
            for row in csv.reader(f, delimiter='\t'):
                if row[0] == '0':
                    # Skip record 0:
                    # The use of dummy records is not being carried forward to the new database
                    continue
                new_row = [row[0], row[1], row[2]]
                index_name = row[4]
                new_row.append(index_name.replace('-', ''))
                new_row.append(row[3])
                historic_county = 127
                npm_admin_county = 127
                if row[1] == 'GB-CYM':
                    if row[2] == 'GLA':
                        historic_county = 1
                    else:
                        historic_county = int(row[5])
                    npm_admin_county = 1 - int(row[5])
                elif row[2] in ['HUM', 'AVN', 'HWR', 'CLV']:
                    historic_county = 0
                    npm_admin_county = 0
                elif row[2] in ['GTM', 'CMA', 'TWR', 'NYK', 'WYK', 'WMD', 'MSY', 'SYK', 'LND', 'SXE', 'SXW', 'IOW']:
                    historic_county = 0
                    npm_admin_county = 1
                elif row[2] in ['CUL', 'HUN', 'MDX', 'WES', 'YKS', 'NRY', 'WRY', 'SSX']:
                    historic_county = 1
                    npm_admin_county = 0
                elif row[1] == 'GB-ENG':
                    historic_county = 1
                    npm_admin_county = 1
                elif row[1] == 'GB-SCT':
                    if row[2] in ['FIF', 'SHI', 'OKI']:
                        historic_county = 1
                        npm_admin_county = 1
                    elif row[2] == 'ROC':
                        historic_county = 0
                        npm_admin_county = 0
                    else:
                        historic_county = int(row[5])
                        npm_admin_county = 1 - historic_county
                elif row[1] in ['GG', 'IM', 'JE']:
                    historic_county = 0
                    npm_admin_county = 1
                elif row[1] == 'IE':
                    historic_county = 1
                    npm_admin_county = 0
                    if row[2] in ['ANT', 'ARM', 'DOW', 'FER', 'LDY', 'TYR']:
                        new_row[1] = 'GB-NIR'
                new_row.append(historic_county)
                new_row.append(npm_admin_county)

                self.new_table.append(new_row)

    def print_csv(self):
        for county in self.new_table:
            print(county)

    def export_csv(self):
        with open(self.env.dest_filename, "w", newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerows(self.new_table)


class MyEnvironment(object):
    def __init__(self):
        data_root = '/home/natasha/CloudStation/npadb/all-the-stations/data'
        self.source_filename = os.path.join(data_root, 'export/natasha.counties.csv')
        self.dest_filename = os.path.join(data_root, 'gazetteer/counties.csv')


class Application(object):
    def __init__(self):
        print("Prepare Counties Table Data")
        self.env = MyEnvironment()
        print(self.env.source_filename)
        print(self.env.dest_filename)

    def run(self):
        counties = Table(self.env)
        counties.prepare_csv()
        counties.print_csv()
        counties.export_csv()


if __name__ == "__main__":
    app = Application()
    app.run()
