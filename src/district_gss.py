import os.path
import csv
import re

from src.environment import MyEnvironment


class DistrictGSSData(object):

    CSV_HEADERS = ['district_name', 'gss_code']
    LINE_TEMPLATE = '{:>4} {:.<44} {} {}'
    GSS_CODE_PATTERN = r'[EWS][0-9]{8}'

    def __init__(self, env: MyEnvironment):
        self.env = env
        print(self.env.args.task)
        self.record_count = 0
        self.error_count = 0
        self.source_filename = self.fetched_source_filename()
        print(self.source_filename)
        self.gss_code_pattern = re.compile(self.GSS_CODE_PATTERN, re.IGNORECASE)

    def data_import(self):
        with open(self.source_filename, newline='') as f:
            c = self.env.dbc.cursor(dictionary=True)
            for row in csv.reader(f, delimiter='\t'):
                update = dict(zip(self.CSV_HEADERS, [row[0], row[1].upper()]))
                self.record_count += 1
                message = [self.record_count, update['district_name'], update['gss_code'], '']
                c.execute(self.districts_search_query(), (update['district_name'],))
                results = c.fetchall()
                n = c.rowcount
                if n == 1:
                    self.process_record(update, results[0], message)
                elif n == 0:
                    self.process_error(message, "District not found.")
                else:
                    self.process_error(message, "Duplicate district names found.")
            c.close()
            self.env.dbc.commit()

        print()
        print(f'Records Processed:  {self.record_count:>4}')
        print(f'Errors Encountered: {self.error_count:>4}')

    def fetched_source_filename(self):
        if self.env.args.source is None:
            return os.path.join(self.env.npadb_data_root, 'updates', 'gss_admin_areas.csv')
        else:
            return self.env.args.source

    @staticmethod
    def districts_update_query():
        q = "update districts "
        q += "set gss_admin_area_code = %s "
        q += "where district_id = %s"
        return q

    @staticmethod
    def districts_search_query():
        q = "select district_id, display_name, upper(gss_admin_area_code) as gss_admin_area_code from districts "
        q += "where index_name = %s "
        q += "and district_type_id <> 17"
        return q

    def process_record(self, update: dict, district: dict, message: list):
        if update['gss_code'] == district['gss_admin_area_code']:
            # Don't do anything if the stored gss_area_admin_code is the same as the update value
            return
        message[1] = f"{district['display_name']} ({district['district_id']})"
        if not re.fullmatch(self.gss_code_pattern, update['gss_code']):
            self.process_error(message, "Non-conformant GSS Code")
            return
        if district['gss_admin_area_code'] is not None:
            # GSS codes are supposed to be invariant.
            # If the stored gss_admin_area_code differs from the update value, manual intervention may be needed.
            self.process_error(message, 'GSS Code mis-match with ' + district['gss_admin_area_code'])
            return

        cursor = self.env.dbc.cursor()
        cursor.execute(self.districts_update_query(), (update['gss_code'], district['district_id']))
        cursor.close()
        message[3] = 'OK'
        print(self.LINE_TEMPLATE.format(*message))

    def process_error(self, message: list, error_message: str):
        self.error_count += 1
        message[3] = error_message
        print(self.LINE_TEMPLATE.format(*message))
