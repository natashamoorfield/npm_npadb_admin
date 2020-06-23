from src.environment import MyEnvironment
from src.exceptions import CodePointOpenError
from mysql.connector.cursor import MySQLCursor
from mysql.connector.errors import Error as MySQLError


import os
import re
import csv


class CodePointOpen(object):
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

    def __init__(self, env: MyEnvironment):
        self.env = env
        (self.edition, self.data_root) = self.verified_edition()
        print('Post Code Build')
        print(self.data_root)
        self.area_count = 0
        self.total_record_count = 0
        self.gss_codes = self.fetched_gss_codes()

    def import_post_code_data(self):
        # If the --all option is set, truncate the existing post_codes table
        if self.env.args.all:
            q = "truncate table post_codes"
            c = self.env.dbc.cursor()
            c.execute(q)
            self.env.dbc.commit()
            c.close()

        for filename in self.data_files():
            post_code_area = filename.split('.')[0].upper()
            try:
                area_record_count, area_error_count = self.process_csv_file(post_code_area, filename)
            except FileNotFoundError:
                w = [
                    f'Skipping Post Code Area "{post_code_area}":',
                    f'>>File {filename} not found'
                ]
                self.env.msg.warning(w)
            else:
                self.area_count += 1
                self.total_record_count += area_record_count
                self.env.msg.ok(f'Processed {area_record_count:,} records in Post Code Area "{post_code_area}"')

        self.final_overview()

    def verified_edition(self):
        """
        Four editions are published per year, nominally in February, May, August and November.
        We denote the edition date in YYYY-MM format.
        Verification fails if
        the edition argument supplied is not in the correct format or
        the specified dataset does not exist in the external data directory.
        :return:
        """
        edition_pattern = r'20[0-9]{2}-(02|05|08|11)'
        if not re.fullmatch(edition_pattern, self.env.args.edition):
            raise CodePointOpenError(f"Bad edition specification '{self.env.args.edition}'")
        data_root = os.path.join(
            self.env.external_data_root,
            'gazetteer',
            'os-code-point-open-' + self.env.args.edition,
            'Data/CSV'
        )
        if os.path.isdir(data_root):
            return self.env.args.edition, data_root
        else:
            e = CodePointOpenError(f'Code Point Open data directory for {self.env.args.edition} not found:')
            e.add_message(f'--{data_root}')
            raise e

    def data_files(self):
        """
        The full dataset is held in multiple csv text files, one for each post code area.
        This method yields the name of each of the required files either by scanning the directory in which the files
        are held (the --all option) or from a list of post code areas specified by the user (the --areas option).
        Duplicate areas in a user-specified list are silently ignored.
        :return:
        """
        if self.env.args.all:
            for entry in os.scandir(self.data_root):
                yield entry.name
        else:
            for entry in set(a.lower() for a in self.env.args.areas):
                yield f'{entry}.csv'

    def process_csv_file(self, post_code_area, filename: str):
        with open(os.path.join(self.data_root, filename), newline='') as f:
            # TODO Delete existing entries in the post_codes table for the post_code_area
            self.env.msg.info(f'Processing Post Code Area "{post_code_area}"')
            record_count = 0
            error_count = 0
            cursor = self.env.dbc.cursor()
            for row in csv.reader(f, delimiter=','):
                try:
                    self.process_post_code(cursor, row)
                except CodePointOpenError as e:
                    self.env.msg.warning(e.messages())
                    error_count += 1
                else:
                    record_count += 1
            cursor.close()
            self.env.dbc.commit()
            return record_count, error_count

    def process_post_code(self, cursor: MySQLCursor, data: list):
        post_code = self.formatted_post_code(data[0])
        gr_source_id = int(data[1]) + 400
        osx = data[2]
        osy = data[3]
        try:
            if data[8] == 'E06000004':  # Stockton-on-Tees
                district_id = self.stockton_division[post_code[:-4]]
            else:
                district_id = self.gss_codes[data[8]]
        except KeyError:
            if data[8] == '' and gr_source_id in [460, 490]:
                district_id = None
            else:
                raise CodePointOpenError(
                    f'Problem with district code {data[8]} at {post_code} [{osx},{osy}] {gr_source_id}')

        try:
            pass
            # q = f'insert into post_codes values (%s, %s, %s, %s, %s)'
            # cursor.execute(q, (post_code, osx, osy, gr_source_id, district_id))
        except MySQLError as mse:
            e = CodePointOpenError(f'Error attempting to insert {post_code} into `post_codes` table')
            e.add_message(mse.args[1])
            e.add_message(f'[{osx}, {osy}] {gr_source_id} {district_id}')
            raise e

    def final_overview(self):
        m = [
            'Processing Complete',
            f'--Post Code Areas processed: {self.area_count}',
            f'--Total Post Codes processed: {self.total_record_count:,}',
            '--'
        ]
        if not self.env.args.quiet:
            print()
        self.env.msg.ok(m)

    def fetched_gss_codes(self):
        gss_codes = {}
        q = "select gss_admin_area_code, district_id from districts where gss_admin_area_code is not null"
        c = self.env.dbc.cursor()
        c.execute(q)
        for row in c:
            gss_codes[row[0]] = row[1]
        c.close()
        return gss_codes

    @staticmethod
    def formatted_post_code(raw_post_code: str) -> str:
        return f'{raw_post_code[:-3].strip()} {raw_post_code[-3:]}'
