from src.environment import MyEnvironment
from src.table import Table
from src.exceptions import *
from src.npadb_tables import NPADBTables

from collections import OrderedDict

import mysql.connector.errors
import os.path
import subprocess


class Application(object):

    def __init__(self, root: str):
        self.env = MyEnvironment(root)
        self.env.render_base_program_info()
        self.schema = NPADBTables(self.env)
        self.tables = OrderedDict()
        self.tables['common'] = [
            'contact_types',
            'copyright_types',
            'gr_sources',
            'name_types',
            'pictures',
            'picture_types'
        ]
        self.tables['network'] = [
            'network_names',
            'network_types',
            'networks',
            'station_names',
            'station_pictures',
            'station_status_codes',
            'station_visit_codes',
            'station_visit_types',
            'stations',
            'timing_points'
        ]
        self.tables['gazetteer'] = [
            'abc_gazetteer',
            'counties',
            'district_types',
            'districts',
            'locality_types',
            'localities',
            'nations',
            'town_type_groups',
            'town_types',
            'towns'
        ]
        self.tables['user'] = [
            'access_levels',
            'action_access_levels',
            'actions',
            'ale_carriers',
            'drinks',
            'gender_descriptors',
            'pronoun_sets',
            'user_moves',
            'user_pub_visits',
            'user_station_visits',
            'users'
        ]
        self.tables['ale'] = [
            'ale_names',
            'ale_quality_definitions',
            'ales',
            'breweries',
            'brewery_address_types',
            'brewery_ales',
            'brewery_contacts',
            'brewery_link_types',
            'brewery_links',
            'brewery_names',
            'brewery_pictures',
            'brewery_type_groups',
            'brewery_types',
            'brewery_status_codes',
            'brewing_status_groups'
        ]
        self.tables['pub'] = [
            'beer_festivals',
            'gbg_entry_types',
            'pubs',
            'pub_contacts',
            'pub_gbg_entries',
            'pub_names',
            'pub_pictures',
            'pub_ratings',
            'pub_status_codes',
            'pub_types',
            'pub_visit_codes'
        ]

    def run(self):
        try:
            if self.env.args.task is None:
                raise NPMException('No task specified.')
            if self.env.args.task == 'init':
                self.build_tables()
            elif self.env.args.task == 'export':
                self.export_tables()
            elif self.env.args.task == 'list':
                self.schema.show_tables()
            elif self.env.args.task == 'version':
                self.version_update()
            elif self.env.args.task == 'dump':
                self.dump_database()
            elif self.env.args.task == 'lgro':
                self.local_government_reorganization()
            elif self.env.args.task == 'redt':
                r = self.env.program.redacted_config()
                self.env.program.save_config_data((r, 'redacted.ini'))
                if self.env.args.show:
                    print(self.env.program.printable_config_render(r))
            elif self.env.args.task == 'cloc':
                self.lines_of_code()
            else:
                error_message = f'Execution path for task "{self.env.args.task}" not found.'
                raise NPMException(error_message)
        except NPMException as e:
            self.env.msg.error(e.args[0])
            print()
        else:
            self.env.msg.ok([f'{self.env.program.program_name} terminated without encountering any fatal errors.',
                             f'--Goodbye, {self.env.user.username}.',
                             ''])
        finally:
            self.env.clean_up()

    def export_tables(self):
        """
        Prepare to export the contents of all tables or list of specified tables
        (passed as arguments with the export task) to csv text files.
        :return:
        """
        if self.env.args.all:
            for table in self.schema.data.items():
                self.export_this_table(*table)
        else:
            for table_name in self.env.args.tables:
                try:
                    self.export_this_table(table_name, self.schema.table(table_name))
                except NPMException as e:
                    self.env.msg.warning([
                        f'Skipping {table_name}',
                        f'--{e.args[0]}'
                    ])

    def export_this_table(self, table_name: str, table_metadata: dict):
        """
        Export the contents of a particular table to a csv text file.
        :param table_name:
        :param table_metadata:
        :return:
        """
        self.env.msg.info(f"Exporting table '{table_name}' in group '{table_metadata['group']}'")
        table = Table(self.env, table_name, table_metadata)
        self.env.msg.debug(table.export_filepath)
        table.export()

    def fetch_table_list(self, database: str = 'all_the_stations'):
        """
        Return a list of table names for all tables in a particular database
        This method is not used at present: it should be used for comparing actual database contents
        with our metadata list.
        :param database:
        :return:
        """
        query = f"show tables from {database}"
        tables = []
        c = self.env.dbc.cursor()
        try:
            c.execute(query)
            tables = [x[0] for x in c.fetchall()]
        except mysql.connector.errors.ProgrammingError as e:
            self.env.msg.warning(f"Database Error: {e.msg}")
        finally:
            c.close()
            return tables

    def build_tables(self):
        query = "SET FOREIGN_KEY_CHECKS = {}"
        c = self.env.dbc.cursor()
        c.execute(query.format(0))

        if self.env.args.all:
            for table in self.schema.data.items():
                self.build_this_table(*table)
        else:
            for table_name in self.env.args.tables:
                try:
                    self.build_this_table(table_name, self.schema.table(table_name))
                except NPMException as e:
                    self.env.msg.warning([
                        f'Skipping {table_name}',
                        f'--{e.args[0]}'
                    ])
        c.execute(query.format(1))
        c.close()
        print()

    def build_this_table(self, table_name: str, table_metadata: dict):
        self.env.msg.info(f"Building table '{table_name}' in group '{table_metadata['group']}'")
        table = Table(self.env, table_name, table_metadata)
        self.env.msg.debug(table.ddl_filepath)
        self.env.msg.debug(table.data_filepath)
        table.create_table()
        table.populate_table()

    def dump_database(self):
        dump_filepath = os.path.join('/home/natasha/Dropbox/db_interface', f'{self.env.database_name}.sql')
        dump_command = [
            'mysqldump',
            self.env.database_name,
            '-r',
            dump_filepath
        ]
        comp_command = ['gzip']
        if self.env.args.verbosity > 1:
            comp_command.append('-v')
        comp_command.append('--force')
        comp_command.append(dump_filepath)
        print(' '.join(dump_command))
        print()
        subprocess.run(dump_command)
        print(' '.join(comp_command))
        subprocess.run(comp_command)
        print()

    def local_government_reorganization(self):
        from src.lgro import LocalGovernmentReorganization
        lgr = LocalGovernmentReorganization(self.env)
        if self.env.args.dry_run:
            lgr.do_dry_run()
        else:
            lgr.reorganize()

    def lines_of_code(self):
        command_line = [
            'cloc',
            self.env.root,
            '--exclude-lang=CSS',
            '--exclude-list-file=.cloc-ignore',
            '--exclude-dir=.idea'
        ]
        if self.env.args.by_file:
            command_line.append('--by-file')
        subprocess.run(command_line)

    def version_update(self):
        if self.env.args.increment:
            self.env.program.inc_version()
        else:
            print(f'** Version {self.env.program.version_string()} **')
