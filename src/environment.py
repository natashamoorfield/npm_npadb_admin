from npm_common.common_utilities  import MyStatusMessage
from npm_common.base_environment import BaseEnvironment
from configparser import ConfigParser

import mysql.connector
import argparse


class MyArguments(object):
    """
    Class to parse command line arguments.
    """

    def __init__(self, config: ConfigParser, test_args: str = None):
        """
        The ``__init__`` method will set up and run the argument parser.

        :param test_args: Rather than use actual command line arguments, the parser can be
            instantiated with pre-set test arguments.
        :type test_args: str
        """
        parser = argparse.ArgumentParser(
            prog=config['program']['tla'],
            description=config['program']['name'],
            epilog="Author: Natalya Petrovna Moorfield"
        )

        subparsers = parser.add_subparsers(
            title='NPADB Admin Tasks',
            dest='task'
        )

        # Add arguments for auxiliary functionality and verbosity
        auxiliary_group = parser.add_argument_group(
            title='Auxiliary Options'
        )

        auxiliary_group.add_argument(
            '-q', '--quiet',
            action='store_true',
            help='report only errors: overrides -v'
        )

        auxiliary_group.add_argument(
            '-v',
            action='count',
            default=0,
            help='increase verbosity: overridden by -q'
        )

        # Add sub-parser for the initial build task
        db_init_parser = subparsers.add_parser(
            'Import',
            description='Create and populate database tables',
            help='Create and populate database tables'
        )

        build_targets = db_init_parser.add_mutually_exclusive_group(required=True)
        build_targets.add_argument(
            '-a', '--all',
            action='store_true',
            help='(re)build all tables'
        )
        build_targets.add_argument(
            '-t', '--tables',
            nargs='+',
            help='table(s) to build',
            metavar='<TABLE>'
        )

        # Add sub-parser for the data export task
        export_parser = subparsers.add_parser(
            'Export',
            description='Export database tables as csv',
            help='Export database tables as csv'
        )
        export_sources = export_parser.add_mutually_exclusive_group(required=True)
        export_sources.add_argument(
            '-a', '--all',
            action='store_true',
            help='Export from all tables'
        )
        export_sources.add_argument(
            '-t', '--tables',
            nargs='+',
            help='Table(s) to export',
            metavar='table'
        )

        # Add sub-parser for the table list task
        list_parser = subparsers.add_parser(
            'ListTables',
            description='List all the database tables',
            help='List all the database tables'
        )

        # Add sub-parser for the mysqldump task
        mysqldump_parser = subparsers.add_parser(
            'Dump',
            description='Dump the database to cloud storage',
            help='Dump the database to cloud storage'
        )

        # Add sub-parser for the 'local government reorganization' task
        lgr_parser = subparsers.add_parser(
            'LGReorg',
            description='Process a Local Govt Reorganization file',
            help='Process a Local Govt Reorganization file'
        )

        lgr_parser.add_argument(
            'year',
            type=int,
            help='Year of the Reorganization'
        )

        lgr_parser.add_argument(
            '-d', '--dry-run',
            action='store_true',
            help='show re-org data but do not commit changes'
        )

        lgr_parser.add_argument(
            '-b', '--backup',
            action='store_true',
            help='Backup the database first (implies not -d)'
        )

        # Add sub-parser for the redact task
        redact_parser = subparsers.add_parser(
            'Redact',
            description="Make a redacted copy of program.ini file",
            help="Make a redacted copy of program.ini file"
        )

        redact_parser.add_argument(
            '-s', '--show',
            action='store_true',
            help='Show the redacted ini file'
        )

        # Add sub-parser for the 'count lines of code' task
        cloc_parser = subparsers.add_parser(
            'Cloc',
            description="Count lines of code",
            help="Count lines of code"
        )

        cloc_parser.add_argument(
            '-f', '--by-file',
            action='store_true',
            help='Show counts file-by-file'
        )

        # Add sub-parser for the 'version' task
        cloc_parser = subparsers.add_parser(
            'Version',
            description="Show or update the version number",
            help="Show or update the version number"
        )

        cloc_parser.add_argument(
            '-i', '--increment',
            action='store_true',
            help='Increment the version number'
        )

        # Add sub-parser for the 'DistrictGSS' task
        district_gss_parser = subparsers.add_parser(
            'DistrictGSS',
            description="Import GSS Admin Area Codes",
            help="Import GSS Admin Area Codes"
        )

        district_gss_parser.add_argument(
            '-s', '--source',
            help='Data source file (if not default)',
            metavar='FILE'
        )

        # Add sub_parser for the 'PostCodeBuild' task
        post_code_builder_parser = subparsers.add_parser(
            'PostCodeBuild',
            description='Build post_codes table from Code Point Open dataset',
            help='Build post_codes table.'
        )

        # Decide whether to parse 'test arguments' provided internally
        # or real arguments from the command line
        if test_args is None:
            self.args = parser.parse_args()
        else:
            self.args = parser.parse_args(test_args.split())

        self.sub_commands = subparsers
        self.main_parser = parser

    @property
    def arguments(self) -> argparse.Namespace:
        """
        :return: the parsed arguments
        :rtype: argparse.Namespace
        """
        if self.args.quiet:
            self.args.verbosity = 0
        else:
            self.args.verbosity = self.args.v + 1
        return self.args

    def printable_render(self) -> str:
        """
        :return: a printable render of what MyArgumentParser believes is in its argparse.Namespace.
            Intended for debugging use only and is not, in itself, meant to be parsable.
        :rtype: str
        """
        line = "{:>24} = {}\n"
        out_string = ""
        arguments = (vars(self.arguments))
        for key, value in arguments.items():
            out_string += line.format(key, value)

        return out_string

    def command_list(self):
        line_template = "{:.<17} {}\n"
        out_string = ''
        for key, value in sorted(self.sub_commands.choices.items(), key=lambda item: item[0].lower()):
            out_string += line_template.format(key, value.description)
        return out_string


class MyEnvironment(BaseEnvironment):

    def __init__(self, caller: str):
        super().__init__(caller)
        self.argument_parser = MyArguments(self.program.config)
        self.args = self.argument_parser.arguments
        self.msg = MyStatusMessage(self.args.verbosity)
        self.database_name = 'all_the_stations'
        self.dbc = mysql.connector.connect(
            user=self.program.config['database']['username'],
            host=self.program.config['database']['host'],
            password=self.program.config['database']['password'],
            database=self.database_name
        )
        self.npadb_data_root = '/home/natasha/CloudStation/npadb/all-the-stations/data'

    def render_base_program_info(self):
        if self.args.verbosity > 0:
            print(self.program.welcome(self.user.username))
        if self.args.verbosity >= 4:
            self.msg.debug(f'ROOT: {self.root}')
            self.msg.debug('CONFIG:')
            lines = self.program.printable_config_render(self.program.redacted_config()).split("\n")
            for line in lines:
                if line != '':
                    print(f'{" " * 12}{line}')
            self.msg.debug('COMMAND LINE ARGUMENTS:')
            print(self.argument_parser.printable_render())

    def clean_up(self):
        self.dbc.close()
