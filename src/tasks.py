from src.exceptions import NPMException
from src.environment import MyEnvironment
from src.npadb_tables import NPADBTables
from src.table import Table

import os
import subprocess


class BaseTask(object):
    def __init__(self, env: MyEnvironment):
        self.env = env
        self.schema = NPADBTables(self.env)

    def run(self):
        raise NPMException(f'{self.__class__.__name__} functionality not implemented.')


class Import(BaseTask):
    def run(self):
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
        self.env.msg.info(f"Building table '{table_name}' from data in group '{table_metadata['group']}'")
        table = Table(self.env, table_name, table_metadata)
        self.env.msg.debug(table.ddl_filepath)
        self.env.msg.debug(table.data_filepath)
        if os.path.isfile(table.data_filepath):
            table.create_table()
            table.populate_table()
        else:
            self.env.msg.warning([
                f"'{table_name}.csv' not found in '{table_metadata['group']}'",
                'No changes have been made to the existing table structure or data.'
            ])


class Export(BaseTask):
    def run(self):
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
        self.env.msg.info(f"Exporting table '{table_name}' from group '{table_metadata['group']}'")
        table = Table(self.env, table_name, table_metadata)
        self.env.msg.debug(table.export_filepath)
        table.export()


class ListTables(BaseTask):
    def run(self):
        self.schema.show_tables()


class Dump(BaseTask):
    """
    Backup NPADB using mysqldump
    :todo: Add process to manage old backups
    :todo: Add process to restore the database from a backup
    """
    def run(self):
        dump_id = self.env.program.start.strftime('%Y-%m-%d-%H%M%S')
        dump_filepath = os.path.join('/home/natasha/Dropbox/db_interface', f'{self.env.database_name}_{dump_id}.sql')
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
        self.env.msg.info(' '.join(dump_command))
        subprocess.run(dump_command)
        self.env.msg.info(' '.join(comp_command))
        subprocess.run(comp_command)


class LGReorg(BaseTask):
    def run(self):
        """
        Run the Local Government Reorganization task.
        Unless over-ridden by options (-q, -d or -b) the user is prompted to confirm that
        she wishes to commit to the irrevocable changes the process will make to the database.
        She can choose to barrel on regardless,
        perform a database backup first,
        revert to dry-run mode or
        abort the process altogether.
        No confirmation is sought if dba is running in quite mode.
        The -b option ensures a backup is carried out even in quiet mode.
        :return: A single, upper case character.
        """
        from src.lgro import LocalGovernmentReorganization
        lgr = LocalGovernmentReorganization(self.env)
        if self.env.args.dry_run:
            lgr.do_dry_run()
        else:
            r = self.confirm_reorganization()
            if r == "Y":
                lgr.reorganize()
            elif r == 'B':
                backup = Dump(self.env)
                backup.run()
                lgr.reorganize()
            elif r == 'D':
                lgr.do_dry_run()
            else:
                self.env.msg.ok("LGReorg safely aborted.")

    def confirm_reorganization(self):
        """
        Obtain confirmation that the LGReorg can proceed.
        In quiet mode or if a backup is requested in the options (-b) , confirmations is automatic.
        Otherwise the user is explicitly asked to confirm what she wants to do.
        :return:
        """
        if self.env.args.backup:
            return 'B'
        if self.env.args.quiet:
            return 'Y'
        self.env.msg.info([
            "This process performs bulk updates on the database that cannot be undone.",
            "--Please confirm your intention to continue:",
            ">>Enter 'Y' to continue",
            ">>Enter 'B' to continue but backup the database first",
            ">>Enter 'D' to look at but not execute the changes",
            ">>Anything else to abort altogether"
        ])
        response = input("\n           Response: ")
        print()
        if len(response) == 0:
            return 'A'
        return response.strip().upper()[0]


class Redact(BaseTask):
    def run(self):
        r = self.env.program.redacted_config()
        self.env.program.save_config_data((r, 'redacted.ini'))
        if self.env.args.show:
            print(self.env.program.printable_config_render(r))


class Cloc(BaseTask):
    def run(self):
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


class Version(BaseTask):
    def run(self):
        if self.env.args.increment:
            self.env.program.inc_version()
        else:
            print(f'** Version {self.env.program.version_string()} **')


class TaskList(BaseTask):
    def run(self):
        print('AVAILABLE COMMANDS:')
        print(self.env.argument_parser.command_list())
