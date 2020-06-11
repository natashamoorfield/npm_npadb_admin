from src.environment import MyEnvironment
from src.exceptions import NPMException
import json
import os.path


class NPADBTables(object):
    """
    Metadata describing tables in NPADB
    """
    def __init__(self, env: MyEnvironment):
        """
        Constructor for class NPADBTables.
        Principally builds a Python dictionary from the metadata stored externally as a json object.
        :param env:
        """
        self.json_filepath = os.path.join(env.npadb_data_root, 'metadata.json')
        try:
            with open(self.json_filepath, 'r') as f:
                self.data = json.load(f)
        except FileNotFoundError:
            error_message = f'Json datafile {self.json_filepath} not found.'
            raise NPMException(error_message)
        self.env = env

    def table(self, name: str) -> dict:
        """
        Return the metadata of an individual table
        :param name:
        :return: Table metadata
        :rtype: dict
        """
        try:
            return self.data[name]
        except KeyError:
            error_message = f'No table "{name}" exists in the internal schema.'
            raise NPMException(error_message)

    def show_tables(self):
        """
        Print a list of all the tables in the schema with individual record counts.
        """
        db_name = self.env.program.bold(self.env.database_name)
        print(f'Tables in database {db_name}:\n')
        if len(self.data) == 0:
            print('    ** No Tables **')
        else:
            total_records = 0
            line_template = '{:<31}{:>8,}'
            for table in self.data.keys():
                c = self.env.dbc.cursor()
                q = f'select count(1) from {table}'
                c.execute(q)
                count = c.fetchone()[0]
                print(line_template.format(table, count))
                total_records += count
                c.close()
            print()
            print(line_template.format('Total Records:', total_records))
            print(line_template.format('Table count:', len(self.data)))
        print()
