from src.tasks import *
from src.environment import MyEnvironment
from src.exceptions import *
from src.npadb_tables import NPADBTables

import mysql.connector.errors


class Application(object):

    def __init__(self, root: str):
        self.env = MyEnvironment(root)
        self.env.render_base_program_info()
        self.schema = NPADBTables(self.env)

    def run(self):
        try:
            command = self.subcommand()
            command.run()
        except NPMException as e:
            self.env.msg.error(e.args[0])
            print()
        else:
            self.env.msg.ok([
                f'{self.env.program.program_name} terminated without encountering any fatal errors.',
                f'--Goodbye, {self.env.user.username}.',
                ''
            ])
        finally:
            self.env.clean_up()

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

    def subcommand(self) -> BaseTask:

        if self.env.args.task is None:
            task_class = 'TaskList'
        else:
            task_class = self.env.args.task
        try:
            task_object = eval(task_class + '(self.env)')
            if not isinstance(task_object, BaseTask):
                raise NameError
        except NameError:
            raise NPMException('Bad command')
        else:
            return task_object
