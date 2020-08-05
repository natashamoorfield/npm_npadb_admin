import os
import mysql.connector.errors
import csv
import uuid

from src.entity_name import EntityName
from src.exceptions import *
from src.environment import MyEnvironment


class Table(object):
    def __init__(self, env: MyEnvironment, table_name: str, table_metadata: dict):
        self.env = env
        self.table_name = table_name
        self.table_metadata = table_metadata
        self.data_filepath = os.path.join(
            self.env.npadb_data_root,
            table_metadata['group'],
            f'{table_name}.csv'
        )
        self.ddl_filepath = os.path.join(
            self.env.npadb_data_root,
            table_metadata['group'],
            f'{table_name}.sql'
        )
        self.export_filepath = os.path.join(
            self.env.npadb_data_root,
            'export',
            f'{table_name}.csv'
        )
        self.record_count = 0

    def export(self):
        """
        Export the data, row by row, from the database to the csv text file.
        Report the number of records exported from the table.
        If there is a database error, halt the export of this table but do not stop the whole program.
        """
        query = f"select * from {self.table_name}"
        c = self.env.dbc.cursor()
        record_count = 0
        try:
            c.execute(query)
        except mysql.connector.errors.ProgrammingError as e:
            self.env.msg.warning(f"Database Error: {e.msg}")
        else:
            with open(self.export_filepath, 'w', newline='') as f:
                writer = csv.writer(f, delimiter='\t')
                for row in c:
                    writer.writerow(self.value_conversions_export(row))
                    record_count += 1
        finally:
            self.env.msg.info(f"Records exported from '{self.table_name}' table = {record_count}")
            c.close()

    def value_conversions_export(self, retrieved_row: tuple) -> list:
        row = list(retrieved_row)
        for field in self.table_metadata['uuid_fields']:
            row[field] = uuid.UUID(bytes=bytes(row[field]))
        return row

    def create_table(self):
        try:
            with open(self.ddl_filepath, 'r') as f:
                sql_file = f.read()
        except FileNotFoundError as e:
            raise NPMException(e)
        c = self.env.dbc.cursor()
        q = 'DROP TABLE IF EXISTS {}'.format(self.table_name)
        c.execute(q)
        statements = sql_file.split(';')
        for q in [item.strip() for item in statements]:
            if len(q) > 0:
                c.execute(q)
        c.close()

    def populate_table(self):
        values_function = None
        generic_function = 'value_conversions_import'
        try:
            fn = f'values_{self.table_name}'
            values_function = getattr(self, fn)
            self.env.msg.debug(f"Using data conversion method '{fn}'")
        except AttributeError:
            # No bespoke field value conversion method exists,
            # so fallback to the generic method which is guided by table metadata
            values_function = getattr(self, generic_function)
            self.env.msg.debug(f"Using generic data conversion method '{generic_function}'")
        finally:
            # populate the table
            q = self.data_insert_statement()
            c = self.env.dbc.cursor()
            record_count = 0
            try:
                with open(self.data_filepath, newline='') as f:
                    for row in csv.reader(f, delimiter='\t'):
                        values = values_function(row)
                        if values is not None:
                            c.execute(q, values)
                            record_count += 1
            except FileNotFoundError:
                self.env.msg.warning(
                    f"Import file '{self.table_name}.csv' not found in '{self.table_metadata['group']}'"
                )
            finally:
                c.close()
                self.env.dbc.commit()
                self.env.msg.info(f"Records inserted into '{self.table_name}' table = {record_count}")

    def data_insert_statement(self):
        q1 = "select column_name from information_schema.columns "
        q1 += "where table_schema = 'all_the_stations' "
        q1 += f"and table_name = '{self.table_name}' "
        q1 += "order by ordinal_position"
        c = self.env.dbc.cursor()
        c.execute(q1)
        field_list = [f"`{x[0]}`" for x in c.fetchall()]
        c.close()
        col_names = ', '.join(field_list)
        placeholders = ', '.join(['%s'] * len(field_list))
        return f'insert into {self.table_name} ({col_names}) values ({placeholders})'

    @staticmethod
    def index_name(display_name: str) -> str:
        """
        Return an index normalized version of the given display name.
        This method is now deprecated in favour of functionality contained within the EntityName class
        """
        return display_name

    def value_conversions_import(self, row: list) -> list:
        """
        Return a modified CSV row depending upon the table's metadata
        :param row:
        :return:
        """
        # Fields which can be null should be null if the field in the CSV is the empty string
        for field in self.table_metadata['nullable_fields']:
            if row[field] == '':
                row[field] = None

        # In the CSV, UUIDs are stored as UUID strings; these must be converted to bytes
        for field in self.table_metadata['uuid_fields']:
            row[field] = uuid.UUID(row[field]).bytes

        # Index names need to be generated according to the field's default indexing rules
        # At present the index_name field must be the field immediately before the display_name field
        for field in self.table_metadata['indexible_names']:
            entity_name = EntityName(row[field['field_id']], row[field['field_id'] - 1])
            row[field['field_id'] - 1] = entity_name.index_name(field['special_index'])

        return row

    """
    If a bespoke import process is required (because, for example, the table format has changed) then
    a special method should be created thus:
    
    def values_<table_name>(self, old_row_items: list) -> list:
        <processing code>
        return new_row_items
    """

    @staticmethod
    def values_station_visit_codes(items):
        return items

    @staticmethod
    def values_station_visit_types(items):
        return items

    @staticmethod
    def values_timing_points(items):
        return items

    @staticmethod
    def values_town_type_groups(items):
        return [
            int(items[0], 16),
            items[1]
        ]

    @staticmethod
    def values_town_types(items):
        try:
            descriptor_long = items[4]
        except IndexError:
            descriptor_long = None
        return [
            int(items[0], 16),
            int(items[1], 16),
            items[2],
            items[3],
            descriptor_long
        ]

    @staticmethod
    def values_user_pub_visits(items):
        return items

    @staticmethod
    def values_user_station_visits(items):
        items[1] = uuid.UUID(items[1]).bytes
        return items
