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
        """
        # TODO: Define the translation of display_name to index_name
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
            entity_name = EntityName(row[field['field_id']])
            row[field['field_id'] - 1] = entity_name.index_name(field['special_index'])

        return row

    def value_conversions_export(self, retrieved_row: tuple) -> list:
        row = list(retrieved_row)
        for field in self.table_metadata['uuid_fields']:
            row[field] = uuid.UUID(bytes=bytes(row[field]))
        return row

    @staticmethod
    def values_access_levels(items):
        return items

    @staticmethod
    def values_action_access_levels(items):
        return items

    @staticmethod
    def values_actions(items):
        items[1] = items[1].replace(' ', '_').lower()
        for i in [3, 5]:
            if items[i] == '':
                items[i] = None
        if items[4] == '':
            items[4] = 0
        return items

    def values_ale_carriers(self, items):
        self.record_count += 1
        items.insert(0, self.record_count)
        return items

    @staticmethod
    def values_ale_quality_definitions(items):
        for i in [3, 5, 6]:
            if items[i] == '':
                items[i] = None
        return items

    def values_ale_names(self, items):
        items[2] = self.index_name(items[3])
        return items

    @staticmethod
    def values_ales(items):
        return items

    @staticmethod
    def values_beer_festivals(items):
        for i in [5, 7, 9, 10, 11]:
            if items[i] == '':
                items[i] = None
        return items

    @staticmethod
    def values_breweries(items):
        if items[1] == 0:
            items[1] = None
        for i in [2, 3, 8, 10, 14, 15, 16]:
            if items[i] == '':
                items[i] = None
        for i in [9, 11]:
            if items[i - 1] is None:
                items[i] = '1FF'
            elif items[i] == "D":
                items[i] = 'FFF'
            elif items[i] == "M":
                items[i] = 'FFB'
            else:
                items[i] = 'FF7'
        return items

    @staticmethod
    def values_brewery_ales(items):
        return items

    @staticmethod
    def values_brewery_address_types(items):
        if items[3] == '':
            items[3] = None
        return items

    @staticmethod
    def values_brewery_contacts(items):
        return items

    @staticmethod
    def values_brewery_link_types(items):
        if items[2] == '':
            items[2] = None
        return items

    def values_brewery_links(self, items):
        self.record_count += 1
        items.insert(0, self.record_count)
        return items

    def values_brewery_names(self, items):
        items[2] = self.index_name(items[3])
        return items

    @staticmethod
    def values_brewery_pictures(items):
        if items[0] == 'brewery_id':
            return None
        return items

    @staticmethod
    def values_brewery_types(items):
        for item in [2, 3]:
            if items[item] == '':
                items[item] = None
        items[4] = int(items[0]) >> 8
        return items

    @staticmethod
    def values_brewery_status_codes(items):
        for item in [2, 3]:
            if items[item] == '':
                items[item] = None
        if items[4] == 'N':
            items[4] = 'No'
        elif items[4] == 'Y':
            items[4] = 'Yes'
        else:
            items[4] = 'Question'
        items[5] = int(items[0]) >> 8
        return items

    @staticmethod
    def values_brewery_type_groups(items):
        if items[2] == '':
            items[2] = None
        return items

    @staticmethod
    def values_brewing_status_groups(items):
        if items[2] == '':
            items[2] = None
        return items

    @staticmethod
    def values_contact_types(items):
        return items

    def values_copyright_types(self, items):
        self.record_count += 1
        if items[0] == '40':
            items[0] = 255
        else:
            items[0] = int(items[0]) * 4
        for i in [2, 3]:
            if items[i] == '':
                items[i] = None
        items.insert(0, self.record_count)
        return items

    @staticmethod
    def values_counties(items):
        return items

    @staticmethod
    def values_district_types(items):
        return items

    def values_drinks(self, items):
        self.record_count += 1
        if items[3] == '':
            items[3] = None
        items.append(1)
        items.insert(2, 0)
        items.insert(0, self.record_count)
        return items

    @staticmethod
    def values_gbg_entry_types(items):
        return items

    @staticmethod
    def values_gender_descriptors(items):
        return [
            uuid.UUID(items[0]).bytes,
            items[1]
        ]

    @staticmethod
    def values_gr_sources(items):
        return items

    @staticmethod
    def values_locality_types(items):
        return items

    @staticmethod
    def values_name_types(items):
        return items

    def values_nations(self, items):
        return [
            items[0],
            self.index_name(items[1]),
            items[1],
            items[2]
        ]

    def values_network_names(self, items):
        return [
            items[0],
            items[1],
            self.index_name(items[2]),
            items[2],
            items[3]
        ]

    @staticmethod
    def values_network_types(items):
        return items

    @staticmethod
    def values_networks(items):
        return items

    @staticmethod
    def values_pictures(items):
        if items[0] == 'picture_id':
            return None
        for i in [3, 4, 8, 9]:
            if items[i] == '':
                items[i] = None
        return items

    @staticmethod
    def values_picture_types(items):
        return items

    @staticmethod
    def values_pronoun_sets(items):
        items[0] = uuid.UUID(items[0]).bytes
        return items

    @staticmethod
    def values_pubs(items):
        # ignore the zero'th record
        if items[0] == '0':
            return None
        # convert empty strings to NULL
        for i in [4, 5, 13, 16, 17, 18]:
            if items[i] == '':
                items[i] = None
        # convert brewery_id zeros to nulls
        if items[14] == '0':
            items[14] = None
        # convert mr4_cask_ales zeros to NULLs
        if len(items[11]) == 3:
            # but not for pubs which do not serve cask ale
            items[12] = '0'
        elif items[12] == '0':
            items[12] = None
        # remove pub contact details
        items.pop(6)
        items.pop(6)
        # add mr4_keg_beers
        items.insert(11, None)
        return items

    @staticmethod
    def values_pub_contacts(items):
        return items

    @staticmethod
    def values_pub_gbg_entries(items):
        return items

    def values_pub_names(self, items):
        if items[0] == '0':
            return None
        items[2] = self.index_name(items[3])
        return items

    @staticmethod
    def values_pub_pictures(items):
        if items[0] == 'pub_id':
            return None
        return items

    @staticmethod
    def values_pub_ratings(items):
        return items

    @staticmethod
    def values_pub_status_codes(items):
        if items[3] == '':
            items[3] = None
        if items[5] == '':
            items[5] = '#ffffff'
        return items

    @staticmethod
    def values_pub_types(items):
        if items[4] == "Deprecated":
            return None
        return_array = []
        flags = 0
        return_array.append(items[0])
        return_array.append(items[1])
        if items[2] == 'Yes':
            flags = 1
        if items[3] == 'Yes':
            flags += 2
        return_array.append(flags)
        if items[4] == '':
            return_array.append(None)
        else:
            return_array.append(items[4])
        return return_array

    @staticmethod
    def values_pub_visit_codes(items):
        if items[0] == '0':
            items[1] = None
            items[2] = None
        return items

    def values_station_names(self, items):
        return [
            items[0],
            items[1],
            self.index_name(items[2]),
            items[2],
            items[3]
        ]

    @staticmethod
    def values_station_pictures(items):
        if items[0] == 'station_id':
            return None
        return items

    @staticmethod
    def values_station_status_codes(items):
        return [
            int(items[0], 16),
            items[1],
            items[2]
        ]

    @staticmethod
    def values_station_visit_codes(items):
        return items

    @staticmethod
    def values_station_visit_types(items):
        return items

    @staticmethod
    def values_stations(items):
        if len(items[3]) == 0:
            # a blank three_letter_code must be NULL not empty string.
            items[3] = None
        for i in range(5):
            items.append(None)
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

    @staticmethod
    def values_users(items):
        for i in [0, 3, 4]:
            items[i] = uuid.UUID(items[i]).bytes
        return items
