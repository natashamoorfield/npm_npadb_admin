from src.environment import MyEnvironment
from src.exceptions import LGRException
from datetime import date, timedelta
import os.path
import json
from mysql.connector import Error as MySQLError


class LGROldDistrict(object):
    """A class for districts being abolished"""

    def __init__(self, env: MyEnvironment, district: str, county: int):
        self.county_id = county
        self.env = env
        self.name = district
        self.id = self.fetched_district_data()

    def fetched_district_data(self):
        q = "select district_id from districts where index_name = %s and county_id = %s"
        # Adding county_id to the search parameters reduces the possibility of false positive and duplicate results.
        c = self.env.dbc.cursor(prepared=True)
        c.execute(q, (self.name, self.county_id))
        results = c.fetchall()
        n = c.rowcount
        c.close()
        if n == 1:
            return results[0][0]
        if n == 0:
            raise LGRException(f'District "{self.name}" not found.')
        else:
            raise LGRException(f'{n} entries for district "{self.name}" found.')


class LGRNewDistrict(object):
    """
    The local government reorganization data for a new local authority
    """

    def __init__(self, env: MyEnvironment, district: dict, county_id: int):
        self.env = env
        self.name = district['new_district_name']
        self.county_id = county_id
        self.id, self.district_type = self.fetched_district_data()
        if self.district_type == 0:
            self.district_type = district['district_type']
        if self.district_type != district['district_type']:
            raise LGRException(f'District Type mismatch for {self.name}.')
        self.old_districts = []
        for d in district['old_districts']:
            self.old_districts.append(LGROldDistrict(self.env, d, self.county_id))

    def fetched_district_data(self):
        q = "select district_id, district_type_id from districts "
        q += "where index_name = %s and county_id = %s and npm_admin_district = 1"
        cursor = self.env.dbc.cursor(prepared=True)
        cursor.execute(q, (self.name, self.county_id))
        results = cursor.fetchall()
        n = cursor.rowcount
        cursor.close()
        if n == 1:
            return results[0][0], results[0][1]
        if n == 0:
            return 0, 0
        else:
            raise LGRException(f'{n} entries for district "{self.name}" found.')

    def post_new_district(self, inauguration_date: date) -> list:
        """
        Insert the new district into the database districts table
        :param inauguration_date:
        :return: A list of error messages or, on success, an empty list.
        :rtype: list
        """
        new_district_data = (
            self.id,
            self.county_id,
            self.name,
            self.name,
            self.district_type,
            1,
            None,
            inauguration_date.isoformat(),
            None
        )
        q = "insert into districts ("
        q += "district_id, county_id, index_name, display_name, district_type_id, npm_admin_district, "
        q += "gss_admin_area_code, inauguration_date, abolition_date"
        q += ") values (%s, $s, $s, $s, $s, $s, $s, $s, $s)"
        self.env.msg.debug(q)
        self.env.msg.debug(new_district_data)
        cursor = self.env.dbc.cursor(prepared=True)
        try:
            # TODO execute the query
            pass
        except MySQLError as e:
            messages = [
                f"An error occurred creating a district record for {self.name}",
                e.msg,
                "Skipping creation of district-level (G3) generic level town.",
                "Skipping creation of district-level (G3) generic level locality."
            ]
            self.env.msg.warning(messages)
        else:
            messages = []
        finally:
            cursor.close()
        return messages


class LGRCounty(object):
    """
    The local government reorganization data for an individual county
    """

    def __init__(self, env: MyEnvironment, county: dict):
        self.env = env
        self.name = county['county_name']
        self.id, self.next_district_id = self.fetched_county_data()
        self.new_districts = []
        for d in county['new_districts']:
            self.new_districts.append(LGRNewDistrict(self.env, d, self.id))

    def fetched_county_data(self):
        q = "select c.county_id, max(d.district_id) + 1 as next_district_id "
        q += "from counties as c inner join districts d on c.county_id = d.county_id "
        q += "where c.index_name = %s group by c.county_id"
        cursor = self.env.dbc.cursor(prepared=True)
        cursor.execute(q, (self.name,))
        results = cursor.fetchall()
        n = cursor.rowcount
        cursor.close()
        if n == 1:
            return results[0][0], results[0][1]
        if n == 0:
            raise LGRException(f'County "{self.name}" not found.')
        else:
            raise LGRException(f'{n} entries for county "{self.name}" found.')


class LGRData(object):
    """
    The data defining a local government reorganization are held in a json file.
    This class loads and parses that information ready for the mapping onto the gazetteer tables.
    """

    def __init__(self, env: MyEnvironment, year: int):
        self.year = year
        self.json_filename = os.path.join(
            env.npadb_data_root,
            'updates',
            f'lgro-{self.year}.json'
        )
        try:
            with open(self.json_filename, 'r') as f:
                self.data = json.load(f)
        except FileNotFoundError:
            error_message = f'Json datafile {self.json_filename} not found.'
            raise LGRException(error_message)
        self.counties = []
        for c in self.data['counties']:
            self.counties.append(LGRCounty(env, c))


class LocalGovernmentReorganization(object):
    """
    Periodically, local government districts in England are reorganized because, if they weren't, the ass hat whose
    bullshit job it is to carry out local government reorganizations would have nothing to do.
    These re-organizations typically involve abolishing county councils (which, although a shameful diminution of
    democracy, is of no concern here) and amalgamating the Districts within the county into one or more Unitary
    Authorities.  Some reorganizations simply involve merging Districts into new, larger Districts which,
    from our perspective, amounts to the same thing.

    These changes are represented by the data in the /data/lgr-yyyy.json files.

    To map these changes onto our gazetteer we need to carry out the following processes:
    Create a new record in the districts table for each new local authority.
    Create new district-level generic localities in the towns and localities table for each new local authority.
    Change the npm_admin_district to 0 and set abolished_date on old district records.
    Transfer entries for the defunct districts in the towns table to the appropriate new district.
    Transfer entries for the defunct districts in the abc_gazetteer table to the appropriate new district.

    We are not concerned, at present, with any GLEs that are allocated to district-level generic localities
    pertaining to the defunct districts, partly because there are likely to be few, if any, such entities and partly
    because the defunct district-level generic locality will give us a more accurate idea of locality than the new ones.
    """
    G3_TOWN_TYPE_ID = 57472
    G3_LOCALITY_TYPE_ID = 5

    def __init__(self, env: MyEnvironment):
        # set up the environment
        self.env = env
        self.dbc = env.dbc
        self.g3_locality_id = 0  # Acts as an error flag as well as actual data; 0 is the error condition.

        # set up dates - the assumption is that all local government reorganizations come in effect on 1 April
        self.lgr_year = self.env.args.year
        self.inauguration_date = date(self.lgr_year, 4, 1)
        self.abolition_date = self.inauguration_date - timedelta(days=1)

        if not self.env.args.quiet:
            # print task title
            msg = f'Local Government Reorganization {self.lgr_year}'
            print(msg)
            print('=' * len(msg))
            print()

        # load datafile and parse JSON data into Python objects
        self.data = LGRData(self.env, self.lgr_year)

        # As this is a somewhat delicate and critical process
        # that could leave the database in an inconsistent state
        # we are forcing status messages to be printed unless
        # the user specifically overrides verbosity with option `-q`
        if self.env.args.verbosity < 3 and not self.env.args.quiet:
            self.env.msg.verbosity = 3
            self.env.msg.info('Verbosity reset to INFO for this task.')
            print()

    def do_dry_run(self):
        """
        Instead of actually performing the reorganization, just display the parsed input date to
        give the user an indication of the changes that will be made when she commits to it.
        :return: Nothing
        """
        # Just display the parsed input data
        for county in self.data.counties:
            print(f'{county.name} ({county.id} {county.next_district_id})')
            for new_district in county.new_districts:
                print(f'    {new_district.name} ({new_district.id}, {new_district.district_type})')
                for old_district in new_district.old_districts:
                    print(f'        {old_district.name} ({old_district.id})')
            print()
        self.env.msg.warning('Dry run only; no changes have been committed to the database.')

    def reorganize(self):
        """
        Commit the reorganization to the gazetteer
        """
        # TODO Confirm that the user wishes to proceed with a 'cannot be undone' warning
        # TODO Carry out a backup of the database as changes cannot otherwise be undone
        for county in self.data.counties:
            for new_district in county.new_districts:
                self.g3_locality_id = -1
                if new_district.id == 0:
                    self.create_new_district(new_district, county)
                # Skip the next bit if there were errors creating the new district
                if self.g3_locality_id != 0:
                    for old_district in new_district.old_districts:
                        self.abolish_old_district(old_district, new_district)
                else:
                    messages = [
                        f'The process to abolish the predecessor districts has not been run.',
                        f'--This is to help preserve the integrity of the gazetteer following earlier errors.'
                    ]
                    self.env.msg.warning(messages)
                self.conditional_blank_line()

    def create_new_district(self, new_district: LGRNewDistrict, county: LGRCounty):
        new_district.id = county.next_district_id
        error_messages = new_district.post_new_district(self.inauguration_date)
        if len(error_messages) == 0:
            self.env.msg.info(f'Create new district {new_district.name} '
                              f'({new_district.id}) in {county.name}: {self.inauguration_date}')
            self.env.msg.debug(error_messages)
            # Only attempt to create a G3 town if the new district was successfully created
            g3_town_id = self.create_g3_town(new_district)
            if g3_town_id > 0:
                # Only attempt to create a G3 locality if G3 town was successfully created
                self.g3_locality_id = self.create_g3_locality(new_district, g3_town_id)
            county.next_district_id += 1
        else:
            self.g3_locality_id = 0

    def abolish_old_district(self, old_district: LGROldDistrict, new_district: LGRNewDistrict):
        messages = self.update_old_district(old_district)
        if len(messages) == 0:
            messages = self.update_towns(old_district, new_district)
            if len(messages) == 0:
                self.update_abc_gazetteer(old_district, new_district)

    def create_g3_town(self, new_district: LGRNewDistrict) -> int:
        """
        Create a district-level (G3) generic locality in towns table
        :returns: The town_id of the new record,
        for subsequent use in creating a generic locality in the localities table.
        :rtype: int
        """
        cursor = self.env.dbc.cursor(prepared=True)
        q = "insert into towns ("
        q += "district_id, index_name, display_name, town_type_id"
        q += ") values (%s, $s, $s, $s)"
        self.env.msg.debug(q)
        new_record_data = (new_district.id, new_district.name, new_district.name, self.G3_TOWN_TYPE_ID)
        self.env.msg.debug(new_record_data)
        try:
            # TODO execute the query
            pass
        except MySQLError as e:
            error_messages = [
                f"An error occurred inserting district-level (G3) generic town {new_district.name}",
                e.msg,
                "Skipping creation of district_level generic (G3) locality in localities table"
            ]
            self.env.msg.warning([error_messages])
            g3_town_id = 0
            self.g3_locality_id = 0
        else:
            g3_town_id = 69
            self.env.msg.info(f'Create a new district-level (G3) generic town for {new_district.name}')
            self.env.msg.debug(f'New town_id is {g3_town_id}')
        finally:
            cursor.close()
        return g3_town_id

    def create_g3_locality(self, new_district: LGRNewDistrict, town_id: int):
        cursor = self.env.dbc.cursor(prepared=True)
        q = "insert into localities ("
        q += "town_id, locality_type_id, index_name, display_name"
        q += ") values (%s, $s, $s, $s)"
        self.env.msg.debug(q)
        new_record_data = (
            town_id,
            self.G3_LOCALITY_TYPE_ID,
            new_district.name,
            new_district.name
        )
        self.env.msg.debug(new_record_data)
        try:
            # TODO execute the query
            pass
        except MySQLError as e:
            error_messages = [
                f"An error occurred inserting district-level (G3) generic locality for {new_district.name}",
                e.msg
            ]
            self.env.msg.warning([error_messages])
            g3_locality_id = 0
        else:
            g3_locality_id = 6964
            self.env.msg.info(f'Create a new district-level (G3) generic locality '
                              f'for {new_district.name} in generic town #{town_id}')
            self.env.msg.debug(f'New locality_id is {g3_locality_id}')
        finally:
            cursor.close()
        return g3_locality_id

    def update_old_district(self, district: LGROldDistrict) -> list:
        """
        Mark district and defunct and enter abolition date
        Return a list of error messages if a database error is raised, otherwise return an empty list.
        :param district:
        :return:
        """
        cursor = self.env.dbc.cursor(prepared=True)
        q = "update districts "
        q += "set npm_admin_district = %s, "
        q += " abolition_date = %s "
        q += " where district_id = %s"
        self.env.msg.debug(q)
        update_arguments = (0, self.abolition_date.isoformat(), district.id)
        self.env.msg.debug(update_arguments)
        try:
            # TODO execute the query
            pass
        except MySQLError as e:
            messages = [
                f"An error occurred updating the district record for {district.name} ({district.id}).",
                e.msg,
                "Skipping updates of the towns and abc_gazetteer tables."
            ]
            self.env.msg.warning(messages)
        else:
            self.env.msg.info(f'Abolish {district.name} ({district.id}) on {self.abolition_date}')
            messages = []
        finally:
            cursor.close()
        return messages

    def update_towns(self, old_district: LGROldDistrict, new_district: LGRNewDistrict) -> list:
        """
        Update the towns table, moving towns in old_district to new_district
        Return a list of error messages if a database error is raised, otherwise return an empty list.
        :param old_district:
        :param new_district:
        :return:
        """
        cursor = self.env.dbc.cursor(prepared=True)
        q = "update towns "
        q += "set district_id = %s "
        q += "where district_id = %s "
        q += "and town_type_id <> %s "  # Be sure not to update the existing district-level (G3) generic localities
        self.env.msg.debug(q)
        update_arguments = (new_district.id, old_district.id, self.G3_TOWN_TYPE_ID)
        self.env.msg.debug(update_arguments)
        try:
            # TODO execute the query
            pass
        except MySQLError as e:
            messages = [
                f"An error occurred updating the towns table for towns that were in "
                f"{old_district.name} ({old_district.id}).",
                e.msg,
                f"Skipping updates of the abc_gazetteer table."
            ]
            self.env.msg.warning(messages)
        else:
            self.env.msg.info(f'Move towns from {old_district.name} ({old_district.id}) '
                              f'to {new_district.name} ({new_district.id})')
            messages = []
        finally:
            cursor.close()
        return messages

    def update_abc_gazetteer(self, old_district: LGROldDistrict, new_district: LGRNewDistrict):
        cursor = self.env.dbc.cursor(prepared=True)
        q = "update abc_gazetteer "
        q += "set district_id = %s "
        q += "where district_id = %s"
        self.env.msg.debug(q)
        update_arguments = (new_district.id, old_district.id)
        self.env.msg.debug(update_arguments)
        try:
            # TODO execute query
            pass
        except MySQLError as e:
            messages = [
                f'An error occurred updating the abc_gazetteer table for towns that were in '
                f'{old_district.name} ({old_district.id})',
                e.msg
            ]
            self.env.msg.warning(messages)
        else:
            self.env.msg.info(f'Move ABC Gazetteer entries from {old_district.name} ({old_district.id}) '
                              f'to {new_district.name} ({new_district.id})')
        finally:
            cursor.close()

    def conditional_blank_line(self):
        """
        Print a blank line unless running in quite mode
        """
        if not self.env.args.quiet:
            print()
