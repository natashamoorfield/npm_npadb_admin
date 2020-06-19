from src.environment import MyEnvironment
from src.exceptions import CodePointOpenError

import os
import re


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

    def import_post_code_data(self):
        for filename in self.data_files():
            print(filename)

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
            e = CodePointOpenError('Code Point Open data directory not found:')
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
