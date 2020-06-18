from src.environment import MyEnvironment

import os


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
        # TODO Make the edition an argument of the task
        edition = '2020-05'
        print('Post Code Build')
        self.data_root = os.path.join(
            '/home/natasha/CloudStation/npadb/all-the-stations/external-data/gazetteer',
            'os-code-point-open-' + edition,
            'Data/CSV'
        )
        print(self.data_root)

    def import_post_code_data(self):
        for filename in self.data_files():
            print(filename)

    def data_files(self):
        # TODO As with the import and export tasks allow the user to choose to process all the input data files
        # or just some of them.
        for entry in os.scandir(self.data_root):
            yield entry.name
