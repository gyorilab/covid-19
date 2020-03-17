from os.path import abspath, dirname, join
import pandas as pd

basepath = join(dirname(abspath(__file__)), '..', 'data')
#paths = {'pmc_comm': join(basepath

metadata_file = join(basepath, 'all_sources_metadata_2020-03-13.csv')

def get_metadata(metadata_file):
    metadata = pd.read_csv(metadata_file)
    return metadata

