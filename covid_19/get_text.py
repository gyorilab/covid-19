import os
from os.path import abspath, dirname, join
import pandas as pd
from indra_db import client

basepath = join(dirname(abspath(__file__)), '..', 'data')
paths = {'pmc_comm': join(basepath, 'comm_use_subset', 'comm_use_subset'),
         'pmc_noncomm': join(basepath, 'noncomm_use_subset',
                                       'noncomm_use_subset'),
         'pmc_custom': join(basepath, 'pmc_custom_license',
                                      'pmc_custom_license'),
         'preprint': join(basepath, 'biorxiv_medrxiv', 'biorxiv_medrxiv')}

metadata_file = join(basepath, 'all_sources_metadata_2020-03-13.csv')

def get_file_data():
    file_data = []
    hashes = []
    for content_type, content_path in paths.items():
        for filename in os.listdir(content_path):
            if filename.endswith('.json'):
                file_hash = filename.split('.')[0]
                hashes.append(file_hash)
                file_data.append((content_path, content_type))
    file_df = pd.DataFrame(file_data, index=hashes,
                           columns=['content_path', 'content_type'])
    metadata = pd.read_csv(metadata_file)
    file_data = metadata.join(file_df, 'sha')
    return file_data


if __name__ == '__main__':
    df = get_file_data()
    df[~pd.isna(df.pmcid)].pmcid.to_csv('covid_pmcids.csv', index=False)
