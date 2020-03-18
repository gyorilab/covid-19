import os
import sys
import json
from os.path import abspath, dirname, join
import pandas as pd


basepath = join(dirname(abspath(__file__)), '..', 'data')

def get_path(subdir):
    return join(basepath, subdir, subdir)

paths = {'pmc_comm': get_path('comm_use_subset'),
         'pmc_noncomm': get_path('noncomm_use_subset'),
         'pmc_custom': get_path('pmc_custom_license'),
         'preprint': get_path('biorxiv_medrxiv'),}


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


def unique_pmids(df):
    return df[~pd.isna(df.pmcid)].pmcid.unique()


def get_text_from_json(json_filename):
    with open(json_filename, 'rt') as f:
        doc_json = json.load(f)
    text = ''
    text += doc_json['metadata']['title']
    text += '.\n'
    for p in doc_json['abstract']:
        text += p['text']
        text += '\n'
    for p in doc_json['body_text']:
        text += p['text']
        text += '\n'
    for cap_dict in doc_json['ref_entries'].values():
        text += cap_dict['text']
        text += '\n'
    return text


def dump_text_files(output_dir, doc_df):
    sha_ix = 1
    path_ix = 15
    title_ix = 3
    abs_ix = 8
    # Start by dumping full texts
    dumped_rows = set()
    text_df = doc_df[~pd.isna(doc_df.content_path)]
    ft_counter = 0
    for row in text_df.itertuples():
        ix = row[0]
        json_file = f'{join(row[path_ix], row[sha_ix])}.json'
        text = get_text_from_json(json_file)
        output_file = join(output_dir, f'CORD19_DOC_{ix}.txt')
        #output_file = f'{join(output_dir, row[sha_ix])}.txt'
        with open(output_file, 'wt') as f:
            f.write(text)
            ft_counter += 1
        dumped_rows.add(ix)
    # Then look at the abstracts
    abstract_df = doc_df[pd.isna(doc_df.content_path) &
                         ~pd.isna(doc_df.abstract)]
    for row in abstract_df.itertuples():
        ix = row[0]
        # If we've already dumped full text, skip it
        if ix in dumped_rows:
            continue
        text = row[title_ix]
        text += '.\n'
        text += row[abs_ix]
        output_file = join(output_dir, f'CORD19_DOC_{ix}.txt')
        with open(output_file, 'wt') as f:
            f.write(text)
    # Finally, dump the metadata to a CSV file
    doc_df.to_csv(join(output_dir, 'metadata.csv'))


if __name__ == '__main__':
    df = get_file_data()
    output_dir = sys.argv[1]
    dump_text_files(output_dir, df)
    #df[~pd.isna(df.pmcid)].pmcid.to_csv('covid_pmcids.csv', index=False)
    """
    - 29,500 total entries in metadata file.
    - 13,219 rows in dataset with non-NA content_path, meaning they have a
      JSON file with hash associated with a row in the metadata dataframe.
    - Of the remaining 16,281 rows with NO content_path, 
    """





