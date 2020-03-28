import os
import sys
import json
import time
from os.path import abspath, dirname, join
import pandas as pd
from indra.util import batch_iter
from indra.statements import stmts_from_json
from indra.tools import assemble_corpus as ac
from indra_db import get_db, get_primary_db
from indra_db.client.readonly import get_statement_jsons_from_papers
from indra_db.util import distill_stmts

basepath = join(dirname(abspath(__file__)), '..', 'data', '2020-03-27')


def get_path(subdir):
    return join(basepath, subdir, subdir)

paths = {'pmc_comm': get_path('comm_use_subset'),
         'pmc_noncomm': get_path('noncomm_use_subset'),
         'pmc_custom': get_path('custom_license'),
         'preprint': get_path('biorxiv_medrxiv'),}


metadata_file = join(basepath, 'metadata.csv')


doc_df = None


def get_article_data():
    file_data = []
    hashes = []
    for content_type, content_path in paths.items():
        for filename in os.listdir(content_path):
            if filename.endswith('.json'):
                file_hash = filename.split('.')[0]
                hashes.append(file_hash)
                file_data.append((content_path, content_type))
    file_df = pd.DataFrame(file_data, index=hashes, dtype='str',
                           columns=['content_path', 'content_type'])
    metadata = pd.read_csv(metadata_file)
    file_data = metadata.join(file_df, 'sha')
    return file_data


def get_pmcids():
    """Get unique PMCIDs from the dataset."""
    global doc_df
    if doc_df is None:
        doc_df = get_article_data()
    unique_pmcids = list(doc_df[~pd.isna(doc_df.pmcid)].pmcid.unique())
    return unique_pmcids


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

