import os
import csv
import sys
import json
import time
import re
import urllib
import logging
import tarfile
from os.path import abspath, dirname, join, isdir
import pandas as pd
from indra.util import zip_string


logger = logging.getLogger(__name__)


baseurl = 'https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/'


def get_latest_available_date():
    """Get the date of the latest CORD19 dataset upload."""
    req = urllib.request.Request((baseurl + 'historical_releases.html')) 
    with urllib.request.urlopen(req) as response: 
        page_content = response.read()
    latest_date = re.search(
        r'<i>Latest release: (.*?)</i>', str(page_content)).group(1)
    logger.info('Latest data release is %s'  % latest_date)
    return latest_date


latest_date = get_latest_available_date()  # For processing latest data
# latest_date = '2020-06-15'  # For processing a different date manually
data_dir = join(dirname(abspath(__file__)), '..', 'data')
basepath = join(data_dir, latest_date)
metadata_file = join(basepath, 'metadata.csv')
doc_gz_path = os.path.join(basepath, 'document_parses.tar.gz')
doc_df = None


def download_metadata():
    """Download metadata file only."""
    # Create missing directories
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
    if not os.path.exists(basepath):
        os.mkdir(basepath)
    if not os.path.exists(metadata_file):
        logger.info('Downloading metadata')
        md_url = baseurl + '%s/metadata.csv'  % latest_date
        urllib.request.urlretrieve(md_url, metadata_file)
    logger.info('Latest metadata is available in %s'  % metadata_file)


def download_latest_data():
    """Download metadata and document parses."""
    download_metadata()
    
    if not os.path.exists(doc_gz_path):
        logger.info('Downloading document parses')
        doc_url = baseurl + '%s/document_parses.tar.gz'  % latest_date
        urllib.request.urlretrieve(doc_url, doc_gz_path)
    logger.info('Latest data is available in %s'  % basepath)


def get_all_texts():
    """Return a dictionary mapping json filenames with full text contents."""
    texts_by_file = {}
    logger.info('Extracting full texts from all document json files...')
    tar = tarfile.open(doc_gz_path)
    members = tar.getmembers()
    for m in members:
        f = tar.extractfile(m)
        doc_json = json.loads(f.read().decode('utf-8'))
        text = get_text_from_json(doc_json)
        texts_by_file[m.name] = text
    tar.close()
    return texts_by_file


def get_zip_texts_for_entry(md_entry, texts_by_file, zip=True):
    texts = []
    if md_entry['pdf_json_files']:
        filenames = [s.strip() for s in md_entry['pdf_json_files'].split(';')]
        pdf_texts = []
        for filename in filenames:
            if texts_by_file.get(filename):
                pdf_texts.append(texts_by_file[filename])
            else:
                logger.warning('Text for %s is missing'  % filename)
        combined_text = '\n'.join(pdf_texts)
        if zip:
            combined_text = zip_string(combined_text)
        texts.append(('cord19_pdf', 'fulltext', combined_text))
    if md_entry['pmc_json_files']:
        filename = md_entry['pmc_json_files']
        if texts_by_file.get(filename):
            text = texts_by_file[filename]
        else:
            logger.warning('Text for %s is missing'  % filename)
        if zip:
            text = zip_string(text)
        texts.append(('cord19_pmc_xml', 'fulltext', text))
    if md_entry['abstract']:
        text = md_entry['abstract']
        if zip:
            text = zip_string(text)
        texts.append(('cord19_abstract', 'abstract', text))
    return texts


def get_metadata_df():
    file_data = []
    hashes = []
    """
    for content_type, content_path in paths.items():
        for filename in os.listdir(content_path):
            if filename.endswith('.json'):
                file_hash = filename.split('.')[0]
                hashes.append(file_hash)
                file_data.append((content_path, content_type))
    file_df = pd.DataFrame(file_data, index=hashes, dtype='str',
                           columns=['content_path', 'content_type'])
    """
    dtype_dict = {
            'cord_uid': 'object',
            'sha': 'object',
            'source_x': 'object',
            'title': 'object',
            'doi': 'object',
            'pmcid': 'object',
            'pubmed_id': 'object',
            'license': 'object',
            'abstract': 'object',
            'publish_time': 'object',
            'authors': 'object',
            'journal': 'object',
            'mag_id': 'object',
            'who_covidence_id': 'object',
            'arxiv_id': 'object',
            'pdf_json_files': 'object',
            'pmc_json_files': 'object',
            'url': 'object',
            's2_id': 'object',
    }
    md = pd.read_csv(metadata_file, dtype=dtype_dict,
                           parse_dates=['publish_time'])
    md = md.where(md.notnull(), None)
    #file_data = metadata.join(file_df, 'sha')
    #return file_data
    return md


def get_ids(id_type):
    """Get unique article identifiers from the dataset.

    Parameters
    ----------
    id_type : str
        Dataframe column name, e.g. 'pubmed_id', 'pmcid', 'doi'.

    Returns
    -------
    list of str
        List of unique identifiers in the dataset, e.g. all unique PMCIDs.
    """
    global doc_df
    if doc_df is None:
        doc_df = get_metadata_df()
    unique_ids = list(doc_df[~pd.isna(doc_df[id_type])][id_type].unique())
    return unique_ids


def get_text_from_json(doc_json):
    text = ''
    text += doc_json['metadata']['title']
    text += '.\n'
    if 'abstract' in doc_json:
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
    # TODO this needs to be updated with new df structure and code updates
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


def get_metadata_dict():
    df = get_metadata_df()
    return df.to_dict(orient='records')


def fix_doi(doi):
    if doi is None:
        return None
    prefixes = ['http://dx.doi.org/', 'doi.org/']
    for prefix in prefixes:
        if doi.startswith(prefix):
            doi = doi[len(prefix):]
    return doi


def fix_pmid(pmid):
    if pmid is None:
        return None
    if not pmid.isdigit():
        pmid = None
    return pmid


def get_text_refs_from_metadata(entry):
    mappings = {
        'cord_uid': 'CORD19_UID',
        'sha': 'CORD19_SHA',
        'doi': 'DOI',
        'pmcid': 'PMCID',
        'pubmed_id': 'PMID',
        'who_covidence_id': 'WHO_COVIDENCE',
        'mag_id': 'MICROSOFT'
    }
    text_refs = {}
    for key, ref_key in mappings.items():
        val = entry.get(key)
        if key == 'doi':
            val = fix_doi(val)
        elif key == 'pubmed_id':
            val = fix_pmid(val)
        if val and not pd.isnull(val):
            # Temporary patch to remove float suffixes
            if val.endswith('.0'):
                val = val[:-2]
            text_refs[ref_key] = val
    return text_refs

