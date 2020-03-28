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


def get_file_data():
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


def unique_pmcids(df):
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


def get_indradb_nxmls(doc_df):
    unique_pmcids = list(df[~pd.isna(df.pmcid)].pmcid.unique())
    return unique_pmcids


def get_pmcids():
    global doc_df
    if doc_df is None:
        doc_df = get_file_data()
    unique_pmcids = list(doc_df[~pd.isna(doc_df.pmcid)].pmcid.unique())
    return unique_pmcids


def get_indradb_pa_stmts():
    """Get preassembled INDRA Stmts for PMC articles from INDRA DB."""
    # Get the list of all PMCIDs from the corpus metadata
    pmcids = get_pmcids()
    paper_refs = [('pmcid', p) for p in pmcids]
    stmt_jsons = []
    batch_size = 1000
    start = time.time()
    for batch_ix, paper_batch in enumerate(batch_iter(paper_refs, batch_size)):
        if batch_ix <= 5:
            continue
        papers = list(paper_batch)
        print("Querying DB for statements for %d papers" % batch_size)
        batch_start = time.time()
        result = get_statement_jsons_from_papers(papers)
        batch_elapsed = time.time() - batch_start
        batch_jsons = [stmt_json for stmt_hash, stmt_json
                                 in result['statements'].items()]
        print("Returned %d stmts in %f sec" %
              (len(batch_jsons), batch_elapsed))
        batch_stmts = stmts_from_json(batch_jsons)
        ac.dump_statements(batch_stmts, 'batch_%02d.pkl' % batch_ix)
        stmt_jsons += batch_jsons
    elapsed = time.time() - start
    print("Total time: %f sec, %d papers" % (elapsed, len(paper_refs)))
    stmts = stmts_from_json(stmt_jsons)
    ac.dump_statements(stmts, 'cord19_pmc_stmts.pkl')
    return stmt_jsons


def get_indradb_raw_stmts():
    pmcids = get_pmcids()
    db = get_primary_db()
    stmt_ids = distill_stmts(db, get_full_stmts=False,
                        clauses=[
                            db.TextRef.pmcid_in(pmcids),
                            db.TextContent.text_ref_id == db.TextRef.id,
                            db.Reading.text_content_id == db.TextContent.id,
                            db.RawStatements.reading_id == db.Reading.id])
    elapsed = time.time() - start
    print(elapsed)
    stmt_results = db.select_all(db.RawStatements.json,
                                 db.RawStatements.id.in_(stmt_ids))
    stmts = []
    for res in stmt_results:
        stmt_json = json.loads(res.json.decode('utf8'))
        stmts.append(stmts_from_json([stmt_json]))
    ac.dump_statements(stmts, 'cord19_pmc_raw_stmts.pkl')


if __name__ == '__main__':
    pass

