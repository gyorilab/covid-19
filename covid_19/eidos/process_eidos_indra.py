"""This script processes Eidos JSON-LD outputs into INDRA Statements.
"""
import re
import os
import glob
import tqdm
import pickle
from indra.sources import eidos
from . import read_metadata, get_text_refs_from_metadata


metadata = read_metadata('../cord19_text/metadata.csv')
metadata_by_id = {entry['ID']: entry for entry in metadata}

fnames = glob.glob('../eidos_output/*.jsonld')
stmts = []
for fname in tqdm.tqdm(fnames):
    cord_indra_id = re.match(r'CORD19_DOC_(\d+).txt.jsonld',
                             os.path.basename(fname)).groups()[0]
    metadata_entry = metadata_by_id[cord_indra_id]
    text_refs = get_text_refs_from_metadata(metadata_entry)
    try:
        ep = eidos.process_json_file(fname)
    except Exception as e:
        print('Processing error for: %s, skipping' % fname)
        continue
    if ep:
        for stmt in ep.statements:
            stmt.evidence[0].text_refs = text_refs
            stmt.evidence[0].pmid = text_refs['PMID'] \
                if 'PMID' in text_refs else None
    stmts += ep.statements

with open('../eidos_statements.pkl', 'wb') as fh:
    pickle.dump(stmts, fh)
