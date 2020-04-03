"""This script processes Eidos JSON-LD outputs into INDRA Statements.
"""
import re
import os
import glob
import tqdm
import pickle
from indra.sources import eidos
from covid_19 import read_metadata, get_text_refs_from_metadata

root = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                    os.pardir, os.pardir)

metadata = read_metadata(os.path.join(root, 'cord19_text', 'metadata.csv'))
metadata_by_id = {entry['ID']: entry for entry in metadata}

fnames = glob.glob(os.path.join(root, 'eidos_output', '*.jsonld'))
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

with open(os.path.join(root, 'stmts', 'eidos_statements.pkl'), 'wb') as fh:
    pickle.dump(stmts, fh)
