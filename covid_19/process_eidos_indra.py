import os
import glob
import tqdm
import pickle
from indra.sources import eidos


fnames = glob.glob('../eidos_output/*.jsonld')
stmts = []
for fname in tqdm.tqdm(fnames):
    try:
    	ep = eidos.process_json_file(fname)
    except Exception as e:
        continue
    if ep:
        for stmt in ep.statements:
            stmt.evidence[0].pmid = os.path.basename(fname)[:-11]
    stmts += ep.statements

with open('eidos_statements.pkl', 'wb') as fh:
    pickle.dump(stmts, fh)
