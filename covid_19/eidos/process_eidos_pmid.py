"""This script searches for relevant PMIDs on PubMed, and then
reads the abstracts corresponding to each PMID with Eidos. It is
complementary to the pipeline which starts with the CORD19 document set."""
import time
import pickle
from tqdm import tqdm
from indra.sources import eidos
from indra.literature import pubmed_client


keywords = ['covid19', 'covid-19', 'sars-cov-2', 'sars-cov2']
ids = []
for kw in keywords:
    ids += pubmed_client.get_ids(kw)


stmts = {}
for pmid in tqdm(ids):
    time.sleep(3)
    abst = pubmed_client.get_abstract(pmid)
    if not abst:
        continue
    ep = eidos.process_text(abst, webservice='http://localhost:9000/')
    for stmt in ep.statements:
        stmt.evidence[0].pmid = pmid
    stmts[pmid] = ep.statements

with open('../data/eidos_abstract_stmts.pkl', 'wb') as fh:
    pickle.dump(stmts, fh)
