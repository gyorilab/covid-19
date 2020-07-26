"""This script generates custom HTML pages for browsing targets
of amodiaquine and hydroxychloroquine."""
import pickle
import boto3
from collections import defaultdict
from collections import OrderedDict
from indra.sources import tas
from indra.sources import indra_db_rest
from indra.assemblers.html import HtmlAssembler
import indra.tools.assemble_corpus as ac
from indra.databases import get_identifiers_url
from indra_db.client.principal.curation import get_curations
from indra_db import get_db


def get_source_counts_dict():
    return OrderedDict(reach=0, phosphosite=0, pc11=0, hprd=0, medscan=0,
                       trrust=0, isi=0, signor=0, sparser=0, rlimsp=0,
                       cbn=0, tas=0, bel_lc=0, biogrid=0, trips=0,
                       eidos=0, ctd=0)

misgrounding_map = {}


def filter_out_source_evidence(stmts, sources):
    new_stmts = []
    for stmt in stmts:
        new_ev = [e for e in stmt.evidence
                  if e.source_api not in sources]
        if not new_ev:
            continue
        stmt.evidence = new_ev
        new_stmts.append(stmt)
    return new_stmts


def filter_misgrounding(drug, stmts):
    misgr = misgrounding_map.get(drug)
    if not misgr:
        return stmts
    new_stmts = []
    for stmt in stmts:
        txt = stmt.obj.db_refs.get('TEXT')
        if txt in misgr:
            print('Filtering out %s' % txt)
            continue
        new_stmts.append(stmt)
    return new_stmts


def get_tas_stmts(drug):
    tas_stmts = [s for s in tp.statements
                 if s.subj.db_refs.get('CHEBI') == drug]
    for stmt in tas_stmts:
        for ev in stmt.evidence:
            chembl_id = stmt.subj.db_refs.get('CHEMBL')
            if chembl_id:
                url = get_identifiers_url('CHEMBL', chembl_id)
                ev.text = 'Experimental assay, see %s' % url
    return tas_stmts


def get_drugbank_stmts(drug):
    with open('/Users/ben/data/drugbank_5.1.pkl', 'rb') as fh:
        stmts = pickle.load(fh)
    drb_stmts = []
    for stmt in stmts:
        if stmt.subj.db_refs.get('CHEBI') == drug:
            drb_stmts.append(stmt)
    print('%d stmts from DrugBank' % len(drb_stmts))
    return drb_stmts


def get_ctd_stmts(drug):
    with open('/Users/ben/data/ctd/ctd_chemical_gene.pkl', 'rb') as fh:
        stmts = pickle.load(fh)
    with open('/Users/ben/data/ctd/ctd_chemical_disease.pkl', 'rb') as fh:
        stmts += pickle.load(fh)
    db_stmts = []
    for stmt in stmts:
        if stmt.agent_list()[0].db_refs.get('CHEBI') == drug:
            db_stmts.append(stmt)
    return db_stmts


def get_db_stmts(drug):
    ip = indra_db_rest.get_statements(subject=drug + '@CHEBI',
                                      ev_limit=10000)
    print('Number of statements from DB: %s' % len(ip.statements))

    db_stmts = filter_out_source_evidence(ip.statements,
                                          {'tas', 'medscan'})
    return db_stmts


def get_statements(drug):
    tas_stmts = get_tas_stmts(drug)
    drugbank_sttms = get_drugbank_stmts(drug)
    ctd_stmts = get_ctd_stmts(drug)
    db_stmts = get_db_stmts(drug)
    stmts = filter_misgrounding(drug, tas_stmts + db_stmts
                                + ctd_stmts + drugbank_sttms)
    stmts = ac.run_preassembly(stmts)
    stmts = ac.filter_by_curation(stmts, db_curations)

    ev_counts = {s.get_hash(): len(s.evidence) for s in stmts}
    source_counts = {}
    for stmt in stmts:
        stmt_source_counts = get_source_counts_dict()
        for ev in stmt.evidence:
            stmt_source_counts[ev.source_api] += 1
        source_counts[stmt.get_hash()] = stmt_source_counts
    return stmts, ev_counts, source_counts


def make_html(stmts, ev_counts, source_counts, fname, drug):
    ha = HtmlAssembler(stmts, ev_totals=ev_counts,
                       source_counts=source_counts,
                       title='Effects of %s assembled by INDRA' % drug,
                       db_rest_url='http://db.indra.bio/latest')
    ha.make_model()
    ha.save_model(fname)


if __name__ == '__main__':
    db = get_db('primary')
    db_curations = get_curations(db=db)
    tp = tas.process_from_web()
    drugs = [('CHEBI:5801', 'hydroxychloroquine'),
             ('CHEBI:2674', 'amodiaquine')]
    all_stmts = []
    all_ev_counts = {}
    for drug, drug_name in drugs:
        stmts, ev_counts, source_counts = get_statements(drug)
        fname = '%s.html' % drug_name
        all_stmts += stmts
        for sh, cnt in ev_counts.items():
            if sh in all_ev_counts:
                all_ev_counts[sh] += ev_counts[sh]
            else:
                all_ev_counts[sh] = ev_counts[sh]
        make_html(stmts, ev_counts, source_counts, fname, drug_name)
        s3_client = boto3.client('s3')
        with open(fname, 'r') as fh:
            html_str = fh.read()
            s3_client.put_object(Bucket='indra-covid19',
                                 Key='targets_for_drug/%s' % fname,
                                 Body=html_str.encode('utf-8'),
                                 ContentType='text/html',
                                 ACL='public-read')
