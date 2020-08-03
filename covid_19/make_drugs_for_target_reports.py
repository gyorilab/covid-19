"""This script generates custom HTML pages for browsing small
molecules that target a given list of proteins."""

import boto3
import pickle
from collections import defaultdict
from collections import OrderedDict
from indra.sources import tas
from indra.sources import indra_db_rest
from indra.assemblers.html import HtmlAssembler
from indra.statements import Inhibition, DecreaseAmount
import indra.tools.assemble_corpus as ac
from indra.databases import get_identifiers_url
from indra_db.client.principal.curation import get_curations
from indra_db import get_db


def get_source_counts_dict():
    return OrderedDict(reach=0, phosphosite=0, pc11=0, hprd=0, medscan=0,
                       trrust=0, isi=0, signor=0, sparser=0, rlimsp=0,
                       cbn=0, tas=0, bel_lc=0, biogrid=0, trips=0,
                       eidos=0)


def is_small_molecule(agent):
    return set(agent.db_refs.keys()) & {'CHEBI', 'PUBCHEM', 'CHEMBL',
                                        'HMS-LINCS', 'DRUGBANK', 'CAS'}


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


def filter_misgrounding(target, stmts):
    misgr = misgrounding_map.get(target)
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


def get_tas_stmts(target):
    tas_stmts = [s for s in tp.statements if s.obj.name == target]
    for stmt in tas_stmts:
        for ev in stmt.evidence:
            chembl_id = stmt.subj.db_refs.get('CHEMBL')
            if chembl_id:
                url = get_identifiers_url('CHEMBL', chembl_id)
                ev.text = 'Experimental assay, see %s' % url
    return tas_stmts


def get_db_stmts(target):
    ip = indra_db_rest.get_statements(object=target,
                                      stmt_type='Inhibition',
                                      ev_limit=10000)
    print('Number of statements from DB: %s' % len(ip.statements))

    db_stmts = [s for s in ip.statements if
                is_small_molecule(s.subj)]
    db_stmts = filter_out_source_evidence(db_stmts,
                                          {'tas', 'medscan'})

    for stmt in db_stmts:
        for agent in stmt.agent_list():
            if agent is not None and 'CHEBI' in agent.db_refs:
                chebi_id = agent.db_refs['CHEBI']
                if not chebi_id.startswith('CHEBI'):
                    agent.db_refs['CHEBI'] = 'CHEBI:%s' % chebi_id

    return db_stmts


def filter_neg(stmts):
    inhib_stmts = ac.filter_by_type(stmts, Inhibition)
    decamt_stmts = ac.filter_by_type(stmts, DecreaseAmount)
    return inhib_stmts + decamt_stmts


def get_statements(target):
    #tas_stmts = get_tas_stmts(target)
    db_stmts = get_db_stmts(target)
    stmts = db_stmts
    #stmts = tas_stmts + db_stmts
    stmts = filter_misgrounding(target, stmts)
    stmts = ac.run_preassembly(stmts)
    stmts = ac.filter_by_curation(stmts, db_curations)
    stmts = filter_neg(stmts)
    return stmts
    """
    # Evidence and source counts not needed anymore
    ev_counts = {s.get_hash(): len(s.evidence) for s in stmts}
    source_counts = {}
    for stmt in stmts:
        stmt_source_counts = get_source_counts_dict()
        for ev in stmt.evidence:
            stmt_source_counts[ev.source_api] += 1
        source_counts[stmt.get_hash()] = stmt_source_counts
    return stmts, ev_counts, source_counts
    """


def make_html(stmts, fname):
    ha = HtmlAssembler(stmts,
        title='Small molecule inhibitors of %s assembled by INDRA' % target,
                       db_rest_url='http://db.indra.bio/latest')
    ha.make_model()
    ha.save_model(fname)


def make_drug_list(stmts, ev_counts):
    agent_by_name = {}
    counts_by_name = defaultdict(int)
    for stmt in stmts:
        subj = stmt.agent_list()[0]
        agent_by_name[subj.name] = subj
        counts_by_name[subj.name] += ev_counts.get(stmt.get_hash(), 0)
    drug_list = []
    for name, agent in sorted(agent_by_name.items(),
                              key=lambda x: counts_by_name[x[0]],
                              reverse=True):
        compound = name
        db_ns, db_id = agent.get_grounding()
        compound += ' (%s:%s)' % (db_ns, db_id) if db_ns else ''
        drug_list.append((compound, counts_by_name[name]))
    with open('indra_drug_list.tsv', 'w') as fh:
        for compound in drug_list:
            fh.write('%s\t%s\t%s\n' % (compound[0], compound[1],
                                       'INDRA (text mining/databases)'))


misgrounding_map = {'CTSL': ['MEP'],
                    'CTSB': ['APPs'],
                    'FURIN': ['pace', 'Fur']}


if __name__ == '__main__':
    db = get_db('primary')
    db_curations = get_curations(db=db)
    tp = tas.process_from_web()
    #targets = ['TMPRSS2', 'ACE2', 'FURIN', 'CTSB', 'CTSL']
    targets = ['PIKFYVE', 'INPP5E', 'PIK3C2A', 'PIK3C2B', 'PIK3C2G',
               'PI4K2A', 'PI4K2B', 'PI4KB', 'EHD3', 'PIK3C3']
    all_stmts = []
    all_ev_counts = {}
    with open('ctd_drugbank_tas_pikfyve.pkl', 'rb') as f:
        all_ctd_stmts = pickle.load(f)
        all_ctd_stmts = filter_neg(all_ctd_stmts)
    for target in targets:
        # Evidence and source counts not needed anymore
        #stmts, ev_counts, source_counts = get_statements(target)
        stmts = get_statements(target)
        fname = '%s.html' % target
        ctd_stmts = ac.filter_gene_list(all_ctd_stmts, [target], policy='one')
        stmts += ctd_stmts
        all_stmts += stmts
        """
        # Evidence and source counts not needed anymore
        for sh, cnt in ev_counts.items():
            if sh in all_ev_counts:
                all_ev_counts[sh] += ev_counts[sh]
            else:
                all_ev_counts[sh] = ev_counts[sh]
        """
        make_html(stmts, fname)
        s3_client = boto3.client('s3')
        with open(fname, 'r') as fh:
            html_str = fh.read()
            s3_client.put_object(Bucket='indra-covid19',
                                 Key='drugs_for_target/%s' % fname,
                                 Body=html_str.encode('utf-8'),
                                 ContentType='text/html',
                                 ACL='public-read')
    make_drug_list(all_stmts, all_ev_counts)
