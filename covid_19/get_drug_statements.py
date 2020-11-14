import pickle
from indra.statements.agent import default_ns_order
from indra.sources import indra_db_rest
import indra.tools.assemble_corpus as ac
from indra.ontology.bio import bio_ontology
from indra.statements import Inhibition, Complex
from indra.databases.identifiers import ensure_chembl_prefix, \
    ensure_chebi_prefix


def filter_db_support(stmts):
    print('Filtering %d statements for DB support' % len(stmts))
    new_stmts = []
    text_mining_sources = {'reach', 'trips', 'sparser', 'eidos',
                           'medscan', 'isi', 'rlimsp'}
    for stmt in stmts:
        sources = {ev.source_api for ev in stmt.evidence}
        if sources - text_mining_sources:
            new_stmts.append(stmt)
    print('Left with %d statements after filter for DB support' %
          len(new_stmts))
    return new_stmts


def fix_invalid(stmts):
    for stmt in stmts:
        for agent in stmt.real_agent_list():
            if 'CHEBI' in agent.db_refs:
                agent.db_refs['CHEBI'] = \
                    ensure_chebi_prefix(agent.db_refs['CHEBI'])
            if 'CHEMBL' in agent.db_refs:
                agent.db_refs['CHEMBL'] = \
                    ensure_chembl_prefix(agent.db_refs['CHEMBL'])

        for ev in stmt.evidence:
            if ev.pmid == 'Other':
                ev.pmid = None
            if ev.text_refs.get('PMID') == 'Other':
                ev.text_refs.pop('PMID', None)
    return stmts


def get_drug_groundings(drug_agents):
    groundings = set()
    ns_order = default_ns_order + ['CHEMBL']
    for agent in drug_agents:
        db_ns, db_id = agent.get_grounding(ns_order=ns_order)
        if db_ns is None:
            print('No grounding for %s (%s)' % (agent, str(agent.db_refs)))
            if 'TEXT' in agent.db_refs:
                db_ns, db_id = ('TEXT', agent.db_refs['TEXT'])
            else:
                db_ns, db_id = ('NAME', agent.name)
        groundings.add((db_ns, db_id))
        for db_ns, db_id in bio_ontology.get_children(db_ns, db_id):
            groundings.add((db_ns, db_id))

    print('Found a total of %d groundings to look up' % len(groundings))
    return groundings


def get_drug_statements(groundings):
    all_stmts = {}
    for db_ns, db_id in groundings:
        print('Searching for %s@%s' % (db_id, db_ns))
        idp = indra_db_rest.get_statements(subject='%s@%s' % (db_id, db_ns),
                                           ev_limit=100)
        stmts = idp.statements
        stmts = ac.filter_by_type(stmts, Inhibition) + \
                ac.filter_by_type(stmts, Complex)
        new_stmts = []
        for stmt in stmts:
            new_ev = []
            for ev in stmt.evidence:
                if ev.source_api != 'medscan':
                    new_ev.append(ev)
            if not new_ev:
                continue
            stmt.evidence = new_ev
            new_stmts.append(stmt)
        for stmt in new_stmts:
            all_stmts[stmt.get_hash()] = stmt

    stmts = list(all_stmts.values())
    stmts = filter_db_support(stmts)
    stmts = fix_invalid(stmts)
    return stmts


if __name__ == '__main__':
    with open('../stmts/covid19_curated_tests.pkl', 'rb') as fh:
        stmts = pickle.load(fh)

    with open('../stmts/covid19_mitre_tests.pkl', 'rb') as fh:
        stmts += pickle.load(fh)

    drug_agents = [stmt.stmt.subj for stmt in stmts]
                   #if stmt.stmt.evidence[0].source_api == 'hypothes.is']
    groundings = get_drug_groundings(drug_agents)
    drug_stmts = get_drug_statements(groundings)
    with open('../stmts/drug_stmts_v3.pkl', 'wb') as fh:
        pickle.dump(drug_stmts, fh)
