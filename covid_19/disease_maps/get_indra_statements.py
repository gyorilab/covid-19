import json
import pickle
from indra.belief import BeliefEngine
from indra.sources import indra_db_rest

def get_stmts_by_grounding(db_ns, db_id):
    ip = indra_db_rest.get_statements(agents=['%s@%s' % (db_id, db_ns)],
                                      ev_limit=100)
    print('%d statements for %s:%s' % (len(ip.statements), db_ns, db_id))
    return ip.statements


def filter_prior_all(stmts, groundings):
    groundings = {tuple(g[:2]) for g in groundings}
    filtered_stmts = []
    for stmt in stmts:
        stmt_groundings = {a.get_grounding() for a in stmt.agent_list()
                           if a is not None}
        if stmt_groundings <= groundings:
            filtered_stmts.append(stmt)
    return filtered_stmts


def make_unique_hashes(stmts):
    return list({stmt.get_hash(): stmt for stmt in stmts}.values())


def reground_stmts(stmts, gm):
    for stmt in stmts:
        for agent in stmt.agent_list():
            if agent is not None:
                txt = agent.db_refs.get('TEXT')
                if txt and txt in gm:
                    agent.db_refs = {'TEXT': txt}
                    agent.db_refs.update(gm[txt])


def filter_out_medscan(stmts):
    new_stmts = []
    for stmt in stmts:
        new_evidence = [e for e in stmt.evidence if e.source_api != 'medscan']
        if not new_evidence:
            continue
        stmt.evidence = new_evidence
        new_stmts.append(stmt)
    return new_stmts


if __name__ == '__main__':
    # List of entities that are not of interest to get INDRA Statements
    # e.g., ATP, oxygen
    with open('black_list.txt', 'r') as fh:
        black_list = {line.strip() for line in fh.readlines()}

    with open('minerva_disease_map_indra_ids.csv', 'r') as fh:
        groundings = [line.strip().split(',') for line in fh.readlines()]
    all_stmts = []
    for db_ns, db_id, name in groundings:
        if db_id in black_list:
            print('Skipping %s in black list' % name)
            continue
        print('Looking up %s' % name)
        all_stmts += get_stmts_by_grounding(db_ns, db_id)
    all_stmts = make_unique_hashes(all_stmts)

    with open('../../grounding_map.json', 'r') as fh:
        gm = json.load(fh)

    reground_stmts(all_stmts, gm)
    be = BeliefEngine()
    be.set_prior_probs(all_stmts)
    all_stmts = filter_out_medscan(all_stmts)

    with open('disease_map_indra_stmts_full.pkl', 'wb') as fh:
        pickle.dump(all_stmts, fh)

    filtered_stmts = filter_prior_all(all_stmts, groundings)
    with open('disease_map_indra_stmts_filtered.pkl', 'wb') as fh:
        pickle.dump(filtered_stmts, fh)

