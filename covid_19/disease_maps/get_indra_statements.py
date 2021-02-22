import csv
import json
import tqdm
import pickle
from indra.sources import indra_db_rest, tas
from indra.tools import assemble_corpus as ac
from indra.databases import get_identifiers_url
from indra.statements import stmts_to_json_file
from indra_db import get_db
from indra_db.client.principal.curation import get_curations


misgrounding_map = {'CTSL': ['MEP'],
                    'CTSB': ['APPs'],
                    'FURIN': ['pace', 'Fur']}


def get_tas_stmts(db_ns, db_id, allow_unnamed=False):
    tas_stmts = [s for s in tas_processor.statements
                 if s.obj.db_refs.get(db_ns) == db_id]
    if not allow_unnamed:
        tas_stmts = [s for s in tas_stmts
                     if not s.subj.name.startswith('CHEMBL')]
    for stmt in tas_stmts:
        for ev in stmt.evidence:
            chembl_id = stmt.subj.db_refs.get('CHEMBL')
            if chembl_id:
                url = get_identifiers_url('CHEMBL', chembl_id)
                ev.text = 'Experimental assay, see %s' % url
    return tas_stmts


def get_db_stmts_by_grounding(db_ns, db_id):
    ip = indra_db_rest.get_statements(agents=['%s@%s' % (db_id, db_ns)],
                                      ev_limit=100, max_stmts=5000)
    stmts = filter_out_source_evidence(ip.statements, {'medscan', 'tas'})
    print('%d statements for %s:%s' % (len(stmts), db_ns, db_id))
    return stmts


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


def reground_stmts(stmts, gm, misgr):
    new_stmts = []
    for stmt in stmts:
        misgrounded = False
        for agent in stmt.agent_list():
            if agent is not None:
                txt = agent.db_refs.get('TEXT')
                if txt and txt in gm:
                    agent.db_refs = {'TEXT': txt}
                    agent.db_refs.update(gm[txt])
                elif txt and txt in misgr:
                    misgrounded = True
                    break
        if not misgrounded:
            new_stmts.append(stmt)
    return new_stmts


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


if __name__ == '__main__':
    version = 'v2'
    # Loading premliminary data structures
    db = get_db('primary')
    db_curations = get_curations(db=db)

    tas_processor = tas.process_from_web()
    # List of entities that are not of interest to get INDRA Statements
    # e.g., ATP, oxygen
    with open('black_list.txt', 'r') as fh:
        black_list = {line.strip() for line in fh.readlines()}

    with open(f'minerva_disease_map_indra_ids_{version}.csv', 'r') as fh:
        reader = csv.reader(fh)
        groundings = [line for line in reader]

    with open('../../grounding_map.json', 'r') as fh:
        grounding_map = json.load(fh)
    #####################

    # Querying for and assembling statements
    all_stmts = []
    for db_ns, db_id, name in tqdm.tqdm(groundings):
        if db_id in black_list:
            print('Skipping %s in black list' % name)
            continue
        print('Looking up %s' % name)
        db_stmts = get_db_stmts_by_grounding(db_ns, db_id)
        tas_stmts = get_tas_stmts(db_ns, db_id) if db_ns == 'HGNC' else []
        stmts = db_stmts + tas_stmts
        smts = ac.filter_by_curation(stmts, db_curations)
        stmts = reground_stmts(stmts, grounding_map,
                               misgrounding_map)
        all_stmts += stmts
    all_stmts = make_unique_hashes(all_stmts)
    all_stmts = ac.run_preassembly(all_stmts)
    ########################################

    # Dunp results
    with open(f'disease_map_indra_stmts_full_{version}.pkl', 'wb') as fh:
        pickle.dump(all_stmts, fh)

    stmts_to_json_file(all_stmts,
                       f'disease_map_indra_stmts_full_{version}.json')

    filtered_stmts = filter_prior_all(all_stmts, groundings)
    with open(f'disease_map_indra_stmts_filtered_{version}.pkl', 'wb') as fh:
        pickle.dump(filtered_stmts, fh)

    stmts_to_json_file(filtered_stmts,
                       f'disease_map_indra_stmts_filtered_{version}.json')
    ##################
