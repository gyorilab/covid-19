"""Thsi script creates inhibiton statements for integration with the
COVID-19 Disease Map EMMAA model."""
import pickle
from indra.statements.agent import default_ns_order
from os.path import join, dirname, abspath, pardir


stmts_path = join(dirname(abspath(__file__)), pardir, pardir, pardir, 'stmts')


def get_sources(stmt):
    return {ev.source_api for ev in stmt.evidence}


namespaces = default_ns_order + ['CHEMBL', 'PUBCHEM', 'DRUGBANK']


if __name__ == '__main__':
    with open('../inhibitors/inhibitors.pkl', 'rb') as fh:
        stmts = pickle.load(fh)
    with open(join(stmts_path, 'covid19_mitre_tests_c19dm.pkl'), 'rb') as fh:
        mitre_tests = pickle.load(fh)
    with open(join(stmts_path, 'covid19_curated_tests_c19dm.pkl'), 'rb') as fh:
        curated_tests = pickle.load(fh)

    all_drugs = {stmt.stmt.subj.get_grounding(namespaces)
                 for stmt in mitre_tests['tests'] + curated_tests}
    all_drugs = {(ns, id) for ns, id in all_drugs if ns is not None}

    readers = {'reach', 'sparser', 'trips', 'isi', 'rlimsp', 'eidos',
               'medscan'}
    all_target_stmts = []
    MAX_PER_TARGET = 20
    for target, sts in stmts.items():
        has_relevant_drug = [stmt for stmt in sts if
                             stmt.subj.get_grounding(namespaces)
                             in all_drugs]
        has_db_support = sorted([stmt for stmt in has_relevant_drug
                                 if (get_sources(stmt) - readers)],
                                key=lambda x: len(x.evidence),
                                reverse=True)
        reading_only = sorted([stmt for stmt in has_relevant_drug
                               if not (get_sources(stmt) - readers)],
                              key=lambda x: len(x.evidence),
                              reverse=True)
        if len(has_db_support) >= MAX_PER_TARGET:
            all_target_stmts += has_db_support[:MAX_PER_TARGET]
        else:
            from_reading = MAX_PER_TARGET - len(has_db_support)
            all_target_stmts += (has_db_support + reading_only[:from_reading])

    with open(join(stmts_path, 'c19dm_inhibitors_by_target.pkl'), 'wb') as fh:
        pickle.dump(all_target_stmts, fh)
