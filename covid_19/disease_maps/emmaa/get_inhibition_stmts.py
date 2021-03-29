"""Thsi script creates inhibiton statements for integration with the
COVID-19 Disease Map EMMAA model."""
import pickle


def get_sources(stmt):
    return {ev.source_api for ev in stmt.evidence}


if __name__ == '__main__':
    with open('../inhibitors/inhibitors.pkl', 'rb') as fh:
        stmts = pickle.load(fh)
    readers = {'reach', 'sparser', 'trips', 'isi', 'rlimsp', 'eidos',
               'medscan'}
    all_target_stmts = []
    for target, sts in stmts.items():
        has_db_support = sorted([stmt for stmt in sts if
                                 get_sources(stmt) - readers],
                                key=lambda x: len(x.evidence),
                                reverse=True)
        all_target_stmts += has_db_support[:10]
    with open('c19dm_inhibitors_by_target.pkl', 'wb') as fh:
        pickle.dump(all_target_stmts, fh)
