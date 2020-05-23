import pickle
from indra.sources import indra_db_rest

# List of entities that are not of interest to get INDRA Statements
# e.g., ATP, oxygen
black_list = {
    ('CHEBI', 'CHEBI:16335'),
    ('CHEBI', 'CHEBI:16335'),
    ('CHEBI', 'CHEBI:16335'),
    ('CHEBI', 'CHEBI:16761'),
}


def get_stmts_by_grounding(db_ns, db_id):
    ip = indra_db_rest.get_statements(agents=['%s@%s' % (db_id, db_ns)],
                                      ev_limit=100)
    print('%d statements for %s:%s' % (len(ip.statements), db_ns, db_id))
    return ip.statements


def filter_prior_all(stmts, groundings):
    groundings = {tuple(g) for g in groundings}
    filtered_stmts = []
    for stmt in stmts:
        stmt_groundings = {a.get_grounding() for a in stmt.agent_list()
                           if a is not None}
        if stmt_groundings <= groundings:
            filtered_stmts.append(stmt)
    return filtered_stmts


def make_unique_hashes(stmts):
    return list({stmt.get_hash(): stmt for stmt in stmts}.values())


if __name__ == '__main__':
    with open('minerva_disease_map_indra_ids.csv', 'r') as fh:
        groundings = [line.strip().split(',') for line in fh.readlines()]
    all_stmts = []
    for db_ns, db_id in groundings:
        all_stmts += get_stmts_by_grounding(db_ns, db_id)
    all_stmts = make_unique_hashes(all_stmts)

    with open('disease_map_indra_stmts_full.pkl', 'wb') as fh:
        pickle.dump(all_stmts, fh)

    filtered_stmts = filter_prior_all(all_stmts, groundings)
    with open('disease_map_indra_stmts_filtered.pkl', 'wb') as fh:
        pickle.dump(filtered_stmts, fh)

