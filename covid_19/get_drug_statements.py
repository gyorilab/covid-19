import pickle
from indra.sources import indra_db_rest
import indra.tools.assemble_corpus as ac
from indra.statements import Inhibition, Complex


with open('../stmts/covid19_curated_tests.pkl', 'rb') as fh:
    stmts = pickle.load(fh)


drug_agents = [stmt.stmt.subj for stmt in stmts
               if stmt.stmt.evidence[0].source_api == 'hypothes.is']

groundings = set()
for agent in drug_agents:
    db_ns, db_id = agent.get_grounding()
    if db_ns is None:
        print('No grounding for %s (%s)' % (agent, str(agent.db_refs)))
        db_ns, db_id = ('TEXT', agent.db_refs['TEXT'])
    groundings.add((db_ns, db_id))


all_stmts = []
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
    all_stmts += new_stmts


with open('../stmts/drug_stmts.pkl', 'wb') as fh:
    pickle.dump(all_stmts, fh)
