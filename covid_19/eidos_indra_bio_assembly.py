# This script assemvles Influences extracted from Eidos
# by grounding the relevant concepts using Gilda and then transforming the
# Influences into RegulateActivity statements.
import tqdm
import gilda
import pickle
import logging
from indra.tools import assemble_corpus as ac
from indra.statements import Influence, Activation, Inhibition, Agent
from indra.preassembler.grounding_mapper.standardize import \
    standardize_agent_name

logging.getLogger('gilda').setLevel(logging.WARNING)

def get_agent(concept):
    txt = concept.name
    matches = gilda.ground(txt)
    if not matches:
        return None
    gr = (matches[0].term.db, matches[0].term.id)
    agent = Agent(concept.name, db_refs={gr[0]: gr[1],
                  'TEXT': concept.name})
    standardize_agent_name(agent, standardize_refs=True)
    return agent


def get_regulate_activity(stmt):
    subj = get_agent(stmt.subj.concept)
    obj = get_agent(stmt.obj.concept)
    if not subj or not obj:
        return None
    pol = stmt.overall_polarity()
    stmt_type = Activation if pol == 1 or not pol else Inhibition
    bio_stmt = stmt_type(subj, obj, evidence=stmt.evidence)
    return bio_stmt


if __name__ == '__main__':
    with open('../eidos_statements.pkl', 'rb') as fh:
        stmts = pickle.load(fh)
    stmts = ac.filter_by_type(stmts, Influence)
    bio_stmts = []
    for stmt in tqdm.tqdm(stmts):
        bio_stmt = get_regulate_activity(stmt)
        if bio_stmt:
            bio_stmts.append(bio_stmt)
    with open('../eidos_bio_statements.pkl', 'wb') as fh:
        pickle.dump(bio_stmts, fh)

