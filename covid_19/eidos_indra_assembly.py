import pickle
import indra.tools.assemble_corpus as ac
from indra.tools.live_curation import Corpus
from collections import Counter
from indra.preassembler.custom_preassembly import agent_name_stmt_type_matches


def norm_name(name):
    return '_'.join(sorted(list(set(name.lower().split()))))


def make_fake_wm(stmts):
    for stmt in stmts:
        for agent in stmt.agent_list():
            agent.db_refs['WM'] = [(norm_name(agent.name), 1.0)]


def filter_name_frequency(stmts, k=2):
    norm_names = []
    for stmt in stmts:
        for agent in stmt.agent_list():
            norm_names.append(norm_name(agent.name))
    cnt = Counter(norm_names)
    names = {n for n, c in cnt.most_common() if c >= k}

    new_stmts = []
    for stmt in stmts:
        found = True
        for agent in stmt.agent_list():
            if norm_name(agent.name) not in names:
                found = False
                break
        if found:
            new_stmts.append(stmt)
    return new_stmts


if __name__ == '__main__':
    with open('../eidos_statements_influence.pkl', 'rb') as fh:
    	stmts = pickle.load(fh)
    make_fake_wm(stmts)
    stmts = filter_name_frequency(stmts, k=2)
    assembled_stmts = ac.run_preassembly(stmts,
                                         matches_fun=agent_name_stmt_type_matches)
    meta_data = 'This corpus was assembled from ~30k papers related to Covid-19.'
    corpus = Corpus(assembled_stmts, raw_statements=stmts, meta_data=meta_data)
    corpus_name = 'covid-20200319-ontfree'
    corpus.s3_put(corpus_name)
