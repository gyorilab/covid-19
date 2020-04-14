import pickle
import gilda
import logging
from collections import Counter
from indra.util import write_unicode_csv
from indra.statements import Complex
from indra.databases import get_identifiers_url
from indra.preassembler.grounding_mapper.standardize import name_from_grounding


logging.getLogger('gilda').setLevel(logging.WARNING)


def get_eidos_gilda_grounding_counts(stmts):
    texts = []
    ev_text_for_agent_text = {}
    for stmt in stmts:
        for agent in stmt.agent_list():
            txt = agent.name
            matches = gilda.ground(txt)
            if matches:
                gr = matches[0].term.db, matches[0].term.id
            else:
                gr = None, None
            standard_name = name_from_grounding(*gr) \
                if gr[0] is not None else ''
            url = get_identifiers_url(*gr) if gr[0] is not None else ''
            ev_text_for_agent_text[txt] = (stmt.evidence[0].pmid,
                                           stmt.evidence[0].text)
            texts.append((txt, ('%s:%s' % gr) if gr[0] else '',
                          standard_name, url, ''))
    # Count the unique text-grounding entries
    cnt = Counter(texts)
    return cnt, ev_text_for_agent_text


def get_text_grounding_counts(stmts):
    texts = []
    ev_text_for_agent_text = {}
    # Iterate over each statement and its agents
    for stmt in stmts:
        for idx, agent in enumerate(stmt.agent_list()):
            if agent is None:
                continue
            # Get some properties of the assembled agent (grounding,
            # standard name, link-out URL)
            gr = agent.get_grounding()
            standard_name = name_from_grounding(*gr) \
                if gr[0] is not None else ''
            url = get_identifiers_url(*gr) if gr[0] is not None else ''
            # For Complexes, we can't rely on the indexing in annotations to
            # get the raw_text of the Agents so we just take the surface-level
            # TEXT entry in the assembled Agent's db_refs
            if isinstance(stmt, Complex):
                agent_texts = [agent.db_refs['TEXT']] \
                    if 'TEXT' in agent.db_refs else []
            # In other cases, we iterate over all the evidences and use the
            # agent index to find the raw text in annotations
            else:
                agent_texts = []
                for ev in stmt.evidence:
                    if 'agents' not in ev.annotations:
                        continue
                    raw_txt = ev.annotations['agents']['raw_text'][idx]
                    if raw_txt:
                        agent_texts.append(raw_txt)
            for t in agent_texts:
                # These entries are presumably overwritten many times but
                # that's okay.
                ev_text_for_agent_text[t] = (ev.pmid, ev.text)
                gilda_grounding = gilda.ground(t)
                gilda_grounding = '%s:%s' % (gilda_grounding[0].term.db,
                                             gilda_grounding[0].term.id) \
                    if gilda_grounding else ''
                # We now add a new entry to the text-grounding list
                texts.append((t, ('%s:%s' % gr) if gr[0] else '',
                              standard_name, url, gilda_grounding))
    # Count the unique text-grounding entries
    cnt = Counter(texts)
    return cnt, ev_text_for_agent_text


def get_raw_statement_text_grounding_counts(stmts):
    texts = []
    ev_text_for_agent_text = {}
    for stmt in stmts:
        for agent in stmt.agent_list():
            if agent is None:
                continue
            txt = agent.db_refs['TEXT']
            ev_text_for_agent_text[txt] = (stmt.evidence[0].pmid,
                                           stmt.evidence[0].text)
            assert txt, agent.db_refs
            gr = agent.get_grounding()
            standard_name = name_from_grounding(*gr) if gr[0] else ''
            url = get_identifiers_url(*gr) if gr[0] is not None else ''
            gilda_grounding = gilda.ground(txt)
            gilda_grounding = '%s:%s' % (gilda_grounding[0].term.db,
                                         gilda_grounding[0].term.id) \
                if gilda_grounding else ''
            texts.append((txt, ('%s:%s' % gr) if gr[0] else '',
                          standard_name, url, gilda_grounding))
    cnt = Counter(texts)
    return cnt, ev_text_for_agent_text


def load_stmts(fname):
    # Load statements
    with open(fname, 'rb') as fh:
        stmts = pickle.load(fh)
    return stmts


def dump_table(text_grounding_cnt, ev_text_for_agent_text, fname):
    # Dump the results into a TSV file
    rows = [['text', 'grounding', 'standard_name', 'url', 'gilda_grounding',
             'count', 'pmid', 'ev_text']]
    for data, count in text_grounding_cnt.most_common():
        pmid, ev_text = ev_text_for_agent_text[data[0]]
        row = list(data) + [str(count), pmid, ev_text]
        rows.append(row)
    write_unicode_csv(fname, rows, delimiter='\t')


if __name__ == '__main__':
    #stmts = load_stmts('../cord19_pmc_stmts_filt.pkl')
    #cnt, ev_text_for_agent_text = get_text_grounding_counts(stmts)
    #dump_table(cnt, ev_text_for_agent_text, '../grounding_table.tsv')

    stmts = load_stmts('../eidos_statements.pkl')
    cnt, ev_text_for_agent_text = get_eidos_gilda_grounding_counts(stmts)
    dump_table(cnt, ev_text_for_agent_text,
               '../grounding_eidos_gilda_table.tsv')

